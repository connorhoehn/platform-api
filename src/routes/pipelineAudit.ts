// platform-api/src/routes/pipelineAudit.ts
//
// hub#2225 — Read-only query API over the `pipeline-audit` DDB table.
// Closes the loop on Wave 2 (hub#2224) which wired every mutating route +
// executor lifecycle event into the audit log: this file gives the UI a
// way to read those rows back.
//
// Endpoints:
//   GET /api/pipelines/:pipelineId/runs/:runId/audit
//     Per-run timeline. Filters: ?since, ?until, ?actions=csv, ?limit, ?cursor.
//     Backed by the `runId-time-index` GSI.
//
//   GET /api/pipelines/audit/search
//     Cross-run search. Filters: ?actor, ?pipelineId, ?action=csv, ?since,
//     ?until, ?limit, ?cursor. Rejects 400 when no filter is supplied.
//     Backed by either the `actor-time-index` or `pipeline-time-index` GSI
//     depending on which filter is present (pipelineId wins when both are
//     supplied — actor is then applied as a non-key filter via FilterExpression
//     would be too lossy, so search() prefers the pipelineId GSI here).
//
// Both routes are read-only — they DO NOT write a new audit row, so the
// "audit log of operators reading the audit log" recursion stops at the
// access logger. The handler does no auth-gating beyond `req.user!.sub`
// being populated; surfacing a per-row authz check is left to the global
// search page (hub#2228) when it lands.

import { Router } from 'express';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import { withContext } from '../lib/logger';
import {
  auditRepo,
  type AuditAction,
  type ListAuditOptions,
  type StoredAuditEvent,
} from '../pipeline/audit-repository';

const log = withContext({ route: 'pipelineAudit' });

/** Hard cap shared by both routes. UI routinely asks for 100; safety net at 1000. */
const MAX_LIMIT = 1000;
const DEFAULT_LIMIT = 100;

/** Parse + validate `?limit=`, bounded to [1, MAX_LIMIT]. */
function parseLimit(raw: unknown): number {
  if (typeof raw !== 'string' || raw.length === 0) return DEFAULT_LIMIT;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    throw new ValidationError(`limit must be a positive integer, got '${raw}'`);
  }
  return Math.min(Math.floor(n), MAX_LIMIT);
}

/** Loose ISO 8601 check — DDB only stores what we wrote, so we just sanity-check shape. */
function parseIso(raw: unknown, name: string): string | undefined {
  if (raw === undefined || raw === null || raw === '') return undefined;
  if (typeof raw !== 'string') {
    throw new ValidationError(`${name} must be an ISO 8601 string`);
  }
  if (Number.isNaN(Date.parse(raw))) {
    throw new ValidationError(`${name} is not a valid ISO 8601 timestamp: '${raw}'`);
  }
  return raw;
}

/** Parse `?actions=foo,bar,baz` → trimmed string list. Empty → undefined. */
function parseActions(raw: unknown): AuditAction[] | undefined {
  if (typeof raw !== 'string' || raw.length === 0) return undefined;
  const list = raw
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0) as AuditAction[];
  return list.length > 0 ? list : undefined;
}

function parseCursor(raw: unknown): string | undefined {
  if (typeof raw !== 'string' || raw.length === 0) return undefined;
  return raw;
}

/** Common query-param → ListAuditOptions conversion. */
function toListOptions(query: Record<string, unknown>): ListAuditOptions {
  return {
    limit: parseLimit(query['limit']),
    ...(parseIso(query['since'], 'since') ? { since: parseIso(query['since'], 'since')! } : {}),
    ...(parseIso(query['until'], 'until') ? { until: parseIso(query['until'], 'until')! } : {}),
    ...(parseActions(query['actions']) ? { actions: parseActions(query['actions'])! } : {}),
    // The search endpoint uses `?action=` (singular) historically; accept both.
    ...(parseActions(query['action']) && !parseActions(query['actions'])
      ? { actions: parseActions(query['action'])! }
      : {}),
    ...(parseCursor(query['cursor']) ? { cursor: parseCursor(query['cursor'])! } : {}),
  };
}

interface AuditListResponse {
  items: StoredAuditEvent[];
  nextCursor?: string;
}

// ---------------------------------------------------------------------------
// Per-run audit listing
//   GET /api/pipelines/:pipelineId/runs/:runId/audit
// Mounted at `/pipelines/:pipelineId/runs/:runId/audit` so :pipelineId is a
// route param rather than a filter — keeps URL shape consistent with the rest
// of /pipelines/:pipelineId/runs/:runId/* and surfaces a 404 surface naturally.
// ---------------------------------------------------------------------------

export const pipelineRunAuditRouter = Router({ mergeParams: true });

pipelineRunAuditRouter.get(
  '/',
  asyncHandler<
    { pipelineId: string; runId: string },
    AuditListResponse | { error: string }
  >(async (req, res) => {
    const { pipelineId, runId } = req.params;
    const userId = req.user?.sub ?? 'anonymous';
    const reqLog = log.child({ userId, pipelineId, runId });

    const opts = toListOptions(req.query as Record<string, unknown>);
    const result = await auditRepo.listByRun(runId, opts);

    reqLog.info(
      { count: result.items.length, hasNext: !!result.nextCursor, opts },
      'audit listByRun',
    );

    res.status(200).json(result);
  }),
);

// ---------------------------------------------------------------------------
// Cross-run audit search
//   GET /api/pipelines/audit/search
// ---------------------------------------------------------------------------

export const pipelineAuditSearchRouter = Router({ mergeParams: true });

pipelineAuditSearchRouter.get(
  '/',
  asyncHandler<
    Record<string, never>,
    AuditListResponse | { error: string }
  >(async (req, res) => {
    const userId = req.user?.sub ?? 'anonymous';
    const query = req.query as Record<string, unknown>;
    const reqLog = log.child({ userId });

    const actor = typeof query['actor'] === 'string' && query['actor'].length > 0
      ? (query['actor'] as string)
      : undefined;
    const pipelineId =
      typeof query['pipelineId'] === 'string' && query['pipelineId'].length > 0
        ? (query['pipelineId'] as string)
        : undefined;

    // At least one filter MUST be supplied. We accept `since`/`until`/`action`
    // alone as insufficient because both GSIs require a hash key, and a
    // table-wide scan is not a thing we want to expose.
    if (!actor && !pipelineId) {
      throw new ValidationError(
        'search requires at least one of `actor` or `pipelineId`',
      );
    }

    const opts = toListOptions(query);
    const result = await auditRepo.search({
      ...opts,
      ...(actor ? { actor } : {}),
      ...(pipelineId ? { pipelineId } : {}),
    });

    reqLog.info(
      { count: result.items.length, hasNext: !!result.nextCursor, actor, pipelineId },
      'audit search',
    );

    res.status(200).json(result);
  }),
);
