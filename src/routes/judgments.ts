// platform-api/src/routes/judgments.ts
//
// Read-only HTTP surface for the llm:judge data plane. Closes the
// frontend lazy-fetch loop on the v2 wave (rawText spillover +
// aggregates rollups) by exposing the singletons in judgeServices.ts
// over auth-gated routes.
//
// Endpoints:
//   GET /api/judgments/:judgmentId/raw-text
//     Returns the full rawModelText body for a judgment. Reads from
//     the inline DDB field when the row was not truncated, or falls
//     back to the S3 spillover when rawTextS3Key is set. Returns 404
//     when the judgment doesn't exist OR when the body is truncated
//     AND no rawTextStore is wired (operator hasn't configured
//     JUDGE_RAW_TEXT_BUCKET).
//
//   GET /api/judgments/aggregates?pipelineId=…&since=…&until=…
//   GET /api/judgments/aggregates?rubricId=…&since=…&until=…
//     Per-pipeline OR per-rubric trend rollups, newest day first.
//     Requires exactly one of `pipelineId` or `rubricId` — both
//     would require a table scan; neither is rejected as 400.
//
// Mounted from src/routes/index.ts under `/api/judgments`. Auth is
// applied by the `requireAuth` middleware in app.ts (mounted before
// the `/api/*` router) — no per-route auth logic here.

import { Router } from 'express';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import { withContext } from '../lib/logger';
import {
  getJudgmentRepo,
  getJudgmentAggregateRepo,
} from '../pipeline/judge/judgeServices';
import { assertRunAccess } from '../pipeline/runAccess';
import { markAccess } from '../middleware/resource-access';
import { definitionsRepo } from '../pipeline/definitions-repository';
import { getSystemPipeline } from '../pipeline/systemPipelines';
import type {
  AggregateRow,
  ListAggregatesOptions,
} from '../pipeline/judge/JudgmentAggregateRepository';

const log = withContext({ route: 'judgments' });

/**
 * Loop 29c (NFR #173): a judgment belongs to the run that made it — the run's
 * owner or a reader of its document may read it (401 / 404 otherwise).
 */
const requireJudgmentRunAccess = markAccess((req, _res, next) => {
  void (async () => {
    const judgmentId = String(req.params.judgmentId ?? '');
    const row = judgmentId ? await getJudgmentRepo().getById(judgmentId).catch(() => null) : null;
    if (!row) {
      // Same answer as a run the caller may not see — but the bearer check first.
      await assertRunAccess(req, undefined, '', 'read');
      return;
    }
    await assertRunAccess(req, row.pipelineId, row.runId, 'read');
    next();
  })().catch(next);
}, 'run:read');

/** A pipeline the caller may see aggregate numbers for: their own definition, or a system pipeline. */
async function ownsPipeline(userId: string, pipelineId: string): Promise<boolean> {
  if (getSystemPipeline(pipelineId)) return true;
  return !!(await definitionsRepo.get(userId, pipelineId).catch(() => null));
}

const MAX_LIMIT = 365;
const DEFAULT_LIMIT = 30;

function parseLimit(raw: unknown): number {
  if (typeof raw !== 'string' || raw.length === 0) return DEFAULT_LIMIT;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    throw new ValidationError(`limit must be a positive integer, got '${raw}'`);
  }
  return Math.min(Math.floor(n), MAX_LIMIT);
}

/** yyyy-mm-dd shape check. dayBucket strings on the rows match this format. */
function parseDayBucket(raw: unknown, name: string): string | undefined {
  if (raw === undefined || raw === null || raw === '') return undefined;
  if (typeof raw !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw new ValidationError(`${name} must be a yyyy-mm-dd date, got '${String(raw)}'`);
  }
  return raw;
}

function toListAggregatesOptions(query: Record<string, unknown>): ListAggregatesOptions {
  return {
    limit: parseLimit(query['limit']),
    ...(parseDayBucket(query['since'], 'since')
      ? { since: parseDayBucket(query['since'], 'since')! }
      : {}),
    ...(parseDayBucket(query['until'], 'until')
      ? { until: parseDayBucket(query['until'], 'until')! }
      : {}),
  };
}

interface RawTextResponse {
  judgmentId: string;
  rawModelText: string;
}

interface AggregatesListResponse {
  items: AggregateRow[];
  nextCursor?: string;
}

// ---------------------------------------------------------------------------
// GET /:judgmentId/raw-text
// ---------------------------------------------------------------------------

export const judgmentsRouter = Router({ mergeParams: true });

judgmentsRouter.get(
  '/:judgmentId/raw-text',
  requireJudgmentRunAccess,
  asyncHandler<
    { judgmentId: string },
    RawTextResponse | { error: string }
  >(async (req, res) => {
    const { judgmentId } = req.params;
    if (!judgmentId || judgmentId.length === 0) {
      throw new ValidationError('judgmentId is required');
    }
    const userId = req.user?.sub ?? 'anonymous';
    const reqLog = log.child({ userId, judgmentId });

    const repo = getJudgmentRepo();
    const text = await repo.getRawText(judgmentId);

    if (text === null) {
      reqLog.info('judgment raw-text miss');
      res.status(404).json({ error: 'judgment raw-text not available' });
      return;
    }

    reqLog.info({ bytes: Buffer.byteLength(text, 'utf8') }, 'judgment raw-text served');
    res.status(200).json({ judgmentId, rawModelText: text });
  }),
);

// ---------------------------------------------------------------------------
// GET /aggregates?pipelineId=…   OR   ?rubricId=…
// ---------------------------------------------------------------------------

judgmentsRouter.get(
  '/aggregates',
  asyncHandler<
    Record<string, never>,
    AggregatesListResponse | { error: string }
  >(async (req, res) => {
    const userId = req.user?.sub ?? 'anonymous';
    const query = req.query as Record<string, unknown>;
    const reqLog = log.child({ userId });

    const pipelineId =
      typeof query['pipelineId'] === 'string' && query['pipelineId'].length > 0
        ? (query['pipelineId'] as string)
        : undefined;
    const rubricId =
      typeof query['rubricId'] === 'string' && query['rubricId'].length > 0
        ? (query['rubricId'] as string)
        : undefined;

    if (!pipelineId && !rubricId) {
      throw new ValidationError(
        'one of `pipelineId` or `rubricId` is required (both are absent)',
      );
    }
    if (pipelineId && rubricId) {
      throw new ValidationError(
        'pass exactly one of `pipelineId` or `rubricId`, not both',
      );
    }

    // Loop 29c (NFR #173): only pipelines the caller owns (or system ones).
    if (pipelineId && !(await ownsPipeline(userId, pipelineId))) {
      res.status(404).json({ error: 'pipeline not found' });
      return;
    }
    const opts = toListAggregatesOptions(query);
    const repo = getJudgmentAggregateRepo();
    const raw = pipelineId
      ? await repo.listByPipeline(pipelineId, opts)
      : await repo.listByRubric(rubricId!, opts);
    const owned = new Map<string, boolean>();
    for (const row of raw.items) {
      if (!owned.has(row.pipelineId)) owned.set(row.pipelineId, await ownsPipeline(userId, row.pipelineId));
    }
    const result = { ...raw, items: raw.items.filter((row) => owned.get(row.pipelineId)) };

    reqLog.info(
      { pipelineId, rubricId, count: result.items.length, opts },
      'judgment aggregates listed',
    );
    res.status(200).json(result);
  }),
);
