// platform-api/src/pipeline/runAccess.ts
//
// Loop 29c (realtime-examples access sweep, NFR #173): who may read, or act
// on, one pipeline run over REST. The rule is the one the gateway already asks
// for run channels (#142, routes/internal-pipeline-access.ts):
//
//   read   — the run's owner (`triggeredBy.userId` on the live executor, or
//            `triggeredBy` on the stored row), or anyone the `document:read`
//            policy (#135) lets read the run's own target document.
//   owner  — the run's owner only (cancel, force-fail, retry, resume,
//            breakpoint resolve/patch).
//
// Refusals follow the established conventions:
//   no bearer                      → 401
//   unknown run / no access        → 404 (a stranger learns nothing)
//   a document reader on an owner action → 403
//
// A stored row whose pipelineId differs from the path's `:pipelineId` is 404
// (the path names the run; it cannot borrow another pipeline's run). The
// gateway's service-signed calls with no user (its PipelineModule shim: GET
// run/history, cancel, approvals — SERVICE_AUTH_SECRET envelope, verified
// here) pass as a trusted internal caller: the gateway gates its own sockets.

import type { NextFunction, Request, Response } from 'express';
import { SERVICE_AUTH_HEADER, verifyEnvelope } from '@connorhoehn/service-runtime';
import { canReadDocument, type EditableDocumentRow } from '../deck/documentEditAccess';
import { AppError, ForbiddenError, NotFoundError } from '../middleware/error-handler';
import { markAccess, type AccessMarkedHandler } from '../middleware/resource-access';
import log from '../lib/logger';

export type RunAccessMode = 'read' | 'owner';

export interface RunFacts {
  ownerId?: string;
  pipelineId?: string;
  documentId?: string;
}

export interface RunAccessDeps {
  /** The live executor's view (this replica), or null. */
  liveRun(runId: string): Promise<RunFacts | null>;
  /** The stored row; `pipelineId` may be unknown (runId-only routes). */
  storedRun(pipelineId: string | undefined, runId: string): Promise<RunFacts | null>;
  documentRow(documentId: string): Promise<EditableDocumentRow | null>;
}

function str(value: unknown): string | undefined {
  return typeof value === 'string' && value ? value : undefined;
}

/** Owner and target document of a live snapshot (shape of bridge.getRun). */
export function factsFromSnapshot(snap: unknown): RunFacts | null {
  if (!snap || typeof snap !== 'object') return null;
  const s = snap as Record<string, any>;
  const trigger = (s.triggeredBy ?? {}) as Record<string, any>;
  const payload = (trigger.payload ?? {}) as Record<string, any>;
  return {
    ownerId: str(trigger.userId) ?? (typeof s.triggeredBy === 'string' ? s.triggeredBy : undefined),
    pipelineId: str(s.pipelineId),
    documentId: str(payload.documentId) ?? str(s.context?.documentId),
  };
}

/* eslint-disable @typescript-eslint/no-require-imports */
export const defaultRunAccessDeps: RunAccessDeps = {
  async liveRun(runId) {
    const { getPipelineBridge } = require('./bridge-types') as typeof import('./bridge-types');
    const bridge = getPipelineBridge() as { getRun?: (id: string) => unknown } | null;
    if (!bridge?.getRun) return null;
    const snap = await Promise.resolve(bridge.getRun(runId)).catch(() => null);
    return factsFromSnapshot(snap);
  },
  async storedRun(pipelineId, runId) {
    const { pipelineRunRepo } = require('../repositories') as typeof import('../repositories');
    let pid = pipelineId && pipelineId !== '_' ? pipelineId : undefined;
    if (!pid) {
      const { lookupPipelineIdByRunId } = require('./run-rollup-subscriber') as typeof import('./run-rollup-subscriber');
      pid = (await lookupPipelineIdByRunId(runId)) ?? undefined;
    }
    if (!pid) return null;
    const row = await pipelineRunRepo.get(pid, runId).catch(() => null);
    if (!row) return null;
    const ctx = ((row.checkpoint as { context?: Record<string, unknown> } | undefined)?.context ?? {}) as Record<string, unknown>;
    return { ownerId: str(row.triggeredBy), pipelineId: row.pipelineId, documentId: str(ctx.documentId) };
  },
  async documentRow(documentId) {
    const { GetCommand } = require('@aws-sdk/lib-dynamodb') as typeof import('@aws-sdk/lib-dynamodb');
    const { docClient } = require('../lib/aws-clients') as typeof import('../lib/aws-clients');
    const { DOCUMENTS_TABLE } = require('../deck/presentationDocuments') as typeof import('../deck/presentationDocuments');
    const res = await docClient.send(new GetCommand({
      TableName: DOCUMENTS_TABLE,
      Key: { documentId },
      ProjectionExpression: 'documentId, createdBy, editors, viewers, #channel',
      ExpressionAttributeNames: { '#channel': 'channel' },
    }));
    return (res.Item as EditableDocumentRow | undefined) ?? null;
  },
};
/* eslint-enable @typescript-eslint/no-require-imports */

let deps: RunAccessDeps = defaultRunAccessDeps;
/** Tests swap the lookups. */
export function setRunAccessDeps(next: RunAccessDeps | null): void {
  deps = next ?? defaultRunAccessDeps;
}

/**
 * A call the gateway signed with the shared service secret and that carries no
 * person: its PipelineModule shim (GET run/history, cancel, approvals). The
 * envelope covers the body the gateway sent ('' for a GET).
 */
export function isTrustedServiceCall(req: Pick<Request, 'header' | 'body' | 'serviceActor' | 'method'>): boolean {
  if (req.serviceActor) return false; // on-behalf-of: a person, checked as one
  const secret = process.env.SERVICE_AUTH_SECRET?.trim();
  const header = req.header(SERVICE_AUTH_HEADER);
  if (!secret || !header) return false;
  const allowedServices = [process.env.WORK_GRAPH_GATEWAY_SERVICE_ID?.trim() || 'realtime-examples'];
  const body = req.body && typeof req.body === 'object' && Object.keys(req.body).length > 0 ? req.body : undefined;
  const candidates: Array<string | object> = body ? [JSON.stringify(body), body] : [''];
  return candidates.some((payload) => {
    try {
      return verifyEnvelope(header, { secret, allowedServices, payload, ttlSec: 30, toleranceSec: 5 }).ok;
    } catch {
      return false;
    }
  });
}

export interface RunAccessResult extends RunFacts {
  role: 'owner' | 'reader' | 'service';
}

/** Throws 401 / 404 / 403 unless the caller may `mode` this run. */
export async function assertRunAccess(
  req: Pick<Request, 'user' | 'headers' | 'header' | 'body' | 'serviceActor' | 'method' | 'path'>,
  pipelineId: string | undefined,
  runId: string,
  mode: RunAccessMode,
): Promise<RunAccessResult> {
  if (isTrustedServiceCall(req)) return { role: 'service' };
  if (!req.serviceActor && !req.headers.authorization?.startsWith('Bearer ')) throw new AppError(401, 'Authorization required');
  const userId = req.user?.sub ?? '';
  if (!runId || !userId) throw new NotFoundError('run not found');
  const pid = pipelineId && pipelineId !== '_' ? pipelineId : undefined;
  const live = await deps.liveRun(runId).catch(() => null);
  const stored = live?.ownerId && (!pid || !live.pipelineId || live.pipelineId === pid)
    ? null
    : await deps.storedRun(pid, runId).catch(() => null);
  const facts: RunFacts | null = live?.ownerId ? { ...live, documentId: live.documentId ?? stored?.documentId } : stored ?? live;
  if (!facts) throw new NotFoundError('run not found');
  const factPipeline = stored?.pipelineId ?? facts.pipelineId;
  if (pid && factPipeline && factPipeline !== pid) throw new NotFoundError('run not found');
  if (facts.ownerId && facts.ownerId === userId) return { ...facts, role: 'owner' };
  let reader = false;
  if (facts.documentId) {
    const row = await deps.documentRow(facts.documentId).catch(() => null);
    reader = !!row && (await canReadDocument(row, req.user, facts.documentId)).allow;
  }
  if (!reader) {
    log.warn({ runId, pipelineId: pid, userId, route: req.path }, 'run access refused: not the owner and cannot read its document');
    throw new NotFoundError('run not found');
  }
  if (mode === 'owner') {
    log.warn({ runId, pipelineId: pid, userId, route: req.path }, 'run action refused: a reader of the run is not its owner');
    throw new ForbiddenError('Only the person who started this run can do that.');
  }
  return { ...facts, role: 'reader' };
}

/**
 * The same as a route middleware. `runIdOf` defaults to `:runId`, the
 * pipeline to `:pipelineId` (mergeParams routers see both).
 */
export function requireRunAccess(
  mode: RunAccessMode,
  runIdOf: (req: Request) => unknown = (req) => req.params?.runId,
  pipelineIdOf: (req: Request) => unknown = (req) => req.params?.pipelineId,
): AccessMarkedHandler {
  return markAccess((req: Request, _res: Response, next: NextFunction) => {
    const runId = str(runIdOf(req)) ?? '';
    const pipelineId = str(pipelineIdOf(req));
    assertRunAccess(req, pipelineId, runId, mode).then((result) => {
      (req as Request & { runAccess?: RunAccessResult }).runAccess = result;
      next();
    }, next);
  }, mode === 'owner' ? 'run:owner' : 'run:read');
}

/** Does the caller own or read this run? For filtering lists (never throws). */
export async function canSeeRun(req: Parameters<typeof assertRunAccess>[0], pipelineId: string | undefined, runId: string): Promise<boolean> {
  try {
    await assertRunAccess(req, pipelineId, runId, 'read');
    return true;
  } catch {
    return false;
  }
}

/**
 * Loop 29c (NFR #173): a pipeline's aggregate surfaces (cost, estimate) are
 * for its owner — the caller's own definition — or a system pipeline (numbers
 * only, no one's runs). Anyone else: 404, as if it did not exist.
 */
export function requirePipelineVisible(
  pipelineIdOf: (req: Request) => unknown = (req) => req.params?.pipelineId,
): AccessMarkedHandler {
  return markAccess((req: Request, _res: Response, next: NextFunction) => {
    void (async () => {
      if (!req.serviceActor && !req.headers.authorization?.startsWith('Bearer ')) throw new AppError(401, 'Authorization required');
      const pipelineId = str(pipelineIdOf(req)) ?? '';
      /* eslint-disable @typescript-eslint/no-require-imports */
      const { getSystemPipeline } = require('./systemPipelines') as typeof import('./systemPipelines');
      const { definitionsRepo } = require('./definitions-repository') as typeof import('./definitions-repository');
      /* eslint-enable @typescript-eslint/no-require-imports */
      if (pipelineId && getSystemPipeline(pipelineId)) { next(); return; }
      const userId = req.user?.sub ?? '';
      const def = pipelineId && userId ? await definitionsRepo.get(userId, pipelineId).catch(() => null) : null;
      if (!def) throw new NotFoundError('pipeline not found');
      next();
    })().catch(next);
  }, 'pipeline:owner');
}

/** The run a queue envelope / dead letter / snapshot names, wherever it keeps it. */
export function runRefOf(item: unknown): { runId?: string; pipelineId?: string } {
  const x = (item ?? {}) as Record<string, any>;
  const payload = (x.payload ?? x.event?.payload ?? x.eventPayload ?? {}) as Record<string, any>;
  return {
    runId: str(x.runId) ?? str(payload.runId) ?? str(payload.run?.runId),
    pipelineId: str(x.pipelineId) ?? str(payload.pipelineId) ?? str(payload.run?.pipelineId),
  };
}

/** Keep the items whose run the caller may read; an item naming no run is dropped. */
export async function filterVisibleRunItems<T>(req: Parameters<typeof assertRunAccess>[0], items: readonly T[]): Promise<T[]> {
  const cache = new Map<string, Promise<boolean>>();
  const seen = await Promise.all(items.map((item) => {
    const { runId, pipelineId } = runRefOf(item);
    if (!runId) return Promise.resolve(false);
    const key = `${pipelineId ?? ''}|${runId}`;
    if (!cache.has(key)) cache.set(key, canSeeRun(req, pipelineId, runId));
    return cache.get(key)!;
  }));
  return items.filter((_i, n) => seen[n]);
}

/** One queue item / dead letter: the run it names, at `mode` (no run named → 404). */
export async function assertItemRunAccess(req: Parameters<typeof assertRunAccess>[0], item: unknown, mode: RunAccessMode): Promise<void> {
  const { runId, pipelineId } = runRefOf(item);
  if (isTrustedServiceCall(req)) return;
  if (!runId) {
    if (!req.serviceActor && !req.headers.authorization?.startsWith('Bearer ')) throw new AppError(401, 'Authorization required');
    throw new NotFoundError('not found');
  }
  await assertRunAccess(req, pipelineId, runId, mode);
}
