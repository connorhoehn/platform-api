// platform-api/src/routes/pipelineDefinitions.ts
//
// REST endpoints for pipeline definitions, backed by DynamoDB via the
// `definitionsRepo` singleton (see `../pipeline/definitions-repository.ts`).
//
// Wave 3 of the persistence migration removed the synchronous in-memory
// mirror that earlier waves kept around for cross-module consumers.
// Those consumers (the schedule evaluator in `src/index.ts` and the
// webhook lookups in `pipelineWebhooks.ts`) now read from
// `pipelineDefinitionsCache`, a Scan-backed snapshot refreshed every
// 60s. We poke the cache from the write paths below so the common "save
// the pipeline then immediately test the webhook" UX doesn't have to
// wait a minute for the cache to catch up.
//
// Endpoints (mounted at /api/pipelines/defs in routes/index.ts — see the
// collision note below):
//   GET    /                         — list the caller's pipeline defs
//   GET    /:pipelineId              — fetch one
//   PUT    /:pipelineId              — upsert (body.id must match :pipelineId)
//   DELETE /:pipelineId              — remove
//   POST   /:pipelineId/publish      — bump version, mark published
//
// Collision note: the existing `pipelineMetricsRouter` is mounted at
// `/api/pipelines/metrics`. To avoid shadowing the static `metrics` segment
// under a `/:pipelineId` route, this router is mounted at `/pipelines/defs`
// (see routes/index.ts). The metrics mount must come BEFORE the defs mount
// anyway — Express resolves mounts in registration order.
//
// Shape is deliberately compatible with the frontend's `PipelineDefinition`
// type; this file keeps its own untyped view so the two repos can evolve
// independently (standalone mirror — see TYPES_SYNC.md).

import { Router, type Request, type Response } from 'express';
import { asyncHandler } from '../middleware/error-handler';
import { generateWebhookSecret } from 'distributed-core';
import { definitionsRepo } from '../pipeline/definitions-repository';
import { pipelineDefinitionsCache } from '../pipeline/definitions-cache';
import { publishInvalidate } from '../pipeline/definitions-invalidator';
import { auditRepo, type AuditEvent } from '../pipeline/audit-repository';
import { withContext } from '../lib/logger';

const log = withContext({ route: 'pipelineDefinitions' });

/**
 * Best-effort cache poke after a write. Refreshes the LOCAL cache
 * immediately AND publishes a Redis invalidation message so other
 * replicas refresh their own caches within milliseconds instead of
 * waiting for their 60s timer (Phase 54.6.2). Both paths are
 * fire-and-forget — neither failure mode affects HTTP latency.
 */
function pokeCacheAfterWrite(reason: string): void {
  pipelineDefinitionsCache.refresh().catch((err) => {
    log.error(
      { err: err instanceof Error ? err.message : String(err) },
      'pipelineDefinitionsCache refresh-after-write failed',
    );
  });
  // Cross-replica push invalidation (Phase 54.6.2).
  void publishInvalidate(reason);
}

/**
 * Fire-and-forget audit write. Audit failures must NOT fail the request —
 * mirrors the pattern in pipelineTriggers.ts:36-46. Logs the error so an
 * audit-table outage still surfaces in observability.
 */
function fireAndForgetAudit(event: AuditEvent): void {
  auditRepo.record(event).catch((err: unknown) => {
    const message = err instanceof Error ? err.message : String(err);
    log.warn(
      { action: event.action, pipelineId: event.pipelineId, err: message },
      'audit write failed',
    );
  });
}

/** Pull a small, bounded snapshot from a definition for before/after fields.
 *  We avoid persisting the full pipeline graph (could be many KB of node
 *  config) — only the fields an operator actually needs to diff version /
 *  publish state mutations. */
function definitionSnapshot(def: unknown): Record<string, unknown> | undefined {
  if (!def || typeof def !== 'object') return undefined;
  const d = def as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const k of ['version', 'publishedVersion', 'status', 'name', 'updatedAt']) {
    if (d[k] !== undefined) out[k] = d[k];
  }
  // Count nodes/edges — useful "did this change?" signal without persisting graph.
  if (Array.isArray(d['nodes'])) out['nodeCount'] = (d['nodes'] as unknown[]).length;
  if (Array.isArray(d['edges'])) out['edgeCount'] = (d['edges'] as unknown[]).length;
  return out;
}

export const pipelineDefinitionsRouter = Router();

// List
pipelineDefinitionsRouter.get(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user!.sub;
    try {
      const items = await definitionsRepo.list(userId);
      res.json({ pipelines: items });
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      log.error({ err: message, action: 'def.list' }, 'failed to list definitions');
      res.status(500).json({ error: 'internal_error', detail: 'failed to list definitions' });
    }
  }),
);

// Get one
pipelineDefinitionsRouter.get(
  '/:pipelineId',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user!.sub;
    const pipelineId = req.params.pipelineId;
    try {
      const def = await definitionsRepo.get(userId, pipelineId);
      if (!def) {
        res.status(404).json({ error: 'not_found' });
        return;
      }
      res.json(def);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      log.error({ err: message, pipelineId, action: 'def.get' }, 'failed to get definition');
      res.status(500).json({ error: 'internal_error', detail: 'failed to get definition' });
    }
  }),
);

// Upsert
pipelineDefinitionsRouter.put(
  '/:pipelineId',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user!.sub;
    const pipelineId = req.params.pipelineId;
    const def = req.body as
      | {
          id?: unknown;
          triggerBinding?: {
            event?: string;
            webhookPath?: string;
            webhookSecret?: string;
          };
        }
      | undefined;
    if (!def || typeof def !== 'object' || def.id !== pipelineId) {
      res.status(400).json({ error: 'invalid_body' });
      return;
    }

    // Capture the BEFORE snapshot for the audit row (best-effort — read failure
    // becomes a missing `before`, not a request failure).
    let beforeSnap: Record<string, unknown> | undefined;
    try {
      const existing = await definitionsRepo.get(userId, pipelineId);
      beforeSnap = definitionSnapshot(existing);
    } catch {
      beforeSnap = undefined;
    }

    // Mint a webhook secret server-side the first time a webhook trigger
    // binding is saved so the secret never has to round-trip through the
    // browser unprotected. Idempotent — once a secret exists we leave it
    // alone (rotation is a separate, explicit operation Phase-5 will add).
    const tb = def.triggerBinding;
    if (
      tb &&
      tb.event === 'webhook' &&
      typeof tb.webhookPath === 'string' &&
      tb.webhookPath.length > 0 &&
      (typeof tb.webhookSecret !== 'string' || tb.webhookSecret.length === 0)
    ) {
      tb.webhookSecret = generateWebhookSecret();
    }

    try {
      await definitionsRepo.put(userId, def as { id: string; [k: string]: unknown });
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      log.error({ err: message, pipelineId, action: 'def.put' }, 'failed to persist definition');
      fireAndForgetAudit({
        action: 'pipeline.definition.put',
        actorUserId: userId,
        pipelineId,
        outcome: 'error',
        decision: 'error',
        details: { error: message },
        ...(beforeSnap ? { before: beforeSnap } : {}),
      });
      res.status(500).json({ error: 'internal_error', detail: 'failed to persist definition' });
      return;
    }

    // Refresh the cross-user cache so the webhook router and the schedule
    // evaluator see the new definition without waiting for the next tick.
    pokeCacheAfterWrite('definition.upsert');

    log.info({ pipelineId, action: 'def.put' }, 'definition updated');
    fireAndForgetAudit({
      action: 'pipeline.definition.put',
      actorUserId: userId,
      pipelineId,
      outcome: 'success',
      ...(beforeSnap ? { before: beforeSnap } : {}),
      ...(definitionSnapshot(def) ? { after: definitionSnapshot(def)! } : {}),
    });
    res.json(def);
  }),
);

// Delete
pipelineDefinitionsRouter.delete(
  '/:pipelineId',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user!.sub;
    const pipelineId = req.params.pipelineId;

    let beforeSnap: Record<string, unknown> | undefined;
    try {
      const existing = await definitionsRepo.get(userId, pipelineId);
      beforeSnap = definitionSnapshot(existing);
    } catch {
      beforeSnap = undefined;
    }

    try {
      await definitionsRepo.delete(userId, pipelineId);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      log.error({ err: message, pipelineId, action: 'def.delete' }, 'failed to delete definition');
      fireAndForgetAudit({
        action: 'pipeline.definition.delete',
        actorUserId: userId,
        pipelineId,
        outcome: 'error',
        decision: 'error',
        details: { error: message },
        ...(beforeSnap ? { before: beforeSnap } : {}),
      });
      res.status(500).json({ error: 'internal_error', detail: 'failed to delete definition' });
      return;
    }

    // Refresh the cross-user cache so removed definitions stop firing
    // schedules / matching webhook paths immediately.
    pokeCacheAfterWrite('definition.delete');

    log.info({ pipelineId, action: 'def.delete' }, 'definition deleted');
    fireAndForgetAudit({
      action: 'pipeline.definition.delete',
      actorUserId: userId,
      pipelineId,
      outcome: 'success',
      ...(beforeSnap ? { before: beforeSnap } : {}),
    });
    res.status(204).end();
  }),
);

// Publish — convenience route
pipelineDefinitionsRouter.post(
  '/:pipelineId/publish',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user!.sub;
    const pipelineId = req.params.pipelineId;

    let current: { id: string; [k: string]: unknown } | null;
    try {
      current = (await definitionsRepo.get(userId, pipelineId)) as
        | { id: string; [k: string]: unknown }
        | null;
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      log.error(
        { err: message, pipelineId, action: 'def.publish.read' },
        'failed to read definition for publish',
      );
      fireAndForgetAudit({
        action: 'pipeline.definition.publish',
        actorUserId: userId,
        pipelineId,
        outcome: 'error',
        decision: 'error',
        details: { error: message, stage: 'read' },
      });
      res.status(500).json({ error: 'internal_error', detail: 'failed to read definition' });
      return;
    }

    if (!current) {
      res.status(404).json({ error: 'not_found' });
      return;
    }

    const beforeSnap = definitionSnapshot(current);

    const next = current as {
      id: string;
      status?: string;
      version?: number;
      publishedVersion?: number;
      [k: string]: unknown;
    };
    next.version = (next.version ?? 0) + 1;
    next.status = 'published';
    next.publishedVersion = next.version;

    try {
      await definitionsRepo.put(userId, next);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      log.error(
        { err: message, pipelineId, action: 'def.publish.write' },
        'failed to persist published definition',
      );
      fireAndForgetAudit({
        action: 'pipeline.definition.publish',
        actorUserId: userId,
        pipelineId,
        outcome: 'error',
        decision: 'error',
        details: { error: message, stage: 'write' },
        ...(beforeSnap ? { before: beforeSnap } : {}),
      });
      res.status(500).json({ error: 'internal_error', detail: 'failed to persist publish' });
      return;
    }

    pokeCacheAfterWrite('definition.publish');

    log.info(
      { pipelineId, version: next.version, action: 'def.publish' },
      'definition published',
    );
    fireAndForgetAudit({
      action: 'pipeline.definition.publish',
      actorUserId: userId,
      pipelineId,
      outcome: 'success',
      decision: 'completed',
      ...(beforeSnap ? { before: beforeSnap } : {}),
      ...(definitionSnapshot(next) ? { after: definitionSnapshot(next)! } : {}),
    });
    res.json(next);
  }),
);
