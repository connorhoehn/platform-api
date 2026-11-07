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
//   POST   /:pipelineId/publish      — validate, bump version, mark published
//
// Validation: PUT stores whatever it is given (a draft is legitimately
// half-finished) but returns a `validation` block so a non-browser writer sees
// what it just stored. POST /publish REJECTS a definition with structural
// errors — see pipeline/validate-shape.ts for why that gate belongs here.
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
import { getSystemPipeline, listSystemPipelines } from '../pipeline/systemPipelines';
import { pipelineDefinitionsCache } from '../pipeline/definitions-cache';
import { publishInvalidate } from '../pipeline/definitions-invalidator';
import { auditRepo, type AuditEvent } from '../pipeline/audit-repository';
import { withContext } from '../lib/logger';
import { validateShape, isPublishable } from '../pipeline/validate-shape';
import { listStepKinds } from '../pipeline/customStepHandlers';
import { decorateDefinition } from '../pipeline/definition-metadata';
import { loadRollups } from '../pipeline/run-rollup-read';
import { pipelineRunRollupRepo } from '../repositories/PipelineRunRollupRepository';
import { PipelineRunRepository } from '../repositories/PipelineRunRepository';
import { docClient } from '../lib/aws-clients';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { idempotency } from '../middleware/idempotency';
import { generateDraftDefinition, InvalidInstructionError, PlannerUnavailableError } from '../pipeline/generate-definition';
import type { LoopPlan } from '../chat/agentLoopPlanner';
import { unapprovedToolSteps, describeToolRequestingSteps } from '../pipeline/agent/userDefinitionPolicy';

const log = withContext({ route: 'pipelineDefinitions' });

/** The kinds this process can execute, for the unrecognised-kind check. */
function knownStepKinds(): string[] {
  return listStepKinds().map((k) => k.kind);
}

/**
 * The handles a step kind declares, for the edge checks in `validateShape`.
 *
 * Built once per validation rather than per edge — `listStepKinds` walks the
 * whole registry, and a definition can carry a lot of edges.
 */
function declaredHandles(): (kind: string) => { outputs?: readonly string[]; inputs?: readonly string[] } | undefined {
  const byKind = new Map<string, { outputs?: readonly string[]; inputs?: readonly string[] }>();
  for (const k of listStepKinds()) {
    if (k.outputs || k.inputs) byKind.set(k.kind, { outputs: k.outputs, inputs: k.inputs });
  }
  return (kind: string) => byKind.get(kind);
}

/** What each kind takes in and hands on, for the payload check in `validateShape`. */
function declaredPayloads(): (kind: string) => { consumes?: string; produces?: string } | undefined {
  const byKind = new Map<string, { consumes?: string; produces?: string }>();
  for (const k of listStepKinds()) {
    if (k.consumes || k.produces) byKind.set(k.kind, { consumes: k.consumes, produces: k.produces });
  }
  return (kind: string) => byKind.get(kind);
}

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

/** `?include=rollup,approvals` — what the list attaches beyond the definitions. */
type ListInclude = 'rollup' | 'approvals';
function parseInclude(raw: unknown): Set<ListInclude> {
  const out = new Set<ListInclude>();
  const values = Array.isArray(raw) ? raw : typeof raw === 'string' ? [raw] : [];
  for (const v of values) {
    for (const part of String(v).split(',')) {
      const p = part.trim().toLowerCase();
      if (p === 'rollup' || p === 'approvals') out.add(p);
      // `rollup` is what the page asks for; approvals ride along with it.
      if (p === 'rollup') out.add('approvals');
    }
  }
  return out;
}

/** The run table, for the rollup backfill. Lazy: a test that never asks for rollups never builds it. */
let runRepoSingleton: PipelineRunRepository | null = null;
function runRepo(): PipelineRunRepository {
  if (!runRepoSingleton) runRepoSingleton = new PipelineRunRepository(docClient);
  return runRepoSingleton;
}

// List
pipelineDefinitionsRouter.get(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user!.sub;
    const include = parseInclude(req.query.include);
    try {
      const items = await definitionsRepo.list(userId);
      // Platform-owned pipelines ride along, read-only, so their runs have a home.
      //
      // A system pipeline that has been EDITED exists in both lists: the repo
      // holds the saved version and the seed still declares the original. This
      // used to concatenate the two, so `document-agent-review` came back twice
      // from a list of seventeen — a duplicate row in the picker and the
      // sidebar, a duplicate React key, and worse for the caller this catalog
      // is really for: an agent building a map by id took whichever entry came
      // last, and the seed was appended last, so it read the ORIGINAL
      // definition and never the saved one.
      //
      // Stored wins, seed fills the gaps — the same precedence `GET /:id` has
      // always used.
      const byId = new Map<string, Record<string, unknown>>();
      for (const sys of listSystemPipelines()) {
        const id = (sys as { id?: unknown }).id;
        // A seed with no stored override is read-only: the UI could not tell a
        // seed from a saved copy before this flag.
        if (typeof id === 'string') byId.set(id, { ...sys, readOnly: true });
      }
      for (const item of items) {
        const id = (item as { id?: unknown }).id;
        if (typeof id === 'string') byId.set(id, item as Record<string, unknown>);
        // A stored row with no usable id cannot collide with anything; keep it
        // rather than dropping someone's pipeline over a missing field.
        else byId.set(`__anon-${byId.size}`, item as Record<string, unknown>);
      }
      // workType / kind / route filled on the response only; never written back.
      const pipelines: Record<string, unknown>[] = [...byId.values()].map((def) => decorateDefinition(def));

      if (include.size > 0) {
        const candidates = pipelines
          .filter((p) => typeof p.id === 'string')
          .map((p) => ({ id: p.id as string, ...(typeof p.updatedAt === 'string' ? { updatedAt: p.updatedAt } : {}) }));
        const rollups = await loadRollups(candidates, {
          rollups: pipelineRunRollupRepo,
          listRuns: (pipelineId, limit) => runRepo().listByPipeline(pipelineId, { limit }),
          countRuns: (pipelineId) => runRepo().countByPipeline(pipelineId),
        });
        for (const p of pipelines) {
          const rollup = typeof p.id === 'string' ? rollups.get(p.id) ?? null : null;
          if (include.has('rollup')) p.rollup = rollup;
          // NFR loop 18: the count comes off the rollup row, where the
          // subscriber keys every approval gate by (runId, stepId), so two
          // replicas answer the same number. The bridge's in-memory queue only
          // knew the runs executing on the replica that answered.
          if (include.has('approvals')) p.pendingApprovals = rollup?.pendingApprovals ?? 0;
        }
      }
      res.json({ pipelines });
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      log.error({ err: message, action: 'def.list' }, 'failed to list definitions');
      res.status(500).json({ error: 'internal_error', detail: 'failed to list definitions' });
    }
  }),
);

// Generate — the /agent planner writing a DRAFT the caller owns.
//
//   POST /api/pipelines/defs/generate
//   { instruction, mode: 'draft' | 'run', requestId?, hints?, model?, contextBudgetTokens? }
//   (`action: 'draft' | 'draft-and-run'` from the plan is accepted as an alias)
//   200 { pipeline, runId?, runError?, planner: { source: 'model' | 'fallback', steps } }
//   400 invalid_body · 422 invalid_instruction · 500 internal_error
//
// Idempotent by `requestId`: it is carried as the Idempotency-Key, so the
// same DynamoDB conditional-write claim that guards run triggers (NFR #97)
// guards this — a retry replays the first answer, a concurrent duplicate gets
// 409 idempotency_in_progress, and a non-2xx releases the claim.
function requestIdAsIdempotencyKey(req: Request, _res: Response, next: () => void): void {
  const body = (req.body ?? {}) as { requestId?: unknown };
  if (!req.headers['idempotency-key'] && typeof body.requestId === 'string' && body.requestId.trim()) {
    req.headers['idempotency-key'] = body.requestId.trim();
  }
  next();
}

interface GenerateBody {
  instruction?: unknown;
  mode?: unknown;
  action?: unknown;
  requestId?: unknown;
  hints?: unknown;
  model?: unknown;
  contextBudgetTokens?: unknown;
  /** Plan and build the definition, return it, persist and run nothing (NFR Loop 39: exercises the planner). */
  dryRun?: unknown;
}

pipelineDefinitionsRouter.post(
  '/generate',
  requestIdAsIdempotencyKey,
  idempotency({ scope: 'pipeline-generate' }),
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user!.sub;
    const organizationId = req.user!.orgId;
    const body = (req.body ?? {}) as GenerateBody;
    if (!body || typeof body !== 'object') {
      res.status(400).json({ error: 'invalid_body' });
      return;
    }
    const mode: 'draft' | 'run' | null =
      body.mode === 'draft' || body.mode === 'run' ? body.mode
        : body.action === 'draft' ? 'draft'
          : body.action === 'draft-and-run' ? 'run'
            : body.mode === undefined && body.action === undefined ? 'draft' : null;
    if (mode === null) {
      res.status(400).json({ error: 'invalid_body', detail: "mode must be 'draft' or 'run'" });
      return;
    }
    const hints = Array.isArray(body.hints) ? body.hints.filter((h): h is string => typeof h === 'string') : undefined;

    let generated: Awaited<ReturnType<typeof generateDraftDefinition>>;
    try {
      generated = await generateDraftDefinition({
        instruction: typeof body.instruction === 'string' ? body.instruction : '',
        ...(hints ? { hints } : {}),
        ...(typeof body.model === 'string' ? { model: body.model } : {}),
        ...(typeof body.contextBudgetTokens === 'number' ? { contextBudgetTokens: body.contextBudgetTokens } : {}),
        userId,
        ...(req.user!.email ? { userName: req.user!.email } : {}),
        ...(organizationId ? { organizationId } : {}),
      });
    } catch (e) {
      if (e instanceof InvalidInstructionError) {
        res.status(422).json({ error: 'invalid_instruction', detail: e.detail });
        return;
      }
      if (e instanceof PlannerUnavailableError) {
        res.status(503).json({ error: 'model_unavailable', detail: e.message });
        return;
      }
      throw e;
    }

    const def = generated.definition;
    if (body.dryRun === true) {
      res.status(200).json({ dryRun: true, plan: generated.plan, definition: def });
      return;
    }
    if (mode === 'run') {
      def.status = 'published';
      def.publishedVersion = 1;
    }

    try {
      await definitionsRepo.put(userId, def);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      log.error({ err: message, pipelineId: def.id, action: 'def.generate' }, 'failed to persist generated definition');
      res.status(500).json({ error: 'internal_error', detail: 'failed to persist definition' });
      return;
    }
    pokeCacheAfterWrite('definition.generate');
    fireAndForgetAudit({
      action: 'pipeline.definition.put',
      actorUserId: userId,
      pipelineId: def.id,
      outcome: 'success',
      details: { origin: 'agent', mode, plannerSource: generated.plan.source, steps: generated.plan.steps.length },
      ...(definitionSnapshot(def) ? { after: definitionSnapshot(def)! } : {}),
    });

    let runId: string | undefined;
    let runError: string | undefined;
    if (mode === 'run') {
      const bridge = getPipelineBridge();
      if (!bridge?.trigger) {
        runError = 'bridge_unavailable';
      } else {
        try {
          const out = await bridge.trigger({
            pipelineId: def.id,
            definition: def,
            triggerPayload: generated.triggerPayload,
            triggeredBy: { userId },
          });
          runId = out.runId;
          // Lazy: the persistence module drags the work-graph projection in with it.
          const { getRunPersistenceService } = await import('../pipeline/runPersistence.js');
          getRunPersistenceService()?.registerTrigger({
            runId,
            pipelineId: def.id,
            userId,
            triggerType: 'manual',
            at: new Date().toISOString(),
            ...(organizationId ? { organizationId } : {}),
          });
        } catch (e) {
          runError = e instanceof Error ? e.message : String(e);
          log.warn({ err: runError, pipelineId: def.id, action: 'def.generate.run' }, 'generated definition saved but the run did not start');
        }
      }
    }

    const planner: { source: 'model' | 'fallback'; steps: number; notice?: string } = {
      source: (generated.plan as LoopPlan).source === 'planner' ? 'model' : 'fallback',
      steps: generated.plan.steps.length,
      ...(generated.plan.notice ? { notice: generated.plan.notice } : {}),
    };
    log.info({ pipelineId: def.id, mode, planner: planner.source, steps: planner.steps, runId, action: 'def.generate' }, 'definition generated');
    res.json({
      pipeline: decorateDefinition(def),
      ...(runId ? { runId } : {}),
      ...(runError ? { runError } : {}),
      planner,
    });
  }),
);

// Get one
pipelineDefinitionsRouter.get(
  '/:pipelineId',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const userId = req.user!.sub;
    const pipelineId = req.params.pipelineId;
    try {
      const stored = await definitionsRepo.get(userId, pipelineId);
      const def = stored ?? getSystemPipeline(pipelineId);
      if (!def) {
        res.status(404).json({ error: 'not_found' });
        return;
      }
      res.json(decorateDefinition(stored ? def : { ...def, readOnly: true }));
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

    // Read the existing def once — reused for (a) the audit BEFORE snapshot,
    // (b) the optimistic-lock check, and (c) authoritative version bumping.
    // (best-effort — read failure becomes a missing `before`, not a failure).
    let existing: Record<string, unknown> | null = null;
    try {
      existing = await definitionsRepo.get(userId, pipelineId);
    } catch {
      existing = null;
    }
    // A definition a person writes carries no NEW agent tools
    // (userDefinitionPolicy.ts): a tool step is kept only when the stored
    // definition already has it, unchanged, on the devbox runtime.
    const toolSteps = unapprovedToolSteps(def, existing);
    if (toolSteps.length > 0) {
      res.status(400).json({
        error: 'agent_tools_not_allowed',
        detail: describeToolRequestingSteps(toolSteps),
        steps: toolSteps,
      });
      return;
    }

    const beforeSnap = definitionSnapshot(existing);

    const existingVersion =
      existing && typeof existing.version === 'number' ? existing.version : 0;

    // Optimistic lock: if the client declares the version it edited from
    // (`expectedVersion`) and the stored version has moved on, reject so a
    // concurrent writer isn't clobbered. Opt-in: clients that don't send
    // expectedVersion get last-writer-wins (but still a monotonic version).
    const expectedVersion = (def as { expectedVersion?: unknown }).expectedVersion;
    // Only when a row actually EXISTS. Optimistic locking guards against
    // clobbering a concurrent writer, and with no stored row there is no writer
    // to clobber — this is a create.
    //
    // Without that condition the SYSTEM pipelines could never be edited at all.
    // They are seeded in code and served by `getSystemPipeline` when the repo
    // has nothing, so the editor loads one at its declared version (1), sends
    // `expectedVersion: 1`, and is compared against a stored version of 0 — a
    // conflict with nothing. Every edit to `document-agent-review` and its
    // siblings 409'd and the control snapped back, with no error shown.
    if (
      existing !== null &&
      typeof expectedVersion === 'number' &&
      expectedVersion !== existingVersion
    ) {
      res.status(409).json({
        error: 'version_conflict',
        detail: 'pipeline was modified by another writer; reload before saving',
        currentVersion: existingVersion,
        expectedVersion,
      });
      return;
    }

    // The SERVER owns the version: monotonically bump from the stored value,
    // ignoring whatever the client computed. Removes the client-driven
    // double-increment / non-monotonic drift entirely. Also stamp updatedAt.
    (def as Record<string, unknown>).version = existingVersion + 1;
    (def as Record<string, unknown>).updatedAt = new Date().toISOString();
    delete (def as { expectedVersion?: unknown }).expectedVersion;

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
    // Report the graph's shape back to whoever wrote it. A draft save is never
    // blocked — half-wired is a legitimate state on the way to finished — but an
    // agent posting definitions has no editor to look at, so the only way it
    // learns that it left a dangling edge is if we say so here.
    res.json({
      ...(def as Record<string, unknown>),
      validation: validateShape(def, {
        knownKinds: knownStepKinds(),
        handlesFor: declaredHandles(),
        payloadFor: declaredPayloads(),
      }),
    });
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

    // The publish gate. Until this existed the only thing checking a pipeline
    // before it went live was the browser's own validation, which an agent
    // posting straight at this API never runs. A definition with dangling
    // edges, two triggers or a cycle was published and failed at run time
    // instead, a long way from whatever produced it.
    const validation = validateShape(current, {
      knownKinds: knownStepKinds(),
      handlesFor: declaredHandles(),
      payloadFor: declaredPayloads(),
    });
    if (!isPublishable(validation)) {
      log.info(
        { pipelineId, action: 'def.publish', errors: validation.errors.length },
        'publish rejected: definition is not structurally sound',
      );
      fireAndForgetAudit({
        action: 'pipeline.definition.publish',
        actorUserId: userId,
        pipelineId,
        outcome: 'error',
        decision: 'rejected',
        details: { stage: 'validate', errors: validation.errors.map((e) => e.code) },
      });
      res.status(422).json({
        error: 'invalid_pipeline',
        detail: 'pipeline has structural errors and cannot be published',
        validation,
      });
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
    res.json({ ...next, validation });
  }),
);
