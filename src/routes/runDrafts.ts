// platform-api/src/routes/runDrafts.ts
//
// Run drafts on a document (docs/design/documents-detail/PLAN.md §2.2) —
// mounted at /documents/:documentId/run-drafts.
//
//   GET  /                     → { drafts: RunDraftView[] }
//   GET  /:draftId             → { draft }
//   PUT  /:draftId             { pipelineId, instruction, model?, contextBudgetTokens?, hints?, sources?, expectedRevision? }
//                              → 201 created | 200 { draft, replayed } | 409 draft_conflict / draft_not_editable
//   POST /:draftId/dispatch    → 200 { draft, runId, replayed } | 409 dispatch_in_progress (Retry-After) | 4xx/5xx trigger refusal
//   POST /:draftId/cancel      → 200 { draft }   (only from `draft`)
//
// `draftId` is the client's requestId: a PUT with the same body replays, a
// changed body needs the current `expectedRevision`.
//
// DISPATCH is three steps, each safe to repeat on any replica:
//   1. CAS  draft → dispatching   (records `dispatchedBy`, the verified caller)
//   2. start the run through `triggerPipelineRun` — the body of
//      `POST /api/pipelines/:pipelineId/runs` — under the Idempotency-Key claim
//      `run-draft:<draftId>` (scope `pipeline-trigger`, user `dispatchedBy`),
//      with `triggerPayload.requestedBy` pinned to `dispatchedBy`;
//   3. CAS  dispatching → dispatched with the runId.
// A replica that crashed between 2 and 3 left a committed claim: any retry
// replays the same runId and finishes 3. One that died INSIDE 2 left the claim
// `pending`. Step 1 therefore also records the run's id (`pendingRunId`,
// generated before the trigger, which starts the run under it) and the
// dispatching replica (`dispatchOwnerNodeId`). A retry that meets the pending
// claim:
//   a. looks the run up by that id (pipeline-runs `runId-index`) and, if the
//      run exists, ADOPTS it — step 3 with that id, no second run;
//   b. else, if the owner's incarnation has lost its liveness lease
//      (replicaLiveness), takes the dispatch over at once: a conditional CAS
//      moves `dispatchOwnerNodeId` to itself (one taker wins), the dead
//      holder's pending claim is released, and the run is triggered under the
//      SAME id;
//   c. else answers 409 `dispatch_in_progress` (the owner is alive, or its
//      liveness cannot be read) until the owner finishes or the claim's lease
//      (PENDING_TTL_SECONDS = 120 s) lapses.
// A trigger taken under a lapsed claim also checks for the run first. A
// trigger refusal returns the draft to `draft` with `dispatchError`, so it can
// be edited.
//
// Auth: every route needs the `document:edit` policy (owner or editor) except
// the GETs, which need the document to exist. Signals: `doc:run_draft_updated`
// on `doc-work:<documentId>` — ids and enums only.

import { randomUUID } from 'crypto';
import { Router, type Request, type Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { AppError, ValidationError } from '../middleware/error-handler';
import { handle } from './documentWork';
import { requireDocumentAccess, type DocumentAccessDeps } from '../documents/documentAccess';
import {
  RUN_DRAFT_MODELS,
  type RunDraft,
  type RunDraftModel,
  type RunDraftSource,
  type RunDraftStatus,
} from '../repositories/RunDraftRepository';
import type { IdempotentOutcome } from '../middleware/idempotency';
import type { TriggerRunRequest, TriggerRunResponse } from './pipelineTriggers';
import log from '../lib/logger';

export const DISPATCH_STALE_MS = 60_000;
export const DISPATCH_IDEMPOTENCY_SCOPE = 'pipeline-trigger';
export const dispatchIdempotencyKey = (draftId: string): string => `run-draft:${draftId}`;

const DRAFT_ID = /^[A-Za-z0-9_-]{1,64}$/;
const PIPELINE_ID = /^[A-Za-z0-9_.:-]{1,128}$/;
const MAX_INSTRUCTION = 4000;
const MAX_HINTS = 10;
const MAX_HINT_CHARS = 200;
const MAX_SOURCES = 20;
const MIN_BUDGET = 1_000;
const MAX_BUDGET = 2_000_000;
const CAS_ATTEMPTS = 4;

export interface RunDraftsDeps extends DocumentAccessDeps {
  drafts: {
    get(documentId: string, draftId: string): Promise<RunDraft | null>;
    listForDocument(documentId: string): Promise<RunDraft[]>;
    create(draft: RunDraft): Promise<boolean>;
    replaceIf(next: RunDraft, seen: { revision: number; fromStatuses: readonly RunDraftStatus[] }): Promise<void>;
  };
  /** `triggerPipelineRun` (pipelineTriggers.ts). Throws PipelineTriggerBridgeError / AppError on refusal. */
  trigger(input: { pipelineId: string; userId: string; organizationId?: string; body: TriggerRunRequest; runId?: string }): Promise<TriggerRunResponse>;
  /** `runIdempotent` (middleware/idempotency.ts). */
  runIdempotent<T>(
    opts: { scope: string; userId: string; key: string; body: unknown },
    handler: () => Promise<{ status: number; body: T }>,
  ): Promise<IdempotentOutcome<T>>;
  publishSignal(channelId: string, eventName: string, payload: Record<string, unknown>): Promise<void>;
  now(): string;
  /** The run with this id, if pipeline-runs has it (`runId-index`). Absent → never adopts. */
  findRun?(runId: string): Promise<{ runId: string; status?: string } | null>;
  /** Is the replica that took the dispatch at `sinceMs` still running? Absent, or unreadable → treated as alive. */
  isOwnerLive?(ownerNodeId: string | undefined, sinceMs: number): Promise<boolean>;
  /** This replica's node id, stamped as the dispatch owner. */
  localNodeId?(): string | undefined;
  /** Drop a still-pending Idempotency-Key claim (`releasePendingClaim`). */
  releasePendingClaim?(opts: { scope: string; userId: string; key: string }): Promise<void>;
  /** The id a new run gets (default `randomUUID`). */
  newRunId?(): string;
}

export interface RunDraftView extends RunDraft {
  /** `dispatching` for longer than DISPATCH_STALE_MS: the UI offers "Dispatch not confirmed — Retry". */
  dispatchStale?: boolean;
}

export function viewOf(draft: RunDraft, nowIso: string): RunDraftView {
  if (draft.status !== 'dispatching' || !draft.dispatchStartedAt) return draft;
  const age = Date.parse(nowIso) - Date.parse(draft.dispatchStartedAt);
  return age > DISPATCH_STALE_MS ? { ...draft, dispatchStale: true } : draft;
}

interface DraftContent {
  pipelineId: string;
  instruction: string;
  model?: RunDraftModel;
  contextBudgetTokens?: number;
  hints?: string[];
  sources?: RunDraftSource[];
}

function parseContent(raw: unknown): DraftContent & { expectedRevision?: number } {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw new ValidationError('body must be an object');
  const b = raw as Record<string, unknown>;
  if (typeof b.pipelineId !== 'string' || !PIPELINE_ID.test(b.pipelineId)) {
    throw new ValidationError('pipelineId is required (create one first with POST /api/pipelines/defs/generate { requestId: <draftId>, mode: "draft" })');
  }
  if (typeof b.instruction !== 'string' || !b.instruction.trim() || b.instruction.length > MAX_INSTRUCTION) {
    throw new ValidationError(`instruction is required (at most ${MAX_INSTRUCTION} characters)`);
  }
  const out: DraftContent & { expectedRevision?: number } = { pipelineId: b.pipelineId, instruction: b.instruction.trim() };
  if (b.model !== undefined && b.model !== null) {
    if (!(RUN_DRAFT_MODELS as readonly unknown[]).includes(b.model)) throw new ValidationError(`model must be ${RUN_DRAFT_MODELS.join(', ')}`);
    out.model = b.model as RunDraftModel;
  }
  if (b.contextBudgetTokens !== undefined && b.contextBudgetTokens !== null) {
    const n = b.contextBudgetTokens;
    if (typeof n !== 'number' || !Number.isInteger(n) || n < MIN_BUDGET || n > MAX_BUDGET) {
      throw new ValidationError(`contextBudgetTokens must be an integer ${MIN_BUDGET}..${MAX_BUDGET}`);
    }
    out.contextBudgetTokens = n;
  }
  if (b.hints !== undefined && b.hints !== null) {
    if (!Array.isArray(b.hints) || b.hints.length > MAX_HINTS || b.hints.some((h) => typeof h !== 'string' || !h.trim() || h.length > MAX_HINT_CHARS)) {
      throw new ValidationError(`hints must be at most ${MAX_HINTS} strings of at most ${MAX_HINT_CHARS} characters`);
    }
    out.hints = (b.hints as string[]).map((h) => h.trim());
  }
  if (b.sources !== undefined && b.sources !== null) {
    if (!Array.isArray(b.sources) || b.sources.length > MAX_SOURCES) throw new ValidationError(`sources must be an array of at most ${MAX_SOURCES}`);
    out.sources = b.sources.map((s, i) => {
      const e = (s ?? {}) as { kind?: unknown; id?: unknown };
      if (e.kind !== 'document' && e.kind !== 'transcript' && e.kind !== 'recording') throw new ValidationError(`sources[${i}].kind must be document, transcript or recording`);
      if (typeof e.id !== 'string' || !e.id || e.id.length > 200) throw new ValidationError(`sources[${i}].id is required`);
      return { kind: e.kind, id: e.id };
    });
  }
  if (b.expectedRevision !== undefined) {
    if (typeof b.expectedRevision !== 'number' || !Number.isInteger(b.expectedRevision) || b.expectedRevision < 0) {
      throw new ValidationError('expectedRevision must be a non-negative integer');
    }
    out.expectedRevision = b.expectedRevision;
  }
  return out;
}

function contentOf(d: DraftContent): string {
  return JSON.stringify([d.pipelineId, d.instruction, d.model ?? null, d.contextBudgetTokens ?? null, d.hints ?? null, d.sources ?? null]);
}

function withContent(base: RunDraft, c: DraftContent): RunDraft {
  const next: RunDraft = { ...base, pipelineId: c.pipelineId, instruction: c.instruction };
  for (const key of ['model', 'contextBudgetTokens', 'hints', 'sources'] as const) {
    if (c[key] === undefined) delete next[key];
    else (next as unknown as Record<string, unknown>)[key] = c[key];
  }
  return next;
}

/** The trigger request a dispatch sends — a pure function of the draft, so every retry hashes identically. */
export function triggerBodyOf(draft: RunDraft): TriggerRunRequest {
  return {
    triggerPayload: {
      documentId: draft.documentId,
      instruction: draft.instruction,
      requestedBy: draft.dispatchedBy ?? draft.createdBy,
      requestId: draft.draftId,
      runDraftId: draft.draftId,
      ...(draft.model ? { model: draft.model } : {}),
      ...(draft.contextBudgetTokens !== undefined ? { contextBudgetTokens: draft.contextBudgetTokens } : {}),
      ...(draft.hints ? { hints: draft.hints } : {}),
      ...(draft.sources ? { sources: draft.sources } : {}),
    },
  };
}

/** The trigger's answer for a run that was already started under the draft's `pendingRunId`. */
function adoptedResponse(draft: RunDraft, runAs: string, runId: string): TriggerRunResponse {
  return { runId, pipelineId: draft.pipelineId, triggeredBy: { userId: runAs, triggerType: 'manual' }, at: draft.dispatchStartedAt ?? draft.updatedAt };
}

function isDraftConflict(err: unknown): boolean {
  return (err as { name?: string } | null)?.name === 'RunDraftConflict';
}

function draftIdOf(req: Request): string {
  const draftId = String(req.params.draftId ?? '');
  if (!DRAFT_ID.test(draftId)) throw new ValidationError('draftId must be the client requestId (letters, digits, - and _, at most 64)');
  return draftId;
}

export function createRunDraftsRouter(deps: RunDraftsDeps = defaultRunDraftsDeps()): Router {
  const router = Router({ mergeParams: true });

  const signal = (d: RunDraft): void => {
    deps.publishSignal(`doc-work:${d.documentId}`, 'doc:run_draft_updated', {
      documentId: d.documentId,
      draftId: d.draftId,
      status: d.status,
      revision: d.revision,
      ...(d.runId ? { runId: d.runId } : {}),
    }).catch((err: unknown) => log.error({ event: 'doc:run_draft_updated', err: err instanceof Error ? err.message : String(err) }, 'run draft signal publish failed'));
  };

  router.get('/', requireAuth, handle(async (req, res) => {
    const documentId = String(req.params.documentId ?? '');
    await requireDocumentAccess(deps, req, documentId, 'read');
    const now = deps.now();
    const drafts = (await deps.drafts.listForDocument(documentId))
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt))
      .map((d) => viewOf(d, now));
    res.status(200).json({ drafts });
  }));

  router.get('/:draftId', requireAuth, handle(async (req, res) => {
    const documentId = String(req.params.documentId ?? '');
    const draftId = draftIdOf(req);
    await requireDocumentAccess(deps, req, documentId, 'read');
    const draft = await deps.drafts.get(documentId, draftId);
    if (!draft) { res.status(404).json({ error: 'run draft not found', code: 'draft-not-found' }); return; }
    res.status(200).json({ draft: viewOf(draft, deps.now()) });
  }));

  router.put('/:draftId', requireAuth, handle(async (req, res) => {
    const documentId = String(req.params.documentId ?? '');
    const draftId = draftIdOf(req);
    const content = parseContent(req.body);
    await requireDocumentAccess(deps, req, documentId, 'edit');
    const userId = req.user!.sub;

    for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
      const now = deps.now();
      const existing = await deps.drafts.get(documentId, draftId);
      if (!existing) {
        const draft: RunDraft = withContent({
          documentId, draftId, createdBy: userId, pipelineId: content.pipelineId, instruction: content.instruction,
          status: 'draft', revision: 1, createdAt: now, updatedAt: now, updatedBy: userId,
          ...(req.user?.orgId ? { organizationId: req.user.orgId } : {}),
        }, content);
        if (await deps.drafts.create(draft)) {
          signal(draft);
          res.status(201).json({ draft, replayed: false });
          return;
        }
        continue; // someone created it between our read and write: re-read and compare
      }
      if (existing.organizationId && req.user?.orgId && existing.organizationId !== req.user.orgId) {
        res.status(404).json({ error: 'run draft not found', code: 'draft-not-found' });
        return;
      }
      if (contentOf(existing) === contentOf(content)) {
        res.status(200).json({ draft: viewOf(existing, now), replayed: true });
        return;
      }
      if (existing.status !== 'draft') {
        res.status(409).json({ error: 'draft_not_editable', message: `This draft is ${existing.status}; start a new draft to change it.`, draft: viewOf(existing, now) });
        return;
      }
      if (content.expectedRevision !== existing.revision) {
        res.status(409).json({ error: 'draft_conflict', message: 'The draft changed since you loaded it (or expectedRevision is missing). Re-read and retry.', draft: viewOf(existing, now) });
        return;
      }
      const next: RunDraft = withContent({ ...existing, revision: existing.revision + 1, updatedAt: now, updatedBy: userId }, content);
      delete next.dispatchError;
      try {
        await deps.drafts.replaceIf(next, { revision: existing.revision, fromStatuses: ['draft'] });
      } catch (err) {
        if (isDraftConflict(err)) continue;
        throw err;
      }
      signal(next);
      res.status(200).json({ draft: next, replayed: false });
      return;
    }
    res.status(409).json({ error: 'draft_busy', message: 'The draft kept changing under this write; retry.' });
  }));

  router.post('/:draftId/cancel', requireAuth, handle(async (req, res) => {
    const documentId = String(req.params.documentId ?? '');
    const draftId = draftIdOf(req);
    await requireDocumentAccess(deps, req, documentId, 'edit');
    for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
      const draft = await deps.drafts.get(documentId, draftId);
      if (!draft) { res.status(404).json({ error: 'run draft not found', code: 'draft-not-found' }); return; }
      if (draft.status === 'cancelled') { res.status(200).json({ draft }); return; }
      if (draft.status !== 'draft') { res.status(409).json({ error: 'draft_not_cancellable', message: `This draft is ${draft.status}.`, draft: viewOf(draft, deps.now()) }); return; }
      const now = deps.now();
      const next: RunDraft = { ...draft, status: 'cancelled', revision: draft.revision + 1, updatedAt: now, updatedBy: req.user!.sub };
      try {
        await deps.drafts.replaceIf(next, { revision: draft.revision, fromStatuses: ['draft'] });
      } catch (err) {
        if (isDraftConflict(err)) continue;
        throw err;
      }
      signal(next);
      res.status(200).json({ draft: next });
      return;
    }
    res.status(409).json({ error: 'draft_busy' });
  }));

  router.post('/:draftId/dispatch', requireAuth, handle(async (req, res) => {
    const documentId = String(req.params.documentId ?? '');
    const draftId = draftIdOf(req);
    await requireDocumentAccess(deps, req, documentId, 'edit');
    const userId = req.user!.sub;

    let draft = await deps.drafts.get(documentId, draftId);
    for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
      if (!draft) { res.status(404).json({ error: 'run draft not found', code: 'draft-not-found' }); return; }
      if (draft.organizationId && req.user?.orgId && draft.organizationId !== req.user.orgId) {
        res.status(404).json({ error: 'run draft not found', code: 'draft-not-found' });
        return;
      }
      if (draft.status === 'dispatched') {
        res.status(200).json({ draft: viewOf(draft, deps.now()), runId: draft.runId, replayed: true });
        return;
      }
      if (draft.status === 'cancelled') {
        res.status(409).json({ error: 'draft_cancelled', message: 'This draft was cancelled.', draft });
        return;
      }

      let tookStep1 = false;
      if (draft.status === 'draft') {
        // Step 1: claim the dispatch, with the run's id chosen up front.
        const now = deps.now();
        const owner = deps.localNodeId?.();
        const next: RunDraft = {
          ...draft, status: 'dispatching', dispatchedBy: userId, dispatchStartedAt: now,
          pendingRunId: (deps.newRunId ?? randomUUID)(),
          ...(owner ? { dispatchOwnerNodeId: owner } : {}),
          revision: draft.revision + 1, updatedAt: now, updatedBy: userId,
        };
        if (!owner) delete next.dispatchOwnerNodeId;
        delete next.dispatchError;
        try {
          await deps.drafts.replaceIf(next, { revision: draft.revision, fromStatuses: ['draft'] });
        } catch (err) {
          if (!isDraftConflict(err)) throw err;
          draft = await deps.drafts.get(documentId, draftId);
          continue;
        }
        draft = next;
        tookStep1 = true;
        signal(draft);
      }

      // Step 2: start the run under the fixed claim, as the person who took step 1.
      const dispatching: RunDraft = draft;
      const runAs = dispatching.dispatchedBy ?? userId;
      const body = triggerBodyOf(dispatching);
      const claimKey = { scope: DISPATCH_IDEMPOTENCY_SCOPE, userId: runAs, key: dispatchIdempotencyKey(draftId) };
      const runId0 = dispatching.pendingRunId;
      let outcome: IdempotentOutcome<TriggerRunResponse>;
      try {
        outcome = await deps.runIdempotent<TriggerRunResponse>(
          { ...claimKey, body },
          async () => {
            // Not the request that chose the id: the run may already exist
            // (its dispatcher died after starting it). Adopt, never re-start.
            const existing = !tookStep1 && runId0 ? await findRun(runId0) : null;
            if (existing) return { status: 202, body: adoptedResponse(dispatching, runAs, existing.runId) };
            return {
              status: 202,
              body: await deps.trigger({
                pipelineId: dispatching.pipelineId,
                userId: runAs,
                ...(dispatching.organizationId ? { organizationId: dispatching.organizationId } : {}),
                body: triggerBodyOf(dispatching),
                ...(runId0 ? { runId: runId0 } : {}),
              }),
            };
          },
        );
      } catch (err) {
        if ((err as { name?: string })?.name === 'IdempotencyStoreUnavailableError') {
          res.status(503).json({ error: 'idempotency-store-unavailable', message: 'The dispatch could not be made idempotent; nothing was started. Retry.' });
          return;
        }
        await returnToDraft(dispatching, err);
        const status = err instanceof AppError ? err.status : (err as { name?: string })?.name === 'PipelineTriggerBridgeError' ? 502 : 500;
        res.status(status).json({ error: 'trigger_failed', message: err instanceof Error ? err.message : String(err), draftId });
        return;
      }
      let adopted = false;
      if (outcome.outcome === 'in_progress' && runId0) {
        // (a) The run already exists: adopt it.
        const existing = await findRun(runId0);
        if (existing) {
          outcome = { outcome: 'replayed', status: 202, body: adoptedResponse(dispatching, runAs, existing.runId) };
          adopted = true;
        } else if (!(await ownerLive(dispatching))) {
          // (b) The owner's process is gone: take the dispatch over now. The CAS
          // makes this replica the owner — exactly one concurrent taker wins;
          // a loser re-reads and sees a live owner.
          const now = deps.now();
          const owner = deps.localNodeId?.();
          const taken: RunDraft = {
            ...dispatching, dispatchStartedAt: now, revision: dispatching.revision + 1, updatedAt: now, updatedBy: userId,
            ...(owner ? { dispatchOwnerNodeId: owner } : {}),
          };
          if (!owner) delete taken.dispatchOwnerNodeId;
          try {
            await deps.drafts.replaceIf(taken, { revision: dispatching.revision, fromStatuses: ['dispatching'] });
          } catch (err) {
            if (!isDraftConflict(err)) throw err;
            draft = await deps.drafts.get(documentId, draftId);
            continue;
          }
          log.warn({ draftId, runId: runId0, deadOwner: dispatching.dispatchOwnerNodeId, owner }, 'run draft: dispatch owner is gone; taking the dispatch over');
          await deps.releasePendingClaim?.(claimKey).catch((err: unknown) => {
            log.warn({ draftId, err: (err as Error).message }, 'run draft: could not release the dead owner\'s claim');
          });
          draft = taken;
          continue; // step 2 again, as the owner, under the same run id
        }
      }
      if (outcome.outcome === 'in_progress') {
        res.setHeader('Retry-After', '2');
        res.status(409).json({
          error: 'dispatch_in_progress',
          message: 'This draft is being dispatched. If the dispatcher stopped, a retry can take over once its claim lapses.',
          retryAfterSeconds: 2,
          claimLeaseSeconds: 120,
          draft: viewOf(dispatching, deps.now()),
        });
        return;
      }
      if (outcome.outcome === 'key_conflict') {
        res.status(409).json({ error: 'dispatch_key_conflict', message: 'The run key was used for a different request.', draftId });
        return;
      }
      if (outcome.status < 200 || outcome.status >= 300 || !outcome.body?.runId) {
        res.status(502).json({ error: 'trigger_failed', message: 'the trigger answered without a run id', draftId });
        return;
      }

      // Step 3: record the run.
      const runId = outcome.body.runId;
      const now = deps.now();
      const done: RunDraft = { ...dispatching, status: 'dispatched', runId, dispatchedAt: now, revision: dispatching.revision + 1, updatedAt: now };
      try {
        await deps.drafts.replaceIf(done, { revision: dispatching.revision, fromStatuses: ['dispatching'] });
      } catch (err) {
        if (!isDraftConflict(err)) throw err;
        draft = await deps.drafts.get(documentId, draftId);
        continue; // another replica finished it (→ dispatched, answered above) or it moved; re-evaluate
      }
      signal(done);
      res.status(200).json({ draft: done, runId, replayed: outcome.outcome === 'replayed', ...(adopted ? { adopted: true } : {}) });
      return;
    }
    res.status(409).json({ error: 'draft_busy', message: 'The draft kept changing under this dispatch; retry.' });

    async function findRun(runId: string): Promise<{ runId: string } | null> {
      if (!deps.findRun) return null;
      try {
        return await deps.findRun(runId);
      } catch (err) {
        // Unknown → do not adopt, and do not start a second run on this basis either:
        // the caller falls through to the claim, which still serialises triggers.
        log.warn({ draftId, runId, err: (err as Error).message }, 'run draft: run lookup failed');
        return null;
      }
    }

    async function ownerLive(d: RunDraft): Promise<boolean> {
      if (!deps.isOwnerLive || !d.dispatchOwnerNodeId) return true;
      const since = Date.parse(d.dispatchStartedAt ?? '');
      try {
        return await deps.isOwnerLive(d.dispatchOwnerNodeId, Number.isFinite(since) ? since : Number.NaN);
      } catch {
        return true;
      }
    }

    async function returnToDraft(d: RunDraft, err: unknown): Promise<void> {
      const now = deps.now();
      const back: RunDraft = { ...d, status: 'draft', revision: d.revision + 1, updatedAt: now, dispatchError: (err instanceof Error ? err.message : String(err)).slice(0, 500) };
      delete back.dispatchedBy;
      delete back.dispatchStartedAt;
      delete back.pendingRunId;
      delete back.dispatchOwnerNodeId;
      try {
        await deps.drafts.replaceIf(back, { revision: d.revision, fromStatuses: ['dispatching'] });
        signal(back);
      } catch (casErr) {
        if (!isDraftConflict(casErr)) log.warn({ draftId: d.draftId, err: (casErr as Error).message }, 'run draft: could not return a refused dispatch to draft');
      }
    }
  }));

  return router;
}

export function defaultRunDraftsDeps(): RunDraftsDeps {
  /* eslint-disable @typescript-eslint/no-require-imports */
  const { runDraftRepo } = require('../repositories/RunDraftRepository') as typeof import('../repositories/RunDraftRepository');
  const repos = require('../repositories') as typeof import('../repositories');
  const { getPresentationDocumentStore } = require('../deck/presentationDocuments') as typeof import('../deck/presentationDocuments');
  const idem = require('../middleware/idempotency') as typeof import('../middleware/idempotency');
  const catalog = require('../lib/event-catalog') as typeof import('../lib/event-catalog');
  const logger = require('../lib/logger') as typeof import('../lib/logger');
  const locator = require('../work-graph/sources/runRowLocator') as typeof import('../work-graph/sources/runRowLocator');
  const liveness = require('../lib/replicaLiveness') as typeof import('../lib/replicaLiveness');
  const awsClients = require('../lib/aws-clients') as typeof import('../lib/aws-clients');
  const { tableName: ddbTable } = require('../lib/ddb-table-name') as typeof import('../lib/ddb-table-name');
  /* eslint-enable @typescript-eslint/no-require-imports */
  return {
    getDocumentRow: (id) => getPresentationDocumentStore().get(id),
    getLifecycle: async (id) => {
      const row = await repos.documentLifecycleRepo.get(id);
      return row ? { status: row.status, ownerId: row.ownerId } : null;
    },
    drafts: runDraftRepo,
    trigger: async ({ pipelineId, userId, organizationId, body, runId }) => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const { triggerPipelineRun } = require('./pipelineTriggers') as typeof import('./pipelineTriggers');
      return triggerPipelineRun({
        pipelineId, userId, body, ...(organizationId ? { organizationId } : {}), ...(runId ? { runId } : {}),
        reqLog: logger.withContext({ route: 'runDrafts', pipelineId, userId }),
      });
    },
    runIdempotent: (opts, handler) => idem.runIdempotent(opts, handler),
    publishSignal: (c, e, p) => catalog.publishSignal(c, e, p),
    now: () => new Date().toISOString(),
    // The run by id through pipeline-runs' runId-index (1291b5a): one indexed read, fresh.
    findRun: async (runId) => {
      const rows = await locator.runRowLocator.rows(awsClients.docClient, ddbTable('pipeline-runs'), [runId]);
      const row = rows.get(runId) as { runId?: string; status?: string } | null | undefined;
      return row ? { runId, ...(typeof row.status === 'string' ? { status: row.status } : {}) } : null;
    },
    // The owner's liveness lease (a07a3f2). No store yet (before bootstrap) → alive: wait out the claim.
    isOwnerLive: async (ownerNodeId, sinceMs) => {
      const store = liveness.getReplicaLeaseStore();
      return store ? liveness.isOwnerLive(store, ownerNodeId, sinceMs) : true;
    },
    localNodeId: () => liveness.getLocalReplica()?.nodeId,
    releasePendingClaim: (opts) => idem.releasePendingClaim(opts),
  };
}

let router: Router | null = null;
/** Mounted at /documents/:documentId/run-drafts. */
export const runDraftsRouter = Router({ mergeParams: true });
runDraftsRouter.use((req: Request, res: Response, next) => { router ??= createRunDraftsRouter(); router(req, res, next); });
