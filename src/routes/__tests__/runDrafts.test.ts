// Run drafts (PLAN §2.2) under two platform replicas: two routers, each with
// its own RunDraftRepository over one fake `document-run-drafts` table, and
// one shared Idempotency-Key claim store (the real `runIdempotent`, the same
// claim a `POST /api/pipelines/:id/runs` takes). The trigger is a fake that
// counts runs.

jest.mock('../../lib/aws-clients', () => ({ docClient: {}, ddbClient: {} }));
jest.mock('../../middleware/auth', () => ({
  requireAuth: (...args: unknown[]) => (jest.requireActual('./helpers/workFakes') as typeof import('./helpers/workFakes')).testAuth(...(args as Parameters<typeof import('./helpers/workFakes').testAuth>)),
}));

import { createRunDraftsRouter, dispatchIdempotencyKey, type RunDraftsDeps } from '../runDrafts';
import { RunDraftRepository, type RunDraft } from '../../repositories/RunDraftRepository';
import { runIdempotent, PENDING_TTL_SECONDS, type IdempotencyStore } from '../../middleware/idempotency';
import { MemoryClaimStore } from '../../deck/deckReviseRequests';
import { ValidationError } from '../../middleware/error-handler';
import { call, DOCS, FakeTable, serve, type Replica } from './helpers/workFakes';
import type { TriggerRunRequest } from '../pipelineTriggers';

type TriggerCall = { pipelineId: string; userId: string; body: TriggerRunRequest };

function claimStore(): IdempotencyStore {
  const claims = new MemoryClaimStore();
  return {
    get: (k) => claims.get(k),
    claim: (k, ttl) => claims.create(k, 'pending', ttl),
    commit: (k, v, ttl) => claims.commit(k, v, ttl),
    release: (k) => claims.release(k),
  };
}

interface World {
  table: FakeTable;
  store: IdempotencyStore;
  triggers: TriggerCall[];
  trigger: (input: TriggerCall) => Promise<{ runId: string; pipelineId: string; triggeredBy: { userId: string; triggerType: string }; at: string }>;
  signals: Array<Record<string, unknown>>;
  now: string;
}

function depsFor(world: World, opts: { crashOnDispatched?: boolean } = {}): RunDraftsDeps {
  const repo = new RunDraftRepository(world.table as any, 'document-run-drafts', async () => undefined);
  const drafts: RunDraftsDeps['drafts'] = {
    get: (d, id) => repo.get(d, id),
    listForDocument: (d) => repo.listForDocument(d),
    create: (d) => repo.create(d),
    replaceIf: async (next, seen) => {
      // A replica that dies after the trigger and before step 3 (the crash under test).
      if (opts.crashOnDispatched && next.status === 'dispatched') throw new Error('replica A died');
      return repo.replaceIf(next, seen);
    },
  };
  return {
    getDocumentRow: async (id) => DOCS[id] ?? null,
    getLifecycle: async () => null,
    drafts,
    trigger: (input) => world.trigger(input as TriggerCall),
    runIdempotent: (o, h) => runIdempotent({ ...o, store: world.store }, h),
    publishSignal: async (_c, _e, p) => { world.signals.push(p); },
    now: () => world.now,
  };
}

const DRAFT = { pipelineId: 'deck-revise', instruction: 'Tighten slide 3', model: 'sonnet', contextBudgetTokens: 50_000 };

describe('run drafts', () => {
  let world: World;
  let a: Replica;
  let b: Replica;

  beforeEach(async () => {
    world = {
      table: new FakeTable('documentId', 'draftId'),
      store: claimStore(),
      triggers: [],
      trigger: async (input) => {
        world.triggers.push(input);
        return { runId: `run-${world.triggers.length}`, pipelineId: input.pipelineId, triggeredBy: { userId: input.userId, triggerType: 'manual' }, at: world.now };
      },
      signals: [],
      now: '2026-09-23T12:00:00.000Z',
    };
    a = await serve([['/api/documents/:documentId/run-drafts', createRunDraftsRouter(depsFor(world))]]);
    b = await serve([['/api/documents/:documentId/run-drafts', createRunDraftsRouter(depsFor(world))]]);
  });
  afterEach(async () => {
    jest.restoreAllMocks();
    await a.close();
    await b.close();
  });

  const put = (r: Replica, user: string, body: unknown, id = 'req-1') => call(r.url, 'PUT', `/api/documents/doc-1/run-drafts/${id}`, user, body);
  const dispatch = (r: Replica, user: string, id = 'req-1') => call(r.url, 'POST', `/api/documents/doc-1/run-drafts/${id}/dispatch`, user);

  it('PUT is an idempotent upsert keyed by the requestId; a changed body needs expectedRevision', async () => {
    const first = await put(a, 'dev-connor', DRAFT);
    expect(first.status).toBe(201);
    expect(first.body.draft).toMatchObject({ draftId: 'req-1', status: 'draft', revision: 1, createdBy: 'dev-connor', organizationId: 'dev-org' });
    const again = await put(b, 'dev-connor', DRAFT);
    expect(again.status).toBe(200);
    expect(again.body).toMatchObject({ replayed: true, draft: { revision: 1 } });

    const changed = await put(b, 'dev-erin', { ...DRAFT, instruction: 'Tighten slide 4' });
    expect(changed.status).toBe(409);
    expect(changed.body.error).toBe('draft_conflict');
    const edited = await put(b, 'dev-erin', { ...DRAFT, instruction: 'Tighten slide 4', expectedRevision: 1 });
    expect(edited.status).toBe(200);
    expect(edited.body.draft).toMatchObject({ revision: 2, instruction: 'Tighten slide 4', updatedBy: 'dev-erin' });
  });

  it('dispatch ×2 on two replicas starts one run and answers one runId', async () => {
    await put(a, 'dev-connor', DRAFT);
    const one = await dispatch(a, 'dev-connor');
    expect(one.status).toBe(200);
    expect(one.body).toMatchObject({ runId: 'run-1', replayed: false, draft: { status: 'dispatched', runId: 'run-1', dispatchedBy: 'dev-connor' } });
    const two = await dispatch(b, 'dev-connor');
    expect(two.status).toBe(200);
    expect(two.body).toMatchObject({ runId: 'run-1', replayed: true });
    expect(world.triggers).toHaveLength(1);
    expect(world.triggers[0]!.body.triggerPayload).toMatchObject({
      documentId: 'doc-1', instruction: 'Tighten slide 3', requestedBy: 'dev-connor', requestId: 'req-1', model: 'sonnet', contextBudgetTokens: 50_000,
    });
    expect(world.signals.map((s) => s.status)).toEqual(['draft', 'dispatching', 'dispatched']);
    for (const s of world.signals) expect(JSON.stringify(s)).not.toContain('Tighten');

    // Concurrent double-click on both replicas, second draft.
    await put(a, 'dev-connor', DRAFT, 'req-2');
    const both = await Promise.all([dispatch(a, 'dev-connor', 'req-2'), dispatch(b, 'dev-connor', 'req-2')]);
    const ok = both.filter((r) => r.status === 200);
    expect(ok.length).toBeGreaterThanOrEqual(1);
    for (const r of ok) expect(r.body.runId).toBe('run-2');
    for (const r of both.filter((x) => x.status !== 200)) expect(r.body.error).toBe('dispatch_in_progress');
    expect(world.triggers).toHaveLength(2);
  });

  it('replica A dies after the trigger and before recording it; the retry on B yields the same single run', async () => {
    const crashing = await serve([['/api/documents/:documentId/run-drafts', createRunDraftsRouter(depsFor(world, { crashOnDispatched: true }))]]);
    try {
      await put(crashing, 'dev-connor', DRAFT);
      const died = await dispatch(crashing, 'dev-connor');
      expect(died.status).toBe(500);
      const stuck = (await call(b.url, 'GET', '/api/documents/doc-1/run-drafts/req-1', 'dev-connor')).body.draft as RunDraft;
      expect(stuck.status).toBe('dispatching');

      // An editor who is not the first dispatcher retries on B: the run is still started as the dispatcher.
      const retry = await dispatch(b, 'dev-erin');
      expect(retry.status).toBe(200);
      expect(retry.body).toMatchObject({ runId: 'run-1', replayed: true, draft: { status: 'dispatched', dispatchedBy: 'dev-connor' } });
      expect(world.triggers).toHaveLength(1);
      expect(world.triggers[0]!.userId).toBe('dev-connor');
    } finally {
      await crashing.close();
    }
  });

  it('a dispatcher that dies holding the claim blocks retries until the 120 s lease lapses', async () => {
    let t = Date.parse(world.now);
    jest.spyOn(Date, 'now').mockImplementation(() => t);
    const hang = new Promise<never>(() => undefined);
    const realTrigger = world.trigger;
    world.trigger = async (input) => { world.triggers.push(input); return hang; };
    await put(a, 'dev-connor', DRAFT);
    const abort = new AbortController();
    const dying = fetch(`${a.url}/api/documents/doc-1/run-drafts/req-1/dispatch`, { method: 'POST', headers: { 'x-test-user': 'dev-connor' }, signal: abort.signal }).catch(() => null);
    while (world.triggers.length === 0) await new Promise((r) => setImmediate(r));
    world.trigger = realTrigger;

    const blocked = await dispatch(b, 'dev-connor');
    expect(blocked.status).toBe(409);
    expect(blocked.body).toMatchObject({ error: 'dispatch_in_progress', retryAfterSeconds: 2, claimLeaseSeconds: 120 });
    expect(blocked.headers.get('retry-after')).toBe('2');

    t += (PENDING_TTL_SECONDS - 1) * 1000;
    expect((await dispatch(b, 'dev-connor')).status).toBe(409);

    t += 2000; // past the lease
    const taken = await dispatch(b, 'dev-connor');
    expect(taken.status).toBe(200);
    expect(taken.body.draft.status).toBe('dispatched');
    // The dead holder's trigger had begun: the take-over starts a second run. This is the
    // residual window (process death inside createResource) the PLAN's Retry copy must name.
    expect(world.triggers).toHaveLength(2);
    abort.abort();
    await dying;
  });

  it('a stuck dispatch reads as stale after 60 s; nothing is written by the read', async () => {
    const crashing = await serve([['/api/documents/:documentId/run-drafts', createRunDraftsRouter(depsFor(world, { crashOnDispatched: true }))]]);
    try {
      await put(crashing, 'dev-connor', DRAFT);
      await dispatch(crashing, 'dev-connor');
      const puts = world.table.puts;
      world.now = '2026-09-23T12:00:30.000Z';
      expect((await call(b.url, 'GET', '/api/documents/doc-1/run-drafts', 'dev-vic')).body.drafts[0].dispatchStale).toBeUndefined();
      world.now = '2026-09-23T12:01:01.000Z';
      expect((await call(b.url, 'GET', '/api/documents/doc-1/run-drafts', 'dev-vic')).body.drafts[0].dispatchStale).toBe(true);
      expect(world.table.puts).toBe(puts);
    } finally {
      await crashing.close();
    }
  });

  it('a refused trigger returns the draft to draft with the reason; the claim is released for the retry', async () => {
    await put(a, 'dev-connor', DRAFT);
    const realTrigger = world.trigger;
    world.trigger = async () => { throw new ValidationError('definition.id does not match'); };
    const refused = await dispatch(a, 'dev-connor');
    expect(refused.status).toBe(400);
    const back = (await call(a.url, 'GET', '/api/documents/doc-1/run-drafts/req-1', 'dev-connor')).body.draft;
    expect(back).toMatchObject({ status: 'draft', dispatchError: 'definition.id does not match' });
    expect(back.dispatchedBy).toBeUndefined();
    expect(await world.store.get(`idem:pipeline-trigger:dev-connor:${dispatchIdempotencyKey('req-1')}`)).toBeNull();
    world.trigger = realTrigger;
    const ok = await dispatch(b, 'dev-connor');
    expect(ok.status).toBe(200);
    expect(ok.body.runId).toBe('run-1');
  });

  it('after dispatch the draft is frozen: a changed PUT is refused, the same PUT replays', async () => {
    await put(a, 'dev-connor', DRAFT);
    await dispatch(a, 'dev-connor');
    expect((await put(b, 'dev-connor', { ...DRAFT, instruction: 'other', expectedRevision: 3 })).body.error).toBe('draft_not_editable');
    expect((await put(b, 'dev-connor', DRAFT)).status).toBe(200);
  });

  it('only the owner or an editor may write or dispatch; viewers may read', async () => {
    expect((await put(a, 'dev-vic', DRAFT)).status).toBe(403);
    expect((await put(a, 'dev-sam', DRAFT)).status).toBe(403);
    await put(a, 'dev-erin', DRAFT);
    const viewerDispatch = await dispatch(a, 'dev-vic');
    expect(viewerDispatch.status).toBe(403);
    expect(viewerDispatch.body.role).toBe('viewer');
    expect((await call(a.url, 'GET', '/api/documents/doc-1/run-drafts', 'dev-vic')).status).toBe(200);
    expect((await call(a.url, 'POST', '/api/documents/doc-1/run-drafts/req-1/cancel', 'dev-vic')).status).toBe(403);
    expect(world.triggers).toHaveLength(0);
    expect((await call(a.url, 'PUT', '/api/documents/missing/run-drafts/req-1', 'dev-connor', DRAFT)).status).toBe(404);
  });

  it('validates the draft', async () => {
    expect((await put(a, 'dev-connor', { instruction: 'x' })).status).toBe(400);
    expect((await put(a, 'dev-connor', { ...DRAFT, model: 'gpt' })).status).toBe(400);
    expect((await put(a, 'dev-connor', DRAFT, 'bad id!')).status).toBe(400);
  });
});
