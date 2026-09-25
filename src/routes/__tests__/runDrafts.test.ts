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
import { runIdempotent, releasePendingClaim, PENDING_TTL_SECONDS, type IdempotencyStore } from '../../middleware/idempotency';
import { MemoryClaimStore } from '../../deck/deckReviseRequests';
import { ValidationError } from '../../middleware/error-handler';
import { call, DOCS, FakeTable, serve, type Replica } from './helpers/workFakes';
import type { TriggerRunRequest } from '../pipelineTriggers';

type TriggerCall = { pipelineId: string; userId: string; body: TriggerRunRequest; runId?: string };

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
  /** pipeline-runs as the runId-index sees it. */
  runs: Set<string>;
  /** Replicas whose liveness lease is held. */
  liveNodes: Set<string>;
  ids: number;
}

function depsFor(world: World, opts: { crashOnDispatched?: boolean; node?: string } = {}): RunDraftsDeps {
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
    findRun: async (runId) => (world.runs.has(runId) ? { runId, status: 'running' } : null),
    isOwnerLive: async (node) => !!node && world.liveNodes.has(node),
    localNodeId: () => opts.node,
    releasePendingClaim: (o) => releasePendingClaim({ ...o, store: world.store }),
    newRunId: () => `run-${++world.ids}`,
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
      // The engine starts the run under the caller's id (distributed-core >= 0.46.2) and it becomes findable.
      trigger: async (input) => {
        world.triggers.push(input);
        const runId = input.runId ?? `anon-${world.triggers.length}`;
        world.runs.add(runId);
        return { runId, pipelineId: input.pipelineId, triggeredBy: { userId: input.userId, triggerType: 'manual' }, at: world.now };
      },
      signals: [],
      now: '2026-09-23T12:00:00.000Z',
      runs: new Set(),
      liveNodes: new Set(['node-a', 'node-b']),
      ids: 0,
    };
    a = await serve([['/api/documents/:documentId/run-drafts', createRunDraftsRouter(depsFor(world, { node: 'node-a' }))]]);
    b = await serve([['/api/documents/:documentId/run-drafts', createRunDraftsRouter(depsFor(world, { node: 'node-b' }))]]);
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
    for (const r of ok) expect(r.body.runId).toBe(ok[0]!.body.runId);
    expect(world.triggers[1]!.runId).toBe(ok[0]!.body.runId);
    for (const r of both.filter((x) => x.status !== 200)) expect(r.body.error).toBe('dispatch_in_progress');
    expect(world.triggers).toHaveLength(2);
  });

  it('replica A dies after the trigger and before recording it; the retry on B yields the same single run', async () => {
    const crashing = await serve([['/api/documents/:documentId/run-drafts', createRunDraftsRouter(depsFor(world, { crashOnDispatched: true, node: 'node-a' }))]]);
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

  // A dispatcher dying INSIDE step 2: A's request is left stuck in the trigger with the
  // claim `pending`, exactly as a process that died there would leave it. `startsRun`
  // says whether the run had already been created (the crash-between-trigger-and-commit case).
  async function dieInsideTrigger(startsRun: boolean): Promise<{ abort: () => Promise<void>; draft: RunDraft }> {
    const realTrigger = world.trigger;
    world.trigger = async (input) => {
      world.triggers.push(input);
      if (startsRun) world.runs.add(input.runId!);
      return new Promise<never>(() => undefined);
    };
    await put(a, 'dev-connor', DRAFT);
    const ctl = new AbortController();
    const dying = fetch(`${a.url}/api/documents/doc-1/run-drafts/req-1/dispatch`, { method: 'POST', headers: { 'x-test-user': 'dev-connor' }, signal: ctl.signal }).catch(() => null);
    while (world.triggers.length === 0) await new Promise((r) => setImmediate(r));
    world.trigger = realTrigger;
    const draft = (await call(b.url, 'GET', '/api/documents/doc-1/run-drafts/req-1', 'dev-connor')).body.draft as RunDraft;
    return { draft, abort: async () => { ctl.abort(); await dying; } };
  }

  it('the run id is chosen before the trigger, recorded on the dispatching draft, and the run is started under it', async () => {
    const { draft, abort } = await dieInsideTrigger(false);
    expect(draft).toMatchObject({ status: 'dispatching', pendingRunId: 'run-1', dispatchOwnerNodeId: 'node-a' });
    expect(world.triggers[0]!.runId).toBe('run-1');
    await abort();
  });

  it('crash between starting the run and committing the claim: a retry adopts the run at once — one run, no 120 s wait', async () => {
    const { abort } = await dieInsideTrigger(true);
    world.liveNodes.delete('node-a');
    const retry = await dispatch(b, 'dev-erin');
    expect(retry.status).toBe(200);
    expect(retry.body).toMatchObject({ runId: 'run-1', adopted: true, replayed: true, draft: { status: 'dispatched', runId: 'run-1', dispatchedBy: 'dev-connor' } });
    expect(world.triggers).toHaveLength(1);
    expect((await dispatch(b, 'dev-connor')).body).toMatchObject({ runId: 'run-1', replayed: true });
    await abort();
  });

  it('the same crash with the owner still alive also adopts, since the run exists', async () => {
    const { abort } = await dieInsideTrigger(true);
    const retry = await dispatch(b, 'dev-connor');
    expect(retry.body).toMatchObject({ runId: 'run-1', adopted: true });
    expect(world.triggers).toHaveLength(1);
    await abort();
  });

  it('dead owner, no run yet: a retry takes over at once and starts the run under the SAME id; a live owner still blocks', async () => {
    let t = Date.parse(world.now);
    jest.spyOn(Date, 'now').mockImplementation(() => t);
    const { abort } = await dieInsideTrigger(false);

    const blocked = await dispatch(b, 'dev-connor');
    expect(blocked.status).toBe(409);
    expect(blocked.body).toMatchObject({ error: 'dispatch_in_progress', retryAfterSeconds: 2, claimLeaseSeconds: 120 });
    expect(blocked.headers.get('retry-after')).toBe('2');

    // A's liveness lease lapses (its process died): the next retry takes over at t+1 s, not t+120 s.
    world.liveNodes.delete('node-a');
    t += 1000;
    const both = await Promise.all([dispatch(b, 'dev-erin'), dispatch(b, 'dev-connor')]);
    const won = both.filter((r) => r.status === 200);
    expect(won.length).toBeGreaterThanOrEqual(1);
    for (const r of won) expect(r.body).toMatchObject({ runId: 'run-1', draft: { status: 'dispatched', runId: 'run-1', dispatchOwnerNodeId: 'node-b' } });
    for (const r of both.filter((x) => x.status !== 200)) expect(r.body.error).toBe('dispatch_in_progress');
    // Two trigger calls, ONE run id: the dead holder's (which never created the run) and the takeover's.
    expect(world.triggers.map((c) => c.runId)).toEqual(['run-1', 'run-1']);
    expect(world.triggers[1]!.userId).toBe('dev-connor');
    await abort();
  });

  it('with the owner reading as alive, the 120 s claim lease still bounds the wait, and the takeover adopts a run that surfaced', async () => {
    let t = Date.parse(world.now);
    jest.spyOn(Date, 'now').mockImplementation(() => t);
    const { abort } = await dieInsideTrigger(false);
    t += (PENDING_TTL_SECONDS - 1) * 1000;
    expect((await dispatch(b, 'dev-connor')).status).toBe(409);
    world.runs.add('run-1');
    t += 2000;
    const taken = await dispatch(b, 'dev-connor');
    expect(taken.status).toBe(200);
    expect(taken.body).toMatchObject({ runId: 'run-1', draft: { status: 'dispatched' } });
    expect(world.triggers).toHaveLength(1);
    await abort();
  });

  it('a stuck dispatch reads as stale after 60 s; nothing is written by the read', async () => {
    const crashing = await serve([['/api/documents/:documentId/run-drafts', createRunDraftsRouter(depsFor(world, { crashOnDispatched: true, node: 'node-a' }))]]);
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
    expect(ok.body.runId).toBe('run-2'); // the refused attempt's id (run-1) started nothing; a new dispatch gets a new id
  });

  it('dispatch passes the model and budget to the trigger; an unpriced model is refused with 400 and the draft does not move', async () => {
    await put(a, 'dev-connor', { ...DRAFT, model: 'haiku', contextBudgetTokens: 50_000 });
    const ok = await dispatch(a, 'dev-connor');
    expect(ok.status).toBe(200);
    expect(world.triggers[0].body.triggerPayload).toMatchObject({ model: 'haiku', contextBudgetTokens: 50_000 });

    await put(a, 'dev-connor', DRAFT, 'req-2');
    // A draft written before the model list existed, or by another writer.
    const key = ['doc-1', 'req-2'].join('\u0000');
    world.table.rows.set(key, { ...world.table.rows.get(key)!, model: 'gpt-9' });
    const refused = await dispatch(a, 'dev-connor', 'req-2');
    expect(refused.status).toBe(400);
    expect(refused.body).toMatchObject({ error: 'invalid_run_overrides', draftId: 'req-2' });
    expect(refused.body.message).toMatch(/gpt-9.*not a priced model/);
    expect(world.triggers).toHaveLength(1);
    const back = (await call(a.url, 'GET', '/api/documents/doc-1/run-drafts/req-2', 'dev-connor')).body.draft;
    expect(back).toMatchObject({ status: 'draft', revision: 1 });
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

  it('refuses sources the caller cannot read, naming every one, and prepares nothing (WS-K)', async () => {
    const refused = await put(a, 'dev-erin', { ...DRAFT, sources: [{ kind: 'document', id: 'doc-1' }, { kind: 'document', id: 'doc-2' }, { kind: 'document', id: 'missing' }] });
    expect(refused.status).toBe(404);
    expect(refused.body).toMatchObject({ code: 'source-unreadable', sourceIds: ['doc-2', 'missing'] });
    expect(refused.body.error).toContain('doc-2, missing');
    expect((await call(a.url, 'GET', '/api/documents/doc-1/run-drafts/req-1', 'dev-connor')).status).toBe(404);
    const ok = await put(a, 'dev-connor', { ...DRAFT, sources: [{ kind: 'document', id: 'doc-1' }, { kind: 'document', id: 'doc-2' }] });
    expect(ok.status).toBe(201);
  });

  it('validates the draft', async () => {
    expect((await put(a, 'dev-connor', { instruction: 'x' })).status).toBe(400);
    expect((await put(a, 'dev-connor', { ...DRAFT, model: 'gpt' })).status).toBe(400);
    expect((await put(a, 'dev-connor', DRAFT, 'bad id!')).status).toBe(400);
  });
});
