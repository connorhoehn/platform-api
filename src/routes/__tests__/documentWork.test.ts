// Work fields on a document (PLAN §2.1): two platform replicas — two routers,
// each over its own DocumentWorkRepository — share one fake `document-work`
// table. Different fields edited concurrently both land; the same field gives
// 409; a replayed criteria op is a no-op; a GET backfills from lifecycle and
// writes nothing; only the owner or an editor may PATCH; signals carry ids
// and enums only.

jest.mock('../../lib/aws-clients', () => ({ docClient: {}, ddbClient: {} }));
jest.mock('../../middleware/auth', () => ({
  requireAuth: (...args: unknown[]) => (jest.requireActual('./helpers/workFakes') as typeof import('./helpers/workFakes')).testAuth(...(args as Parameters<typeof import('./helpers/workFakes').testAuth>)),
}));

import { createDocumentWorkRouters, type DocumentWorkDeps } from '../documentWork';
import { DocumentWorkRepository } from '../../repositories/DocumentWorkRepository';
import { call, DOCS, FakeTable, serve, type Replica } from './helpers/workFakes';
import type { RunDraft } from '../../repositories/RunDraftRepository';

const LIFECYCLE: Record<string, { status: string; ownerId: string }> = {
  'doc-1': { status: 'in_review', ownerId: 'dev-connor' },
  'doc-2': { status: 'approved', ownerId: 'dev-connor' },
};

function depsFor(table: FakeTable, signals: Array<[string, string, Record<string, unknown>]>, durable: Array<[string, Record<string, unknown>]>, drafts: RunDraft[] = []): DocumentWorkDeps {
  const work = new DocumentWorkRepository(table as any, 'document-work', async () => undefined);
  return {
    getDocumentRow: async (id) => DOCS[id] ?? null,
    getLifecycle: async (id) => LIFECYCLE[id] ?? null,
    getLifecycleMany: async (ids) => new Map(ids.map((id) => [id, LIFECYCLE[id] ?? null])),
    work,
    drafts: { listForDocument: async (id) => drafts.filter((d) => d.documentId === id) },
    recentRuns: async (ids) => new Map(ids.map((id) => [id, [{ runId: 'run-1', status: 'running' as const, startedAt: '2026-09-23T00:00:00Z', at: '2026-09-23T00:00:01Z' }]])),
    runCost: async () => 0.25,
    publishSignal: async (c, e, p) => { signals.push([c, e, p]); },
    publishDurable: async (e, p) => { durable.push([e, p]); },
    now: () => new Date().toISOString(),
  };
}

async function replica(deps: DocumentWorkDeps): Promise<Replica> {
  const { documentWorkRouter, documentWorkListRouter } = createDocumentWorkRouters(deps);
  return serve([['/api/documents/:documentId/work', documentWorkRouter], ['/api/document-work', documentWorkListRouter]]);
}

describe('document work routes', () => {
  let table: FakeTable;
  let signals: Array<[string, string, Record<string, unknown>]>;
  let durable: Array<[string, Record<string, unknown>]>;
  let a: Replica;
  let b: Replica;

  beforeEach(async () => {
    table = new FakeTable('documentId');
    signals = [];
    durable = [];
    a = await replica(depsFor(table, signals, durable));
    b = await replica(depsFor(table, signals, durable));
  });
  afterEach(async () => { await a.close(); await b.close(); });

  it('GET backfills status and owner from lifecycle and writes nothing', async () => {
    const res = await call(a.url, 'GET', '/api/documents/doc-1/work', 'dev-vic');
    expect(res.status).toBe(200);
    expect(res.body.work).toMatchObject({ documentId: 'doc-1', status: 'in_review', ownerId: 'dev-connor', revision: 0, tracked: false, backfilled: ['status', 'ownerId'] });
    const done = await call(a.url, 'GET', '/api/documents/doc-2/work', 'dev-connor');
    expect(done.body.work.status).toBe('done');
    expect(table.puts).toBe(0);
    expect(table.rows.size).toBe(0);
  });

  it('two replicas editing different fields from the same revision both land; the same field gives 409', async () => {
    const [ra, rb] = await Promise.all([
      call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', { expectedRevision: 0, set: { status: 'in_progress' } }),
      call(b.url, 'PATCH', '/api/documents/doc-1/work', 'dev-erin', { expectedRevision: 0, set: { points: 5 } }),
    ]);
    expect([ra.status, rb.status]).toEqual([200, 200]);
    const after = await call(b.url, 'GET', '/api/documents/doc-1/work', 'dev-connor');
    expect(after.body.work).toMatchObject({ status: 'in_progress', points: 5, revision: 2, tracked: true });
    expect(after.body.work.backfilled).toEqual(['ownerId']);

    // Erin still holds revision 0 and changes status, which moved at a later revision.
    const conflict = await call(b.url, 'PATCH', '/api/documents/doc-1/work', 'dev-erin', { expectedRevision: 0, set: { status: 'done' } });
    expect(conflict.status).toBe(409);
    expect(conflict.body).toMatchObject({ error: 'work_conflict', fields: ['status'] });
    expect(conflict.body.work.status).toBe('in_progress');

    // With a fresh revision the same edit lands.
    const ok = await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-erin', { expectedRevision: 2, set: { status: 'done' } });
    expect(ok.status).toBe(200);
    expect(ok.body).toMatchObject({ applied: true, changed: ['status'], work: { status: 'done', revision: 3 } });
  });

  it('a replayed criteria op is a no-op and writes nothing', async () => {
    const add = { expectedRevision: 0, criteria: [{ op: 'add', id: 'c1', text: 'Charts use the Q3 numbers' }, { op: 'check', id: 'c1', done: true }] };
    const first = await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', add);
    expect(first.status).toBe(200);
    expect(first.body.work.criteria.c1).toMatchObject({ text: 'Charts use the Q3 numbers', done: true, doneBy: 'dev-connor', rev: 1 });
    const puts = table.puts;
    const replay = await call(b.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', add);
    expect(replay.status).toBe(200);
    expect(replay.body).toMatchObject({ applied: false, changed: [] });
    expect(table.puts).toBe(puts);
    expect(replay.body.work.revision).toBe(1);
  });

  it('criteria edits on different items do not conflict; the same item does', async () => {
    await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', { expectedRevision: 0, criteria: [{ op: 'add', id: 'c1', text: 'one' }, { op: 'add', id: 'c2', text: 'two' }] });
    const e1 = await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', { expectedRevision: 1, criteria: [{ op: 'edit', id: 'c1', text: 'one!' }] });
    const e2 = await call(b.url, 'PATCH', '/api/documents/doc-1/work', 'dev-erin', { expectedRevision: 1, criteria: [{ op: 'check', id: 'c2', done: true }] });
    expect([e1.status, e2.status]).toEqual([200, 200]);
    const same = await call(b.url, 'PATCH', '/api/documents/doc-1/work', 'dev-erin', { expectedRevision: 1, criteria: [{ op: 'edit', id: 'c1', text: 'uno' }] });
    expect(same.status).toBe(409);
    expect(same.body.fields).toEqual(['criteria:c1']);
  });

  it('only the owner or an editor may PATCH; a missing document is 404', async () => {
    const viewer = await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-vic', { expectedRevision: 0, set: { points: 3 } });
    expect(viewer.status).toBe(403);
    expect(viewer.body).toMatchObject({ code: 'forbidden', role: 'viewer' });
    const stranger = await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-sam', { expectedRevision: 0, set: { points: 3 } });
    expect(stranger.status).toBe(403);
    const missing = await call(a.url, 'PATCH', '/api/documents/nope/work', 'dev-connor', { expectedRevision: 0, set: { points: 3 } });
    expect(missing.status).toBe(404);
    const anon = await call(a.url, 'PATCH', '/api/documents/doc-1/work', null, { expectedRevision: 0, set: { points: 3 } });
    expect(anon.status).toBe(401);
    expect(table.puts).toBe(0);
  });

  it('validates the patch', async () => {
    const bad = await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', { expectedRevision: 0, set: { points: 101 } });
    expect(bad.status).toBe(400);
    const noRev = await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', { set: { points: 1 } });
    expect(noRev.status).toBe(400);
    const unknown = await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', { expectedRevision: 0, set: { title: 'x' } });
    expect(unknown.status).toBe(400);
  });

  it('signals carry ids and enums only, on the document and scope channels, plus the durable event', async () => {
    await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', {
      expectedRevision: 0,
      set: { status: 'next', outcome: 'A secret outcome sentence', scopeId: 'folder-1', rank: 'a0' },
      links: [{ op: 'add', id: 'l1', documentId: 'doc-2', relation: 'decided-by' }],
    });
    const channels = signals.map(([c]) => c);
    expect(channels).toEqual(['doc-work:doc-1', 'doc-work-scope:folder-1']);
    for (const [, event, payload] of signals) {
      expect(event).toBe('doc:work_updated');
      expect(JSON.stringify(payload)).not.toContain('secret');
    }
    expect(signals[1]![2]).toMatchObject({ documentId: 'doc-1', revision: 1, status: 'next', rank: 'a0' });
    expect(durable).toHaveLength(1);
    expect(durable[0]![0]).toBe('document.work.updated');
    expect(JSON.stringify(durable[0]![1])).not.toContain('secret');
  });

  it('the scope list backfills, carries the active run and the header rollup', async () => {
    const drafts: RunDraft[] = [{
      documentId: 'doc-1', draftId: 'd1', createdBy: 'dev-connor', pipelineId: 'p1', instruction: 'x',
      status: 'dispatched', runId: 'run-1', dispatchedAt: '2026-09-23T00:00:00Z', revision: 3, createdAt: '', updatedAt: '',
    }];
    const c = await replica(depsFor(table, signals, durable, drafts));
    try {
      await call(c.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', { expectedRevision: 0, set: { scopeId: 'folder-1', points: 3, rank: 'a1' }, links: [{ op: 'add', id: 'l1', documentId: 'doc-2', relation: 'decided-by' }] });
      await call(c.url, 'PATCH', '/api/documents/doc-2/work', 'dev-connor', { expectedRevision: 0, set: { scopeId: 'folder-1', points: 5, rank: 'a0' } });
      const list = await call(c.url, 'GET', '/api/document-work?scope=folder-1', 'dev-connor');
      expect(list.status).toBe(200);
      expect(list.body.scope).toBe('folder-1');
      expect(list.body.rows.map((i: any) => i.documentId)).toEqual(['doc-2', 'doc-1']);
      expect(list.body.rows[0]).toMatchObject({ status: 'done', lifecycleStatus: 'approved', backfilled: ['status', 'ownerId'] });
      expect(list.body.rows[1].activeRun).toEqual({ runId: 'run-1', pipelineId: 'p1', draftId: 'd1', status: 'running', startedAt: '2026-09-23T00:00:00Z', dispatchedAt: '2026-09-23T00:00:00Z' });
      expect(list.body.rollup).toEqual({ pointsDone: 5, pointsTotal: 8, decisions: 1, agents: 1, spentUsd: 0.25 });
    } finally {
      await c.close();
    }
  });

  it('a type scope publishes on an org-qualified channel and lists only the caller\'s org', async () => {
    await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', { expectedRevision: 0, set: { scopeId: 'type:page', points: 2 } });
    expect(signals.map(([c]) => c)).toEqual(['doc-work:doc-1', 'doc-work-scope:dev-org:type:page']);
    // A parent id may not spell another org's type channel.
    const spoof = await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', { expectedRevision: 1, set: { scopeId: 'other-org:type:page' } });
    expect(spoof.status).toBe(400);
    // A row from another org under the same type folder stays out of this org's list.
    table.rows.set('doc-x', { documentId: 'doc-x', organizationId: 'other-org', scopeId: 'type:page', criteria: {}, links: {}, fieldRevisions: {}, revision: 1 });
    const list = await call(a.url, 'GET', '/api/document-work?scope=type:page', 'dev-connor');
    expect(list.body.rows.map((r: any) => r.documentId)).toEqual(['doc-1']);
  });

  it('a link edit changes its relation once; a replay is a no-op', async () => {
    await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', { expectedRevision: 0, links: [{ op: 'add', id: 'l1', documentId: 'doc-2', relation: 'depends-on' }] });
    const edit = { expectedRevision: 1, links: [{ op: 'edit', id: 'l1', relation: 'decided-by' }] };
    const first = await call(a.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', edit);
    expect(first.body.work.links.l1).toMatchObject({ documentId: 'doc-2', relation: 'decided-by', rev: 2 });
    const again = await call(b.url, 'PATCH', '/api/documents/doc-1/work', 'dev-connor', edit);
    expect(again.body).toMatchObject({ applied: false });
  });
});
