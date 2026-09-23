/**
 * NFR #39: two replicas must not race full-row puts on one `pipeline-runs`
 * row. Each write is conditioned on this node owning the run (or holding its
 * resume claim); a replica that does not gets `false`, writes nothing and
 * stops writing that run.
 */
jest.mock('../../deck/presentationDocuments', () => ({ getPresentationDocumentStore: () => ({ get: async () => null }) }));
jest.mock('../../work-graph/sources/platformPublisher', () => ({
  platformWorkGraphSourcePublisher: { pipeline: jest.fn(async () => ({ published: true })), document: jest.fn(async () => ({ published: true })), meeting: jest.fn(async () => ({ published: true })) },
}));

import { RunPersistenceService } from '../runPersistence';
import type { PipelineRunItem, PipelineRunRepository } from '../../repositories/PipelineRunRepository';

type Handler = (event: { id?: string; payload: Record<string, unknown>; sourceNodeId?: string }) => Promise<void>;

/** One table both replicas write, applying putOwned's condition the way DynamoDB would. */
function sharedTable() {
  const rows = new Map<string, Record<string, unknown>>();
  const writes: string[] = [];
  const repoFor = (node: string) => ({
    put: jest.fn(async (item: PipelineRunItem) => { rows.set(item.runId, { ...item }); writes.push(`${node}:put`); }),
    putOwned: jest.fn(async (item: PipelineRunItem, owner: string) => {
      const row = rows.get(item.runId);
      const ok = !row || row.ownerNodeId === undefined || row.resumeClaimedBy === owner
        || (row.ownerNodeId === owner && row.resumeClaimedBy === undefined);
      if (!ok) { writes.push(`${node}:refused`); return false; }
      rows.set(item.runId, { ...(row?.resumeClaimedBy ? { resumeClaimedBy: row.resumeClaimedBy } : {}), ...item });
      writes.push(`${node}:put`);
      return true;
    }),
  });
  return { rows, writes, repoFor };
}

function replica(node: string, repository: ReturnType<ReturnType<typeof sharedTable>['repoFor']>) {
  const handlers = new Map<string, Handler>();
  const eventBus = { subscribe: (name: string, handler: Handler) => { handlers.set(name, handler); return name; } };
  const service = new RunPersistenceService({ eventBus: eventBus as never, repository: repository as unknown as PipelineRunRepository });
  service.start();
  let n = 0;
  const emit = async (name: string, payload: Record<string, unknown>) => {
    n += 1;
    await handlers.get(name)!({ id: `${node}-${n}`, payload, sourceNodeId: node });
  };
  return { service, emit };
}

const started = { runId: 'run-1', pipelineId: 'p-1', triggeredBy: { triggerType: 'manual', userId: 'u' }, at: '2026-09-23T00:00:00.000Z' };
const done = (status = 'completed') => ({ runId: 'run-1', pipelineId: 'p-1', status, at: '2026-09-23T00:00:02.000Z', durationMs: 5 });

describe('run rows are written by their owner only (NFR #39)', () => {
  it('a second replica that starts writing a run it does not own is refused and stops', async () => {
    const table = sharedTable();
    const a = replica('node-a', table.repoFor('a'));
    const b = replica('node-b', table.repoFor('b'));
    await a.emit('pipeline:run:started', started);
    await b.emit('pipeline:run:started', started);
    await b.emit('pipeline:run:completed', done());
    await a.emit('pipeline:run:completed', done());
    expect(table.rows.get('run-1')!.ownerNodeId).toBe('node-a');
    expect(table.rows.get('run-1')!.status).toBe('completed');
    // b never wrote the row: refused at start, dropped the run, refused again
    // when its terminal event rebuilt a state for it.
    expect(table.writes.filter((w) => w === 'b:put')).toEqual([]);
    expect(b.service.ownershipLost).toBe(2);
    expect(table.writes.filter((w) => w === 'a:put').length).toBeGreaterThanOrEqual(2);
  });

  it('after another node claims the run for resume, the old owner writes nothing more', async () => {
    const table = sharedTable();
    const a = replica('node-a', table.repoFor('a'));
    await a.emit('pipeline:run:started', started);
    table.rows.get('run-1')!.resumeClaimedBy = 'node-b';
    await a.emit('pipeline:run:completed', done());
    expect(table.writes.at(-1)).toBe('a:refused');
    expect(a.service.ownershipLost).toBe(1);
  });
});
