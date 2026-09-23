/**
 * A finished run row never goes back to `running`. The bus handlers persist
 * full rows concurrently, so the checkpoint put taken after the last step that started BEFORE the
 * run-completed put can land AFTER it. Live 2026-09-23 (deck-revise
 * 15655b83): the row kept `status: running` with the last step's
 * `lastEventAt`, the runs list said Running, and the run page seeded from it
 * said Running after every reload although the executor had finished.
 */
jest.mock('../../deck/presentationDocuments', () => ({ getPresentationDocumentStore: () => ({ get: async () => null }) }));
jest.mock('../../work-graph/sources/platformPublisher', () => ({
  platformWorkGraphSourcePublisher: { pipeline: jest.fn(async () => ({ published: true })), document: jest.fn(async () => ({ published: true })), meeting: jest.fn(async () => ({ published: true })) },
}));

import { RunPersistenceService } from '../runPersistence';
import { PipelineRunRepository, isSupersededBy, type PipelineRunItem } from '../../repositories/PipelineRunRepository';

type Handler = (event: { id?: string; payload: Record<string, unknown>; sourceNodeId?: string }) => Promise<void>;

describe('a finished run row stays finished', () => {
  it('a checkpoint put that lands after the run-completed put is refused as superseded', async () => {
    const rows = new Map<string, PipelineRunItem>();
    let releaseStepPut: (() => void) | null = null;
    const write = async (item: PipelineRunItem): Promise<true | 'superseded'> => {
        // Hold the checkpoint write taken after the last step (status running)
        // until the run-completed write has landed.
        if (item.status === 'running' && item.checkpoint) {
          await new Promise<void>((resolve) => { releaseStepPut = resolve; });
        }
        const row = rows.get(item.runId);
        if (row && isSupersededBy(row, item)) return 'superseded' as const;
        rows.set(item.runId, { ...item });
        return true;
    };
    const repository = { put: jest.fn(async (item: PipelineRunItem) => { await write(item); }), putOwned: jest.fn(write) };
    const handlers = new Map<string, Handler>();
    const eventBus = { subscribe: (name: string, handler: Handler) => { handlers.set(name, handler); return name; } };
    const service = new RunPersistenceService({ eventBus: eventBus as never, repository: repository as unknown as PipelineRunRepository });
    service.start();
    let n = 0;
    const emit = (name: string, payload: Record<string, unknown>) => handlers.get(name)!({ id: `e-${++n}`, payload, sourceNodeId: 'node-a' });

    await emit('pipeline:run:started', { runId: 'r', pipelineId: 'deck-revise', triggeredBy: { triggerType: 'manual', userId: 'u' }, at: '2026-09-23T11:32:32.006Z' });
    await emit('pipeline:step:completed', { runId: 'r', stepId: 'write', output: {}, durationMs: 1, at: '2026-09-23T11:32:37.543Z' });
    const stepPut = emit('pipeline:run:checkpoint', { runId: 'r', context: {}, currentStepIds: [], sequenceNumber: 4, at: '2026-09-23T11:32:37.543Z', steps: { write: { nodeId: 'write', status: 'completed' } } });
    for (let i = 0; i < 100 && !releaseStepPut; i++) await new Promise((r) => setTimeout(r, 5));
    expect(releaseStepPut).not.toBeNull();
    await emit('pipeline:run:completed', { runId: 'r', pipelineId: 'deck-revise', at: '2026-09-23T11:32:37.549Z', durationMs: 5543 });
    expect(rows.get('r')!.status).toBe('completed');
    releaseStepPut!();
    await stepPut;
    expect(rows.get('r')!.status).toBe('completed');
    expect(rows.get('r')!.completedAt).toBe('2026-09-23T11:32:37.549Z');
    expect(service.staleWritesDropped).toBe(1);
    expect(service.ownershipLost).toBe(0);
  });

  it('isSupersededBy: only a non-terminal write older than the stored finish', () => {
    const row = { completedAt: '2026-09-23T00:00:02.000Z' };
    expect(isSupersededBy(row, { status: 'running', lastEventAt: '2026-09-23T00:00:01.000Z' })).toBe(true);
    // Resume-from-step of a failed run: its events are newer than the old finish.
    expect(isSupersededBy(row, { status: 'running', lastEventAt: '2026-09-23T00:00:03.000Z' })).toBe(false);
    expect(isSupersededBy(row, { status: 'completed', lastEventAt: '2026-09-23T00:00:01.000Z' })).toBe(false);
    expect(isSupersededBy({}, { status: 'running', lastEventAt: '2026-09-23T00:00:01.000Z' })).toBe(false);
  });

  it('the repository conditions non-terminal puts on the stored finish and reports superseded', async () => {
    const sent: Array<{ input: Record<string, unknown> }> = [];
    const stored: PipelineRunItem = { pipelineId: 'p', runId: 'r', status: 'completed', completedAt: '2026-09-23T00:00:02.000Z' } as PipelineRunItem;
    const docClient = {
      send: jest.fn(async (cmd: { input: Record<string, unknown>; constructor: { name: string } }) => {
        sent.push(cmd);
        if (cmd.constructor.name === 'GetCommand') return { Item: stored };
        const cond = String(cmd.input.ConditionExpression ?? '');
        if (cond.includes('completedAt <= :lastEventAt')) {
          const err = new Error('conditional'); (err as { name: string }).name = 'ConditionalCheckFailedException'; throw err;
        }
        return {};
      }),
    };
    const repo = new PipelineRunRepository(docClient as never);
    const running = { pipelineId: 'p', runId: 'r', status: 'running', lastEventAt: '2026-09-23T00:00:01.000Z' } as PipelineRunItem;
    await expect(repo.putOwned(running, 'node-a')).resolves.toBe('superseded');
    await expect(repo.put(running)).resolves.toBeUndefined();
    const finished = { ...running, status: 'completed', completedAt: '2026-09-23T00:00:03.000Z' } as PipelineRunItem;
    await expect(repo.putOwned(finished, 'node-a')).resolves.toBe(true);
    const lastPut = sent.at(-1)!.input;
    expect(String(lastPut.ConditionExpression)).not.toContain('completedAt');
  });
});
