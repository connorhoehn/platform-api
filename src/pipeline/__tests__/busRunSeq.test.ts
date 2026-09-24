// NFR #170: every lane carries a contiguous per-run number on progress events.

import { stampPipelineRunSeq } from '../busRunSeq';

function fakeBus() {
  const published: Array<{ type: string; payload: any }> = [];
  const bus = {
    publish: async (type: string, payload: any) => {
      published.push({ type, payload });
      return { id: `e${published.length}`, type, payload };
    },
  };
  return { bus, published };
}

describe('stampPipelineRunSeq', () => {
  it('numbers each run\'s progress events 1, 2, 3 … independently, in call order', async () => {
    const { bus, published } = fakeBus();
    stampPipelineRunSeq(bus);
    stampPipelineRunSeq(bus); // idempotent: one wrap

    const calls = [
      bus.publish('pipeline:run:started', { runId: 'r1', at: 't' }),
      bus.publish('pipeline:run:started', { runId: 'r2', at: 't' }),
      bus.publish('pipeline:step:started', { runId: 'r1', stepId: 'a', at: 't' }),
      bus.publish('pipeline:llm:token', { runId: 'r1', stepId: 'a', token: 'x' }),
      bus.publish('pipeline:step:completed', { runId: 'r1', stepId: 'a', at: 't' }),
      bus.publish('pipeline:run:completed', { runId: 'r1', at: 't' }),
    ];
    await Promise.all(calls);

    expect(published.map((e) => [e.type, e.payload.runId, e.payload.runSeq])).toEqual([
      ['pipeline:run:started', 'r1', 1],
      ['pipeline:run:started', 'r2', 1],
      ['pipeline:step:started', 'r1', 2],
      ['pipeline:llm:token', 'r1', undefined], // hot lane: not numbered, no gap
      ['pipeline:step:completed', 'r1', 3],
      ['pipeline:run:completed', 'r1', 4],
    ]);
  });

  it('carries the run\'s owner from its start onto every later progress event', async () => {
    const { bus, published } = fakeBus();
    stampPipelineRunSeq(bus);
    await bus.publish('pipeline:run:started', { runId: 'r1', triggeredBy: { userId: 'dev-connor', triggerType: 'manual', payload: {} } });
    await bus.publish('pipeline:step:started', { runId: 'r1', stepId: 'a' });
    await bus.publish('pipeline:run:started', { runId: 'r2', triggeredBy: { triggerType: 'schedule', payload: {} } });
    await bus.publish('pipeline:step:started', { runId: 'r2', stepId: 'a' });
    expect(published.map((e) => e.payload.runOwnerId)).toEqual(['dev-connor', 'dev-connor', undefined, undefined]);
  });

  it('leaves events without a run, and an already-numbered event, alone', async () => {
    const { bus, published } = fakeBus();
    stampPipelineRunSeq(bus);
    const payload = { at: 't' };
    await bus.publish('pipeline:run:started', payload);
    await bus.publish('pipeline:run:started', { runId: 'r1', runSeq: 7 });
    expect(published[0].payload).toBe(payload);
    expect(published[1].payload.runSeq).toBe(7);
  });
});
