import { withStepRetry } from '../withStepRetry';

const node = (retryPolicy?: unknown) => ({ id: 'await', type: 'x', position: { x: 0, y: 0 }, data: { type: 'x', ...(retryPolicy ? { retryPolicy } : {}) } }) as never;
const helpers = () => { const events: Array<[string, Record<string, unknown>]> = []; return { h: { runId: 'r', ownerNodeId: 'n', emit: async (e: string, p: Record<string, unknown>) => { events.push([e, p]); } } as never, events }; };

describe('withStepRetry', () => {
  it('runs a step once when it carries no policy', async () => {
    const execute = jest.fn().mockResolvedValue({ status: 'failed', error: 'nope' });
    const { h } = helpers();
    const out = await withStepRetry({ kind: 'x', execute }, { sleep: async () => undefined }).execute(node(), {}, h);
    expect(out.status).toBe('failed');
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('retries a failing step up to maxAttempts with growing backoff, announcing each attempt', async () => {
    const execute = jest.fn()
      .mockResolvedValueOnce({ status: 'failed', error: 'compose is still queued' })
      .mockRejectedValueOnce(new Error('LVS 502'))
      .mockResolvedValueOnce({ status: 'completed', output: { composeStatus: 'ok' } });
    const waits: number[] = [];
    const { h, events } = helpers();
    const out = await withStepRetry({ kind: 'x', execute }, { sleep: async (ms) => { waits.push(ms); } }).execute(node({ maxAttempts: 5, backoffMs: 100, backoffMultiplier: 2 }), {}, h);
    expect(out.status).toBe('completed');
    expect(execute).toHaveBeenCalledTimes(3);
    expect(waits).toEqual([100, 200]);
    expect(events.filter(([e]) => e === 'pipeline:step:attempt:started')).toHaveLength(3);
    expect(events.filter(([e]) => e === 'pipeline:step:attempt:failed').map(([, p]) => p.willRetry)).toEqual([true, true]);
  });

  it('gives up after the budget and returns the last failure', async () => {
    const execute = jest.fn().mockResolvedValue({ status: 'failed', error: 'still queued' });
    const { h, events } = helpers();
    const out = await withStepRetry({ kind: 'x', execute }, { sleep: async () => undefined }).execute(node({ maxAttempts: 3, backoffMs: 1, backoffMultiplier: 1 }), {}, h);
    expect(out).toEqual({ status: 'failed', error: 'still queued' });
    expect(execute).toHaveBeenCalledTimes(3);
    const failed = events.filter(([e]) => e === 'pipeline:step:attempt:failed').map(([, p]) => p.willRetry);
    expect(failed).toEqual([true, true, false]);
  });
});
