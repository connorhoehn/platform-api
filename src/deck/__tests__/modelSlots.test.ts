// The deck's model-slot cap with no Redis (the per-process fallback): at most
// DECK_MODEL_CONCURRENCY calls at once, the rest wait in order and are told
// their place, and a wait past DECK_MODEL_SLOT_WAIT_MS fails with a reason.
// The fleet-wide path is distributed-core's RedisSemaphore (tested there) and
// the 2-replica done-check in realtime-examples docs/design/nfr (Loop 35 ppt).
jest.mock('../../lib/redis-client', () => ({ getRedisClient: async () => null }));
import { ModelSlotTimeoutError, resetModelSlots, withModelSlot } from '../modelSlots';

const tick = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('withModelSlot (no Redis)', () => {
  const env = { ...process.env };
  beforeEach(() => { resetModelSlots(); process.env.DECK_MODEL_CONCURRENCY = '2'; process.env.DECK_MODEL_SLOT_WAIT_MS = '1000'; });
  afterEach(() => { process.env = { ...env }; });

  it('runs at most the limit at once and serves the rest in order', async () => {
    let inFlight = 0; let peak = 0;
    const order: number[] = [];
    const waits: Array<[number, number]> = [];
    const call = (i: number) => withModelSlot(async () => {
      order.push(i); inFlight += 1; peak = Math.max(peak, inFlight);
      await tick(20);
      inFlight -= 1;
      return i;
    }, { onWaiting: (w) => waits.push([i, w.position]) });
    const out = await Promise.all([0, 1, 2, 3, 4].map(call));
    expect(out).toEqual([0, 1, 2, 3, 4]);
    expect(peak).toBe(2);
    expect(order).toEqual([0, 1, 2, 3, 4]);
    expect(waits).toEqual([[2, 0], [3, 1], [4, 2]]);
  });

  it('gives the slot back when the call throws', async () => {
    await expect(withModelSlot(async () => { throw new Error('429 rate limited'); })).rejects.toThrow('429');
    await expect(withModelSlot(async () => 'a')).resolves.toBe('a');
    await expect(withModelSlot(async () => 'b')).resolves.toBe('b');
  });

  it('fails a wait that runs past the limit, with its place in line', async () => {
    process.env.DECK_MODEL_SLOT_WAIT_MS = '50';
    const long = () => withModelSlot(() => tick(200));
    const a = long(); const b = long();
    const err = await withModelSlot(async () => 'never').catch((e) => e);
    expect(err).toBeInstanceOf(ModelSlotTimeoutError);
    expect(err.message).toMatch(/no model slot came free within 0 s \(1 in line, 2 slots\)/);
    await Promise.all([a, b]);
  });

  it('DECK_MODEL_CONCURRENCY=0 turns the cap off', async () => {
    process.env.DECK_MODEL_CONCURRENCY = '0';
    let peak = 0; let inFlight = 0;
    await Promise.all([1, 2, 3, 4].map(() => withModelSlot(async () => { inFlight += 1; peak = Math.max(peak, inFlight); await tick(10); inFlight -= 1; })));
    expect(peak).toBe(4);
  });
});
