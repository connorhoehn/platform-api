// The durable copy of the DLQ comes back at boot and goes away with the
// entry it mirrors. The store and the queue are fakes; under test is the
// paging, the cap, and that a rehydrated entry is dated when it failed.
import { envelopeFromStoredEntry, forgetDeadLetters, rehydrateDeadLetters, setDeadLetterStore, getDeadLetterStore } from '../deadLetterStore';
import type { DLQEntry } from '../../repositories/DLQRepository';

function entry(id: string, failedAt: string, extra: Partial<DLQEntry> = {}): DLQEntry {
  return {
    messageId: id,
    eventType: 'pipeline:run:retrigger',
    eventPayload: { pipelineId: 'p', failedRunId: id.replace('run-failed:', ''), triggerPayload: {}, error: { nodeId: 'n', message: 'm' } },
    lastError: 'PipelineRunFailed: [n] m',
    failedAt,
    totalAttempts: 1,
    ttl: 1,
    ...extra,
  };
}

function fakeDlq() {
  const puts: Array<{ env: { id: string; body: { type: string; payload: unknown }; enqueuedAtMs: number; attemptCount: number }; meta: { lastError: string; failedAtMs: number; totalAttempts: number } }> = [];
  return { puts, put: async (env: unknown, meta: unknown) => { puts.push({ env: env as never, meta: meta as never }); } };
}

/** A store whose `list` hands out `pages` in order, each with a cursor to the next. */
function pagedStore(pages: DLQEntry[][]) {
  const calls: Array<{ limit?: number; exclusiveStartKey?: Record<string, unknown> }> = [];
  return {
    calls,
    list: async (opts: { limit?: number; exclusiveStartKey?: Record<string, unknown> } = {}) => {
      calls.push(opts);
      const i = opts.exclusiveStartKey ? Number(opts.exclusiveStartKey.page) : 0;
      const entries = pages[i] ?? [];
      return { entries, ...(i + 1 < pages.length ? { lastEvaluatedKey: { page: i + 1 } } : {}) };
    },
  };
}

describe('rehydrateDeadLetters', () => {
  it('puts every persisted entry back, keyed by its messageId and dated when it failed', async () => {
    const dlq = fakeDlq();
    const store = pagedStore([[entry('run-failed:a', '2026-09-01T00:00:00.000Z'), entry('run-failed:b', '2026-09-02T00:00:00.000Z', { totalAttempts: 3, lastError: 'Error: bus' })]]);
    const n = await rehydrateDeadLetters(dlq, store);
    expect(n).toBe(2);
    expect(dlq.puts.map((p) => p.env.id)).toEqual(['run-failed:a', 'run-failed:b']);
    const b = dlq.puts[1];
    expect(b.env.body.type).toBe('pipeline:run:retrigger');
    expect(b.env.body.payload).toMatchObject({ failedRunId: 'b' });
    expect(b.env.enqueuedAtMs).toBe(Date.parse('2026-09-02T00:00:00.000Z'));
    expect(b.env.attemptCount).toBe(3);
    expect(b.meta).toEqual({ lastError: 'Error: bus', failedAtMs: Date.parse('2026-09-02T00:00:00.000Z'), totalAttempts: 3 });
  });

  it('follows the store cursor across pages', async () => {
    const dlq = fakeDlq();
    const store = pagedStore([[entry('1', '2026-09-03T00:00:00.000Z')], [entry('2', '2026-09-02T00:00:00.000Z')], [entry('3', '2026-09-01T00:00:00.000Z')]]);
    expect(await rehydrateDeadLetters(dlq, store)).toBe(3);
    expect(store.calls.map((c) => c.exclusiveStartKey)).toEqual([undefined, { page: 1 }, { page: 2 }]);
  });

  it('stops at the cap and never asks the store for more than it will keep', async () => {
    const dlq = fakeDlq();
    const many = Array.from({ length: 7 }, (_, i) => entry(`e${i}`, '2026-09-01T00:00:00.000Z'));
    const store = pagedStore([many.slice(0, 3), many.slice(3, 6), many.slice(6)]);
    expect(await rehydrateDeadLetters(dlq, store, { cap: 4 })).toBe(4);
    expect(dlq.puts).toHaveLength(4);
    expect(store.calls).toHaveLength(2);
    expect(store.calls[1].limit).toBe(1);
  });

  it('puts nothing and asks once when the store is empty', async () => {
    const dlq = fakeDlq();
    const store = pagedStore([[]]);
    expect(await rehydrateDeadLetters(dlq, store)).toBe(0);
    expect(store.calls).toHaveLength(1);
    expect(dlq.puts).toHaveLength(0);
  });

  it('dates an entry with an unparseable failedAt now rather than at epoch 0', () => {
    const before = Date.now();
    const { failedAtMs } = envelopeFromStoredEntry(entry('x', 'not-a-date'));
    expect(failedAtMs).toBeGreaterThanOrEqual(before);
  });
});

describe('forgetDeadLetters', () => {
  it('deletes every id and survives a store that throws on one of them', async () => {
    const deleted: string[] = [];
    const store = { delete: async (id: string) => { if (id === 'bad') throw new Error('nope'); deleted.push(id); } };
    await expect(forgetDeadLetters(store, ['a', 'bad', 'b'])).resolves.toBeUndefined();
    expect(deleted.sort()).toEqual(['a', 'b']);
  });

  it('is a no-op without a store', async () => {
    await expect(forgetDeadLetters(null, ['a'])).resolves.toBeUndefined();
  });
});

describe('the store singleton', () => {
  afterEach(() => setDeadLetterStore(null));
  it('hands back what bootstrap set', () => {
    expect(getDeadLetterStore()).toBeNull();
    const s = { create: async () => undefined, delete: async () => undefined, list: async () => ({ entries: [] }) };
    setDeadLetterStore(s);
    expect(getDeadLetterStore()).toBe(s);
  });
});
