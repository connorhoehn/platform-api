// social-api/src/pipeline/__tests__/dlq.test.ts
//
// T9 + T1 (lib-expansion-3) — verifies the bootstrap-constructed
// InMemoryDeadLetterQueue<BusEvent> captures failed events and exposes
// list/peek/redrive/purge per the v0.11.0 contract.
//
// We provoke a dead letter the same way eventBusDLQ.test.ts does — subscribe
// a known-throwing handler then publish — and inspect the DLQ via the
// bootstrap return value (T9 adds `dlq` to the bootstrap result).

import { FixtureLLMClient } from 'distributed-core/testing';
import { bootstrapPipeline } from '../bootstrap';

jest.setTimeout(20_000);

describe('Pipeline EventBus DLQ — T9 inspect/redrive surface', () => {
  let originalError: typeof console.error;
  beforeAll(() => {
    originalError = console.error;
    console.error = (..._args: unknown[]): void => { /* suppress dead-letter chatter */ };
  });
  afterAll(() => {
    console.error = originalError;
  });

  test('subscriber throw lands in dlq.list with the matching lastError', async () => {
    const fixture = new FixtureLLMClient(['ok']);
    const { module, dlq, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    try {
      expect(dlq).not.toBeNull();
      const bus = module.getEventBus() as unknown as {
        subscribe(type: string, handler: (event: unknown) => Promise<void>): string;
        publish(type: string, payload: unknown): Promise<unknown>;
      };

      bus.subscribe('dlq-probe-event', async () => {
        throw new Error('probe-throw-message');
      });
      await bus.publish('dlq-probe-event', { probe: 1 });

      const page = await dlq!.list({ limit: 50 });
      const probeEntries = page.items.filter((e) =>
        e.envelope.body.type === 'dlq-probe-event',
      );
      expect(probeEntries.length).toBe(1);
      // bootstrap stores `${error.name}: ${error.message}` so the
      // failureKindMatches filter has a class prefix to anchor on.
      expect(probeEntries[0].lastError).toBe('Error: probe-throw-message');
      expect(probeEntries[0].failedAtMs).toBeGreaterThan(0);
    } finally {
      await shutdown();
    }
  });

  test('peek returns null for unknown id, the entry for a known id', async () => {
    const fixture = new FixtureLLMClient(['ok']);
    const { module, dlq, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    try {
      const bus = module.getEventBus() as unknown as {
        subscribe(type: string, handler: (event: unknown) => Promise<void>): string;
        publish(type: string, payload: unknown): Promise<{ id: string }>;
      };
      bus.subscribe('dlq-peek-event', async () => { throw new Error('peek-throw'); });
      const published = await bus.publish('dlq-peek-event', { peek: 1 });

      const miss = await dlq!.peek('does-not-exist');
      const hit  = await dlq!.peek(published.id);
      expect(miss).toBeNull();
      expect(hit?.lastError).toBe('Error: peek-throw');
    } finally {
      await shutdown();
    }
  });

  test('purge removes the entry from the DLQ', async () => {
    const fixture = new FixtureLLMClient(['ok']);
    const { module, dlq, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    try {
      const bus = module.getEventBus() as unknown as {
        subscribe(type: string, handler: (event: unknown) => Promise<void>): string;
        publish(type: string, payload: unknown): Promise<{ id: string }>;
      };
      bus.subscribe('dlq-purge-event', async () => { throw new Error('purge-throw'); });
      const published = await bus.publish('dlq-purge-event', { purge: 1 });

      const beforeCount = await dlq!.size();
      const purgeResult = await dlq!.purge([published.id]);
      const afterCount  = await dlq!.size();

      expect(purgeResult.purged).toBe(1);
      expect(afterCount).toBe(beforeCount - 1);
      expect(await dlq!.peek(published.id)).toBeNull();
    } finally {
      await shutdown();
    }
  });

  test('redrive re-publishes through the EventBus and removes the DLQ entry', async () => {
    const fixture = new FixtureLLMClient(['ok']);
    const { module, dlq, shutdown } = await bootstrapPipeline({ llmClient: fixture });

    try {
      const bus = module.getEventBus() as unknown as {
        subscribe(type: string, handler: (event: unknown) => Promise<void>): string;
        publish(type: string, payload: unknown): Promise<{ id: string }>;
      };

      // First subscriber always throws on the first attempt; flips to
      // succeeding for the redrive replay so we can assert the redrive sink
      // actually re-published rather than swallowed.
      let attempts = 0;
      bus.subscribe('dlq-redrive-event', async () => {
        attempts++;
        if (attempts === 1) throw new Error('first-attempt-throws');
      });

      const published = await bus.publish('dlq-redrive-event', { redrive: 1 });
      // First pass throws → entry lands in DLQ.
      const inDlq = await dlq!.peek(published.id);
      expect(inDlq).not.toBeNull();

      const result = await dlq!.redrive([published.id], { resetAttempts: true });
      expect(result.redriven).toBe(1);
      expect(result.failed.length).toBe(0);
      // Redrive emitted a fresh publish; subscriber re-ran and succeeded
      // (`attempts === 2`). The redriven entry is removed from the DLQ.
      expect(attempts).toBeGreaterThanOrEqual(2);
      expect(await dlq!.peek(published.id)).toBeNull();
    } finally {
      await shutdown();
    }
  });
});
