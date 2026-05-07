// social-api/src/pipeline/ingest-steps/__tests__/event-trigger.test.ts

import { IngestEventTrigger, buildTriggerPayload } from '../event-trigger';
import type { IRunStarter } from '../event-trigger';
import type { IngestEvent } from '../../../ingest/types';
import type { IngestEventBus } from '../../../ingest/engine';

class FakeBus implements IngestEventBus {
  private handler: ((e: IngestEvent) => void) | null = null;
  emit(event: IngestEvent): void {
    this.handler?.(event);
  }
  on(h: (e: IngestEvent) => void): void {
    this.handler = h;
  }
  off(h: (e: IngestEvent) => void): void {
    if (this.handler === h) this.handler = null;
  }
  hasHandler(): boolean {
    return this.handler !== null;
  }
}

function makeRunStarter() {
  const calls: Array<{ pipelineId: string; triggerPayload: unknown }> = [];
  const starter: IRunStarter = {
    trigger: jest.fn(async (args) => {
      calls.push({ pipelineId: args.pipelineId, triggerPayload: args.triggerPayload });
      return { runId: `run-${calls.length}` };
    }),
  };
  return { starter, calls };
}

const materializedEvent: IngestEvent = {
  type: 'item.materialized',
  ingestId: 'ingest-1',
  documentId: 'doc-1',
  timestamp: '2026-05-06T00:00:00Z',
};

const mappedEvent: IngestEvent = {
  type: 'item.mapped',
  ingestId: 'ingest-1',
  typeId: 'doc-type-A',
  typeName: 'doc-type-A',
  method: 'rule',
  confidence: 1.0,
  timestamp: '2026-05-06T00:00:00Z',
};

const fetchedEvent: IngestEvent = {
  type: 'sync.file.fetched',
  sourceId: 'src-1',
  filePath: '/x.md',
  index: 1,
  total: 1,
  timestamp: '2026-05-06T00:00:00Z',
};

describe('buildTriggerPayload', () => {
  test('extracts ingestId/documentId from item.materialized', () => {
    const p = buildTriggerPayload(materializedEvent);
    expect(p.triggerType).toBe('ingest:event');
    expect(p.ingestId).toBe('ingest-1');
    expect(p.documentId).toBe('doc-1');
  });

  test('extracts typeId from item.mapped as documentTypeId', () => {
    const p = buildTriggerPayload(mappedEvent);
    expect(p.documentTypeId).toBe('doc-type-A');
  });

  test('extracts sourceId from sync events', () => {
    const p = buildTriggerPayload(fetchedEvent);
    expect(p.sourceId).toBe('src-1');
  });
});

describe('IngestEventTrigger — registration + dispatch', () => {
  test('default eventTypes binding fires only on item.materialized', async () => {
    const bus = new FakeBus();
    const { starter, calls } = makeRunStarter();
    const trigger = new IngestEventTrigger(bus, starter);
    trigger.register({ pipelineId: 'pipe-A', binding: {} });

    bus.emit(fetchedEvent);
    bus.emit(mappedEvent);
    bus.emit(materializedEvent);
    await flushMicrotasks();

    expect(calls.length).toBe(1);
    expect(calls[0].pipelineId).toBe('pipe-A');
    expect((calls[0].triggerPayload as { ingestId?: string }).ingestId).toBe('ingest-1');
  });

  test('eventTypes filter — fires on multiple matching event types', async () => {
    const bus = new FakeBus();
    const { starter, calls } = makeRunStarter();
    const trigger = new IngestEventTrigger(bus, starter);
    trigger.register({
      pipelineId: 'pipe-B',
      binding: { eventTypes: ['item.mapped', 'item.materialized'] },
    });

    bus.emit(materializedEvent);
    bus.emit(mappedEvent);
    bus.emit(fetchedEvent);
    await flushMicrotasks();

    expect(calls.length).toBe(2);
  });

  test('sourceIds filter rejects non-matching sources', async () => {
    const bus = new FakeBus();
    const { starter, calls } = makeRunStarter();
    const trigger = new IngestEventTrigger(bus, starter);
    trigger.register({
      pipelineId: 'pipe-C',
      binding: {
        eventTypes: ['sync.file.fetched'],
        sourceIds: ['src-other'],
      },
    });

    bus.emit(fetchedEvent); // sourceId: 'src-1' — should not match
    await flushMicrotasks();
    expect(calls.length).toBe(0);

    trigger.register({
      pipelineId: 'pipe-D',
      binding: {
        eventTypes: ['sync.file.fetched'],
        sourceIds: ['src-1'],
      },
    });
    bus.emit(fetchedEvent);
    await flushMicrotasks();
    expect(calls.length).toBe(1);
    expect(calls[0].pipelineId).toBe('pipe-D');
  });

  test('documentTypeId filter — uses event typeId when present', async () => {
    const bus = new FakeBus();
    const { starter, calls } = makeRunStarter();
    const trigger = new IngestEventTrigger(bus, starter);
    trigger.register({
      pipelineId: 'pipe-typed',
      binding: { eventTypes: ['item.mapped'], documentTypeId: 'doc-type-A' },
    });
    trigger.register({
      pipelineId: 'pipe-other',
      binding: { eventTypes: ['item.mapped'], documentTypeId: 'doc-type-B' },
    });

    bus.emit(mappedEvent);
    await flushMicrotasks();
    expect(calls.length).toBe(1);
    expect(calls[0].pipelineId).toBe('pipe-typed');
  });

  test('documentTypeId filter — falls back to resolveDocumentTypeId hook', async () => {
    const bus = new FakeBus();
    const { starter, calls } = makeRunStarter();
    const trigger = new IngestEventTrigger(bus, starter);
    trigger.register({
      pipelineId: 'pipe-async',
      binding: { eventTypes: ['item.materialized'], documentTypeId: 'doc-type-Z' },
      resolveDocumentTypeId: jest.fn(async () => 'doc-type-Z'),
    });

    bus.emit(materializedEvent);
    await flushMicrotasks();
    expect(calls.length).toBe(1);
  });

  test('multiple bindings — single event fans out to all matchers', async () => {
    const bus = new FakeBus();
    const { starter, calls } = makeRunStarter();
    const trigger = new IngestEventTrigger(bus, starter);
    trigger.register({ pipelineId: 'pipe-1', binding: {} });
    trigger.register({ pipelineId: 'pipe-2', binding: {} });

    bus.emit(materializedEvent);
    await flushMicrotasks();
    expect(calls.map((c) => c.pipelineId).sort()).toEqual(['pipe-1', 'pipe-2']);
  });

  test('unsubscribes from bus when last registration disposes', () => {
    const bus = new FakeBus();
    const { starter } = makeRunStarter();
    const trigger = new IngestEventTrigger(bus, starter);
    const dispose1 = trigger.register({ pipelineId: 'p1', binding: {} });
    const dispose2 = trigger.register({ pipelineId: 'p2', binding: {} });
    expect(bus.hasHandler()).toBe(true);
    dispose1();
    expect(bus.hasHandler()).toBe(true);
    dispose2();
    expect(bus.hasHandler()).toBe(false);
  });

  test('errors thrown by runStarter are routed to onError', async () => {
    const bus = new FakeBus();
    const onError = jest.fn();
    const failingStarter: IRunStarter = {
      trigger: jest.fn(async () => { throw new Error('upstream-fail'); }),
    };
    const trigger = new IngestEventTrigger(bus, failingStarter, { onError });
    trigger.register({ pipelineId: 'pipe-err', binding: {} });

    bus.emit(materializedEvent);
    await flushMicrotasks();

    expect(onError).toHaveBeenCalledTimes(1);
    expect((onError.mock.calls[0][0] as Error).message).toBe('upstream-fail');
  });
});

async function flushMicrotasks(): Promise<void> {
  // Drain the fire-and-forget chain — the trigger uses `void this.dispatch()`
  // which schedules an unawaited promise; two ticks cover it.
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}
