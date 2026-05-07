// social-api/src/pipeline/__tests__/executor-audit-subscriber.test.ts
//
// Hub#2224 Wave 2 — unit tests for the bridge-side executor audit subscriber.
// Verifies that PipelineModule EventBus events translate to the right
// AuditAction + payload shape, that the runId→pipelineId index is populated
// from `pipeline:run:started` and used for downstream events, and that
// checkpoint sampling drops 4-of-5 events while always keeping seq=1.

import type { EventBus, PipelineEventMap } from 'distributed-core';
import { ExecutorAuditSubscriber } from '../executor-audit-subscriber';
import type { AuditEvent } from '../audit-repository';

// Minimal in-memory EventBus stub mimicking the subscribe(eventName, handler)
// shape that PipelineModule exposes. We only need enough to capture handlers
// and re-invoke them with synthetic payloads.
type Handler = (event: { payload: unknown }) => void | Promise<void>;

class StubBus {
  private handlers = new Map<string, Handler[]>();
  private nextId = 1;

  subscribe(eventName: string, handler: Handler): string {
    const list = this.handlers.get(eventName) ?? [];
    list.push(handler);
    this.handlers.set(eventName, list);
    return `sub-${this.nextId++}`;
  }

  unsubscribe(_id: string): void {
    // not exercised in these tests
  }

  async emit(eventName: keyof PipelineEventMap, payload: unknown): Promise<void> {
    const list = this.handlers.get(eventName) ?? [];
    for (const h of list) await h({ payload });
  }
}

function makeSubscriber(opts?: { sampleRate?: number }): {
  subscriber: ExecutorAuditSubscriber;
  bus: StubBus;
  records: AuditEvent[];
  recordMock: jest.Mock;
} {
  const records: AuditEvent[] = [];
  const recordMock = jest.fn((evt: AuditEvent) => {
    records.push(evt);
    return Promise.resolve();
  });
  const bus = new StubBus();
  const subscriber = new ExecutorAuditSubscriber({
    eventBus: bus as unknown as EventBus<PipelineEventMap>,
    auditRepo: { record: recordMock },
    ...(opts?.sampleRate !== undefined ? { checkpointSampleRate: opts.sampleRate } : {}),
  });
  subscriber.start();
  return { subscriber, bus, records, recordMock };
}

// Small helper — yields once so fire-and-forget catch chains can settle.
const flush = (): Promise<void> => new Promise((r) => setImmediate(r));

describe('ExecutorAuditSubscriber', () => {
  it('does NOT record an audit row for pipeline:run:started (avoids duplicating pipeline.trigger)', async () => {
    const { bus, records } = makeSubscriber();
    await bus.emit('pipeline:run:started', {
      runId: 'r1',
      pipelineId: 'p1',
      triggeredBy: { userId: 'u1' },
      at: '2026-05-06T10:00:00Z',
    });
    await flush();
    expect(records).toHaveLength(0);
  });

  it('indexes runId→pipelineId on run:started so downstream events get pipelineId', async () => {
    const { bus, records } = makeSubscriber();
    await bus.emit('pipeline:run:started', {
      runId: 'r1',
      pipelineId: 'pipe-A',
      triggeredBy: { userId: 'u1' },
      at: '2026-05-06T10:00:00Z',
    });
    await bus.emit('pipeline:step:started', {
      runId: 'r1',
      stepId: 's1',
      nodeType: 'llm',
      at: '2026-05-06T10:00:01Z',
    });
    await flush();
    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({
      action: 'pipeline.step.started',
      actorUserId: 'system',
      pipelineId: 'pipe-A',
      runId: 'r1',
      outcome: 'success',
      trace: { stepId: 's1', nodeType: 'llm', at: '2026-05-06T10:00:01Z' },
    });
  });

  it('records pipelineId="unknown" when no run:started was observed for the runId', async () => {
    const { bus, records } = makeSubscriber();
    await bus.emit('pipeline:step:started', {
      runId: 'orphan',
      stepId: 's1',
      nodeType: 'transform',
      at: 't',
    });
    await flush();
    expect(records[0]).toMatchObject({ pipelineId: 'unknown', runId: 'orphan' });
  });

  it('maps pipeline:step:completed → pipeline.step.completed (success)', async () => {
    const { bus, records } = makeSubscriber();
    await bus.emit('pipeline:run:started', {
      runId: 'r2',
      pipelineId: 'p2',
      triggeredBy: { userId: 'u' },
      at: 't',
    });
    await bus.emit('pipeline:step:completed', {
      runId: 'r2',
      stepId: 's1',
      durationMs: 123,
      at: 't',
    });
    await flush();
    expect(records.find((r) => r.action === 'pipeline.step.completed')).toMatchObject({
      pipelineId: 'p2',
      runId: 'r2',
      outcome: 'success',
      trace: { stepId: 's1', durationMs: 123 },
    });
  });

  it('maps pipeline:step:failed → pipeline.step.failed (error) and carries error message', async () => {
    const { bus, records } = makeSubscriber();
    await bus.emit('pipeline:run:started', {
      runId: 'r3',
      pipelineId: 'p3',
      triggeredBy: { userId: 'u' },
      at: 't',
    });
    await bus.emit('pipeline:step:failed', {
      runId: 'r3',
      stepId: 'sX',
      error: 'NetworkError: ECONNREFUSED',
      at: 't',
    });
    await flush();
    const failed = records.find((r) => r.action === 'pipeline.step.failed');
    expect(failed).toMatchObject({
      pipelineId: 'p3',
      runId: 'r3',
      outcome: 'error',
      decision: 'error',
      trace: { stepId: 'sX', error: 'NetworkError: ECONNREFUSED' },
    });
  });

  it('maps pipeline:step:breakpoint → pipeline.step.breakpoint.hit', async () => {
    const { bus, records } = makeSubscriber();
    await bus.emit('pipeline:run:started', {
      runId: 'r4',
      pipelineId: 'p4',
      triggeredBy: { userId: 'u' },
      at: 't',
    });
    await bus.emit('pipeline:step:breakpoint', { runId: 'r4', stepId: 'bp1', at: 't' });
    await flush();
    const bp = records.find((r) => r.action === 'pipeline.step.breakpoint.hit');
    expect(bp).toBeDefined();
    expect(bp!.trace).toMatchObject({ stepId: 'bp1' });
  });

  it('samples checkpoint events 1-of-N (default 5) and always keeps seq=1', async () => {
    const { bus, records } = makeSubscriber({ sampleRate: 5 });
    await bus.emit('pipeline:run:started', {
      runId: 'r5',
      pipelineId: 'p5',
      triggeredBy: { userId: 'u' },
      at: 't',
    });
    for (let seq = 1; seq <= 12; seq++) {
      await bus.emit('pipeline:run:checkpoint', {
        runId: 'r5',
        context: {},
        currentStepIds: ['s' + seq],
        sequenceNumber: seq,
        at: 't',
      });
    }
    await flush();
    const cps = records.filter((r) => r.action === 'pipeline.run.checkpoint');
    // seq 1 (always), 5, 10 → 3 rows
    expect(cps).toHaveLength(3);
    expect(cps.map((c) => (c.trace as { sequenceNumber: number }).sequenceNumber)).toEqual(
      [1, 5, 10],
    );
  });

  it('maps pipeline:run:completed → pipeline.run.completed and evicts the runId index', async () => {
    const { bus, records, subscriber } = makeSubscriber();
    await bus.emit('pipeline:run:started', {
      runId: 'r6',
      pipelineId: 'p6',
      triggeredBy: { userId: 'u' },
      at: 't',
    });
    expect(subscriber.indexSize()).toBe(1);
    await bus.emit('pipeline:run:completed', { runId: 'r6', durationMs: 999, at: 't' });
    await flush();
    expect(records.find((r) => r.action === 'pipeline.run.completed')).toMatchObject({
      pipelineId: 'p6',
      runId: 'r6',
      decision: 'completed',
      trace: { durationMs: 999 },
    });
    expect(subscriber.indexSize()).toBe(0);
  });

  it('maps pipeline:run:failed → pipeline.run.failed with extracted message', async () => {
    const { bus, records } = makeSubscriber();
    await bus.emit('pipeline:run:started', {
      runId: 'r7',
      pipelineId: 'p7',
      triggeredBy: { userId: 'u' },
      at: 't',
    });
    await bus.emit('pipeline:run:failed', {
      runId: 'r7',
      error: { message: 'boom', stack: '...' },
      at: 't',
    });
    await flush();
    const f = records.find((r) => r.action === 'pipeline.run.failed');
    expect(f).toMatchObject({
      pipelineId: 'p7',
      decision: 'failed',
      outcome: 'error',
      trace: { error: 'boom' },
    });
  });

  it('does not propagate audit-write rejections to the bus subscriber', async () => {
    const failingMock = jest.fn(() => Promise.reject(new Error('ddb-down')));
    const bus = new StubBus();
    const sub = new ExecutorAuditSubscriber({
      eventBus: bus as unknown as EventBus<PipelineEventMap>,
      auditRepo: { record: failingMock },
    });
    sub.start();
    await bus.emit('pipeline:run:started', {
      runId: 'r8',
      pipelineId: 'p8',
      triggeredBy: { userId: 'u' },
      at: 't',
    });
    await expect(
      bus.emit('pipeline:step:started', {
        runId: 'r8',
        stepId: 's',
        nodeType: 'transform',
        at: 't',
      }),
    ).resolves.toBeUndefined();
    await flush();
    expect(failingMock).toHaveBeenCalled();
  });

  // ---------------------------------------------------------------------------
  // LLM observability — full prompt/response in trace, no truncation.
  // ---------------------------------------------------------------------------

  it('maps pipeline:llm:prompt → pipeline.llm.prompt and persists the FULL prompt text in trace', async () => {
    const { bus, records } = makeSubscriber();
    await bus.emit('pipeline:run:started', {
      runId: 'r-llm-1',
      pipelineId: 'p-llm-1',
      triggeredBy: { userId: 'u' },
      at: 't',
    });
    // Long prompt — explicit length to prove no truncation happens.
    const longPrompt = 'You are a helpful assistant. ' + 'x'.repeat(5_000);
    await bus.emit('pipeline:llm:prompt', {
      runId: 'r-llm-1',
      stepId: 'llm-step-1',
      model: 'claude-opus-4-7',
      prompt: longPrompt,
      at: '2026-05-06T12:00:00Z',
    });
    await flush();
    const promptRow = records.find((r) => r.action === 'pipeline.llm.prompt');
    expect(promptRow).toMatchObject({
      action: 'pipeline.llm.prompt',
      actorUserId: 'system',
      pipelineId: 'p-llm-1',
      runId: 'r-llm-1',
      outcome: 'success',
      trace: {
        stepId: 'llm-step-1',
        model: 'claude-opus-4-7',
        at: '2026-05-06T12:00:00Z',
      },
    });
    // Verify the FULL prompt landed verbatim — no slicing/truncating.
    expect((promptRow!.trace as { prompt: string }).prompt).toBe(longPrompt);
    expect((promptRow!.trace as { prompt: string }).prompt.length).toBe(longPrompt.length);
  });

  it('maps pipeline:llm:response → pipeline.llm.response and persists FULL response text + token counts', async () => {
    const { bus, records } = makeSubscriber();
    await bus.emit('pipeline:run:started', {
      runId: 'r-llm-2',
      pipelineId: 'p-llm-2',
      triggeredBy: { userId: 'u' },
      at: 't',
    });
    const longResponse = '# Markdown response\n\n' + 'lorem ipsum '.repeat(500);
    await bus.emit('pipeline:llm:response', {
      runId: 'r-llm-2',
      stepId: 'llm-step-2',
      response: longResponse,
      tokensIn: 1234,
      tokensOut: 5678,
      at: '2026-05-06T12:00:05Z',
    });
    await flush();
    const respRow = records.find((r) => r.action === 'pipeline.llm.response');
    expect(respRow).toMatchObject({
      action: 'pipeline.llm.response',
      actorUserId: 'system',
      pipelineId: 'p-llm-2',
      runId: 'r-llm-2',
      outcome: 'success',
      trace: {
        stepId: 'llm-step-2',
        tokensIn: 1234,
        tokensOut: 5678,
        at: '2026-05-06T12:00:05Z',
      },
    });
    expect((respRow!.trace as { response: string }).response).toBe(longResponse);
    expect((respRow!.trace as { response: string }).response.length).toBe(longResponse.length);
  });

  it('maps pipeline:llm:stream:opened → pipeline.llm.stream.opened (first-token latency marker)', async () => {
    const { bus, records } = makeSubscriber();
    await bus.emit('pipeline:run:started', {
      runId: 'r-llm-3',
      pipelineId: 'p-llm-3',
      triggeredBy: { userId: 'u' },
      at: 't',
    });
    await bus.emit('pipeline:llm:stream:opened', {
      runId: 'r-llm-3',
      stepId: 'llm-step-3',
      model: 'claude-opus-4-7',
      openedAt: '2026-05-06T12:00:00.250Z',
    });
    await flush();
    const openedRow = records.find((r) => r.action === 'pipeline.llm.stream.opened');
    expect(openedRow).toMatchObject({
      action: 'pipeline.llm.stream.opened',
      actorUserId: 'system',
      pipelineId: 'p-llm-3',
      runId: 'r-llm-3',
      outcome: 'success',
      trace: {
        stepId: 'llm-step-3',
        model: 'claude-opus-4-7',
        openedAt: '2026-05-06T12:00:00.250Z',
      },
    });
  });

  it('start() is idempotent', () => {
    const { subscriber, recordMock } = makeSubscriber();
    subscriber.start();
    subscriber.start();
    // No audit calls expected from start() itself.
    expect(recordMock).not.toHaveBeenCalled();
  });
});
