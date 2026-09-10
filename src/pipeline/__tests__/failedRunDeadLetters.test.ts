// A failed run is a dead letter; a redrive runs it again with what it was
// triggered with. The bus and the DLQ are stubs — what is under test is the
// bookkeeping between three events that each carry a different third of the
// story (run:started has the pipeline, the trigger step has the payload,
// run:failed has the error).
import { attachFailedRunDeadLetters, retriggerFromDeadLetter, isRunRetrigger, RUN_RETRIGGER_EVENT, type RunRetriggerPayload } from '../failedRunDeadLetters';

type Handler = (e: { payload: unknown }) => void | Promise<void>;

function fakeBus() {
  const handlers = new Map<string, Handler[]>();
  const unsubscribed: string[] = [];
  let n = 0;
  return {
    subscribe(type: string, h: Handler) { handlers.set(type, [...(handlers.get(type) ?? []), h]); return `sub-${++n}`; },
    unsubscribe(id: string) { unsubscribed.push(id); },
    async emit(type: string, payload: unknown) { for (const h of handlers.get(type) ?? []) await h({ payload }); },
    unsubscribed,
  };
}

function fakeDlq() {
  const puts: Array<{ env: { id: string; body: { type: string; payload: RunRetriggerPayload } }; meta: { lastError: string; failedAtMs: number } }> = [];
  return {
    puts,
    put: async (env: unknown, meta: unknown) => { puts.push({ env: env as never, meta: meta as never }); },
  };
}

describe('attachFailedRunDeadLetters', () => {
  it('files a failed run under its pipeline with the trigger payload and the failing node', async () => {
    const bus = fakeBus();
    const dlq = fakeDlq();
    attachFailedRunDeadLetters({ eventBus: bus, dlq: dlq as never, now: () => 1000 });

    await bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'recording-finalize', triggeredBy: { userId: 'system', triggerType: 'manual', payload: {} } });
    await bus.emit('pipeline:step:completed', { runId: 'r1', stepId: 'trigger', output: { arn: 'arn:x', recordingId: 'rec-1' } });
    await bus.emit('pipeline:step:completed', { runId: 'r1', stepId: 'resolve', output: { lobbyName: 'design' } });
    await bus.emit('pipeline:run:failed', { runId: 'r1', error: { nodeId: 'await-compose', message: 'compose is still queued' }, at: 'now' });

    expect(dlq.puts).toHaveLength(1);
    const [{ env, meta }] = dlq.puts;
    expect(env.body.type).toBe(RUN_RETRIGGER_EVENT);
    expect(isRunRetrigger(env.body)).toBe(true);
    expect(env.body.payload).toMatchObject({
      pipelineId: 'recording-finalize',
      failedRunId: 'r1',
      // The FIRST completed step is the trigger; a later step's output must not replace it.
      triggerPayload: { arn: 'arn:x', recordingId: 'rec-1' },
      triggeredBy: { userId: 'system' },
      error: { nodeId: 'await-compose', message: 'compose is still queued' },
    });
    expect(meta.lastError).toBe('PipelineRunFailed: [await-compose] compose is still queued');
    expect(meta.failedAtMs).toBe(1000);
  });

  it('does not file a run that completed, and forgets it', async () => {
    const bus = fakeBus();
    const dlq = fakeDlq();
    attachFailedRunDeadLetters({ eventBus: bus, dlq: dlq as never });
    await bus.emit('pipeline:run:started', { runId: 'r2', pipelineId: 'p', triggeredBy: { triggerType: 'manual', payload: {} } });
    await bus.emit('pipeline:run:completed', { runId: 'r2', durationMs: 1, at: 'now' });
    // A late failure for a run already gone: nothing to file with, nothing filed.
    await bus.emit('pipeline:run:failed', { runId: 'r2', error: { nodeId: 'x', message: 'late' }, at: 'now' });
    expect(dlq.puts).toHaveLength(0);
  });

  it('ignores a failure for a run it never saw start', async () => {
    const bus = fakeBus();
    const dlq = fakeDlq();
    attachFailedRunDeadLetters({ eventBus: bus, dlq: dlq as never });
    await bus.emit('pipeline:run:failed', { runId: 'ghost', error: { nodeId: 'x', message: 'boom' }, at: 'now' });
    expect(dlq.puts).toHaveLength(0);
  });

  it('detaches every subscription', () => {
    const bus = fakeBus();
    const detach = attachFailedRunDeadLetters({ eventBus: bus, dlq: fakeDlq() as never });
    detach();
    expect(bus.unsubscribed).toHaveLength(5);
  });
});

describe('retriggerFromDeadLetter', () => {
  const payload: RunRetriggerPayload = {
    pipelineId: 'call-transcription', failedRunId: 'r-old',
    triggerPayload: { recordingId: 'rec-9' }, triggeredBy: { userId: 'dev-grace' },
    error: { nodeId: 'transcribe', message: 'whisper timed out' },
  };

  it('runs the same pipeline again with the same payload, marked as a redrive', async () => {
    const trigger = jest.fn(async () => ({ runId: 'r-new' }));
    const resolveDefinition = jest.fn(async () => ({ id: 'call-transcription', nodes: [] }));
    const out = await retriggerFromDeadLetter(payload, { resolveDefinition, trigger });
    expect(out.runId).toBe('r-new');
    expect(resolveDefinition).toHaveBeenCalledWith('call-transcription', 'dev-grace');
    expect(trigger).toHaveBeenCalledWith({
      pipelineId: 'call-transcription',
      definition: { id: 'call-transcription', nodes: [] },
      triggerPayload: { recordingId: 'rec-9', redriveOf: 'r-old' },
      triggeredBy: { userId: 'dev-grace' },
    });
  });

  it('refuses when the pipeline no longer has a definition', async () => {
    const trigger = jest.fn(async () => ({ runId: 'never' }));
    await expect(retriggerFromDeadLetter(payload, { resolveDefinition: async () => null, trigger })).rejects.toThrow(/no definition/);
    expect(trigger).not.toHaveBeenCalled();
  });
});

describe('attachFailedRunDeadLetters — durable copy', () => {
  it('persists the same entry it put in memory, dated when the run failed, expiring in 30 days', async () => {
    const bus = fakeBus();
    const dlq = fakeDlq();
    const persist = jest.fn(async () => undefined);
    const failedAtMs = Date.parse('2026-09-09T12:00:00.000Z');
    attachFailedRunDeadLetters({ eventBus: bus, dlq: dlq as never, persist, now: () => failedAtMs });

    await bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'call-transcription', triggeredBy: { userId: 'dev-grace', triggerType: 'manual', payload: {} } });
    await bus.emit('pipeline:step:completed', { runId: 'r1', stepId: 'trigger', output: { recordingId: 'rec-1' } });
    await bus.emit('pipeline:run:failed', { runId: 'r1', error: { nodeId: 'transcribe', message: 'no file' }, at: 'now' });

    expect(persist).toHaveBeenCalledTimes(1);
    const [entry] = (persist.mock.calls[0] as unknown as [import('../../repositories/DLQRepository').DLQEntry]);
    expect(entry).toEqual({
      messageId: 'run-failed:r1',
      eventType: RUN_RETRIGGER_EVENT,
      eventPayload: {
        pipelineId: 'call-transcription',
        failedRunId: 'r1',
        triggerPayload: { recordingId: 'rec-1' },
        triggeredBy: { userId: 'dev-grace', triggerType: 'manual' },
        error: { nodeId: 'transcribe', message: 'no file' },
      },
      lastError: 'PipelineRunFailed: [transcribe] no file',
      failedAt: '2026-09-09T12:00:00.000Z',
      totalAttempts: 1,
      ttl: Math.floor(failedAtMs / 1000) + 30 * 24 * 60 * 60,
    });
    // The durable id and the in-memory id are the same so a rehydrate is idempotent.
    expect(dlq.puts[0].env.id).toBe(entry.messageId);
  });

  it('keeps the in-memory entry when the store write fails', async () => {
    const bus = fakeBus();
    const dlq = fakeDlq();
    const persist = jest.fn(async () => { throw new Error('ddb down'); });
    attachFailedRunDeadLetters({ eventBus: bus, dlq: dlq as never, persist });
    await bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'p', triggeredBy: { triggerType: 'manual', payload: {} } });
    await bus.emit('pipeline:run:failed', { runId: 'r1', error: { nodeId: 'x', message: 'boom' }, at: 'now' });
    expect(dlq.puts).toHaveLength(1);
    expect(persist).toHaveBeenCalledTimes(1);
  });

  it('does not persist when the in-memory put failed', async () => {
    const bus = fakeBus();
    const persist = jest.fn(async () => undefined);
    attachFailedRunDeadLetters({ eventBus: bus, dlq: { put: async () => { throw new Error('full'); } } as never, persist });
    await bus.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'p', triggeredBy: { triggerType: 'manual', payload: {} } });
    await bus.emit('pipeline:run:failed', { runId: 'r1', error: { nodeId: 'x', message: 'boom' }, at: 'now' });
    expect(persist).not.toHaveBeenCalled();
  });
});

describe('retriggerFromDeadLetter — lineage', () => {
  const payload: RunRetriggerPayload = {
    pipelineId: 'call-transcription', failedRunId: 'r-old',
    triggerPayload: { recordingId: 'rec-9' }, triggeredBy: { userId: 'dev-grace' },
    error: { nodeId: 'transcribe', message: 'whisper timed out' },
  };

  it('records the new run on the original after the trigger returns', async () => {
    const order: string[] = [];
    const trigger = jest.fn(async () => { order.push('trigger'); return { runId: 'r-new' }; });
    const linkRedrive = jest.fn(async () => { order.push('link'); });
    await retriggerFromDeadLetter(payload, { resolveDefinition: async () => ({ id: 'call-transcription' }), trigger, linkRedrive });
    expect(linkRedrive).toHaveBeenCalledWith({ pipelineId: 'call-transcription', failedRunId: 'r-old', newRunId: 'r-new' });
    expect(order).toEqual(['trigger', 'link']);
  });

  it('still reports the new run when the link cannot be written', async () => {
    const trigger = jest.fn(async () => ({ runId: 'r-new' }));
    const linkRedrive = jest.fn(async () => { throw new Error('row gone'); });
    const out = await retriggerFromDeadLetter(payload, { resolveDefinition: async () => ({ id: 'p' }), trigger, linkRedrive });
    expect(out.runId).toBe('r-new');
  });

  it('does not link when the trigger itself fails', async () => {
    const linkRedrive = jest.fn(async () => undefined);
    await expect(retriggerFromDeadLetter(payload, { resolveDefinition: async () => ({ id: 'p' }), trigger: async () => { throw new Error('engine down'); }, linkRedrive })).rejects.toThrow(/engine down/);
    expect(linkRedrive).not.toHaveBeenCalled();
  });
});
