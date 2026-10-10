// A terminal summary failure is a separate fact from successful transcription.
const failPendingSummaryMock = jest.fn().mockResolvedValue(true);
jest.mock('../../repositories', () => ({ callRecordingRepo: { failPendingSummary: failPendingSummaryMock } }));
jest.mock('../../work-graph/sources/platformPublisher', () => ({ platformWorkGraphSourcePublisher: {} }));

import { RunPersistenceService } from '../../pipeline/runPersistence';
import type { PipelineRunRepository } from '../../repositories/PipelineRunRepository';
import { settleRecordingSummaryFailure } from '../summaryFailure';

function harness() {
  const subscribers = new Map<string, (event: { payload: unknown }) => Promise<void>>();
  const bus = { subscribe: (name: string, fn: (event: { payload: unknown }) => Promise<void>) => { subscribers.set(name, fn); return name; } };
  const put = jest.fn().mockResolvedValue(undefined);
  const service = new RunPersistenceService({
    eventBus: bus as unknown as ConstructorParameters<typeof RunPersistenceService>[0]['eventBus'],
    repository: { put } as unknown as PipelineRunRepository,
  });
  service.start();
  return {
    put,
    emit: async (name: string, payload: unknown) => { await subscribers.get(name)!({ payload }); },
  };
}

beforeEach(() => failPendingSummaryMock.mockClear());

describe('native call-transcription terminal summary projection', () => {
  it.each(['summarize', 'store-summary'])('settles only the summary when %s exhausts its retries', async (nodeId) => {
    const h = harness();
    await h.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'call-transcription', triggeredBy: { triggerType: 'platform' }, at: '2026-10-10T00:00:00Z' });
    await h.emit('pipeline:step:completed', { runId: 'r1', stepId: 'store', output: { callId: 'call-1', recordingId: 'recording-1' }, durationMs: 1, at: '2026-10-10T00:00:01Z' });
    await h.emit('pipeline:run:failed', { runId: 'r1', error: { nodeId, message: 'model credentials unavailable' }, at: '2026-10-10T00:00:02Z' });
    expect(failPendingSummaryMock).toHaveBeenCalledWith('call-1', 'recording-1', 'r1', 'model credentials unavailable');
    expect(h.put).toHaveBeenLastCalledWith(expect.objectContaining({ status: 'failed', error: `${nodeId}: model credentials unavailable` }));
  });

  it.each([
    ['call-transcription', 'transcribe'],
    ['call-transcription', 'publish'],
    ['some-other-pipeline', 'summarize'],
  ])('does not settle unrelated %s / %s failures', async (pipelineId, nodeId) => {
    const h = harness();
    await h.emit('pipeline:run:started', { runId: 'r1', pipelineId, triggeredBy: { triggerType: 'platform' }, at: '2026-10-10T00:00:00Z' });
    await h.emit('pipeline:step:completed', { runId: 'r1', stepId: 'store', output: { callId: 'c1', recordingId: 'rec1' }, durationMs: 1, at: '2026-10-10T00:00:01Z' });
    await h.emit('pipeline:run:failed', { runId: 'r1', error: { nodeId, message: 'terminal failure' }, at: '2026-10-10T00:00:02Z' });
    expect(failPendingSummaryMock).not.toHaveBeenCalled();
  });

  it('keeps a retrying summary pending until the native run actually terminates', async () => {
    const h = harness();
    await h.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'call-transcription', triggeredBy: { triggerType: 'platform' }, at: '2026-10-10T00:00:00Z' });
    await h.emit('pipeline:step:failed', { runId: 'r1', stepId: 'summarize', error: 'temporary model error', at: '2026-10-10T00:00:01Z' });
    expect(failPendingSummaryMock).not.toHaveBeenCalled();
  });

  it('uses the native checkpoint join when a large store output was truncated', async () => {
    const h = harness();
    await h.emit('pipeline:run:started', { runId: 'r1', pipelineId: 'call-transcription', triggeredBy: { triggerType: 'platform' }, at: '2026-10-10T00:00:00Z' });
    await h.emit('pipeline:step:completed', { runId: 'r1', stepId: 'store', output: { transcriptText: 'x'.repeat(110 * 1024), callId: 'call-1', recordingId: 'recording-1' }, durationMs: 1, at: '2026-10-10T00:00:01Z' });
    await h.emit('pipeline:run:checkpoint', { runId: 'r1', context: { callId: 'call-1', recordingId: 'recording-1' }, currentStepIds: ['summarize'], sequenceNumber: 5, at: '2026-10-10T00:00:01Z' });
    await h.emit('pipeline:run:failed', { runId: 'r1', error: { nodeId: 'summarize', message: 'terminal model failure' }, at: '2026-10-10T00:00:02Z' });
    expect(failPendingSummaryMock).toHaveBeenCalledWith('call-1', 'recording-1', 'r1', 'terminal model failure');
  });

  it('does not infer a recording when the native context has no join', async () => {
    await expect(settleRecordingSummaryFailure({ runId: 'r1', context: { callId: 'c1' }, error: 'failed' })).resolves.toBe(false);
    await expect(settleRecordingSummaryFailure({ runId: 'r1', context: null, error: 'failed' })).resolves.toBe(false);
    expect(failPendingSummaryMock).not.toHaveBeenCalled();
  });
});
