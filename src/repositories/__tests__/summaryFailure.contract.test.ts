import { UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { CallRecordingRepository } from '../CallRecordingRepository';

jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

describe('summary failure ownership condition', () => {
  const send = jest.fn();
  const repository = new CallRecordingRepository({ send } as unknown as DynamoDBDocumentClient);
  beforeEach(() => { send.mockReset(); send.mockResolvedValue({}); });

  it('stores the owning native run and clears a previous summary error on regeneration', async () => {
    await repository.updateTranscriptStatus('c1', 'rec1', { status: 'ready', summaryStatus: 'pending', summaryRunId: 'r2', summaryError: null });
    const command = send.mock.calls[0][0] as UpdateCommand;
    expect(command.input.UpdateExpression).toContain('summaryRunId = :sr');
    expect(command.input.UpdateExpression).toContain('REMOVE summaryError');
    expect(command.input.ExpressionAttributeValues?.[':sr']).toBe('r2');
  });
  it('clears old artifacts and ownership when a trusted starter claims a fresh generation', async () => {
    await repository.updateTranscriptStatus('c1', 'rec1', { status: 'pending', processingAttemptId: 'generation-2', summaryStatus: 'pending', summaryRunId: null, summaryMarkdown: null, summaryKey: null, summaryError: null });
    const command = send.mock.calls[0][0] as UpdateCommand;
    expect(command.input.UpdateExpression).toContain('processingAttemptId = :pa');
    expect(command.input.UpdateExpression).toContain('summaryStatusAt = :ssa');
    expect(command.input.UpdateExpression).toContain('REMOVE summaryRunId, summaryError, summaryKey, summaryMarkdown');
    expect(command.input.ConditionExpression).toBe('attribute_exists(callId)');
  });
  it('atomically fences successful summary writes by generation, run and unsettled summary state', async () => {
    await repository.updateTranscriptStatus('c1', 'rec1', { status: 'ready', summaryStatus: 'ready', summaryMarkdown: '## Summary\nCurrent answer.', expectedProcessingAttemptId: 'generation-2', expectedSummaryRunId: 'run-2' });
    const command = send.mock.calls[0][0] as UpdateCommand;
    expect(command.input.ConditionExpression).toBe('attribute_exists(callId) AND processingAttemptId = :epa AND summaryRunId = :esr AND transcriptStatus = :ready AND (summaryStatus IN (:pending, :processing) OR (summaryStatus = :ss AND summaryMarkdown = :sm))');
    expect(command.input.ExpressionAttributeValues).toMatchObject({ ':epa': 'generation-2', ':esr': 'run-2' });
    send.mockRejectedValue(Object.assign(new Error('obsolete run'), { name: 'ConditionalCheckFailedException' }));
    await expect(repository.updateTranscriptStatus('c1', 'rec1', { status: 'ready', summaryStatus: 'ready', expectedSummaryRunId: 'run-1' })).rejects.toThrow('obsolete run');
  });
  it('permits exact same-run result replay but never restarts a settled summary model call', async () => {
    await repository.updateTranscriptStatus('c1', 'rec1', { status: 'ready', summaryStatus: 'processing', expectedSummaryRunId: 'run-2' });
    const command = send.mock.calls[0][0] as UpdateCommand;
    expect(command.input.ConditionExpression).not.toContain(' OR ');
    expect(command.input.ConditionExpression).toContain('summaryStatus IN (:pending, :processing)');
  });

  it('atomically fails only the same run’s pending summary and never rewrites media or transcription', async () => {
    await expect(repository.failPendingSummary('c1', 'rec1', 'r1', 'x'.repeat(300))).resolves.toBe(true);
    const command = send.mock.calls[0][0] as UpdateCommand;
    expect(command).toBeInstanceOf(UpdateCommand);
    expect(command.input.Key).toEqual({ callId: 'c1', recordingId: 'rec1' });
    expect(command.input.ConditionExpression).toBe('summaryRunId = :run AND transcriptStatus = :ready AND summaryStatus IN (:pending, :processing)');
    expect(command.input.UpdateExpression).toBe('SET summaryStatus = :failed, summaryError = :error');
    expect(command.input.ExpressionAttributeValues).toEqual({ ':run': 'r1', ':ready': 'ready', ':pending': 'pending', ':processing': 'processing', ':failed': 'failed', ':error': 'x'.repeat(200) });
  });

  it('treats stale, settled or legacy ownership as a no-op', async () => {
    send.mockRejectedValue(Object.assign(new Error('ownership changed'), { name: 'ConditionalCheckFailedException' }));
    await expect(repository.failPendingSummary('c1', 'rec1', 'r1', 'old error')).resolves.toBe(false);
  });

  it('surfaces storage outages so the native projection can record its failure', async () => {
    send.mockRejectedValue(new Error('storage unavailable'));
    await expect(repository.failPendingSummary('c1', 'rec1', 'r1', 'error')).rejects.toThrow('storage unavailable');
  });
});
