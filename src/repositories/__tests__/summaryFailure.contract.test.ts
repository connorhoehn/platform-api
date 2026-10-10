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
