import { PlatformWorkGraphSourcePublisher } from '../publisher';

const resolve = jest.fn();
const publish = jest.fn();
const producer = new PlatformWorkGraphSourcePublisher({
  accounts: { resolve },
  publisher: { publish },
  credentialId: 'server-credential',
  now: () => new Date('2026-09-19T12:00:05.000Z'),
});

beforeEach(() => {
  resolve.mockReset();
  publish.mockReset();
  resolve.mockResolvedValue({ organizationId: 'org-1', actorId: 'person-1' });
  publish.mockResolvedValue(undefined);
});

describe('PlatformWorkGraphSourcePublisher', () => {
  it('publishes a pipeline transition only after resolving its durable source-account join', async () => {
    const result = await producer.pipeline({
      kind: 'pipeline-run',
      run: { runId: 'run-1', pipelineId: 'pipe-1', status: 'completed' },
      attributedSubject: 'subject-1',
      occurredAt: '2026-09-19T12:00:00.000Z',
    }, { organizationId: 'org-1', sourceSubject: 'subject-1' });

    expect(result.published).toBe(true);
    expect(resolve).toHaveBeenCalledWith('pipeline', 'platform-api', 'org-1', 'subject-1');
    expect(publish).toHaveBeenCalledWith('work-graph.pipeline.lifecycle.v1', expect.objectContaining({
      actor: { actorId: 'person-1', organizationId: 'org-1', sourceSubject: 'subject-1' },
      producer: { serviceId: 'platform-api', organizationId: 'org-1', credentialId: 'server-credential' },
    }));
  });

  it('fails closed without a source account and never publishes a caller-selected actor', async () => {
    resolve.mockResolvedValue(null);
    await expect(producer.pipeline({
      kind: 'pipeline-run', run: { runId: 'run-1', pipelineId: 'pipe-1', status: 'running' },
      attributedSubject: 'subject-1', occurredAt: '2026-09-19T12:00:00.000Z',
    }, { organizationId: 'org-1', sourceSubject: 'subject-1' })).resolves.toEqual({
      published: false, reason: 'source-account-unmapped',
    });
    expect(publish).not.toHaveBeenCalled();
  });

  it('emits explicit recording and transcript terminal transitions without media content', async () => {
    const recording = await producer.meeting({
      kind: 'recording', meetingId: 'call-1', recordingId: 'rec-1', recordingOwnerId: 'person-1',
      eventType: 'sfu.recording.completed', organizationId: 'org-1', participantSubject: 'subject-1',
      occurredAt: '2026-09-19T12:00:00.000Z', destination: 's3://private',
    }, { organizationId: 'org-1', sourceSubject: 'subject-1' });
    const transcript = await producer.meeting({
      kind: 'transcript', meetingId: 'call-1', recordingId: 'rec-1', transcriptId: 'tx-1',
      recordingOwnerId: 'person-1', status: 'deleted', organizationId: 'org-1', participantSubject: 'subject-1',
      occurredAt: '2026-09-19T12:05:00.000Z', transcriptText: 'private',
    }, { organizationId: 'org-1', sourceSubject: 'subject-1' });
    expect(recording.published).toBe(true);
    expect(transcript.published).toBe(true);
    expect(publish.mock.calls.map(([name]) => name)).toEqual([
      'work-graph.meeting.lifecycle.v1', 'work-graph.meeting.lifecycle.v1',
    ]);
    expect(publish.mock.calls[1][1].payload).not.toHaveProperty('transcriptText');
  });
});
