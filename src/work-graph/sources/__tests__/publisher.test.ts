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

describe('a deck published by the platform', () => {
  const identity = { organizationId: 'dev-org', sourceSubject: 'dev-frank' };
  const record = {
    kind: 'document-revision' as const,
    documentId: 'deck-1',
    revisionId: 'v1',
    occurredAt: '2026-09-22T04:36:48.000Z',
    safeLabel: 'Sprint review',
    producedByRunId: 'run-1',
  };

  function publisherWith(rows: Array<[string, string, string, string]>) {
    const publish = jest.fn().mockResolvedValue(undefined);
    const accounts = {
      resolve: jest.fn(async (source: string, producerServiceId: string, org: string, subject: string) => (
        rows.some(([s, p, o, u]) => s === source && p === producerServiceId && o === org && u === subject)
          ? { organizationId: org, actorId: subject }
          : null
      )),
    };
    return {
      publish,
      accounts,
      publisher: new PlatformWorkGraphSourcePublisher({
        accounts: accounts as never,
        publisher: { publish } as never,
        credentialId: 'platform-api-runtime',
      }),
    };
  }

  it('looks up the join the gateway already wrote, not one of its own', async () => {
    // The join binds the PERSON to the source. Its producer is a record of who
    // established it and is not part of the row's key, so a second writer that
    // insisted on its own name found nothing for every person alive.
    const { publisher, publish, accounts } = publisherWith([
      ['document', 'websocket-gateway', 'dev-org', 'dev-frank'],
    ]);
    const result = await publisher.document(record, identity);
    expect(result.published).toBe(true);
    expect(accounts.resolve).toHaveBeenCalledWith('document', 'websocket-gateway', 'dev-org', 'dev-frank');
    expect(publish).toHaveBeenCalledWith('work-graph.document.lifecycle.v1', expect.objectContaining({
      source: 'document',
      producer: expect.objectContaining({ serviceId: 'platform-api' }),
      payload: expect.objectContaining({ documentId: 'deck-1', producedByRunId: 'run-1' }),
    }));
  });

  it('still refuses a person with no document join at all', async () => {
    const { publisher, publish } = publisherWith([['pipeline', 'platform-api', 'dev-org', 'dev-frank']]);
    expect(await publisher.document(record, identity))
      .toEqual({ published: false, reason: 'source-account-unmapped' });
    expect(publish).not.toHaveBeenCalled();
  });

  it('says the platform produced it, whoever the join was recorded under', async () => {
    const { publisher, publish } = publisherWith([['document', 'websocket-gateway', 'dev-org', 'dev-frank']]);
    await publisher.document(record, identity);
    expect((publish.mock.calls[0][1] as { producer: { serviceId: string } }).producer.serviceId).toBe('platform-api');
  });
});
