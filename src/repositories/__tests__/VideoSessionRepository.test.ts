// social-api/src/repositories/__tests__/VideoSessionRepository.test.ts
//
// Tests for VideoSessionRepository: createSession, endSession (SET status+endedAt),
// addParticipant (list_append), getSession (found/null), getSessionsByDocument
// (sorted newest-first by startedAt).

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { VideoSessionRepository, type VideoSessionRecord, type VideoSessionParticipant } from '../VideoSessionRepository';

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const docClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

const SESSION: VideoSessionRecord = {
  documentId: 'doc-1',
  sessionId: 'sess-1',
  channelArn: 'arn:aws:ivs:us-east-1:123:channel/abc',
  streamKey: 'sk-abc123',
  status: 'active',
  startedAt: '2026-01-01T10:00:00Z',
  startedBy: 'user-1',
  participants: [],
};

const PARTICIPANT: VideoSessionParticipant = {
  userId: 'user-2',
  displayName: 'Bob',
  joinedAt: '2026-01-01T10:05:00Z',
};

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// createSession
// ---------------------------------------------------------------------------

describe('VideoSessionRepository.createSession', () => {
  it('sends PutCommand with the session record', async () => {
    const repo = new VideoSessionRepository(docClient);
    await repo.createSession(SESSION);

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.TableName).toBe('test-document-video-sessions');
    expect(cmd.input.Item).toMatchObject({ documentId: 'doc-1', sessionId: 'sess-1' });
  });

  it('returns the session record', async () => {
    const repo = new VideoSessionRepository(docClient);
    const result = await repo.createSession(SESSION);
    expect(result).toEqual(SESSION);
  });
});

// ---------------------------------------------------------------------------
// endSession
// ---------------------------------------------------------------------------

describe('VideoSessionRepository.endSession', () => {
  it('sends UpdateCommand SET status=ended and endedAt', async () => {
    const repo = new VideoSessionRepository(docClient);
    await repo.endSession('doc-1', 'sess-1', '2026-01-01T11:00:00Z');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ documentId: 'doc-1', sessionId: 'sess-1' });
    expect(cmd.input.UpdateExpression).toContain('#status');
    expect(cmd.input.ExpressionAttributeNames['#status']).toBe('status');
    expect(cmd.input.ExpressionAttributeValues[':status']).toBe('ended');
    expect(cmd.input.ExpressionAttributeValues[':endedAt']).toBe('2026-01-01T11:00:00Z');
  });

  it('stamps a TTL ~7 days past endedAt for DDB native expiry', async () => {
    const repo = new VideoSessionRepository(docClient);
    const endedAt = '2026-01-01T11:00:00Z';
    await repo.endSession('doc-1', 'sess-1', endedAt);

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toContain('#ttl');
    expect(cmd.input.ExpressionAttributeNames['#ttl']).toBe('ttl');
    const ttl = cmd.input.ExpressionAttributeValues[':ttl'] as number;
    const expected = Math.floor(Date.parse(endedAt) / 1000) + 7 * 24 * 60 * 60;
    expect(ttl).toBe(expected);
  });
});

// ---------------------------------------------------------------------------
// addParticipant
// ---------------------------------------------------------------------------

describe('VideoSessionRepository.addParticipant', () => {
  it('sends UpdateCommand using list_append expression', async () => {
    const repo = new VideoSessionRepository(docClient);
    await repo.addParticipant('doc-1', 'sess-1', PARTICIPANT);

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ documentId: 'doc-1', sessionId: 'sess-1' });
    expect(cmd.input.UpdateExpression).toContain('list_append');
    expect(cmd.input.ExpressionAttributeValues[':participant']).toEqual([PARTICIPANT]);
    expect(cmd.input.ExpressionAttributeValues[':empty']).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// getSession
// ---------------------------------------------------------------------------

describe('VideoSessionRepository.getSession', () => {
  it('returns the session when found', async () => {
    mockSend.mockResolvedValue({ Item: SESSION });
    const repo = new VideoSessionRepository(docClient);
    const result = await repo.getSession('doc-1', 'sess-1');
    expect(result).toEqual(SESSION);
  });

  it('sends GetCommand with composite key', async () => {
    mockSend.mockResolvedValue({ Item: SESSION });
    const repo = new VideoSessionRepository(docClient);
    await repo.getSession('doc-1', 'sess-1');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ documentId: 'doc-1', sessionId: 'sess-1' });
  });

  it('returns null when session is not found', async () => {
    mockSend.mockResolvedValue({});
    const repo = new VideoSessionRepository(docClient);
    const result = await repo.getSession('doc-1', 'nope');
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// getSessionsByDocument
// ---------------------------------------------------------------------------

describe('VideoSessionRepository.getSessionsByDocument', () => {
  it('queries by documentId', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new VideoSessionRepository(docClient);
    await repo.getSessionsByDocument('doc-1');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ExpressionAttributeValues[':docId']).toBe('doc-1');
  });

  it('returns sessions sorted newest-first by startedAt', async () => {
    const items: VideoSessionRecord[] = [
      { ...SESSION, sessionId: 'a', startedAt: '2026-01-01T08:00:00Z' },
      { ...SESSION, sessionId: 'b', startedAt: '2026-01-01T10:00:00Z' },
      { ...SESSION, sessionId: 'c', startedAt: '2026-01-01T09:00:00Z' },
    ];
    mockSend.mockResolvedValue({ Items: items });
    const repo = new VideoSessionRepository(docClient);
    const result = await repo.getSessionsByDocument('doc-1');
    expect(result.map((s) => s.sessionId)).toEqual(['b', 'c', 'a']);
  });
});
