// platform-api/src/repositories/__tests__/CallRecordingRepository.test.ts
//
// Owner fan-out regression: when a recording arrives with NO participants[]
// (e.g. caller dropped before any /join minted a participant token), the
// fan-out must still write ONE row keyed by `startedBy` so the recording
// owner sees it in their "Past Calls" list. Without this, the row is
// stranded in the primary table and invisible to user-scoped queries.
//
// Also covers W6: listByLobbyName — room-scoped recording filter used by
// the RoomAdminPage "View recordings" link. v1 is a Scan + FilterExpression
// since lobbyName has no GSI yet; we cap at 100 rows per page.

import { DynamoDBDocumentClient, PutCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { CallRecordingRepository } from '../CallRecordingRepository';

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const docClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

describe('CallRecordingRepository.putRecording — owner fan-out', () => {
  test('empty participants[] but startedBy=u-hank writes one fan-out row for u-hank', async () => {
    const repo = new CallRecordingRepository(docClient);

    await repo.putRecording({
      callId: 'c1',
      recordingId: 'r1',
      channelArn: 'arn:x',
      lobbyName: 'lobby-x',
      sessionId: 'sess-1',
      startedAt: '2026-05-23T00:00:00.000Z',
      endedAt: '2026-05-23T00:01:00.000Z',
      durationMs: 60_000,
      totalBytes: 1024,
      segments: [],
      destination: 'local',
      participants: [],            // ← empty on purpose
      startedBy: 'u-hank',
    });

    // Find the PutCommand sent at the participants table.
    const participantPuts = mockSend.mock.calls
      .map((c) => c[0])
      .filter(
        (cmd) =>
          cmd instanceof PutCommand &&
          cmd.input.TableName === 'test-video-call-recording-participants',
      );

    expect(participantPuts).toHaveLength(1);
    const item = (participantPuts[0] as PutCommand).input.Item!;
    expect(item.userId).toBe('u-hank');
    expect(item.callId).toBe('c1');
    expect(item.recordingId).toBe('r1');
    expect(item.channelArn).toBe('arn:x');
    expect(item.startedAtRecordingId).toBe('2026-05-23T00:00:00.000Z#r1');
  });

  test('empty participants[] AND no startedBy → no fan-out rows written', async () => {
    const repo = new CallRecordingRepository(docClient);

    await repo.putRecording({
      callId: 'c2',
      recordingId: 'r2',
      channelArn: 'arn:y',
      lobbyName: 'lobby-y',
      startedAt: '2026-05-23T01:00:00.000Z',
      endedAt: '2026-05-23T01:01:00.000Z',
      durationMs: 60_000,
      totalBytes: 0,
      segments: [],
      destination: 'local',
      participants: [],
      // no startedBy
    });

    const participantPuts = mockSend.mock.calls
      .map((c) => c[0])
      .filter(
        (cmd) =>
          cmd instanceof PutCommand &&
          cmd.input.TableName === 'test-video-call-recording-participants',
      );

    expect(participantPuts).toHaveLength(0);

    // Primary table write still happened.
    const primaryPuts = mockSend.mock.calls
      .map((c) => c[0])
      .filter(
        (cmd) =>
          cmd instanceof PutCommand &&
          cmd.input.TableName === 'test-video-call-recordings',
      );
    expect(primaryPuts).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// W6 — listByLobbyName: room-scoped recording filter (RoomAdminPage)
// ---------------------------------------------------------------------------

describe('CallRecordingRepository.listByLobbyName', () => {
  test('issues a Scan filtered by lobbyName with the configured cap', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({
      Items: [
        {
          callId: 'c1',
          recordingId: 'r1',
          startedAt: '2026-05-23T00:00:00.000Z',
          endedAt: '2026-05-23T00:05:00.000Z',
          durationMs: 300_000,
          channelArn: 'arn:x',
          lobbyName: 'room:lounge',
          thumbnailUrl: 'http://thumb/1.png',
        },
        {
          callId: 'c2',
          recordingId: 'r2',
          startedAt: '2026-05-23T01:00:00.000Z',
          endedAt: '2026-05-23T01:10:00.000Z',
          durationMs: 600_000,
          channelArn: 'arn:y',
          lobbyName: 'room:lounge',
        },
      ],
      LastEvaluatedKey: { callId: 'c2', recordingId: 'r2' },
    });

    const out = await repo.listByLobbyName('room:lounge', 50);

    const scans = mockSend.mock.calls
      .map((c) => c[0])
      .filter((cmd) => cmd instanceof ScanCommand) as ScanCommand[];
    expect(scans).toHaveLength(1);
    expect(scans[0].input.TableName).toBe('test-video-call-recordings');
    expect(scans[0].input.FilterExpression).toBe('lobbyName = :l');
    expect(scans[0].input.ExpressionAttributeValues).toEqual({ ':l': 'room:lounge' });
    expect(scans[0].input.Limit).toBe(50);

    expect(out.items).toHaveLength(2);
    expect(out.items[0]).toEqual({
      callId: 'c1',
      recordingId: 'r1',
      startedAt: '2026-05-23T00:00:00.000Z',
      endedAt: '2026-05-23T00:05:00.000Z',
      durationMs: 300_000,
      channelArn: 'arn:x',
      lobbyName: 'room:lounge',
      thumbnailUrl: 'http://thumb/1.png',
    });
    expect(out.cursor).toBeDefined();
    const decoded = JSON.parse(Buffer.from(out.cursor!, 'base64').toString('utf8'));
    expect(decoded).toEqual({ callId: 'c2', recordingId: 'r2' });
  });

  test('caps the Limit at 100 even when the caller asks for more', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({ Items: [] });

    await repo.listByLobbyName('room:lounge', 500);

    const scan = mockSend.mock.calls
      .map((c) => c[0])
      .find((cmd) => cmd instanceof ScanCommand) as ScanCommand | undefined;
    expect(scan?.input.Limit).toBe(100);
  });

  test('decodes the cursor back into ExclusiveStartKey', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({ Items: [] });

    const cursor = Buffer.from(
      JSON.stringify({ callId: 'cX', recordingId: 'rX' }),
      'utf8',
    ).toString('base64');
    await repo.listByLobbyName('room:lounge', 25, cursor);

    const scan = mockSend.mock.calls
      .map((c) => c[0])
      .find((cmd) => cmd instanceof ScanCommand) as ScanCommand | undefined;
    expect(scan?.input.ExclusiveStartKey).toEqual({ callId: 'cX', recordingId: 'rX' });
  });

  test('returns empty cursor when DDB has no LastEvaluatedKey', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({ Items: [] });

    const out = await repo.listByLobbyName('room:lounge', 50);
    expect(out.cursor).toBeUndefined();
  });

  test('silently ignores an unparseable cursor (starts from top)', async () => {
    const repo = new CallRecordingRepository(docClient);
    mockSend.mockResolvedValueOnce({ Items: [] });

    await repo.listByLobbyName('room:lounge', 25, 'not-base64-json');

    const scan = mockSend.mock.calls
      .map((c) => c[0])
      .find((cmd) => cmd instanceof ScanCommand) as ScanCommand | undefined;
    expect(scan?.input.ExclusiveStartKey).toBeUndefined();
  });
});
