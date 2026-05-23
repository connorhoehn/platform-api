// platform-api/src/repositories/__tests__/CallRecordingRepository.test.ts
//
// Owner fan-out regression: when a recording arrives with NO participants[]
// (e.g. caller dropped before any /join minted a participant token), the
// fan-out must still write ONE row keyed by `startedBy` so the recording
// owner sees it in their "Past Calls" list. Without this, the row is
// stranded in the primary table and invisible to user-scoped queries.
//
// Tests CallRecordingRepository.putRecording with empty participants[].

import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
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
