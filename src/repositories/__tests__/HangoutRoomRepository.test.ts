// platform-api/src/repositories/__tests__/HangoutRoomRepository.test.ts
//
// Tests for HangoutRoomRepository: createRoom (happy path + slug conflict),
// listRoomsByOrg (GSI query + cursor), archiveRoom (state/ttl flip),
// touchLastActive (best-effort, never throws).

import {
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  HangoutRoomRepository,
  RoomSlugConflictError,
  type HangoutRoom,
} from '../HangoutRoomRepository';

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const docClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

const SAMPLE_ROOM: HangoutRoom = {
  orgId: 'org-1',
  roomId: '01HX',
  slug: 'team-standup',
  name: 'Team Standup',
  createdBy: 'user-1',
  createdAt: '2026-05-23T00:00:00.000Z',
  visibility: 'org',
  settings: { audioOnly: false, recordingEnabled: false, softCap: 25 },
  state: 'active',
  lastActiveAt: '2026-05-23T00:00:00.000Z',
};

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// createRoom
// ---------------------------------------------------------------------------

describe('HangoutRoomRepository.createRoom', () => {
  test('writes the room when slug is free', async () => {
    // First call (slug lookup via GSI) returns empty.
    // Second call (PutItem) returns ok.
    mockSend
      .mockResolvedValueOnce({ Items: [] })
      .mockResolvedValueOnce({});

    const repo = new HangoutRoomRepository(docClient);
    const room = await repo.createRoom({
      orgId: 'org-1',
      slug: 'team-standup',
      name: 'Team Standup',
      visibility: 'org',
      settings: { audioOnly: false, recordingEnabled: false, softCap: 25 },
      createdBy: 'user-1',
    });

    expect(room.slug).toBe('team-standup');
    expect(room.orgId).toBe('org-1');
    expect(room.state).toBe('active');
    expect(room.visibility).toBe('org');
    expect(typeof room.roomId).toBe('string');
    expect(room.roomId.length).toBeGreaterThan(0);

    // Verify the put hit the rooms table with the right item.
    const putCall = mockSend.mock.calls.find(
      ([cmd]) => cmd instanceof PutCommand,
    );
    expect(putCall).toBeDefined();
    const putCmd = putCall![0] as PutCommand;
    expect(putCmd.input.TableName).toBe('test-hangout-rooms');
    expect(putCmd.input.Item).toMatchObject({
      orgId: 'org-1',
      slug: 'team-standup',
      name: 'Team Standup',
      visibility: 'org',
      state: 'active',
      createdBy: 'user-1',
    });
  });

  test('throws RoomSlugConflictError when slug already exists in org', async () => {
    // Lookup returns an existing room.
    mockSend.mockResolvedValueOnce({ Items: [SAMPLE_ROOM] });

    const repo = new HangoutRoomRepository(docClient);
    await expect(
      repo.createRoom({
        orgId: 'org-1',
        slug: 'team-standup',
        name: 'Team Standup',
        visibility: 'org',
        settings: { audioOnly: false, recordingEnabled: false, softCap: 25 },
        createdBy: 'user-1',
      }),
    ).rejects.toBeInstanceOf(RoomSlugConflictError);

    // Should NOT have written anything.
    const putCalls = mockSend.mock.calls.filter(
      ([cmd]) => cmd instanceof PutCommand,
    );
    expect(putCalls).toHaveLength(0);
  });

  test('omits description when not provided', async () => {
    mockSend.mockResolvedValueOnce({ Items: [] }).mockResolvedValueOnce({});
    const repo = new HangoutRoomRepository(docClient);
    const room = await repo.createRoom({
      orgId: 'org-1',
      slug: 's',
      name: 'n',
      visibility: 'org',
      settings: { audioOnly: false, recordingEnabled: false, softCap: 25 },
      createdBy: 'u',
    });
    expect(room.description).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// listRoomsByOrg
// ---------------------------------------------------------------------------

describe('HangoutRoomRepository.listRoomsByOrg', () => {
  test('queries the recency GSI with descending order + state filter', async () => {
    mockSend.mockResolvedValueOnce({ Items: [SAMPLE_ROOM] });

    const repo = new HangoutRoomRepository(docClient);
    const result = await repo.listRoomsByOrg('org-1', { state: 'active', limit: 10 });

    expect(result.items).toHaveLength(1);
    expect(result.items[0]?.slug).toBe('team-standup');
    expect(result.cursor).toBeUndefined();

    const queryCmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(queryCmd.input.TableName).toBe('test-hangout-rooms');
    expect(queryCmd.input.IndexName).toBe('orgId-state-lastActiveAt-index');
    expect(queryCmd.input.ScanIndexForward).toBe(false);
    expect(queryCmd.input.Limit).toBe(10);
    expect(queryCmd.input.KeyConditionExpression).toBe('orgId = :o');
    expect(queryCmd.input.FilterExpression).toBe('#st = :st');
    expect(queryCmd.input.ExpressionAttributeNames).toEqual({ '#st': 'state' });
    expect(queryCmd.input.ExpressionAttributeValues).toEqual({
      ':o': 'org-1',
      ':st': 'active',
    });
  });

  test('round-trips a cursor when more pages are available', async () => {
    const lastKey = { orgId: 'org-1', roomId: 'r-1', lastActiveAt: 't' };
    mockSend.mockResolvedValueOnce({
      Items: [SAMPLE_ROOM],
      LastEvaluatedKey: lastKey,
    });

    const repo = new HangoutRoomRepository(docClient);
    const page1 = await repo.listRoomsByOrg('org-1', { limit: 1 });
    expect(page1.cursor).toBeDefined();
    const decoded = JSON.parse(Buffer.from(page1.cursor!, 'base64url').toString('utf8'));
    expect(decoded).toEqual(lastKey);

    // Subsequent call should pass the same key as ExclusiveStartKey.
    mockSend.mockResolvedValueOnce({ Items: [] });
    await repo.listRoomsByOrg('org-1', { limit: 1, cursor: page1.cursor! });
    const secondQuery = mockSend.mock.calls[1][0] as QueryCommand;
    expect(secondQuery.input.ExclusiveStartKey).toEqual(lastKey);
  });

  test('defaults state filter to "active" when omitted', async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    const repo = new HangoutRoomRepository(docClient);
    await repo.listRoomsByOrg('org-1');
    const queryCmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(queryCmd.input.ExpressionAttributeValues).toMatchObject({ ':st': 'active' });
  });
});

// ---------------------------------------------------------------------------
// archiveRoom
// ---------------------------------------------------------------------------

describe('HangoutRoomRepository.archiveRoom', () => {
  test('sets state=archived + ttl + archivedAt via UpdateCommand', async () => {
    const repo = new HangoutRoomRepository(docClient);
    await repo.archiveRoom('org-1', 'room-1');

    const updateCmd = mockSend.mock.calls[0][0] as UpdateCommand;
    expect(updateCmd.input.TableName).toBe('test-hangout-rooms');
    expect(updateCmd.input.Key).toEqual({ orgId: 'org-1', roomId: 'room-1' });
    expect(updateCmd.input.UpdateExpression).toBe(
      'SET #st = :s, archivedAt = :a, #ttl = :t, lastActiveAt = :a',
    );
    expect(updateCmd.input.ExpressionAttributeNames).toEqual({
      '#st': 'state',
      '#ttl': 'ttl',
    });
    const vals = updateCmd.input.ExpressionAttributeValues!;
    expect(vals[':s']).toBe('archived');
    expect(typeof vals[':a']).toBe('string');
    expect(typeof vals[':t']).toBe('number');
    // ttl should be ~90 days out from now (epoch seconds).
    const nowSec = Math.floor(Date.now() / 1000);
    expect(vals[':t']).toBeGreaterThan(nowSec + 89 * 86400);
    expect(vals[':t']).toBeLessThan(nowSec + 91 * 86400);
    expect(updateCmd.input.ConditionExpression).toBe('attribute_exists(roomId)');
  });
});

// ---------------------------------------------------------------------------
// touchLastActive
// ---------------------------------------------------------------------------

describe('HangoutRoomRepository.touchLastActive', () => {
  test('issues an UpdateCommand that only bumps lastActiveAt', async () => {
    const repo = new HangoutRoomRepository(docClient);
    await repo.touchLastActive('org-1', 'room-1');
    const updateCmd = mockSend.mock.calls[0][0] as UpdateCommand;
    expect(updateCmd.input.UpdateExpression).toBe('SET lastActiveAt = :la');
    expect(updateCmd.input.Key).toEqual({ orgId: 'org-1', roomId: 'room-1' });
  });

  test('swallows DDB errors (best-effort)', async () => {
    mockSend.mockRejectedValueOnce(new Error('boom'));
    const repo = new HangoutRoomRepository(docClient);
    await expect(repo.touchLastActive('org-1', 'room-1')).resolves.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// getRoomBySlug
// ---------------------------------------------------------------------------

describe('HangoutRoomRepository.getRoomBySlug', () => {
  test('queries the slug GSI and returns the first match', async () => {
    mockSend.mockResolvedValueOnce({ Items: [SAMPLE_ROOM] });
    const repo = new HangoutRoomRepository(docClient);
    const room = await repo.getRoomBySlug('org-1', 'team-standup');
    expect(room?.slug).toBe('team-standup');

    const queryCmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(queryCmd.input.IndexName).toBe('orgId-slug-index');
    expect(queryCmd.input.Limit).toBe(1);
    expect(queryCmd.input.ExpressionAttributeValues).toEqual({
      ':o': 'org-1',
      ':s': 'team-standup',
    });
  });

  test('returns null when no match', async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    const repo = new HangoutRoomRepository(docClient);
    expect(await repo.getRoomBySlug('org-1', 'nope')).toBeNull();
  });
});
