// social-api/src/repositories/__tests__/RoomRepository.test.ts
//
// Tests for RoomRepository: room CRUD, conditional create, membership
// operations, getRoomsByUser (Query → BatchGet), addMemberWithOutbox (TransactWrite).

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { RoomRepository, type RoomItem, type RoomMemberItem } from '../RoomRepository';

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const docClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

const ROOM: RoomItem = {
  roomId: 'room-1',
  channelId: 'ch-1',
  name: 'General',
  type: 'standalone',
  ownerId: 'user-1',
  createdAt: '2026-01-01T00:00:00Z',
  updatedAt: '2026-01-01T00:00:00Z',
};

const MEMBER: RoomMemberItem = {
  roomId: 'room-1',
  userId: 'user-1',
  role: 'owner',
  joinedAt: '2026-01-01T00:00:00Z',
};

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// getRoom
// ---------------------------------------------------------------------------

describe('RoomRepository.getRoom', () => {
  it('returns the room when found', async () => {
    mockSend.mockResolvedValue({ Item: ROOM });
    const repo = new RoomRepository(docClient);
    expect(await repo.getRoom('room-1')).toEqual(ROOM);
  });

  it('returns null when not found', async () => {
    mockSend.mockResolvedValue({});
    const repo = new RoomRepository(docClient);
    expect(await repo.getRoom('nope')).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// createRoom / createRoomConditional
// ---------------------------------------------------------------------------

describe('RoomRepository.createRoom', () => {
  it('sends PutCommand with room item', async () => {
    const repo = new RoomRepository(docClient);
    await repo.createRoom(ROOM);
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.TableName).toBe('test-social-rooms');
    expect(cmd.input.Item).toMatchObject({ roomId: 'room-1', name: 'General' });
  });
});

describe('RoomRepository.createRoomConditional', () => {
  it('sends PutCommand with attribute_not_exists condition', async () => {
    const repo = new RoomRepository(docClient);
    await repo.createRoomConditional(ROOM);
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ConditionExpression).toBe('attribute_not_exists(roomId)');
  });
});

// ---------------------------------------------------------------------------
// getMembership / isMember
// ---------------------------------------------------------------------------

describe('RoomRepository.getMembership', () => {
  it('queries with composite key { roomId, userId }', async () => {
    mockSend.mockResolvedValue({ Item: MEMBER });
    const repo = new RoomRepository(docClient);
    await repo.getMembership('room-1', 'user-1');
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ roomId: 'room-1', userId: 'user-1' });
  });
});

describe('RoomRepository.isMember', () => {
  it('returns true when member exists', async () => {
    mockSend.mockResolvedValue({ Item: MEMBER });
    const repo = new RoomRepository(docClient);
    expect(await repo.isMember('room-1', 'user-1')).toBe(true);
  });

  it('returns false when member does not exist', async () => {
    mockSend.mockResolvedValue({});
    const repo = new RoomRepository(docClient);
    expect(await repo.isMember('room-1', 'stranger')).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// addMember / removeMember / getRoomMembers
// ---------------------------------------------------------------------------

describe('RoomRepository.addMember', () => {
  it('sends PutCommand to the room-members table', async () => {
    const repo = new RoomRepository(docClient);
    await repo.addMember(MEMBER);
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.TableName).toBe('test-social-room-members');
    expect(cmd.input.Item).toMatchObject({ roomId: 'room-1', userId: 'user-1' });
  });
});

describe('RoomRepository.removeMember', () => {
  it('sends DeleteCommand with composite key', async () => {
    const repo = new RoomRepository(docClient);
    await repo.removeMember('room-1', 'user-7');
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ roomId: 'room-1', userId: 'user-7' });
  });
});

describe('RoomRepository.getRoomMembers', () => {
  it('queries members table by roomId', async () => {
    mockSend.mockResolvedValue({ Items: [MEMBER] });
    const repo = new RoomRepository(docClient);
    await repo.getRoomMembers('room-1');
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ExpressionAttributeValues[':rid']).toBe('room-1');
  });
});

// ---------------------------------------------------------------------------
// getRoomsByUser
// ---------------------------------------------------------------------------

describe('RoomRepository.getRoomsByUser', () => {
  it('returns empty array when user has no memberships', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new RoomRepository(docClient);
    const result = await repo.getRoomsByUser('user-none');
    expect(result).toEqual([]);
    expect(mockSend).toHaveBeenCalledTimes(1); // only the Query call
  });

  it('queries userId-roomId-index GSI then batch-gets rooms', async () => {
    const memberships = [{ roomId: 'room-1', userId: 'user-1', role: 'member', joinedAt: '' }];
    mockSend
      .mockResolvedValueOnce({ Items: memberships })
      .mockResolvedValueOnce({ Responses: { 'test-social-rooms': [ROOM] } });

    const repo = new RoomRepository(docClient);
    const result = await repo.getRoomsByUser('user-1');

    expect(mockSend).toHaveBeenCalledTimes(2);
    const queryCmdInput = mockSend.mock.calls[0][0].input;
    expect(queryCmdInput.IndexName).toBe('userId-roomId-index');

    const batchCmdInput = mockSend.mock.calls[1][0].input;
    expect(batchCmdInput.RequestItems['test-social-rooms'].Keys).toEqual([{ roomId: 'room-1' }]);

    expect(result).toHaveLength(1);
    expect(result[0]?.roomId).toBe('room-1');
  });
});

// ---------------------------------------------------------------------------
// addMemberWithOutbox
// ---------------------------------------------------------------------------

describe('RoomRepository.addMemberWithOutbox', () => {
  it('sends TransactWriteCommand with member + outbox puts', async () => {
    const outboxItem = { eventId: 'evt-1', type: 'member.added' };
    const repo = new RoomRepository(docClient);
    await repo.addMemberWithOutbox(MEMBER, outboxItem);

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.TransactItems).toHaveLength(2);

    const [memberPut, outboxPut] = cmd.input.TransactItems;
    expect(memberPut.Put.TableName).toBe('test-social-room-members');
    expect(outboxPut.Put.TableName).toBe('test-social-outbox');
    expect(outboxPut.Put.Item).toMatchObject({ eventId: 'evt-1' });
  });
});
