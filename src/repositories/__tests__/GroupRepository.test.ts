// social-api/src/repositories/__tests__/GroupRepository.test.ts
//
// Tests for GroupRepository: group CRUD, TransactWrite create+owner,
// updateMemberStatus (with/without joinedAt), getGroupMembers filter expression.

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { GroupRepository, type GroupItem, type GroupMemberItem } from '../GroupRepository';

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const docClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

const GROUP: GroupItem = {
  groupId: 'g-1',
  name: 'Devs',
  description: 'Developer group',
  visibility: 'public',
  ownerId: 'user-1',
  createdAt: '2026-01-01T00:00:00Z',
  updatedAt: '2026-01-01T00:00:00Z',
};

const MEMBER: GroupMemberItem = {
  groupId: 'g-1',
  userId: 'user-1',
  role: 'owner',
  status: 'active',
  joinedAt: '2026-01-01T00:00:00Z',
};

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// getGroup
// ---------------------------------------------------------------------------

describe('GroupRepository.getGroup', () => {
  it('returns the group when found', async () => {
    mockSend.mockResolvedValue({ Item: GROUP });
    const repo = new GroupRepository(docClient);
    const result = await repo.getGroup('g-1');
    expect(result).toEqual(GROUP);
  });

  it('returns null when not found', async () => {
    mockSend.mockResolvedValue({});
    const repo = new GroupRepository(docClient);
    const result = await repo.getGroup('nope');
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// createGroupWithOwner
// ---------------------------------------------------------------------------

describe('GroupRepository.createGroupWithOwner', () => {
  it('sends TransactWriteCommand with two Put items', async () => {
    const repo = new GroupRepository(docClient);
    await repo.createGroupWithOwner(GROUP, MEMBER);

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.TransactItems).toHaveLength(2);
  });

  it('first item writes to groups table with attribute_not_exists condition', async () => {
    const repo = new GroupRepository(docClient);
    await repo.createGroupWithOwner(GROUP, MEMBER);

    const cmd = mockSend.mock.calls[0][0];
    const groupPut = cmd.input.TransactItems[0].Put;
    expect(groupPut.TableName).toBe('test-social-groups');
    expect(groupPut.ConditionExpression).toBe('attribute_not_exists(groupId)');
    expect(groupPut.Item).toMatchObject({ groupId: 'g-1', ownerId: 'user-1' });
  });

  it('second item writes to members table without condition', async () => {
    const repo = new GroupRepository(docClient);
    await repo.createGroupWithOwner(GROUP, MEMBER);

    const cmd = mockSend.mock.calls[0][0];
    const memberPut = cmd.input.TransactItems[1].Put;
    expect(memberPut.TableName).toBe('test-social-group-members');
    expect(memberPut.Item).toMatchObject({ groupId: 'g-1', userId: 'user-1', role: 'owner' });
  });
});

// ---------------------------------------------------------------------------
// deleteGroup
// ---------------------------------------------------------------------------

describe('GroupRepository.deleteGroup', () => {
  it('sends DeleteCommand with groupId key', async () => {
    const repo = new GroupRepository(docClient);
    await repo.deleteGroup('g-1');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ groupId: 'g-1' });
  });
});

// ---------------------------------------------------------------------------
// updateGroupVisibility
// ---------------------------------------------------------------------------

describe('GroupRepository.updateGroupVisibility', () => {
  it('sends UpdateCommand with SET visibility and updatedAt', async () => {
    mockSend.mockResolvedValue({ Attributes: { ...GROUP, visibility: 'private' } });
    const repo = new GroupRepository(docClient);
    await repo.updateGroupVisibility('g-1', 'private');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ groupId: 'g-1' });
    expect(cmd.input.UpdateExpression).toContain('visibility');
    expect(cmd.input.ExpressionAttributeValues[':v']).toBe('private');
    expect(cmd.input.ReturnValues).toBe('ALL_NEW');
  });
});

// ---------------------------------------------------------------------------
// getMembership
// ---------------------------------------------------------------------------

describe('GroupRepository.getMembership', () => {
  it('queries by composite key { groupId, userId }', async () => {
    mockSend.mockResolvedValue({ Item: MEMBER });
    const repo = new GroupRepository(docClient);
    await repo.getMembership('g-1', 'user-1');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ groupId: 'g-1', userId: 'user-1' });
  });

  it('returns null when member not found', async () => {
    mockSend.mockResolvedValue({});
    const repo = new GroupRepository(docClient);
    const result = await repo.getMembership('g-1', 'unknown');
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// addMember / removeMember
// ---------------------------------------------------------------------------

describe('GroupRepository.addMember', () => {
  it('sends PutCommand with member item to the members table', async () => {
    const repo = new GroupRepository(docClient);
    await repo.addMember(MEMBER);

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.TableName).toBe('test-social-group-members');
    expect(cmd.input.Item).toMatchObject({ groupId: 'g-1', userId: 'user-1' });
  });
});

describe('GroupRepository.removeMember', () => {
  it('sends DeleteCommand with { groupId, userId }', async () => {
    const repo = new GroupRepository(docClient);
    await repo.removeMember('g-1', 'user-77');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ groupId: 'g-1', userId: 'user-77' });
  });
});

// ---------------------------------------------------------------------------
// updateMemberStatus
// ---------------------------------------------------------------------------

describe('GroupRepository.updateMemberStatus', () => {
  it('sets status without joinedAt when joinedAt is not provided', async () => {
    const repo = new GroupRepository(docClient);
    await repo.updateMemberStatus('g-1', 'user-1', 'active');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toBe('SET #s = :s');
    expect(cmd.input.ExpressionAttributeValues).toEqual({ ':s': 'active' });
  });

  it('includes joinedAt in update when provided', async () => {
    const repo = new GroupRepository(docClient);
    await repo.updateMemberStatus('g-1', 'user-1', 'active', '2026-01-01T00:00:00Z');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toContain('joinedAt');
    expect(cmd.input.ExpressionAttributeValues[':j']).toBe('2026-01-01T00:00:00Z');
  });

  it('uses ExpressionAttributeNames alias for reserved word "status"', async () => {
    const repo = new GroupRepository(docClient);
    await repo.updateMemberStatus('g-1', 'user-1', 'invited');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ExpressionAttributeNames['#s']).toBe('status');
  });
});

// ---------------------------------------------------------------------------
// getGroupMembers
// ---------------------------------------------------------------------------

describe('GroupRepository.getGroupMembers', () => {
  it('queries by groupId with active status filter', async () => {
    mockSend.mockResolvedValue({ Items: [MEMBER] });
    const repo = new GroupRepository(docClient);
    await repo.getGroupMembers('g-1');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ExpressionAttributeValues[':gid']).toBe('g-1');
    expect(cmd.input.FilterExpression).toContain('active');
    expect(cmd.input.ExpressionAttributeValues[':active']).toBe('active');
  });
});
