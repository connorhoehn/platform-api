// social-api/src/repositories/__tests__/ProfileRepository.test.ts
//
// Tests for ProfileRepository: getProfile, createProfile, updateProfile
// (dynamic SET expression, always includes updatedAt), batchGetProfiles,
// searchProfiles (empty guard, case-insensitive filter, limit slice).

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { ProfileRepository, type ProfileItem } from '../ProfileRepository';

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const docClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

const PROFILE: ProfileItem = {
  userId: 'u-1',
  displayName: 'Alice',
  bio: 'Engineer',
  avatarUrl: 'https://example.com/alice.png',
  visibility: 'public',
  createdAt: '2026-01-01T00:00:00Z',
  updatedAt: '2026-01-01T00:00:00Z',
};

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// getProfile
// ---------------------------------------------------------------------------

describe('ProfileRepository.getProfile', () => {
  it('returns the profile when found', async () => {
    mockSend.mockResolvedValue({ Item: PROFILE });
    const repo = new ProfileRepository(docClient);
    const result = await repo.getProfile('u-1');
    expect(result).toEqual(PROFILE);
  });

  it('sends GetCommand with { userId }', async () => {
    mockSend.mockResolvedValue({ Item: PROFILE });
    const repo = new ProfileRepository(docClient);
    await repo.getProfile('u-42');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ userId: 'u-42' });
    expect(cmd.input.TableName).toBe('test-social-profiles');
  });

  it('returns null when not found', async () => {
    mockSend.mockResolvedValue({});
    const repo = new ProfileRepository(docClient);
    const result = await repo.getProfile('unknown');
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// createProfile
// ---------------------------------------------------------------------------

describe('ProfileRepository.createProfile', () => {
  it('sends PutCommand with the full profile item', async () => {
    const repo = new ProfileRepository(docClient);
    await repo.createProfile(PROFILE);

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Item).toMatchObject({ userId: 'u-1', displayName: 'Alice' });
  });
});

// ---------------------------------------------------------------------------
// updateProfile
// ---------------------------------------------------------------------------

describe('ProfileRepository.updateProfile', () => {
  beforeEach(() => {
    mockSend.mockResolvedValue({ Attributes: PROFILE });
  });

  it('always includes updatedAt in SET expression', async () => {
    const repo = new ProfileRepository(docClient);
    await repo.updateProfile('u-1', { displayName: 'Alice 2' });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toContain('updatedAt');
    expect(cmd.input.ExpressionAttributeValues[':updatedAt']).toBeDefined();
  });

  it('includes only specified fields in SET expression', async () => {
    const repo = new ProfileRepository(docClient);
    await repo.updateProfile('u-1', { displayName: 'Bob' });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toContain('displayName');
    expect(cmd.input.UpdateExpression).not.toContain('bio');
    expect(cmd.input.UpdateExpression).not.toContain('avatarUrl');
  });

  it('includes all four updatable fields when all provided', async () => {
    const repo = new ProfileRepository(docClient);
    await repo.updateProfile('u-1', {
      displayName: 'Bob',
      bio: 'Designer',
      avatarUrl: 'https://example.com/bob.png',
      visibility: 'private',
    });

    const cmd = mockSend.mock.calls[0][0];
    const expr: string = cmd.input.UpdateExpression;
    expect(expr).toContain('displayName');
    expect(expr).toContain('bio');
    expect(expr).toContain('avatarUrl');
    expect(expr).toContain('visibility');
  });

  it('uses ReturnValues=ALL_NEW', async () => {
    const repo = new ProfileRepository(docClient);
    await repo.updateProfile('u-1', { bio: 'New bio' });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ReturnValues).toBe('ALL_NEW');
  });
});

// ---------------------------------------------------------------------------
// batchGetProfiles
// ---------------------------------------------------------------------------

describe('ProfileRepository.batchGetProfiles', () => {
  it('returns empty array without calling docClient when userIds is empty', async () => {
    const repo = new ProfileRepository(docClient);
    const result = await repo.batchGetProfiles([]);
    expect(result).toEqual([]);
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('sends BatchGetCommand with mapped userId keys', async () => {
    mockSend.mockResolvedValue({
      Responses: { 'test-social-profiles': [PROFILE] },
    });
    const repo = new ProfileRepository(docClient);
    await repo.batchGetProfiles(['u-1', 'u-2']);

    const cmd = mockSend.mock.calls[0][0];
    const keys = cmd.input.RequestItems['test-social-profiles'].Keys;
    expect(keys).toEqual([{ userId: 'u-1' }, { userId: 'u-2' }]);
  });

  it('returns profiles from Responses', async () => {
    mockSend.mockResolvedValue({
      Responses: { 'test-social-profiles': [PROFILE] },
    });
    const repo = new ProfileRepository(docClient);
    const result = await repo.batchGetProfiles(['u-1']);
    expect(result).toHaveLength(1);
    expect(result[0]?.userId).toBe('u-1');
  });
});

// ---------------------------------------------------------------------------
// searchProfiles
// ---------------------------------------------------------------------------

describe('ProfileRepository.searchProfiles', () => {
  it('returns empty array without scanning when query is empty string', async () => {
    const repo = new ProfileRepository(docClient);
    const result = await repo.searchProfiles('', 10);
    expect(result).toEqual([]);
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('returns empty array without scanning when query is whitespace only', async () => {
    const repo = new ProfileRepository(docClient);
    const result = await repo.searchProfiles('   ', 10);
    expect(result).toEqual([]);
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('case-insensitively matches displayName substring', async () => {
    const items: ProfileItem[] = [
      { ...PROFILE, userId: 'u-1', displayName: 'Alice Smith' },
      { ...PROFILE, userId: 'u-2', displayName: 'Bob Jones' },
      { ...PROFILE, userId: 'u-3', displayName: 'ALICE Wu' },
    ];
    mockSend.mockResolvedValue({ Items: items });
    const repo = new ProfileRepository(docClient);
    const result = await repo.searchProfiles('alice', 10);
    expect(result).toHaveLength(2);
    expect(result.map((p) => p.userId)).toEqual(expect.arrayContaining(['u-1', 'u-3']));
  });

  it('slices results to the specified limit', async () => {
    const items: ProfileItem[] = Array.from({ length: 10 }, (_, i) => ({
      ...PROFILE,
      userId: `u-${i}`,
      displayName: 'Alice',
    }));
    mockSend.mockResolvedValue({ Items: items });
    const repo = new ProfileRepository(docClient);
    const result = await repo.searchProfiles('alice', 3);
    expect(result).toHaveLength(3);
  });
});
