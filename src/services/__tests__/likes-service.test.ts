const mockSend = jest.fn();
const mockPublishSocialEvent = jest.fn().mockResolvedValue(undefined);
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: (...args: unknown[]) => mockSend(...args) },
  publishSocialEvent: (...args: unknown[]) => mockPublishSocialEvent(...args),
}));
jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (base: string) => `test-${base}`,
}));

const mockBroadcast = jest.fn().mockResolvedValue(undefined);
jest.mock('../room-broadcaster', () => ({
  broadcastToRoom: (...args: unknown[]) => mockBroadcast(...args),
}));

const mockBatchGetProfiles = jest.fn();
jest.mock('../../repositories', () => ({
  profileRepo: { batchGetProfiles: (...args: unknown[]) => mockBatchGetProfiles(...args) },
}));

import {
  likePost,
  unlikePost,
  listPostLikes,
  likeComment,
  unlikeComment,
} from '../likes-service';
import { NotFoundError, ConflictError } from '../../middleware/error-handler';

beforeEach(() => {
  mockSend.mockReset();
  mockBroadcast.mockReset().mockResolvedValue(undefined);
  mockPublishSocialEvent.mockReset().mockResolvedValue(undefined);
  mockBatchGetProfiles.mockReset();
});

describe('likePost', () => {
  it('creates a like when post exists', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { roomId: 'r-1', postId: 'p-1' } }) // assertPostExists
      .mockResolvedValueOnce({}); // insertLike
    const result = await likePost('r-1', 'p-1', 'u-1');
    expect(result.targetId).toBe('post:p-1');
    expect(result.type).toBe('like');
    expect(mockBroadcast).toHaveBeenCalledWith('r-1', 'social:like', expect.objectContaining({ type: 'like' }));
  });

  it('throws NotFoundError when post missing', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    await expect(likePost('r-1', 'p-1', 'u-1')).rejects.toThrow(NotFoundError);
  });

  it('throws ConflictError when already liked', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { roomId: 'r-1', postId: 'p-1' } })
      .mockRejectedValueOnce(Object.assign(new Error('condition'), { name: 'ConditionalCheckFailedException' }));
    await expect(likePost('r-1', 'p-1', 'u-1')).rejects.toThrow(ConflictError);
  });
});

describe('unlikePost', () => {
  it('deletes an existing like', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { targetId: 'post:p-1', userId: 'u-1' } })
      .mockResolvedValueOnce({});
    await expect(unlikePost('p-1', 'u-1')).resolves.toBeUndefined();
  });

  it('throws NotFoundError when like missing', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    await expect(unlikePost('p-1', 'u-1')).rejects.toThrow(NotFoundError);
  });
});

describe('listPostLikes', () => {
  it('returns enriched likedBy list', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { roomId: 'r-1', postId: 'p-1' } }) // assertPostExists
      .mockResolvedValueOnce({ Items: [{ userId: 'u-1' }, { userId: 'u-2' }] }); // query likes
    mockBatchGetProfiles.mockResolvedValue([
      { userId: 'u-1', displayName: 'Alice' },
      { userId: 'u-2', displayName: 'Bob' },
    ]);
    const result = await listPostLikes('r-1', 'p-1');
    expect(result.count).toBe(2);
    expect(result.likedBy).toEqual([
      { userId: 'u-1', displayName: 'Alice' },
      { userId: 'u-2', displayName: 'Bob' },
    ]);
  });

  it('falls back to userId when profile has no displayName', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { roomId: 'r-1', postId: 'p-1' } })
      .mockResolvedValueOnce({ Items: [{ userId: 'u-3' }] });
    mockBatchGetProfiles.mockResolvedValue([]);
    const result = await listPostLikes('r-1', 'p-1');
    expect(result.likedBy[0].displayName).toBe('u-3');
  });

  it('returns empty list when no likes', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { roomId: 'r-1', postId: 'p-1' } })
      .mockResolvedValueOnce({ Items: [] });
    const result = await listPostLikes('r-1', 'p-1');
    expect(result.count).toBe(0);
    expect(result.likedBy).toEqual([]);
    expect(mockBatchGetProfiles).not.toHaveBeenCalled();
  });
});

describe('likeComment', () => {
  it('creates a like when comment exists', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { postId: 'p-1', commentId: 'c-1' } })
      .mockResolvedValueOnce({});
    const result = await likeComment('r-1', 'p-1', 'c-1', 'u-1');
    expect(result.targetId).toBe('comment:c-1');
    expect(result.type).toBe('like');
  });

  it('throws NotFoundError when comment missing', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    await expect(likeComment('r-1', 'p-1', 'c-1', 'u-1')).rejects.toThrow(NotFoundError);
  });
});

describe('unlikeComment', () => {
  it('deletes an existing comment like', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { targetId: 'comment:c-1', userId: 'u-1' } })
      .mockResolvedValueOnce({});
    await expect(unlikeComment('c-1', 'u-1')).resolves.toBeUndefined();
  });

  it('throws NotFoundError when like missing', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    await expect(unlikeComment('c-1', 'u-1')).rejects.toThrow(NotFoundError);
  });
});
