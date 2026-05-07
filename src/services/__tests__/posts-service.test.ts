const mockSend = jest.fn();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: (...args: unknown[]) => mockSend(...args) },
}));
jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (base: string) => `test-${base}`,
}));

const mockBroadcast = jest.fn().mockResolvedValue(undefined);
jest.mock('../room-broadcaster', () => ({
  broadcastToRoom: (...args: unknown[]) => mockBroadcast(...args),
}));

const mockPublishWithOutbox = jest.fn().mockResolvedValue(undefined);
jest.mock('../outbox-publisher', () => ({
  publishWithOutbox: (...args: unknown[]) => mockPublishWithOutbox(...args),
}));

import {
  createPost,
  editPost,
  deletePost,
  listRoomPosts,
  listUserPosts,
} from '../posts-service';
import {
  ValidationError,
  NotFoundError,
  ForbiddenError,
} from '../../middleware/error-handler';

beforeEach(() => {
  mockSend.mockReset();
  mockBroadcast.mockReset().mockResolvedValue(undefined);
  mockPublishWithOutbox.mockReset().mockResolvedValue(undefined);
});

describe('createPost', () => {
  it('creates a post and broadcasts', async () => {
    const result = await createPost('r-1', 'u-1', 'Hello world');
    expect(result.roomId).toBe('r-1');
    expect(result.authorId).toBe('u-1');
    expect(result.content).toBe('Hello world');
    expect(result.postId).toBeDefined();
    expect(mockPublishWithOutbox).toHaveBeenCalledTimes(1);
    expect(mockBroadcast).toHaveBeenCalledWith('r-1', 'social:post', expect.objectContaining({ content: 'Hello world' }));
  });

  it('trims whitespace from content', async () => {
    const result = await createPost('r-1', 'u-1', '  trimmed  ');
    expect(result.content).toBe('trimmed');
  });

  it('rejects empty content', async () => {
    await expect(createPost('r-1', 'u-1', '')).rejects.toThrow(ValidationError);
  });

  it('rejects undefined content', async () => {
    await expect(createPost('r-1', 'u-1', undefined)).rejects.toThrow(ValidationError);
  });

  it('rejects content over 10000 chars', async () => {
    const long = 'x'.repeat(10001);
    await expect(createPost('r-1', 'u-1', long)).rejects.toThrow(ValidationError);
  });

  it('accepts content exactly 10000 chars', async () => {
    const exact = 'x'.repeat(10000);
    const result = await createPost('r-1', 'u-1', exact);
    expect(result.content).toBe(exact);
  });
});

describe('editPost', () => {
  it('updates content when author matches', async () => {
    mockSend
      .mockResolvedValueOnce({ Items: [{ roomId: 'r-1', postId: 'p-1', authorId: 'u-1' }] })
      .mockResolvedValueOnce({});
    const result = await editPost('r-1', 'p-1', 'u-1', 'Updated content');
    expect(result.content).toBe('Updated content');
    expect(result.updatedAt).toBeDefined();
  });

  it('rejects when post not found', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    await expect(editPost('r-1', 'p-1', 'u-1', 'Updated')).rejects.toThrow(NotFoundError);
  });

  it('rejects when caller is not the author', async () => {
    mockSend.mockResolvedValue({ Items: [{ roomId: 'r-1', postId: 'p-1', authorId: 'u-other' }] });
    await expect(editPost('r-1', 'p-1', 'u-1', 'Updated')).rejects.toThrow(ForbiddenError);
  });

  it('validates content on edit', async () => {
    await expect(editPost('r-1', 'p-1', 'u-1', '')).rejects.toThrow(ValidationError);
  });
});

describe('deletePost', () => {
  it('deletes when author matches', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { roomId: 'r-1', postId: 'p-1', authorId: 'u-1' } })
      .mockResolvedValueOnce({});
    await expect(deletePost('r-1', 'p-1', 'u-1')).resolves.toBeUndefined();
    expect(mockSend).toHaveBeenCalledTimes(2);
  });

  it('rejects when post not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    await expect(deletePost('r-1', 'p-1', 'u-1')).rejects.toThrow(NotFoundError);
  });

  it('rejects when caller is not the author', async () => {
    mockSend.mockResolvedValue({ Item: { roomId: 'r-1', postId: 'p-1', authorId: 'u-other' } });
    await expect(deletePost('r-1', 'p-1', 'u-1')).rejects.toThrow(ForbiddenError);
  });
});

describe('listRoomPosts', () => {
  it('returns posts with default limit', async () => {
    mockSend.mockResolvedValue({ Items: [{ postId: 'p-1' }] });
    const result = await listRoomPosts('r-1');
    expect(result.posts).toHaveLength(1);
    expect(result.nextCursor).toBeNull();
  });

  it('returns nextCursor when LastEvaluatedKey is present', async () => {
    mockSend.mockResolvedValue({
      Items: [{ postId: 'p-1' }],
      LastEvaluatedKey: { roomId: 'r-1', postId: 'p-1' },
    });
    const result = await listRoomPosts('r-1');
    expect(result.nextCursor).toBeTruthy();
    const decoded = JSON.parse(Buffer.from(result.nextCursor!, 'base64').toString('utf8'));
    expect(decoded).toEqual({ roomId: 'r-1', postId: 'p-1' });
  });

  it('caps limit at 100', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    await listRoomPosts('r-1', { limit: 500 });
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Limit).toBe(100);
  });

  it('rejects invalid cursor', async () => {
    await expect(listRoomPosts('r-1', { cursor: 'not-base64-json!!!' })).rejects.toThrow(ValidationError);
  });

  it('uses cursor as ExclusiveStartKey', async () => {
    const key = { roomId: 'r-1', postId: 'p-0' };
    const cursor = Buffer.from(JSON.stringify(key)).toString('base64');
    mockSend.mockResolvedValue({ Items: [] });
    await listRoomPosts('r-1', { cursor });
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ExclusiveStartKey).toEqual(key);
  });
});

describe('listUserPosts', () => {
  it('queries the authorId GSI', async () => {
    mockSend.mockResolvedValue({ Items: [{ postId: 'p-1' }, { postId: 'p-2' }] });
    const result = await listUserPosts('u-1');
    expect(result).toHaveLength(2);
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.IndexName).toBe('authorId-postId-index');
  });
});
