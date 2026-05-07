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

import {
  createComment,
  listComments,
  deleteComment,
} from '../comments-service';
import {
  ValidationError,
  NotFoundError,
  ForbiddenError,
} from '../../middleware/error-handler';

beforeEach(() => {
  mockSend.mockReset();
  mockBroadcast.mockReset().mockResolvedValue(undefined);
  mockPublishSocialEvent.mockReset().mockResolvedValue(undefined);
});

describe('createComment', () => {
  it('creates a comment on an existing post', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { roomId: 'r-1', postId: 'p-1' } }) // post exists
      .mockResolvedValueOnce({}); // PutCommand
    const result = await createComment('r-1', 'p-1', 'u-1', { content: 'Nice post!' });
    expect(result.postId).toBe('p-1');
    expect(result.authorId).toBe('u-1');
    expect(result.content).toBe('Nice post!');
    expect(result.commentId).toBeDefined();
    expect(mockBroadcast).toHaveBeenCalledWith('r-1', 'social:comment', expect.objectContaining({ content: 'Nice post!' }));
  });

  it('supports parentCommentId for threaded replies', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { roomId: 'r-1', postId: 'p-1' } }) // post exists
      .mockResolvedValueOnce({ Item: { postId: 'p-1', commentId: 'c-parent' } }) // parent exists
      .mockResolvedValueOnce({}); // PutCommand
    const result = await createComment('r-1', 'p-1', 'u-1', {
      content: 'Reply!',
      parentCommentId: 'c-parent',
    });
    expect(result.parentCommentId).toBe('c-parent');
  });

  it('rejects when post not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    await expect(createComment('r-1', 'p-1', 'u-1', { content: 'hi' })).rejects.toThrow(NotFoundError);
  });

  it('rejects when parent comment not found', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { roomId: 'r-1', postId: 'p-1' } })
      .mockResolvedValueOnce({ Item: undefined });
    await expect(
      createComment('r-1', 'p-1', 'u-1', { content: 'Reply', parentCommentId: 'c-missing' }),
    ).rejects.toThrow(NotFoundError);
  });

  it('rejects empty content', async () => {
    await expect(createComment('r-1', 'p-1', 'u-1', { content: '' })).rejects.toThrow(ValidationError);
  });

  it('rejects content over 10000 chars', async () => {
    await expect(
      createComment('r-1', 'p-1', 'u-1', { content: 'x'.repeat(10001) }),
    ).rejects.toThrow(ValidationError);
  });

  it('trims whitespace', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { roomId: 'r-1', postId: 'p-1' } })
      .mockResolvedValueOnce({});
    const result = await createComment('r-1', 'p-1', 'u-1', { content: '  trimmed  ' });
    expect(result.content).toBe('trimmed');
  });
});

describe('listComments', () => {
  it('returns comments for an existing post', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { roomId: 'r-1', postId: 'p-1' } })
      .mockResolvedValueOnce({ Items: [{ commentId: 'c-1' }, { commentId: 'c-2' }] });
    const result = await listComments('r-1', 'p-1');
    expect(result).toHaveLength(2);
  });

  it('rejects when post not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    await expect(listComments('r-1', 'p-1')).rejects.toThrow(NotFoundError);
  });

  it('returns empty array when no comments', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { roomId: 'r-1', postId: 'p-1' } })
      .mockResolvedValueOnce({ Items: [] });
    const result = await listComments('r-1', 'p-1');
    expect(result).toEqual([]);
  });
});

describe('deleteComment', () => {
  it('deletes when caller is the author', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { postId: 'p-1', commentId: 'c-1', authorId: 'u-1' } })
      .mockResolvedValueOnce({});
    await expect(deleteComment('p-1', 'c-1', 'u-1')).resolves.toBeUndefined();
  });

  it('rejects when comment not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    await expect(deleteComment('p-1', 'c-1', 'u-1')).rejects.toThrow(NotFoundError);
  });

  it('rejects when caller is not the author', async () => {
    mockSend.mockResolvedValue({ Item: { postId: 'p-1', commentId: 'c-1', authorId: 'u-other' } });
    await expect(deleteComment('p-1', 'c-1', 'u-1')).rejects.toThrow(ForbiddenError);
  });
});
