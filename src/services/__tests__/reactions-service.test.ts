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

import { addReaction, removeReaction, VALID_EMOJI } from '../reactions-service';
import { ValidationError, NotFoundError } from '../../middleware/error-handler';

beforeEach(() => {
  mockSend.mockReset();
  mockBroadcast.mockReset().mockResolvedValue(undefined);
  mockPublishWithOutbox.mockReset().mockResolvedValue(undefined);
});

describe('addReaction', () => {
  it('creates a reaction for a valid emoji and existing post', async () => {
    mockSend.mockResolvedValue({ Item: { roomId: 'r-1', postId: 'p-1' } });
    const result = await addReaction('r-1', 'p-1', 'u-1', '❤️');
    expect(result.emoji).toBe('❤️');
    expect(result.userId).toBe('u-1');
    expect(result.type).toBe('reaction');
    expect(mockPublishWithOutbox).toHaveBeenCalledTimes(1);
    expect(mockBroadcast).toHaveBeenCalledWith('r-1', 'social:like', expect.objectContaining({ emoji: '❤️' }));
  });

  it('rejects undefined emoji with ValidationError', async () => {
    await expect(addReaction('r-1', 'p-1', 'u-1', undefined)).rejects.toThrow(ValidationError);
  });

  it('rejects invalid emoji with ValidationError', async () => {
    await expect(addReaction('r-1', 'p-1', 'u-1', '😀')).rejects.toThrow(ValidationError);
  });

  it('rejects when post not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    await expect(addReaction('r-1', 'p-1', 'u-1', '❤️')).rejects.toThrow(NotFoundError);
  });

  it('builds outbox params with correct conflictMessage', async () => {
    mockSend.mockResolvedValue({ Item: { roomId: 'r-1', postId: 'p-1' } });
    await addReaction('r-1', 'p-1', 'u-1', '🚀');
    const params = mockPublishWithOutbox.mock.calls[0][0];
    expect(params.conflictMessage).toBe('Already reacted. Delete your existing reaction first.');
    expect(params.target.ConditionExpression).toBe('attribute_not_exists(#uid)');
  });
});

describe('removeReaction', () => {
  it('deletes an existing reaction', async () => {
    mockSend
      .mockResolvedValueOnce({ Item: { targetId: 'post:p-1:reaction', userId: 'u-1' } })
      .mockResolvedValueOnce({});
    await removeReaction('p-1', 'u-1', '❤️');
    expect(mockSend).toHaveBeenCalledTimes(2);
  });

  it('rejects invalid emoji', async () => {
    await expect(removeReaction('p-1', 'u-1', 'invalid')).rejects.toThrow(ValidationError);
  });

  it('rejects when reaction not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    await expect(removeReaction('p-1', 'u-1', '❤️')).rejects.toThrow(NotFoundError);
  });
});

describe('VALID_EMOJI', () => {
  it('contains exactly 12 supported emoji', () => {
    expect(VALID_EMOJI.size).toBe(12);
  });
});
