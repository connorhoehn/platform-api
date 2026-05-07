/**
 * Tests for social-api ReactionsService.
 *
 * Mocks DynamoDB DocClient + broadcaster + outbox publisher. Covers:
 * emoji whitelist validation, post existence, outbox-based write on add,
 * conflict propagation from outbox, and reaction-not-found on delete.
 */

jest.mock('../src/lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
}));

jest.mock('../src/services/room-broadcaster', () => ({
  broadcastToRoom: jest.fn(() => Promise.resolve()),
}));

jest.mock('../src/services/outbox-publisher', () => ({
  publishWithOutbox: jest.fn(() => Promise.resolve()),
}));

// eslint-disable-next-line @typescript-eslint/no-require-imports
const { GetCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');

import * as reactionsService from '../src/services/reactions-service';
import { docClient } from '../src/lib/aws-clients';
import { publishWithOutbox } from '../src/services/outbox-publisher';
import {
  ValidationError,
  NotFoundError,
  ConflictError,
} from '../src/middleware/error-handler';

const sendMock = docClient.send as unknown as jest.Mock;
const outboxMock = publishWithOutbox as unknown as jest.Mock;

const HEART = '\u2764\uFE0F';
const ROCKET = '\uD83D\uDE80';

describe('reactionsService', () => {
  beforeEach(() => {
    sendMock.mockReset();
    outboxMock.mockReset();
    outboxMock.mockResolvedValue(undefined);
  });

  test('addReaction: happy path calls publishWithOutbox with reaction payload', async () => {
    sendMock.mockResolvedValueOnce({ Item: { postId: 'p' } }); // post exists

    const result = await reactionsService.addReaction('r', 'p', 'u1', HEART);

    expect(outboxMock).toHaveBeenCalledTimes(1);
    const call = outboxMock.mock.calls[0][0];
    expect(call.eventType).toBe('social.reaction');
    expect(call.target.TableName).toBe('social-likes');
    expect(call.target.Item).toMatchObject({
      targetId: 'post:p:reaction',
      userId: 'u1',
      type: 'reaction',
      emoji: HEART,
    });
    expect(call.conflictMessage).toBe('Already reacted. Delete your existing reaction first.');

    expect(result).toMatchObject({ targetId: 'post:p:reaction', emoji: HEART, type: 'reaction' });
  });

  test('addReaction: unsupported emoji → ValidationError', async () => {
    await expect(
      reactionsService.addReaction('r', 'p', 'u1', '🧡'),
    ).rejects.toBeInstanceOf(ValidationError);
    expect(sendMock).not.toHaveBeenCalled();
    expect(outboxMock).not.toHaveBeenCalled();
  });

  test('addReaction: missing emoji → ValidationError', async () => {
    await expect(
      reactionsService.addReaction('r', 'p', 'u1', undefined),
    ).rejects.toBeInstanceOf(ValidationError);
  });

  test('addReaction: post missing → NotFoundError', async () => {
    sendMock.mockResolvedValueOnce({ Item: undefined });
    await expect(
      reactionsService.addReaction('r', 'p', 'u1', ROCKET),
    ).rejects.toBeInstanceOf(NotFoundError);
    expect(outboxMock).not.toHaveBeenCalled();
  });

  test('addReaction: outbox ConflictError propagates', async () => {
    sendMock.mockResolvedValueOnce({ Item: { postId: 'p' } });
    outboxMock.mockRejectedValueOnce(
      new ConflictError('Already reacted. Delete your existing reaction first.'),
    );

    await expect(
      reactionsService.addReaction('r', 'p', 'u1', HEART),
    ).rejects.toBeInstanceOf(ConflictError);
  });

  test('removeReaction: reaction missing → NotFoundError', async () => {
    sendMock.mockResolvedValueOnce({ Item: undefined });
    await expect(
      reactionsService.removeReaction('p', 'u1', HEART),
    ).rejects.toBeInstanceOf(NotFoundError);
  });

  test('removeReaction: happy path issues DeleteCommand', async () => {
    sendMock.mockResolvedValueOnce({ Item: { targetId: 'post:p:reaction', userId: 'u1' } });
    sendMock.mockResolvedValueOnce({});
    await reactionsService.removeReaction('p', 'u1', HEART);
    expect(sendMock.mock.calls[0][0]).toBeInstanceOf(GetCommand);
    expect(sendMock.mock.calls[1][0]).toBeInstanceOf(DeleteCommand);
  });

  test('removeReaction: unsupported emoji → ValidationError, no reads', async () => {
    await expect(
      reactionsService.removeReaction('p', 'u1', '🧡'),
    ).rejects.toBeInstanceOf(ValidationError);
    expect(sendMock).not.toHaveBeenCalled();
  });
});
