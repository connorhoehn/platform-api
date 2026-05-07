/**
 * Tests for social-api PostsService.
 *
 * Mocks the DynamoDB DocClient + broadcaster + outbox publisher. Verifies
 * business rules (content validation, ownership authorization, not-found
 * handling) independently of Express.
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
const { GetCommand, UpdateCommand, DeleteCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');

import * as postsService from '../src/services/posts-service';
import { docClient } from '../src/lib/aws-clients';
import { broadcastToRoom } from '../src/services/room-broadcaster';
import { publishWithOutbox } from '../src/services/outbox-publisher';
import {
  ValidationError,
  NotFoundError,
  ForbiddenError,
} from '../src/middleware/error-handler';

const sendMock = docClient.send as unknown as jest.Mock;
const broadcastMock = broadcastToRoom as unknown as jest.Mock;
const outboxMock = publishWithOutbox as unknown as jest.Mock;

describe('postsService', () => {
  beforeEach(() => {
    sendMock.mockReset();
    broadcastMock.mockClear();
    outboxMock.mockClear();
  });

  test('createPost: writes through outbox + broadcasts + returns DTO', async () => {
    const result = await postsService.createPost('room-1', 'user-1', 'hello world');

    expect(outboxMock).toHaveBeenCalledTimes(1);
    const call = outboxMock.mock.calls[0][0];
    expect(call.eventType).toBe('social.post.created');
    expect(call.target.TableName).toBe('social-posts');
    expect(call.target.Item.roomId).toBe('room-1');
    expect(call.target.Item.authorId).toBe('user-1');
    expect(call.target.Item.content).toBe('hello world');

    expect(broadcastMock).toHaveBeenCalledWith(
      'room-1',
      'social:post',
      expect.objectContaining({ roomId: 'room-1', authorId: 'user-1', content: 'hello world' }),
    );

    expect(result).toMatchObject({
      roomId: 'room-1',
      authorId: 'user-1',
      content: 'hello world',
    });
    expect(typeof result.postId).toBe('string');
    expect(result.postId.length).toBeGreaterThan(0);
  });

  test('createPost: empty content → ValidationError', async () => {
    await expect(postsService.createPost('r', 'u', '   ')).rejects.toBeInstanceOf(ValidationError);
    await expect(postsService.createPost('r', 'u', undefined)).rejects.toBeInstanceOf(ValidationError);
    expect(outboxMock).not.toHaveBeenCalled();
  });

  test('editPost: author match → UpdateCommand issued', async () => {
    sendMock.mockResolvedValueOnce({ Items: [{ roomId: 'r', postId: 'p', authorId: 'u1', content: 'old' }] });
    sendMock.mockResolvedValueOnce({});

    const out = await postsService.editPost('r', 'p', 'u1', 'new content');

    expect(out).toMatchObject({ roomId: 'r', postId: 'p', content: 'new content' });
    expect(sendMock).toHaveBeenCalledTimes(2);
    expect(sendMock.mock.calls[0][0]).toBeInstanceOf(QueryCommand);
    expect(sendMock.mock.calls[1][0]).toBeInstanceOf(UpdateCommand);
  });

  test('editPost: different author → ForbiddenError, no write', async () => {
    sendMock.mockResolvedValueOnce({ Items: [{ roomId: 'r', postId: 'p', authorId: 'someone-else' }] });

    await expect(postsService.editPost('r', 'p', 'u1', 'x')).rejects.toBeInstanceOf(ForbiddenError);
    expect(sendMock).toHaveBeenCalledTimes(1); // only the read
  });

  test('editPost: post missing → NotFoundError', async () => {
    sendMock.mockResolvedValueOnce({ Items: [] });
    await expect(postsService.editPost('r', 'p', 'u1', 'x')).rejects.toBeInstanceOf(NotFoundError);
  });

  test('deletePost: author match → DeleteCommand issued', async () => {
    sendMock.mockResolvedValueOnce({ Item: { authorId: 'u1' } });
    sendMock.mockResolvedValueOnce({});

    await postsService.deletePost('r', 'p', 'u1');

    expect(sendMock.mock.calls[0][0]).toBeInstanceOf(GetCommand);
    expect(sendMock.mock.calls[1][0]).toBeInstanceOf(DeleteCommand);
  });

  test('deletePost: different author → ForbiddenError', async () => {
    sendMock.mockResolvedValueOnce({ Item: { authorId: 'someone-else' } });
    await expect(postsService.deletePost('r', 'p', 'u1')).rejects.toBeInstanceOf(ForbiddenError);
  });

  test('listRoomPosts: invalid cursor → ValidationError', async () => {
    await expect(
      postsService.listRoomPosts('r', { cursor: 'not-base64-json' }),
    ).rejects.toBeInstanceOf(ValidationError);
  });
});
