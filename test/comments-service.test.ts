/**
 * Tests for social-api CommentsService.
 *
 * Mocks DynamoDB DocClient + broadcaster + publishSocialEvent. Covers
 * validation, parent-entity existence (post + parent comment), happy-path
 * create/list, and ownership authorization on delete.
 */

jest.mock('../src/lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
  publishSocialEvent: jest.fn(() => Promise.resolve()),
}));

jest.mock('../src/services/room-broadcaster', () => ({
  broadcastToRoom: jest.fn(() => Promise.resolve()),
}));

// eslint-disable-next-line @typescript-eslint/no-require-imports
const { GetCommand, PutCommand, DeleteCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');

import * as commentsService from '../src/services/comments-service';
import { docClient } from '../src/lib/aws-clients';
import { broadcastToRoom } from '../src/services/room-broadcaster';
import {
  ValidationError,
  NotFoundError,
  ForbiddenError,
} from '../src/middleware/error-handler';

const sendMock = docClient.send as unknown as jest.Mock;
const broadcastMock = broadcastToRoom as unknown as jest.Mock;

describe('commentsService', () => {
  beforeEach(() => {
    sendMock.mockReset();
    broadcastMock.mockClear();
  });

  test('createComment: happy path writes comment + broadcasts', async () => {
    // post exists
    sendMock.mockResolvedValueOnce({ Item: { roomId: 'r', postId: 'p' } });
    // put
    sendMock.mockResolvedValueOnce({});

    const item = await commentsService.createComment('r', 'p', 'u1', { content: 'hi there' });

    expect(item).toMatchObject({ postId: 'p', authorId: 'u1', content: 'hi there' });
    expect(typeof item.commentId).toBe('string');

    expect(sendMock.mock.calls[0][0]).toBeInstanceOf(GetCommand);
    expect(sendMock.mock.calls[1][0]).toBeInstanceOf(PutCommand);

    expect(broadcastMock).toHaveBeenCalledWith(
      'r',
      'social:comment',
      expect.objectContaining({ roomId: 'r', postId: 'p', authorId: 'u1', content: 'hi there' }),
    );
  });

  test('createComment: empty content → ValidationError', async () => {
    await expect(
      commentsService.createComment('r', 'p', 'u1', { content: '' }),
    ).rejects.toBeInstanceOf(ValidationError);
    expect(sendMock).not.toHaveBeenCalled();
  });

  test('createComment: post missing → NotFoundError', async () => {
    sendMock.mockResolvedValueOnce({ Item: undefined });
    await expect(
      commentsService.createComment('r', 'p', 'u1', { content: 'hi' }),
    ).rejects.toBeInstanceOf(NotFoundError);
  });

  test('createComment: parent comment missing → NotFoundError', async () => {
    // post exists
    sendMock.mockResolvedValueOnce({ Item: { roomId: 'r', postId: 'p' } });
    // parent comment missing
    sendMock.mockResolvedValueOnce({ Item: undefined });

    await expect(
      commentsService.createComment('r', 'p', 'u1', { content: 'hi', parentCommentId: 'c-parent' }),
    ).rejects.toBeInstanceOf(NotFoundError);
  });

  test('listComments: post missing → NotFoundError', async () => {
    sendMock.mockResolvedValueOnce({ Item: undefined });
    await expect(commentsService.listComments('r', 'p')).rejects.toBeInstanceOf(NotFoundError);
  });

  test('listComments: returns items from QueryCommand', async () => {
    sendMock.mockResolvedValueOnce({ Item: { roomId: 'r', postId: 'p' } });
    sendMock.mockResolvedValueOnce({ Items: [{ commentId: 'c1' }, { commentId: 'c2' }] });

    const comments = await commentsService.listComments('r', 'p');
    expect(comments).toHaveLength(2);
    expect(sendMock.mock.calls[1][0]).toBeInstanceOf(QueryCommand);
  });

  test('deleteComment: different author → ForbiddenError, no delete', async () => {
    sendMock.mockResolvedValueOnce({ Item: { authorId: 'someone-else' } });
    await expect(
      commentsService.deleteComment('p', 'c', 'u1'),
    ).rejects.toBeInstanceOf(ForbiddenError);
    expect(sendMock).toHaveBeenCalledTimes(1);
  });

  test('deleteComment: author match → DeleteCommand issued', async () => {
    sendMock.mockResolvedValueOnce({ Item: { authorId: 'u1' } });
    sendMock.mockResolvedValueOnce({});
    await commentsService.deleteComment('p', 'c', 'u1');
    expect(sendMock.mock.calls[1][0]).toBeInstanceOf(DeleteCommand);
  });
});
