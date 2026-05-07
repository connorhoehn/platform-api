/**
 * Tests for social-api LikesService.
 *
 * Mocks DynamoDB DocClient + broadcaster + publishSocialEvent + profileRepo.
 * Covers: post/comment existence preconditions, ConditionalCheckFailed →
 * ConflictError, not-found on delete, enrichment with display names.
 */

jest.mock('../src/lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
  publishSocialEvent: jest.fn(() => Promise.resolve()),
}));

jest.mock('../src/services/room-broadcaster', () => ({
  broadcastToRoom: jest.fn(() => Promise.resolve()),
}));

jest.mock('../src/repositories', () => ({
  profileRepo: {
    batchGetProfiles: jest.fn(),
  },
}));

// eslint-disable-next-line @typescript-eslint/no-require-imports
const { GetCommand, PutCommand, DeleteCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');

import * as likesService from '../src/services/likes-service';
import { docClient } from '../src/lib/aws-clients';
import { profileRepo } from '../src/repositories';
import {
  NotFoundError,
  ConflictError,
} from '../src/middleware/error-handler';

const sendMock = docClient.send as unknown as jest.Mock;
const batchGetMock = profileRepo.batchGetProfiles as unknown as jest.Mock;

describe('likesService', () => {
  beforeEach(() => {
    sendMock.mockReset();
    batchGetMock.mockReset();
  });

  test('likePost: happy path → Put, returns record', async () => {
    sendMock.mockResolvedValueOnce({ Item: { postId: 'p' } }); // post exists
    sendMock.mockResolvedValueOnce({});                          // put

    const result = await likesService.likePost('r', 'p', 'u1');

    expect(result).toMatchObject({ targetId: 'post:p', userId: 'u1', type: 'like' });
    expect(sendMock.mock.calls[0][0]).toBeInstanceOf(GetCommand);
    expect(sendMock.mock.calls[1][0]).toBeInstanceOf(PutCommand);
  });

  test('likePost: post missing → NotFoundError', async () => {
    sendMock.mockResolvedValueOnce({ Item: undefined });
    await expect(likesService.likePost('r', 'p', 'u1')).rejects.toBeInstanceOf(NotFoundError);
  });

  test('likePost: ConditionalCheckFailed → ConflictError', async () => {
    sendMock.mockResolvedValueOnce({ Item: { postId: 'p' } });
    const err = Object.assign(new Error('already'), { name: 'ConditionalCheckFailedException' });
    sendMock.mockRejectedValueOnce(err);

    await expect(likesService.likePost('r', 'p', 'u1')).rejects.toBeInstanceOf(ConflictError);
  });

  test('unlikePost: like missing → NotFoundError, no delete', async () => {
    sendMock.mockResolvedValueOnce({ Item: undefined });
    await expect(likesService.unlikePost('p', 'u1')).rejects.toBeInstanceOf(NotFoundError);
    expect(sendMock).toHaveBeenCalledTimes(1);
  });

  test('unlikePost: happy path → DeleteCommand issued', async () => {
    sendMock.mockResolvedValueOnce({ Item: { targetId: 'post:p', userId: 'u1' } });
    sendMock.mockResolvedValueOnce({});
    await likesService.unlikePost('p', 'u1');
    expect(sendMock.mock.calls[1][0]).toBeInstanceOf(DeleteCommand);
  });

  test('listPostLikes: enriches with displayName from profileRepo', async () => {
    sendMock.mockResolvedValueOnce({ Item: { postId: 'p' } });
    sendMock.mockResolvedValueOnce({ Items: [{ userId: 'u1' }, { userId: 'u2' }] });
    batchGetMock.mockResolvedValueOnce([
      { userId: 'u1', displayName: 'Alice' },
      { userId: 'u2', displayName: 'Bob' },
    ]);

    const result = await likesService.listPostLikes('r', 'p');

    expect(sendMock.mock.calls[1][0]).toBeInstanceOf(QueryCommand);
    expect(result).toEqual({
      count: 2,
      likedBy: [
        { userId: 'u1', displayName: 'Alice' },
        { userId: 'u2', displayName: 'Bob' },
      ],
    });
  });

  test('listPostLikes: post missing → NotFoundError', async () => {
    sendMock.mockResolvedValueOnce({ Item: undefined });
    await expect(likesService.listPostLikes('r', 'p')).rejects.toBeInstanceOf(NotFoundError);
  });

  test('likeComment: comment missing → NotFoundError', async () => {
    sendMock.mockResolvedValueOnce({ Item: undefined });
    await expect(
      likesService.likeComment('r', 'p', 'c', 'u1'),
    ).rejects.toBeInstanceOf(NotFoundError);
  });
});
