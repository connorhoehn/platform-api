/**
 * Tests for social-api OutboxPublisher.
 *
 * Mocks the DynamoDB DocClient. Verifies that:
 *  - TransactWriteCommand receives both target + outbox entries
 *  - The outbox row has the expected shape (status, eventType, queueName, payload, etc.)
 *  - TransactionCanceledException with ConditionalCheckFailed → ConflictError
 *  - Other errors bubble up unchanged
 */

jest.mock('../src/lib/aws-clients', () => ({
  docClient: {
    send: jest.fn(),
  },
}));

// Note: the test file lives in the gateway repo but exercises social-api code.
// The gateway package.json doesn't depend on @aws-sdk/lib-dynamodb, so we import
// it through social-api's node_modules via an absolute path. TransactionCanceledException
// is also available via the gateway's own @aws-sdk/client-dynamodb dependency.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const { TransactWriteCommand } = require('@aws-sdk/lib-dynamodb');
import { TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { publishWithOutbox } from '../src/services/outbox-publisher';
import { docClient } from '../src/lib/aws-clients';
import { ConflictError } from '../src/middleware/error-handler';

const sendMock = docClient.send as unknown as jest.Mock;

describe('publishWithOutbox', () => {
  beforeEach(() => {
    sendMock.mockReset();
  });

  test('builds TransactWriteCommand with target + outbox items', async () => {
    sendMock.mockResolvedValueOnce({});

    await publishWithOutbox({
      target: {
        TableName: 'social-posts',
        Item: { roomId: 'r1', postId: 'p1', authorId: 'u1' },
      },
      eventType: 'social.post.created',
      queueName: 'social-posts',
      eventPayload: { roomId: 'r1', postId: 'p1' },
    });

    expect(sendMock).toHaveBeenCalledTimes(1);
    const cmd = sendMock.mock.calls[0][0];
    expect(cmd).toBeInstanceOf(TransactWriteCommand);

    const input = cmd.input;
    expect(input.TransactItems).toHaveLength(2);

    // First entry = target
    const target = input.TransactItems[0].Put;
    expect(target.TableName).toBe('social-posts');
    expect(target.Item).toEqual({ roomId: 'r1', postId: 'p1', authorId: 'u1' });

    // Second entry = outbox row
    const outbox = input.TransactItems[1].Put;
    expect(outbox.TableName).toBe('social-outbox');
    expect(outbox.Item).toEqual(expect.objectContaining({
      status: 'UNPROCESSED',
      eventType: 'social.post.created',
      queueName: 'social-posts',
    }));
    expect(typeof outbox.Item.outboxId).toBe('string');
    expect(outbox.Item.outboxId.length).toBeGreaterThan(0);
    expect(typeof outbox.Item.createdAt).toBe('string');

    // Payload is a JSON string containing the caller's event payload + timestamp
    const parsed = JSON.parse(outbox.Item.payload as string);
    expect(parsed).toEqual(expect.objectContaining({ roomId: 'r1', postId: 'p1' }));
    expect(parsed.timestamp).toBeDefined();
  });

  test('passes through ConditionExpression + ExpressionAttributeNames/Values', async () => {
    sendMock.mockResolvedValueOnce({});

    await publishWithOutbox({
      target: {
        TableName: 'social-likes',
        Item: { targetId: 'x', userId: 'u1' },
        ConditionExpression: 'attribute_not_exists(#uid)',
        ExpressionAttributeNames: { '#uid': 'userId' },
        ExpressionAttributeValues: { ':foo': 'bar' },
      },
      eventType: 'social.reaction',
      queueName: 'social-reactions',
      eventPayload: {},
    });

    const target = sendMock.mock.calls[0][0].input.TransactItems[0].Put;
    expect(target.ConditionExpression).toBe('attribute_not_exists(#uid)');
    expect(target.ExpressionAttributeNames).toEqual({ '#uid': 'userId' });
    expect(target.ExpressionAttributeValues).toEqual({ ':foo': 'bar' });
  });

  test('TransactionCanceledException + ConditionalCheckFailed → ConflictError', async () => {
    const cancelErr = new TransactionCanceledException({
      message: 'txn canceled',
      $metadata: {},
      CancellationReasons: [
        { Code: 'ConditionalCheckFailed' },
        { Code: 'None' },
      ],
    });
    sendMock.mockRejectedValueOnce(cancelErr);

    await expect(
      publishWithOutbox({
        target: { TableName: 'x', Item: {} },
        eventType: 'e',
        queueName: 'q',
        eventPayload: {},
        conflictMessage: 'Already exists',
      }),
    ).rejects.toMatchObject({
      status: 409,
      message: 'Already exists',
    });
  });

  test('TransactionCanceledException without ConditionalCheckFailed → rethrows original', async () => {
    const cancelErr = new TransactionCanceledException({
      message: 'other',
      $metadata: {},
      CancellationReasons: [{ Code: 'TransactionConflict' }],
    });
    sendMock.mockRejectedValueOnce(cancelErr);

    await expect(
      publishWithOutbox({
        target: { TableName: 'x', Item: {} },
        eventType: 'e',
        queueName: 'q',
        eventPayload: {},
      }),
    ).rejects.toBe(cancelErr);
  });

  test('non-transactional error bubbles up unchanged', async () => {
    const err = new Error('network flaked');
    sendMock.mockRejectedValueOnce(err);

    await expect(
      publishWithOutbox({
        target: { TableName: 'x', Item: {} },
        eventType: 'e',
        queueName: 'q',
        eventPayload: {},
      }),
    ).rejects.toBe(err);
  });

  test('ConflictError thrown is an instance of ConflictError (for error middleware)', async () => {
    const cancelErr = new TransactionCanceledException({
      message: 'txn canceled',
      $metadata: {},
      CancellationReasons: [{ Code: 'ConditionalCheckFailed' }],
    });
    sendMock.mockRejectedValueOnce(cancelErr);

    try {
      await publishWithOutbox({
        target: { TableName: 'x', Item: {} },
        eventType: 'e',
        queueName: 'q',
        eventPayload: {},
      });
      fail('expected ConflictError');
    } catch (err) {
      expect(err).toBeInstanceOf(ConflictError);
    }
  });
});
