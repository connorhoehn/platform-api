import { TransactionCanceledException } from '@aws-sdk/client-dynamodb';

const mockSend = jest.fn();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: (...args: unknown[]) => mockSend(...args) },
}));

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (base: string) => `test-${base}`,
}));

import { publishWithOutbox, type PublishWithOutboxParams } from '../outbox-publisher';
import { ConflictError } from '../../middleware/error-handler';

function baseParams(overrides: Partial<PublishWithOutboxParams> = {}): PublishWithOutboxParams {
  return {
    target: {
      TableName: 'test-posts',
      Item: { postId: 'p-1', body: 'hello' },
    },
    eventType: 'social.post.created',
    queueName: 'post-events',
    eventPayload: { postId: 'p-1' },
    ...overrides,
  };
}

beforeEach(() => mockSend.mockReset());

describe('publishWithOutbox', () => {
  it('sends a TransactWriteCommand with target + outbox items', async () => {
    mockSend.mockResolvedValue({});
    await publishWithOutbox(baseParams());
    expect(mockSend).toHaveBeenCalledTimes(1);
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.TransactItems).toHaveLength(2);
    expect(cmd.input.TransactItems[0].Put.TableName).toBe('test-posts');
    expect(cmd.input.TransactItems[0].Put.Item.postId).toBe('p-1');
    expect(cmd.input.TransactItems[1].Put.TableName).toBe('test-social-outbox');
    expect(cmd.input.TransactItems[1].Put.Item.status).toBe('UNPROCESSED');
    expect(cmd.input.TransactItems[1].Put.Item.eventType).toBe('social.post.created');
  });

  it('includes ConditionExpression when provided', async () => {
    mockSend.mockResolvedValue({});
    await publishWithOutbox(baseParams({
      target: {
        TableName: 'test-posts',
        Item: { postId: 'p-1' },
        ConditionExpression: 'attribute_not_exists(postId)',
        ExpressionAttributeNames: { '#pk': 'postId' },
        ExpressionAttributeValues: { ':pk': 'p-1' },
      },
    }));
    const put = mockSend.mock.calls[0][0].input.TransactItems[0].Put;
    expect(put.ConditionExpression).toBe('attribute_not_exists(postId)');
    expect(put.ExpressionAttributeNames).toEqual({ '#pk': 'postId' });
    expect(put.ExpressionAttributeValues).toEqual({ ':pk': 'p-1' });
  });

  it('serialises eventPayload as JSON string in outbox row', async () => {
    mockSend.mockResolvedValue({});
    await publishWithOutbox(baseParams({ eventPayload: { key: 'val', num: 42 } }));
    const outboxItem = mockSend.mock.calls[0][0].input.TransactItems[1].Put.Item;
    const parsed = JSON.parse(outboxItem.payload);
    expect(parsed.key).toBe('val');
    expect(parsed.num).toBe(42);
    expect(parsed.timestamp).toBeDefined();
  });

  it('throws ConflictError on ConditionalCheckFailed', async () => {
    const err = new TransactionCanceledException({
      $metadata: {},
      message: 'Transaction cancelled',
      CancellationReasons: [{ Code: 'ConditionalCheckFailed' }, { Code: 'None' }],
    });
    mockSend.mockRejectedValue(err);
    await expect(publishWithOutbox(baseParams())).rejects.toThrow(ConflictError);
  });

  it('uses custom conflictMessage when provided', async () => {
    const err = new TransactionCanceledException({
      $metadata: {},
      message: 'Transaction cancelled',
      CancellationReasons: [{ Code: 'ConditionalCheckFailed' }, { Code: 'None' }],
    });
    mockSend.mockRejectedValue(err);
    await expect(
      publishWithOutbox(baseParams({ conflictMessage: 'Already reacted' })),
    ).rejects.toThrow('Already reacted');
  });

  it('re-throws non-ConditionalCheckFailed TransactionCanceledException', async () => {
    const err = new TransactionCanceledException({
      $metadata: {},
      message: 'Transaction cancelled',
      CancellationReasons: [{ Code: 'ValidationError' }, { Code: 'None' }],
    });
    mockSend.mockRejectedValue(err);
    await expect(publishWithOutbox(baseParams())).rejects.toThrow(TransactionCanceledException);
  });

  it('re-throws unexpected errors', async () => {
    mockSend.mockRejectedValue(new Error('network timeout'));
    await expect(publishWithOutbox(baseParams())).rejects.toThrow('network timeout');
  });

  it('generates unique outboxId per call', async () => {
    mockSend.mockResolvedValue({});
    await publishWithOutbox(baseParams());
    await publishWithOutbox(baseParams());
    const id1 = mockSend.mock.calls[0][0].input.TransactItems[1].Put.Item.outboxId;
    const id2 = mockSend.mock.calls[1][0].input.TransactItems[1].Put.Item.outboxId;
    expect(id1).not.toBe(id2);
  });
});
