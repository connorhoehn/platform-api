// social-api/src/repositories/__tests__/BaseRepository.test.ts
//
// Tests for BaseRepository: getItem (found / null), putItem, putItemConditional
// (with and without ExpressionAttributeNames), deleteItem, updateItem (returns
// Attributes), query (empty and populated Items), queryWithPagination
// (lastEvaluatedKey present / absent).

import { describe, it, expect, jest, beforeEach } from '@jest/globals';
import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from '../BaseRepository';

// ---------------------------------------------------------------------------
// Mock docClient
// ---------------------------------------------------------------------------

const mockSend = jest.fn<(cmd: unknown) => Promise<unknown>>();

const docClient = {
  send: mockSend,
} as unknown as DynamoDBDocumentClient;

class TestRepo extends BaseRepository {
  constructor() {
    super('test-table', docClient);
  }
}

beforeEach(() => {
  jest.resetAllMocks();
});

// ---------------------------------------------------------------------------
// getItem
// ---------------------------------------------------------------------------

describe('BaseRepository.getItem()', () => {
  it('returns the Item when found', async () => {
    mockSend.mockResolvedValue({ Item: { pk: 'user#1', name: 'Alice' } });

    const repo = new TestRepo();
    const result = await repo.getItem({ pk: 'user#1' });
    expect(result).toEqual({ pk: 'user#1', name: 'Alice' });
  });

  it('returns null when Item is undefined', async () => {
    mockSend.mockResolvedValue({});

    const repo = new TestRepo();
    const result = await repo.getItem({ pk: 'missing' });
    expect(result).toBeNull();
  });

  it('passes Key and TableName to GetCommand', async () => {
    mockSend.mockResolvedValue({ Item: {} });

    const repo = new TestRepo();
    await repo.getItem({ pk: 'x', sk: 'y' });

    const cmd = (mockSend.mock.calls[0] as [{ input: unknown }])[0];
    expect(cmd.input).toMatchObject({
      TableName: 'test-table',
      Key: { pk: 'x', sk: 'y' },
    });
  });
});

// ---------------------------------------------------------------------------
// putItem
// ---------------------------------------------------------------------------

describe('BaseRepository.putItem()', () => {
  it('sends PutCommand with the item', async () => {
    mockSend.mockResolvedValue({});

    const repo = new TestRepo();
    await repo.putItem({ pk: 'user#1', name: 'Bob' });

    const cmd = (mockSend.mock.calls[0] as [{ input: unknown }])[0];
    expect(cmd.input).toMatchObject({
      TableName: 'test-table',
      Item: { pk: 'user#1', name: 'Bob' },
    });
  });

  it('resolves to undefined (void)', async () => {
    mockSend.mockResolvedValue({});
    const repo = new TestRepo();
    await expect(repo.putItem({ pk: 'a' })).resolves.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// putItemConditional
// ---------------------------------------------------------------------------

describe('BaseRepository.putItemConditional()', () => {
  it('includes ConditionExpression', async () => {
    mockSend.mockResolvedValue({});

    const repo = new TestRepo();
    await repo.putItemConditional({ pk: 'x' }, 'attribute_not_exists(pk)');

    const cmd = (mockSend.mock.calls[0] as [{ input: unknown }])[0];
    expect(cmd.input).toMatchObject({
      TableName: 'test-table',
      ConditionExpression: 'attribute_not_exists(pk)',
    });
  });

  it('includes ExpressionAttributeNames when provided', async () => {
    mockSend.mockResolvedValue({});

    const repo = new TestRepo();
    await repo.putItemConditional({ pk: 'x' }, 'attribute_not_exists(#n)', {
      '#n': 'name',
    });

    const cmd = (mockSend.mock.calls[0] as [{ input: unknown }])[0];
    expect(cmd.input).toMatchObject({
      ExpressionAttributeNames: { '#n': 'name' },
    });
  });

  it('omits ExpressionAttributeNames when not provided', async () => {
    mockSend.mockResolvedValue({});

    const repo = new TestRepo();
    await repo.putItemConditional({ pk: 'x' }, 'attribute_not_exists(pk)');

    const cmd = (mockSend.mock.calls[0] as [{ input: unknown }])[0];
    expect((cmd.input as Record<string, unknown>)['ExpressionAttributeNames']).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// deleteItem
// ---------------------------------------------------------------------------

describe('BaseRepository.deleteItem()', () => {
  it('sends DeleteCommand with key and tableName', async () => {
    mockSend.mockResolvedValue({});

    const repo = new TestRepo();
    await repo.deleteItem({ pk: 'user#1', sk: 'profile' });

    const cmd = (mockSend.mock.calls[0] as [{ input: unknown }])[0];
    expect(cmd.input).toMatchObject({
      TableName: 'test-table',
      Key: { pk: 'user#1', sk: 'profile' },
    });
  });
});

// ---------------------------------------------------------------------------
// updateItem
// ---------------------------------------------------------------------------

describe('BaseRepository.updateItem()', () => {
  it('returns result.Attributes', async () => {
    mockSend.mockResolvedValue({ Attributes: { pk: 'x', count: 5 } });

    const repo = new TestRepo();
    const result = await repo.updateItem({
      Key: { pk: 'x' },
      UpdateExpression: 'SET #c = :v',
      ExpressionAttributeNames: { '#c': 'count' },
      ExpressionAttributeValues: { ':v': 5 },
    });

    expect(result).toEqual({ pk: 'x', count: 5 });
  });

  it('returns undefined when Attributes is absent', async () => {
    mockSend.mockResolvedValue({});

    const repo = new TestRepo();
    const result = await repo.updateItem({ Key: { pk: 'x' } });
    expect(result).toBeUndefined();
  });

  it('merges params with TableName', async () => {
    mockSend.mockResolvedValue({});

    const repo = new TestRepo();
    await repo.updateItem({ Key: { pk: 'z' }, UpdateExpression: 'SET x = :x' });

    const cmd = (mockSend.mock.calls[0] as [{ input: unknown }])[0];
    expect(cmd.input).toMatchObject({
      TableName: 'test-table',
      Key: { pk: 'z' },
    });
  });
});

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------

describe('BaseRepository.query()', () => {
  it('returns Items from the response', async () => {
    const items = [{ pk: 'a' }, { pk: 'b' }];
    mockSend.mockResolvedValue({ Items: items });

    const repo = new TestRepo();
    const result = await repo.query({ KeyConditionExpression: 'pk = :pk' });
    expect(result).toEqual(items);
  });

  it('returns [] when Items is undefined', async () => {
    mockSend.mockResolvedValue({});

    const repo = new TestRepo();
    const result = await repo.query({});
    expect(result).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// queryWithPagination
// ---------------------------------------------------------------------------

describe('BaseRepository.queryWithPagination()', () => {
  it('returns items and lastEvaluatedKey when present', async () => {
    const items = [{ pk: 'x' }];
    const lek = { pk: 'x', sk: 'last' };
    mockSend.mockResolvedValue({ Items: items, LastEvaluatedKey: lek });

    const repo = new TestRepo();
    const result = await repo.queryWithPagination({});
    expect(result.items).toEqual(items);
    expect(result.lastEvaluatedKey).toEqual(lek);
  });

  it('returns lastEvaluatedKey as undefined when absent', async () => {
    mockSend.mockResolvedValue({ Items: [] });

    const repo = new TestRepo();
    const result = await repo.queryWithPagination({});
    expect(result.lastEvaluatedKey).toBeUndefined();
  });

  it('returns [] for items when Items is undefined', async () => {
    mockSend.mockResolvedValue({});

    const repo = new TestRepo();
    const result = await repo.queryWithPagination({});
    expect(result.items).toEqual([]);
  });
});
