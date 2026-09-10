// social-api/src/repositories/__tests__/DLQRepository.test.ts
//
// Tests for DLQRepository: create, get (found/not-found), delete,
// list (limit clamping, errorKind filter, pagination, exclusiveStartKey).

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DLQRepository, type DLQEntry } from '../DLQRepository';

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const docClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

function makeEntry(overrides: Partial<DLQEntry> = {}): DLQEntry {
  return {
    messageId: 'msg-1',
    eventType: 'order.placed',
    eventPayload: { orderId: '123' },
    lastError: 'ValidationError: bad input',
    failedAt: '2026-01-01T00:00:00Z',
    totalAttempts: 3,
    ttl: 1700000000,
    ...overrides,
  };
}

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// create
// ---------------------------------------------------------------------------

describe('DLQRepository.create', () => {
  it('sends PutCommand with all entry fields', async () => {
    const repo = new DLQRepository(docClient);
    const entry = makeEntry();
    await repo.create(entry);

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.TableName).toBe('test-pipeline-dlq');
    expect(cmd.input.Item).toMatchObject({
      messageId: 'msg-1',
      eventType: 'order.placed',
      lastError: 'ValidationError: bad input',
      totalAttempts: 3,
      ttl: 1700000000,
    });
  });

  it('stamps the failedAt-index hash key so list() can see the row', async () => {
    const repo = new DLQRepository(docClient);
    await repo.create(makeEntry());
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Item.partitionKey).toBe('DLQ');
  });
});

// ---------------------------------------------------------------------------
// get
// ---------------------------------------------------------------------------

describe('DLQRepository.get', () => {
  it('returns the entry when found', async () => {
    const entry = makeEntry();
    mockSend.mockResolvedValue({ Item: entry });

    const repo = new DLQRepository(docClient);
    const result = await repo.get('msg-1');
    expect(result).toEqual(entry);
  });

  it('sends GetCommand with the correct key', async () => {
    mockSend.mockResolvedValue({ Item: makeEntry() });
    const repo = new DLQRepository(docClient);
    await repo.get('msg-42');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ messageId: 'msg-42' });
  });

  it('returns null when item is not found', async () => {
    mockSend.mockResolvedValue({});
    const repo = new DLQRepository(docClient);
    const result = await repo.get('nonexistent');
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------

describe('DLQRepository.delete', () => {
  it('sends DeleteCommand with the correct key', async () => {
    const repo = new DLQRepository(docClient);
    await repo.delete('msg-del');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ messageId: 'msg-del' });
    expect(cmd.input.TableName).toBe('test-pipeline-dlq');
  });
});

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

describe('DLQRepository.list — limit clamping', () => {
  beforeEach(() => {
    mockSend.mockResolvedValue({ Items: [] });
  });

  it('defaults limit to 50', async () => {
    const repo = new DLQRepository(docClient);
    await repo.list();
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Limit).toBe(50);
  });

  it('accepts a custom limit', async () => {
    const repo = new DLQRepository(docClient);
    await repo.list({ limit: 10 });
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Limit).toBe(10);
  });

  it('clamps limit to 100', async () => {
    const repo = new DLQRepository(docClient);
    await repo.list({ limit: 999 });
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Limit).toBe(100);
  });

  it('clamps limit to minimum 1', async () => {
    const repo = new DLQRepository(docClient);
    await repo.list({ limit: 0 });
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Limit).toBe(1);
  });
});

describe('DLQRepository.list — query shape', () => {
  beforeEach(() => {
    mockSend.mockResolvedValue({ Items: [] });
  });

  it('queries the failedAt-index with partitionKey=DLQ, newest-first', async () => {
    const repo = new DLQRepository(docClient);
    await repo.list();
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.IndexName).toBe('failedAt-index');
    expect(cmd.input.ScanIndexForward).toBe(false);
    expect(cmd.input.ExpressionAttributeValues[':pk']).toBe('DLQ');
  });

  it('passes exclusiveStartKey through to the query', async () => {
    const startKey = { messageId: 'cursor-token' };
    const repo = new DLQRepository(docClient);
    await repo.list({ exclusiveStartKey: startKey });
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ExclusiveStartKey).toEqual(startKey);
  });

  it('returns lastEvaluatedKey from the response', async () => {
    const lek = { messageId: 'last-seen' };
    mockSend.mockResolvedValue({ Items: [], LastEvaluatedKey: lek });
    const repo = new DLQRepository(docClient);
    const result = await repo.list();
    expect(result.lastEvaluatedKey).toEqual(lek);
  });
});

describe('DLQRepository.list — errorKind filter', () => {
  it('filters entries by lastError prefix when errorKind is specified', async () => {
    const items: DLQEntry[] = [
      makeEntry({ messageId: 'm1', lastError: 'ValidationError: x' }),
      makeEntry({ messageId: 'm2', lastError: 'TimeoutError: y' }),
      makeEntry({ messageId: 'm3', lastError: 'ValidationError: z' }),
    ];
    mockSend.mockResolvedValue({ Items: items });
    const repo = new DLQRepository(docClient);
    const result = await repo.list({ errorKind: 'ValidationError' });
    expect(result.entries).toHaveLength(2);
    expect(result.entries.map((e) => e.messageId)).toEqual(['m1', 'm3']);
  });

  it('returns all entries when errorKind is not specified', async () => {
    const items = [makeEntry({ messageId: 'm1' }), makeEntry({ messageId: 'm2' })];
    mockSend.mockResolvedValue({ Items: items });
    const repo = new DLQRepository(docClient);
    const result = await repo.list();
    expect(result.entries).toHaveLength(2);
  });
});
