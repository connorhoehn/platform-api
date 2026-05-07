// social-api/src/repositories/__tests__/PipelineDLQRepository.test.ts
//
// Unit tests for PipelineDLQRepository: query construction, limit clamping,
// listAll sort order, and update expression shapes for markRetried/markDiscarded.

import { DynamoDBDocumentClient, QueryCommand, ScanCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { PipelineDLQRepository, type PipelineDLQItem } from '../PipelineDLQRepository';

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

function dlqItem(overrides: Partial<PipelineDLQItem> = {}): PipelineDLQItem {
  return {
    messageId: 'msg-1',
    topic: 'pipeline.event',
    payload: {},
    error: 'Connection refused',
    status: 'pending',
    failedAt: '2026-05-01T10:00:00.000Z',
    retryCount: 0,
    ...overrides,
  };
}

beforeEach(() => {
  mockSend.mockReset();
});

describe('PipelineDLQRepository — listPending', () => {
  test('queries the failedAt-index GSI with status=pending, newest first', async () => {
    mockSend.mockResolvedValue({ Items: [dlqItem()] });
    const repo = new PipelineDLQRepository(mockDocClient);
    const result = await repo.listPending();

    expect(mockSend).toHaveBeenCalledTimes(1);
    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('failedAt-index');
    expect(cmd.input.ScanIndexForward).toBe(false);
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':status': 'pending' });
    expect(result).toHaveLength(1);
  });

  test('defaults limit to 50', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new PipelineDLQRepository(mockDocClient);
    await repo.listPending();

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(50);
  });

  test('uses provided limit', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new PipelineDLQRepository(mockDocClient);
    await repo.listPending({ limit: 10 });

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(10);
  });

  test('clamps limit at 200', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new PipelineDLQRepository(mockDocClient);
    await repo.listPending({ limit: 999 });

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(200);
  });
});

describe('PipelineDLQRepository — listAll', () => {
  test('uses ScanCommand and sorts results newest-failedAt first', async () => {
    const older = dlqItem({ messageId: 'old', failedAt: '2026-05-01T08:00:00.000Z' });
    const newer = dlqItem({ messageId: 'new', failedAt: '2026-05-01T12:00:00.000Z' });
    mockSend.mockResolvedValue({ Items: [older, newer] });

    const repo = new PipelineDLQRepository(mockDocClient);
    const result = await repo.listAll();

    const cmd = mockSend.mock.calls[0][0] as ScanCommand;
    expect(cmd).toBeInstanceOf(ScanCommand);
    expect(result[0]!.messageId).toBe('new');
    expect(result[1]!.messageId).toBe('old');
  });

  test('clamps limit at 200', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new PipelineDLQRepository(mockDocClient);
    await repo.listAll({ limit: 500 });

    const cmd = mockSend.mock.calls[0][0] as ScanCommand;
    expect(cmd.input.Limit).toBe(200);
  });

  test('returns empty array when scan yields no items', async () => {
    mockSend.mockResolvedValue({ Items: undefined });
    const repo = new PipelineDLQRepository(mockDocClient);
    const result = await repo.listAll();
    expect(result).toEqual([]);
  });
});

describe('PipelineDLQRepository — markRetried', () => {
  test('sets status=retried and retriedAt in the update expression', async () => {
    mockSend.mockResolvedValue({});
    const repo = new PipelineDLQRepository(mockDocClient);
    await repo.markRetried('msg-99');

    expect(mockSend).toHaveBeenCalledTimes(1);
    const cmd = mockSend.mock.calls[0][0] as UpdateCommand;
    expect(cmd.input.Key).toEqual({ messageId: 'msg-99' });
    expect(cmd.input.UpdateExpression).toContain('retriedAt');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':status': 'retried' });
    // retriedAt value should be a non-empty ISO string.
    const retriedAt = cmd.input.ExpressionAttributeValues?.[':retriedAt'];
    expect(typeof retriedAt).toBe('string');
    expect((retriedAt as string).length).toBeGreaterThan(10);
  });
});

describe('PipelineDLQRepository — markDiscarded', () => {
  test('sets status=discarded without touching retriedAt', async () => {
    mockSend.mockResolvedValue({});
    const repo = new PipelineDLQRepository(mockDocClient);
    await repo.markDiscarded('msg-77');

    const cmd = mockSend.mock.calls[0][0] as UpdateCommand;
    expect(cmd.input.Key).toEqual({ messageId: 'msg-77' });
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':status': 'discarded' });
    expect(cmd.input.UpdateExpression).not.toContain('retriedAt');
  });
});
