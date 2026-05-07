// social-api/src/ingest/repositories/__tests__/IngestAuditRepository.test.ts
//
// Tests for IngestAuditRepository (hub#2213). Covers: table-name wiring,
// put delegation to BaseRepository, getById, listByTarget + listByActor
// GSI usage, listAll filter-expression building + newest-first sort.

import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  ScanCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { IngestAuditRepository } from '../IngestAuditRepository';
import type { IngestAuditEntry } from '../../types';

jest.mock('../../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

function makeEntry(over: Partial<IngestAuditEntry> = {}): IngestAuditEntry {
  return {
    auditId: 'a-1',
    createdAt: '2026-04-23T00:00:00.000Z',
    actor: 'op@example.com',
    eventType: 'source.updated',
    targetType: 'source',
    targetId: 'src-1',
    summary: 'updated mapping rules',
    detail: { before: { rules: 0 }, after: { rules: 2 } },
    ...over,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('IngestAuditRepository — constructor', () => {
  test('uses test-ingest-audit table name', () => {
    const repo = new IngestAuditRepository(mockDocClient);
    mockSend.mockResolvedValue({ Item: null });
    void repo.getById('a-1');
    const cmd = mockSend.mock.calls[0][0] as GetCommand;
    expect(cmd.input.TableName).toBe('test-ingest-audit');
    expect(cmd.input.Key).toEqual({ auditId: 'a-1' });
  });
});

describe('put / getById', () => {
  test('put delegates to PutCommand with the entry as the Item', async () => {
    const repo = new IngestAuditRepository(mockDocClient);
    mockSend.mockResolvedValue({});
    const entry = makeEntry();
    await repo.put(entry);
    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd.input.TableName).toBe('test-ingest-audit');
    expect(cmd.input.Item).toEqual(entry);
  });

  test('getById returns null when DDB has no item', async () => {
    const repo = new IngestAuditRepository(mockDocClient);
    mockSend.mockResolvedValue({ Item: undefined });
    const out = await repo.getById('missing');
    expect(out).toBeNull();
  });

  test('getById returns the row when DDB has an item', async () => {
    const repo = new IngestAuditRepository(mockDocClient);
    const entry = makeEntry({ auditId: 'a-found' });
    mockSend.mockResolvedValue({ Item: entry });
    const out = await repo.getById('a-found');
    expect(out).toEqual(entry);
  });
});

describe('listByTarget — targetId-createdAt GSI', () => {
  test('queries the right GSI in descending order with the right limit', async () => {
    const repo = new IngestAuditRepository(mockDocClient);
    mockSend.mockResolvedValue({ Items: [makeEntry()] });
    await repo.listByTarget('src-x', 50);
    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('targetId-createdAt-index');
    expect(cmd.input.KeyConditionExpression).toBe('targetId = :targetId');
    expect(cmd.input.ExpressionAttributeValues).toEqual({ ':targetId': 'src-x' });
    expect(cmd.input.ScanIndexForward).toBe(false);
    expect(cmd.input.Limit).toBe(50);
  });

  test('caps limit at 500 even when caller passes a larger value', async () => {
    const repo = new IngestAuditRepository(mockDocClient);
    mockSend.mockResolvedValue({ Items: [] });
    await repo.listByTarget('src-x', 9999);
    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(500);
  });
});

describe('listByActor — actor-createdAt GSI', () => {
  test('queries the right GSI keyed on actor', async () => {
    const repo = new IngestAuditRepository(mockDocClient);
    mockSend.mockResolvedValue({ Items: [makeEntry({ actor: 'system' })] });
    await repo.listByActor('system');
    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('actor-createdAt-index');
    expect(cmd.input.KeyConditionExpression).toBe('actor = :actor');
    expect(cmd.input.ExpressionAttributeValues).toEqual({ ':actor': 'system' });
    expect(cmd.input.ScanIndexForward).toBe(false);
  });
});

describe('listAll — scan + filter expressions', () => {
  test('emits no FilterExpression when no filters are passed', async () => {
    const repo = new IngestAuditRepository(mockDocClient);
    mockSend.mockResolvedValue({ Items: [makeEntry()] });
    await repo.listAll();
    const cmd = mockSend.mock.calls[0][0] as ScanCommand;
    expect(cmd.input.FilterExpression).toBeUndefined();
    expect(cmd.input.Limit).toBe(100);
  });

  test('builds a since + until + eventType FilterExpression with the right values', async () => {
    const repo = new IngestAuditRepository(mockDocClient);
    mockSend.mockResolvedValue({ Items: [] });
    await repo.listAll({
      since: '2026-04-23T00:00:00Z',
      until: '2026-04-24T00:00:00Z',
      eventType: 'source.paused',
    });
    const cmd = mockSend.mock.calls[0][0] as ScanCommand;
    expect(cmd.input.FilterExpression).toContain('createdAt >= :since');
    expect(cmd.input.FilterExpression).toContain('createdAt < :until');
    expect(cmd.input.FilterExpression).toContain('#eventType = :eventType');
    expect(cmd.input.ExpressionAttributeNames).toEqual({ '#eventType': 'eventType' });
    expect(cmd.input.ExpressionAttributeValues).toEqual({
      ':since': '2026-04-23T00:00:00Z',
      ':until': '2026-04-24T00:00:00Z',
      ':eventType': 'source.paused',
    });
  });

  test('returns rows sorted newest-first regardless of scan order', async () => {
    const repo = new IngestAuditRepository(mockDocClient);
    const older = makeEntry({ auditId: 'old', createdAt: '2026-04-20T00:00:00Z' });
    const newer = makeEntry({ auditId: 'new', createdAt: '2026-04-24T00:00:00Z' });
    // Scan returns un-ordered rows; the repo sorts.
    mockSend.mockResolvedValue({ Items: [older, newer] });
    const out = await repo.listAll();
    expect(out.map((e) => e.auditId)).toEqual(['new', 'old']);
  });

  test('respects custom limit, capped at 500', async () => {
    const repo = new IngestAuditRepository(mockDocClient);
    mockSend.mockResolvedValue({ Items: [] });
    await repo.listAll({ limit: 9999 });
    const cmd = mockSend.mock.calls[0][0] as ScanCommand;
    expect(cmd.input.Limit).toBe(500);
  });
});
