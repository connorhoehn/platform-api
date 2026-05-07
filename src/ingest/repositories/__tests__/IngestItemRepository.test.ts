// social-api/src/ingest/repositories/__tests__/IngestItemRepository.test.ts
//
// Tests for IngestItemRepository: CRUD delegation to BaseRepository,
// updateStatus expression building, listBySource (with/without status),
// listByStatus scan, delete.

import { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, DeleteCommand, ScanCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { IngestItemRepository } from '../IngestItemRepository';
import type { IngestItem } from '../../types';

jest.mock('../../../lib/ddb-table-name', () => ({
    tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

function makeItem(overrides: Partial<IngestItem> = {}): IngestItem {
    return {
        ingestId: 'item-1',
        sourceId: 'src-1',
        filePath: '/docs/a.txt',
        fileHash: 'abc',
        mimeType: 'text/plain',
        fileSize: 100,
        status: 'queued',
        mappedTypeId: null,
        mappingConfidence: null,
        mappingMethod: null,
        documentId: null,
        pipelineRunId: null,
        error: null,
        attempts: 0,
        createdAt: '2026-01-01T00:00:00Z',
        updatedAt: '2026-01-01T00:00:00Z',
        ...overrides,
    };
}

beforeEach(() => {
    jest.clearAllMocks();
});

describe('IngestItemRepository — constructor', () => {
    test('uses test-ingest-items table name', () => {
        const repo = new IngestItemRepository(mockDocClient);
        // Access via getById which passes tableName to GetCommand
        mockSend.mockResolvedValue({ Item: null });
        repo.getById('x');
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd.input.TableName).toBe('test-ingest-items');
    });
});

describe('IngestItemRepository — getById', () => {
    test('returns item when found', async () => {
        const repo = new IngestItemRepository(mockDocClient);
        const item = makeItem();
        mockSend.mockResolvedValue({ Item: item });
        const result = await repo.getById('item-1');
        expect(result).toEqual(item);
        expect(mockSend.mock.calls[0][0]).toBeInstanceOf(GetCommand);
    });

    test('returns null when not found', async () => {
        const repo = new IngestItemRepository(mockDocClient);
        mockSend.mockResolvedValue({ Item: undefined });
        const result = await repo.getById('missing');
        expect(result).toBeNull();
    });
});

describe('IngestItemRepository — put', () => {
    test('calls PutCommand with the item', async () => {
        const repo = new IngestItemRepository(mockDocClient);
        mockSend.mockResolvedValue({});
        const item = makeItem();
        await repo.put(item);
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(PutCommand);
        expect(cmd.input.Item).toEqual(item);
    });
});

describe('IngestItemRepository — updateStatus', () => {
    test('calls UpdateCommand with status and updatedAt', async () => {
        const repo = new IngestItemRepository(mockDocClient);
        mockSend.mockResolvedValue({});
        await repo.updateStatus('item-1', 'done');
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(UpdateCommand);
        expect(cmd.input.Key).toEqual({ ingestId: 'item-1' });
        expect(cmd.input.ExpressionAttributeValues[':status']).toBe('done');
        expect(typeof cmd.input.ExpressionAttributeValues[':updatedAt']).toBe('string');
    });

    test('includes extra fields in the update expression', async () => {
        const repo = new IngestItemRepository(mockDocClient);
        mockSend.mockResolvedValue({});
        await repo.updateStatus('item-1', 'failed', { error: 'boom', attempts: 3 });
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd.input.ExpressionAttributeValues[':error']).toBe('boom');
        expect(cmd.input.ExpressionAttributeValues[':attempts']).toBe(3);
        expect(cmd.input.UpdateExpression).toContain('#error = :error');
        expect(cmd.input.UpdateExpression).toContain('#attempts = :attempts');
    });
});

describe('IngestItemRepository — listBySource', () => {
    test('queries sourceId-status-index with just sourceId when no status', async () => {
        const repo = new IngestItemRepository(mockDocClient);
        mockSend.mockResolvedValue({ Items: [makeItem()] });
        const results = await repo.listBySource('src-1');
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(QueryCommand);
        expect(cmd.input.IndexName).toBe('sourceId-status-index');
        expect(cmd.input.ExpressionAttributeValues[':sourceId']).toBe('src-1');
        expect(results).toHaveLength(1);
    });

    test('queries with status filter when status provided', async () => {
        const repo = new IngestItemRepository(mockDocClient);
        mockSend.mockResolvedValue({ Items: [] });
        await repo.listBySource('src-1', 'done');
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd.input.ExpressionAttributeValues[':status']).toBe('done');
        expect(cmd.input.KeyConditionExpression).toContain('#status');
    });

    test('returns empty array when DDB returns no items', async () => {
        const repo = new IngestItemRepository(mockDocClient);
        mockSend.mockResolvedValue({ Items: undefined });
        // query in BaseRepository wraps items array
        mockSend.mockResolvedValue({ Items: [] });
        const results = await repo.listBySource('src-empty');
        expect(results).toEqual([]);
    });
});

describe('IngestItemRepository — listByStatus', () => {
    test('scans with FilterExpression on status', async () => {
        const repo = new IngestItemRepository(mockDocClient);
        mockSend.mockResolvedValue({ Items: [makeItem({ status: 'queued' })] });
        const results = await repo.listByStatus('queued');
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(ScanCommand);
        expect(cmd.input.FilterExpression).toBe('#status = :status');
        expect(cmd.input.ExpressionAttributeValues[':status']).toBe('queued');
        expect(results).toHaveLength(1);
    });

    test('caps limit at 200', async () => {
        const repo = new IngestItemRepository(mockDocClient);
        mockSend.mockResolvedValue({ Items: [] });
        await repo.listByStatus('done', 999);
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd.input.Limit).toBe(200);
    });

    test('omits Limit when not provided', async () => {
        const repo = new IngestItemRepository(mockDocClient);
        mockSend.mockResolvedValue({ Items: [] });
        await repo.listByStatus('done');
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd.input.Limit).toBeUndefined();
    });
});

describe('IngestItemRepository — delete', () => {
    test('calls DeleteCommand with ingestId key', async () => {
        const repo = new IngestItemRepository(mockDocClient);
        mockSend.mockResolvedValue({});
        await repo.delete('item-del');
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(DeleteCommand);
        expect(cmd.input.Key).toEqual({ ingestId: 'item-del' });
    });
});
