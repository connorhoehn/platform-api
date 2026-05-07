// social-api/src/ingest/repositories/__tests__/SyncCursorRepository.test.ts
//
// Tests for SyncCursorRepository: get, put, listBySource, deleteBySource
// (batch-delete with chunking).

import { DynamoDBDocumentClient, GetCommand, PutCommand, BatchWriteCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { SyncCursorRepository } from '../SyncCursorRepository';
import type { SyncCursorEntry } from '../../types';

jest.mock('../../../lib/ddb-table-name', () => ({
    tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

function makeEntry(sourceId: string, filePath: string): SyncCursorEntry {
    return {
        sourceId,
        filePath,
        fileHash: 'abc',
        lastModified: '2026-01-01T00:00:00Z',
        lastIngestId: 'ingest-1',
        documentId: null,
    };
}

beforeEach(() => {
    jest.clearAllMocks();
});

describe('SyncCursorRepository — get', () => {
    test('returns entry when found', async () => {
        const repo = new SyncCursorRepository(mockDocClient);
        const entry = makeEntry('src-1', '/docs/a.txt');
        mockSend.mockResolvedValue({ Item: entry });
        const result = await repo.get('src-1', '/docs/a.txt');
        expect(result).toEqual(entry);
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(GetCommand);
        expect(cmd.input.Key).toEqual({ sourceId: 'src-1', filePath: '/docs/a.txt' });
    });

    test('returns null when not found', async () => {
        const repo = new SyncCursorRepository(mockDocClient);
        mockSend.mockResolvedValue({ Item: undefined });
        expect(await repo.get('src-1', '/ghost.txt')).toBeNull();
    });

    test('uses test-ingest-sync-state table name', async () => {
        const repo = new SyncCursorRepository(mockDocClient);
        mockSend.mockResolvedValue({ Item: null });
        await repo.get('src-1', '/f.txt');
        expect(mockSend.mock.calls[0][0].input.TableName).toBe('test-ingest-sync-state');
    });
});

describe('SyncCursorRepository — put', () => {
    test('calls PutCommand with entry', async () => {
        const repo = new SyncCursorRepository(mockDocClient);
        mockSend.mockResolvedValue({});
        const entry = makeEntry('src-1', '/docs/b.txt');
        await repo.put(entry);
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(PutCommand);
        expect(cmd.input.Item).toEqual(entry);
    });
});

describe('SyncCursorRepository — listBySource', () => {
    test('queries with sourceId key condition', async () => {
        const repo = new SyncCursorRepository(mockDocClient);
        const entries = [makeEntry('src-1', '/a.txt'), makeEntry('src-1', '/b.txt')];
        mockSend.mockResolvedValue({ Items: entries });
        const result = await repo.listBySource('src-1');
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(QueryCommand);
        expect(cmd.input.KeyConditionExpression).toBe('sourceId = :sourceId');
        expect(cmd.input.ExpressionAttributeValues[':sourceId']).toBe('src-1');
        expect(result).toHaveLength(2);
    });

    test('returns empty array when no entries found', async () => {
        const repo = new SyncCursorRepository(mockDocClient);
        mockSend.mockResolvedValue({ Items: [] });
        expect(await repo.listBySource('src-empty')).toEqual([]);
    });
});

describe('SyncCursorRepository — deleteBySource', () => {
    test('is a no-op when source has no entries', async () => {
        const repo = new SyncCursorRepository(mockDocClient);
        mockSend.mockResolvedValue({ Items: [] }); // listBySource returns empty
        await repo.deleteBySource('src-empty');
        // Only the query call, no BatchWriteCommand
        expect(mockSend).toHaveBeenCalledTimes(1);
        expect(mockSend.mock.calls[0][0]).toBeInstanceOf(QueryCommand);
    });

    test('sends one BatchWriteCommand for up to 25 entries', async () => {
        const repo = new SyncCursorRepository(mockDocClient);
        const entries = Array.from({ length: 10 }, (_, i) => makeEntry('src-1', `/file-${i}.txt`));
        mockSend
            .mockResolvedValueOnce({ Items: entries }) // listBySource
            .mockResolvedValue({});                    // BatchWriteCommand
        await repo.deleteBySource('src-1');
        expect(mockSend).toHaveBeenCalledTimes(2);
        const batchCmd = mockSend.mock.calls[1][0];
        expect(batchCmd).toBeInstanceOf(BatchWriteCommand);
        expect(batchCmd.input.RequestItems['test-ingest-sync-state']).toHaveLength(10);
    });

    test('chunks 30 entries into 2 BatchWriteCommands (25 + 5)', async () => {
        const repo = new SyncCursorRepository(mockDocClient);
        const entries = Array.from({ length: 30 }, (_, i) => makeEntry('src-1', `/file-${i}.txt`));
        mockSend
            .mockResolvedValueOnce({ Items: entries }) // listBySource
            .mockResolvedValue({});                    // BatchWriteCommand calls
        await repo.deleteBySource('src-1');
        // 1 query + 2 batch deletes
        expect(mockSend).toHaveBeenCalledTimes(3);
        const batch1 = mockSend.mock.calls[1][0];
        const batch2 = mockSend.mock.calls[2][0];
        expect(batch1.input.RequestItems['test-ingest-sync-state']).toHaveLength(25);
        expect(batch2.input.RequestItems['test-ingest-sync-state']).toHaveLength(5);
    });

    test('each delete request has correct key shape', async () => {
        const repo = new SyncCursorRepository(mockDocClient);
        const entry = makeEntry('src-1', '/specific.txt');
        mockSend
            .mockResolvedValueOnce({ Items: [entry] })
            .mockResolvedValue({});
        await repo.deleteBySource('src-1');
        const batchCmd = mockSend.mock.calls[1][0];
        const deleteRequest = batchCmd.input.RequestItems['test-ingest-sync-state'][0];
        expect(deleteRequest.DeleteRequest.Key).toEqual({ sourceId: 'src-1', filePath: '/specific.txt' });
    });
});
