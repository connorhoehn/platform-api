// social-api/src/ingest/repositories/__tests__/IngestSourceRepository.test.ts
//
// Tests for IngestSourceRepository: getById, putSource, updateSource
// (expression building, no-op on empty updates), deleteSource, listAll,
// listByStatus.

import { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, DeleteCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { IngestSourceRepository } from '../IngestSourceRepository';
import type { IngestSource } from '../../types';

jest.mock('../../../lib/ddb-table-name', () => ({
    tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

function makeSource(overrides: Partial<IngestSource> = {}): IngestSource {
    return {
        sourceId: 'src-1',
        name: 'Test Source',
        type: 'local',
        status: 'active',
        config: { path: '/data' },
        createdBy: 'user-1',
        createdAt: '2026-01-01T00:00:00Z',
        updatedAt: '2026-01-01T00:00:00Z',
        ...overrides,
    } as IngestSource;
}

beforeEach(() => {
    jest.clearAllMocks();
});

describe('IngestSourceRepository — getById', () => {
    test('returns source when found', async () => {
        const repo = new IngestSourceRepository(mockDocClient);
        const source = makeSource();
        mockSend.mockResolvedValue({ Item: source });
        const result = await repo.getById('src-1');
        expect(result).toEqual(source);
        expect(mockSend.mock.calls[0][0]).toBeInstanceOf(GetCommand);
    });

    test('returns null when not found', async () => {
        const repo = new IngestSourceRepository(mockDocClient);
        mockSend.mockResolvedValue({ Item: undefined });
        expect(await repo.getById('missing')).toBeNull();
    });

    test('uses test-ingest-sources table name', async () => {
        const repo = new IngestSourceRepository(mockDocClient);
        mockSend.mockResolvedValue({ Item: null });
        await repo.getById('src-1');
        expect(mockSend.mock.calls[0][0].input.TableName).toBe('test-ingest-sources');
    });
});

describe('IngestSourceRepository — putSource', () => {
    test('calls PutCommand with source data', async () => {
        const repo = new IngestSourceRepository(mockDocClient);
        mockSend.mockResolvedValue({});
        const source = makeSource();
        await repo.putSource(source);
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(PutCommand);
        expect(cmd.input.Item).toEqual(source);
    });
});

describe('IngestSourceRepository — updateSource', () => {
    test('calls UpdateCommand with correct expression for single field', async () => {
        const repo = new IngestSourceRepository(mockDocClient);
        mockSend.mockResolvedValue({});
        await repo.updateSource('src-1', { name: 'New Name' });
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(UpdateCommand);
        expect(cmd.input.Key).toEqual({ sourceId: 'src-1' });
        expect(cmd.input.ExpressionAttributeValues[':name']).toBe('New Name');
        expect(cmd.input.UpdateExpression).toContain('#name = :name');
    });

    test('builds expression for multiple fields', async () => {
        const repo = new IngestSourceRepository(mockDocClient);
        mockSend.mockResolvedValue({});
        await repo.updateSource('src-1', { name: 'X', status: 'paused' as never });
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd.input.UpdateExpression).toContain('#name = :name');
        expect(cmd.input.UpdateExpression).toContain('#status = :status');
    });

    test('is a no-op when updates object is empty', async () => {
        const repo = new IngestSourceRepository(mockDocClient);
        await repo.updateSource('src-1', {});
        expect(mockSend).not.toHaveBeenCalled();
    });
});

describe('IngestSourceRepository — deleteSource', () => {
    test('calls DeleteCommand with sourceId key', async () => {
        const repo = new IngestSourceRepository(mockDocClient);
        mockSend.mockResolvedValue({});
        await repo.deleteSource('src-del');
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(DeleteCommand);
        expect(cmd.input.Key).toEqual({ sourceId: 'src-del' });
    });
});

describe('IngestSourceRepository — listAll', () => {
    test('scans the table and returns items', async () => {
        const repo = new IngestSourceRepository(mockDocClient);
        const sources = [makeSource({ sourceId: 'a' }), makeSource({ sourceId: 'b' })];
        mockSend.mockResolvedValue({ Items: sources });
        const result = await repo.listAll();
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(ScanCommand);
        expect(result).toHaveLength(2);
    });

    test('caps limit at 500', async () => {
        const repo = new IngestSourceRepository(mockDocClient);
        mockSend.mockResolvedValue({ Items: [] });
        await repo.listAll(999);
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd.input.Limit).toBe(500);
    });

    test('returns empty array when DDB returns no items', async () => {
        const repo = new IngestSourceRepository(mockDocClient);
        mockSend.mockResolvedValue({ Items: undefined });
        const result = await repo.listAll();
        expect(result).toEqual([]);
    });
});

describe('IngestSourceRepository — listByStatus', () => {
    test('scans with FilterExpression on status', async () => {
        const repo = new IngestSourceRepository(mockDocClient);
        mockSend.mockResolvedValue({ Items: [makeSource()] });
        const result = await repo.listByStatus('active');
        const cmd = mockSend.mock.calls[0][0];
        expect(cmd).toBeInstanceOf(ScanCommand);
        expect(cmd.input.FilterExpression).toBe('#status = :status');
        expect(cmd.input.ExpressionAttributeValues[':status']).toBe('active');
        expect(result).toHaveLength(1);
    });
});
