// Unit tests for TypedDocumentRepository bulk operations.
//
// Mocks DynamoDBDocumentClient to test chunking logic, error handling,
// and edge cases in bulk operations without touching DDB.

import { DynamoDBDocumentClient, ScanCommand, BatchWriteCommand } from '@aws-sdk/lib-dynamodb';
import { TypedDocumentRepository, TypedDocumentItem } from '../TypedDocumentRepository';

// Mock the DDB client
const mockSend = jest.fn();
const mockDocClient = {
  send: mockSend,
} as unknown as DynamoDBDocumentClient;

// Mock the table name function
jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

describe('TypedDocumentRepository', () => {
  let repo: TypedDocumentRepository;

  beforeEach(() => {
    jest.clearAllMocks();
    repo = new TypedDocumentRepository(mockDocClient);
  });

  describe('bulkCreate', () => {
    test('chunks 30 items into 2 BatchWriteCommand calls (25 + 5)', async () => {
      // Create 30 test items
      const items: TypedDocumentItem[] = Array.from({ length: 30 }, (_, i) => ({
        documentId: `doc-${i}`,
        typeId: 'test-type',
        values: { field1: `value-${i}` },
        createdBy: 'tester',
        createdAt: '2026-05-04T00:00:00Z',
        updatedAt: '2026-05-04T00:00:00Z',
      }));

      mockSend.mockResolvedValue({});

      await repo.bulkCreate(items);

      // Verify 2 BatchWriteCommand calls
      expect(mockSend).toHaveBeenCalledTimes(2);

      // First call: 25 items
      const firstCall = mockSend.mock.calls[0][0];
      expect(firstCall).toBeInstanceOf(BatchWriteCommand);
      expect(firstCall.input.RequestItems?.['test-typed-documents']).toHaveLength(25);

      // Second call: 5 items
      const secondCall = mockSend.mock.calls[1][0];
      expect(secondCall).toBeInstanceOf(BatchWriteCommand);
      expect(secondCall.input.RequestItems?.['test-typed-documents']).toHaveLength(5);
    });

    test('handles single item without chunking', async () => {
      const item: TypedDocumentItem = {
        documentId: 'doc-1',
        typeId: 'test-type',
        values: { field1: 'value-1' },
        createdBy: 'tester',
        createdAt: '2026-05-04T00:00:00Z',
        updatedAt: '2026-05-04T00:00:00Z',
      };

      mockSend.mockResolvedValue({});

      await repo.bulkCreate([item]);

      expect(mockSend).toHaveBeenCalledTimes(1);
      const call = mockSend.mock.calls[0][0];
      expect(call.input.RequestItems?.['test-typed-documents']).toHaveLength(1);
    });

    test('handles empty array', async () => {
      await repo.bulkCreate([]);
      expect(mockSend).not.toHaveBeenCalled();
    });

    test('handles exact multiple of 25', async () => {
      const items: TypedDocumentItem[] = Array.from({ length: 50 }, (_, i) => ({
        documentId: `doc-${i}`,
        typeId: 'test-type',
        values: { field1: `value-${i}` },
        createdBy: 'tester',
        createdAt: '2026-05-04T00:00:00Z',
        updatedAt: '2026-05-04T00:00:00Z',
      }));

      mockSend.mockResolvedValue({});

      await repo.bulkCreate(items);

      // Verify 2 BatchWriteCommand calls of 25 each
      expect(mockSend).toHaveBeenCalledTimes(2);
      mockSend.mock.calls.forEach((call) => {
        expect(call[0].input.RequestItems?.['test-typed-documents']).toHaveLength(25);
      });
    });
  });

  describe('bulkUpdate', () => {
    test('updates only existing documents', async () => {
      const updates = [
        { documentId: 'doc-1', values: { field1: 'updated-1' }, updatedAt: '2026-05-04T01:00:00Z' },
        { documentId: 'doc-2', values: { field1: 'updated-2' }, updatedAt: '2026-05-04T01:00:00Z' },
        { documentId: 'doc-999', values: { field1: 'updated-999' }, updatedAt: '2026-05-04T01:00:00Z' },
      ];

      // Mock get calls - doc-1 and doc-2 exist, doc-999 does not
      const mockGetItem = jest.spyOn(repo, 'get');
      mockGetItem.mockImplementation(async (documentId: string) => {
        if (documentId === 'doc-1') {
          return {
            documentId: 'doc-1',
            typeId: 'test-type',
            values: { field1: 'original-1' },
            createdBy: 'tester',
            createdAt: '2026-05-04T00:00:00Z',
            updatedAt: '2026-05-04T00:00:00Z',
          };
        }
        if (documentId === 'doc-2') {
          return {
            documentId: 'doc-2',
            typeId: 'test-type',
            values: { field1: 'original-2' },
            createdBy: 'tester',
            createdAt: '2026-05-04T00:00:00Z',
            updatedAt: '2026-05-04T00:00:00Z',
          };
        }
        return null; // doc-999 not found
      });

      mockSend.mockResolvedValue({});

      await repo.bulkUpdate(updates);

      // Verify only 2 items were put (doc-1 and doc-2, not doc-999)
      expect(mockSend).toHaveBeenCalledTimes(1);
      const call = mockSend.mock.calls[0][0];
      expect(call).toBeInstanceOf(BatchWriteCommand);
      expect(call.input.RequestItems?.['test-typed-documents']).toHaveLength(2);

      // Verify merged values
      const putItems = call.input.RequestItems?.['test-typed-documents'];
      expect(putItems?.[0].PutRequest?.Item?.values).toEqual({ field1: 'updated-1' });
      expect(putItems?.[1].PutRequest?.Item?.values).toEqual({ field1: 'updated-2' });
    });

    test('chunks 30 updates into 2 calls', async () => {
      const updates = Array.from({ length: 30 }, (_, i) => ({
        documentId: `doc-${i}`,
        values: { field1: `updated-${i}` },
        updatedAt: '2026-05-04T01:00:00Z',
      }));

      // Mock get to return all items as existing
      const mockGetItem = jest.spyOn(repo, 'get');
      mockGetItem.mockImplementation(async (documentId: string) => ({
        documentId,
        typeId: 'test-type',
        values: { field1: 'original' },
        createdBy: 'tester',
        createdAt: '2026-05-04T00:00:00Z',
        updatedAt: '2026-05-04T00:00:00Z',
      }));

      mockSend.mockResolvedValue({});

      await repo.bulkUpdate(updates);

      // Verify 2 BatchWriteCommand calls (25 + 5)
      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    test('handles empty updates array', async () => {
      await repo.bulkUpdate([]);
      expect(mockSend).not.toHaveBeenCalled();
    });

    test('handles all non-existing documents', async () => {
      const updates = [
        { documentId: 'doc-999', values: { field1: 'updated-999' }, updatedAt: '2026-05-04T01:00:00Z' },
      ];

      const mockGetItem = jest.spyOn(repo, 'get');
      mockGetItem.mockResolvedValue(null);

      mockSend.mockResolvedValue({});

      await repo.bulkUpdate(updates);

      // No BatchWriteCommand should be sent since no items exist
      expect(mockSend).not.toHaveBeenCalled();
    });
  });

  describe('bulkDelete', () => {
    test('chunks 26 deletes into 2 BatchWriteCommand calls (25 + 1)', async () => {
      const documentIds = Array.from({ length: 26 }, (_, i) => `doc-${i}`);

      mockSend.mockResolvedValue({});

      await repo.bulkDelete(documentIds);

      // Verify 2 BatchWriteCommand calls
      expect(mockSend).toHaveBeenCalledTimes(2);

      // First call: 25 items
      const firstCall = mockSend.mock.calls[0][0];
      expect(firstCall).toBeInstanceOf(BatchWriteCommand);
      expect(firstCall.input.RequestItems?.['test-typed-documents']).toHaveLength(25);

      // Second call: 1 item
      const secondCall = mockSend.mock.calls[1][0];
      expect(secondCall).toBeInstanceOf(BatchWriteCommand);
      expect(secondCall.input.RequestItems?.['test-typed-documents']).toHaveLength(1);
    });

    test('handles single delete', async () => {
      mockSend.mockResolvedValue({});

      await repo.bulkDelete(['doc-1']);

      expect(mockSend).toHaveBeenCalledTimes(1);
      const call = mockSend.mock.calls[0][0];
      expect(call.input.RequestItems?.['test-typed-documents']).toHaveLength(1);
      expect(call.input.RequestItems?.['test-typed-documents'][0].DeleteRequest?.Key).toEqual({
        documentId: 'doc-1',
      });
    });

    test('handles empty array', async () => {
      await repo.bulkDelete([]);
      expect(mockSend).not.toHaveBeenCalled();
    });
  });

  describe('listByType', () => {
    test('returns empty array when no items match', async () => {
      mockSend.mockResolvedValue({ Items: [] });

      const result = await repo.listByType('test-type');

      expect(result).toEqual([]);
      expect(mockSend).toHaveBeenCalledTimes(1);
      const call = mockSend.mock.calls[0][0];
      expect(call).toBeInstanceOf(ScanCommand);
      expect(call.input.FilterExpression).toBe('typeId = :typeId');
      expect(call.input.ExpressionAttributeValues).toEqual({ ':typeId': 'test-type' });
    });

    test('returns matching items', async () => {
      const mockItems: TypedDocumentItem[] = [
        {
          documentId: 'doc-1',
          typeId: 'test-type',
          values: { field1: 'value-1' },
          createdBy: 'tester',
          createdAt: '2026-05-04T00:00:00Z',
          updatedAt: '2026-05-04T00:00:00Z',
        },
        {
          documentId: 'doc-2',
          typeId: 'test-type',
          values: { field1: 'value-2' },
          createdBy: 'tester',
          createdAt: '2026-05-04T00:00:00Z',
          updatedAt: '2026-05-04T00:00:00Z',
        },
      ];

      mockSend.mockResolvedValue({ Items: mockItems });

      const result = await repo.listByType('test-type');

      expect(result).toEqual(mockItems);
    });

    test('respects limit parameter', async () => {
      mockSend.mockResolvedValue({ Items: [] });

      await repo.listByType('test-type', 100);

      const call = mockSend.mock.calls[0][0];
      expect(call.input.Limit).toBe(100);
    });

    test('caps limit at 200', async () => {
      mockSend.mockResolvedValue({ Items: [] });

      await repo.listByType('test-type', 500);

      const call = mockSend.mock.calls[0][0];
      expect(call.input.Limit).toBe(200); // Should be capped
    });

    test('handles undefined Items in response', async () => {
      mockSend.mockResolvedValue({}); // No Items field

      const result = await repo.listByType('test-type');

      expect(result).toEqual([]);
    });
  });
});
