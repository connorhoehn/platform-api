// social-api/src/repositories/__tests__/DocumentSectionRepository.test.ts
//
// Tests for DocumentSectionRepository: createSection (ULID, optional fields,
// sortOrder), getSectionsForDocument (sorted), updateSectionFields (dynamic SET),
// deleteSectionById, reorderSections (sortOrder reassignment, batch-split at 25).

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DocumentSectionRepository, type DocumentSectionFields } from '../DocumentSectionRepository';

jest.mock('ulid', () => ({ ulid: jest.fn(() => 'MOCK-ULID-SECTION') }));
jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const docClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

function section(overrides: Partial<DocumentSectionFields> = {}): DocumentSectionFields {
  return {
    documentId: 'doc-1',
    sectionId: 'sec-1',
    type: 'text',
    title: 'Introduction',
    sortOrder: 0,
    createdAt: '2026-01-01T00:00:00Z',
    updatedAt: '2026-01-01T00:00:00Z',
    ...overrides,
  };
}

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// createSection
// ---------------------------------------------------------------------------

describe('DocumentSectionRepository.createSection', () => {
  it('generates ULID sectionId when not provided', async () => {
    const repo = new DocumentSectionRepository(docClient);
    const result = await repo.createSection({ documentId: 'doc-1', type: 'text', title: 'Intro', sortOrder: 0 });
    expect(result.sectionId).toBe('MOCK-ULID-SECTION');
  });

  it('uses explicit sectionId when provided', async () => {
    const repo = new DocumentSectionRepository(docClient);
    const result = await repo.createSection({
      documentId: 'doc-1',
      sectionId: 'explicit-sec-id',
      type: 'text',
      title: 'Intro',
      sortOrder: 1,
    });
    expect(result.sectionId).toBe('explicit-sec-id');
  });

  it('sends PutCommand with all required fields', async () => {
    const repo = new DocumentSectionRepository(docClient);
    await repo.createSection({ documentId: 'doc-1', type: 'text', title: 'Title', sortOrder: 3 });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.TableName).toBe('test-document-sections');
    expect(cmd.input.Item).toMatchObject({
      documentId: 'doc-1',
      type: 'text',
      title: 'Title',
      sortOrder: 3,
    });
  });

  it('omits optional fields when not provided', async () => {
    const repo = new DocumentSectionRepository(docClient);
    const result = await repo.createSection({ documentId: 'doc-1', type: 'text', title: 'T', sortOrder: 0 });
    expect(result.sectionType).toBeUndefined();
    expect(result.metadata).toBeUndefined();
    expect(result.placeholder).toBeUndefined();
  });

  it('includes optional fields when provided', async () => {
    const repo = new DocumentSectionRepository(docClient);
    const result = await repo.createSection({
      documentId: 'doc-1',
      type: 'checklist',
      title: 'T',
      sortOrder: 0,
      sectionType: 'tasks',
      metadata: { version: 1 },
      placeholder: 'Add a task...',
    });
    expect(result.sectionType).toBe('tasks');
    expect(result.metadata).toEqual({ version: 1 });
    expect(result.placeholder).toBe('Add a task...');
  });
});

// ---------------------------------------------------------------------------
// getSectionsForDocument
// ---------------------------------------------------------------------------

describe('DocumentSectionRepository.getSectionsForDocument', () => {
  it('queries by documentId', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new DocumentSectionRepository(docClient);
    await repo.getSectionsForDocument('doc-9');
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ExpressionAttributeValues[':d']).toBe('doc-9');
  });

  it('returns sections sorted by sortOrder ascending', async () => {
    const items = [
      section({ sectionId: 'c', sortOrder: 2 }),
      section({ sectionId: 'a', sortOrder: 0 }),
      section({ sectionId: 'b', sortOrder: 1 }),
    ];
    mockSend.mockResolvedValue({ Items: items });
    const repo = new DocumentSectionRepository(docClient);
    const result = await repo.getSectionsForDocument('doc-1');
    expect(result.map((s) => s.sectionId)).toEqual(['a', 'b', 'c']);
  });
});

// ---------------------------------------------------------------------------
// updateSectionFields
// ---------------------------------------------------------------------------

describe('DocumentSectionRepository.updateSectionFields', () => {
  it('builds dynamic SET with aliases and always includes updatedAt', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new DocumentSectionRepository(docClient);
    await repo.updateSectionFields('doc-1', 'sec-1', { title: 'New Title' });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toContain('SET');
    expect(cmd.input.UpdateExpression).toContain('#title');
    expect(cmd.input.ExpressionAttributeNames['#title']).toBe('title');
    expect(cmd.input.ExpressionAttributeValues[':title']).toBe('New Title');
    expect(cmd.input.UpdateExpression).toContain('#updatedAt');
  });

  it('sends updatedAt-only update when updates is empty', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new DocumentSectionRepository(docClient);
    await repo.updateSectionFields('doc-1', 'sec-1', {});

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toBe('SET #updatedAt = :updatedAt');
  });

  it('uses composite { documentId, sectionId } as key', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new DocumentSectionRepository(docClient);
    await repo.updateSectionFields('doc-5', 'sec-9', { sortOrder: 3 });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ documentId: 'doc-5', sectionId: 'sec-9' });
  });
});

// ---------------------------------------------------------------------------
// deleteSectionById
// ---------------------------------------------------------------------------

describe('DocumentSectionRepository.deleteSectionById', () => {
  it('sends DeleteCommand with composite key', async () => {
    const repo = new DocumentSectionRepository(docClient);
    await repo.deleteSectionById('doc-1', 'sec-42');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ documentId: 'doc-1', sectionId: 'sec-42' });
  });
});

// ---------------------------------------------------------------------------
// reorderSections
// ---------------------------------------------------------------------------

describe('DocumentSectionRepository.reorderSections', () => {
  it('reassigns sortOrder based on position in sectionIds array', async () => {
    const existing = [
      section({ sectionId: 'a', sortOrder: 2 }),
      section({ sectionId: 'b', sortOrder: 0 }),
    ];
    // First call: Query for getSectionsForDocument, second call: BatchWrite
    mockSend
      .mockResolvedValueOnce({ Items: existing })
      .mockResolvedValueOnce({});

    const repo = new DocumentSectionRepository(docClient);
    await repo.reorderSections('doc-1', ['b', 'a']);

    // Second call is the BatchWriteCommand
    const batchCmd = mockSend.mock.calls[1][0];
    const items = batchCmd.input.RequestItems['test-document-sections'];
    const bPut = items.find((r: { PutRequest: { Item: DocumentSectionFields } }) => r.PutRequest.Item.sectionId === 'b');
    const aPut = items.find((r: { PutRequest: { Item: DocumentSectionFields } }) => r.PutRequest.Item.sectionId === 'a');
    expect(bPut.PutRequest.Item.sortOrder).toBe(0);
    expect(aPut.PutRequest.Item.sortOrder).toBe(1);
  });

  it('skips sectionIds not found in the existing sections', async () => {
    const existing = [section({ sectionId: 'known', sortOrder: 0 })];
    mockSend
      .mockResolvedValueOnce({ Items: existing })
      .mockResolvedValueOnce({});

    const repo = new DocumentSectionRepository(docClient);
    await repo.reorderSections('doc-1', ['unknown', 'known']);

    const batchCmd = mockSend.mock.calls[1][0];
    const items = batchCmd.input.RequestItems['test-document-sections'];
    expect(items).toHaveLength(1);
    expect(items[0].PutRequest.Item.sectionId).toBe('known');
  });

  it('splits into multiple BatchWrite calls when more than 25 sections', async () => {
    const existing = Array.from({ length: 26 }, (_, i) =>
      section({ sectionId: `s${i}`, sortOrder: i }),
    );
    // First call: Query, then two BatchWrite calls (25 + 1)
    mockSend
      .mockResolvedValueOnce({ Items: existing })
      .mockResolvedValue({});

    const repo = new DocumentSectionRepository(docClient);
    await repo.reorderSections('doc-1', existing.map((s) => s.sectionId));

    // 1 Query + 2 BatchWrite = 3 total calls
    expect(mockSend).toHaveBeenCalledTimes(3);
  });
});
