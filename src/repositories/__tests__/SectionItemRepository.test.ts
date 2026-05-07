// social-api/src/repositories/__tests__/SectionItemRepository.test.ts
//
// Tests for SectionItemRepository: createItem (sectionKey composite, ULID id,
// status default, optional fields), getItemsForSection (limit), updateItemFields
// (dynamic SET aliases, empty guard), deleteItemById, getItemsByAssignee
// (with/without status), getItemsForDocument, ackItem.

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { SectionItemRepository } from '../SectionItemRepository';

jest.mock('ulid', () => ({ ulid: jest.fn(() => 'MOCK-ULID-ITEM') }));
jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const docClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

const BASE_INPUT = {
  documentId: 'doc-1',
  sectionId: 'sec-1',
  text: 'Do the thing',
};

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// createItem
// ---------------------------------------------------------------------------

describe('SectionItemRepository.createItem', () => {
  it('constructs sectionKey as documentId:sectionId', async () => {
    const repo = new SectionItemRepository(docClient);
    const result = await repo.createItem(BASE_INPUT);
    expect(result.sectionKey).toBe('doc-1:sec-1');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Item.sectionKey).toBe('doc-1:sec-1');
  });

  it('generates ULID itemId when not provided', async () => {
    const repo = new SectionItemRepository(docClient);
    const result = await repo.createItem(BASE_INPUT);
    expect(result.itemId).toBe('MOCK-ULID-ITEM');
  });

  it('uses explicit itemId when provided', async () => {
    const repo = new SectionItemRepository(docClient);
    const result = await repo.createItem({ ...BASE_INPUT, itemId: 'explicit-id' });
    expect(result.itemId).toBe('explicit-id');
  });

  it('defaults status to "open" when not specified', async () => {
    const repo = new SectionItemRepository(docClient);
    const result = await repo.createItem(BASE_INPUT);
    expect(result.status).toBe('open');
  });

  it('uses provided status value', async () => {
    const repo = new SectionItemRepository(docClient);
    const result = await repo.createItem({ ...BASE_INPUT, status: 'done' });
    expect(result.status).toBe('done');
  });

  it('omits optional fields when not provided', async () => {
    const repo = new SectionItemRepository(docClient);
    const result = await repo.createItem(BASE_INPUT);
    expect(result.assignee).toBeUndefined();
    expect(result.dueDate).toBeUndefined();
    expect(result.notes).toBeUndefined();
  });

  it('includes optional fields when provided', async () => {
    const repo = new SectionItemRepository(docClient);
    const result = await repo.createItem({
      ...BASE_INPUT,
      assignee: 'user-9',
      priority: 'high',
      notes: 'Very important',
    });
    expect(result.assignee).toBe('user-9');
    expect(result.priority).toBe('high');
    expect(result.notes).toBe('Very important');
  });
});

// ---------------------------------------------------------------------------
// getItemsForSection
// ---------------------------------------------------------------------------

describe('SectionItemRepository.getItemsForSection', () => {
  beforeEach(() => {
    mockSend.mockResolvedValue({ Items: [] });
  });

  it('queries by sectionKey composite value', async () => {
    const repo = new SectionItemRepository(docClient);
    await repo.getItemsForSection('doc-1', 'sec-2');
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ExpressionAttributeValues[':sk']).toBe('doc-1:sec-2');
  });

  it('includes Limit when provided', async () => {
    const repo = new SectionItemRepository(docClient);
    await repo.getItemsForSection('doc-1', 'sec-1', 15);
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Limit).toBe(15);
  });

  it('omits Limit when not provided', async () => {
    const repo = new SectionItemRepository(docClient);
    await repo.getItemsForSection('doc-1', 'sec-1');
    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Limit).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// updateItemFields
// ---------------------------------------------------------------------------

describe('SectionItemRepository.updateItemFields', () => {
  it('builds dynamic SET expression with aliased attribute names', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new SectionItemRepository(docClient);
    await repo.updateItemFields('doc-1', 'sec-1', 'item-1', { text: 'Updated text' });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toContain('SET');
    expect(cmd.input.UpdateExpression).toContain('#text');
    expect(cmd.input.ExpressionAttributeNames['#text']).toBe('text');
    expect(cmd.input.ExpressionAttributeValues[':text']).toBe('Updated text');
  });

  it('always includes updatedAt in the update', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new SectionItemRepository(docClient);
    await repo.updateItemFields('doc-1', 'sec-1', 'item-1', { text: 'x' });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toContain('#updatedAt');
    expect(cmd.input.ExpressionAttributeValues[':updatedAt']).toBeDefined();
  });

  it('sends only updatedAt update when updates is empty (updatedAt always included)', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new SectionItemRepository(docClient);
    await repo.updateItemFields('doc-1', 'sec-1', 'item-1', {});

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toBe('SET #updatedAt = :updatedAt');
    expect(Object.keys(cmd.input.ExpressionAttributeNames)).toEqual(['#updatedAt']);
  });

  it('uses composite sectionKey as part of the update key', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new SectionItemRepository(docClient);
    await repo.updateItemFields('doc-5', 'sec-9', 'item-x', { status: 'done' });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ sectionKey: 'doc-5:sec-9', itemId: 'item-x' });
  });
});

// ---------------------------------------------------------------------------
// deleteItemById
// ---------------------------------------------------------------------------

describe('SectionItemRepository.deleteItemById', () => {
  it('sends DeleteCommand with composite sectionKey + itemId', async () => {
    const repo = new SectionItemRepository(docClient);
    await repo.deleteItemById('doc-1', 'sec-1', 'item-42');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ sectionKey: 'doc-1:sec-1', itemId: 'item-42' });
  });
});

// ---------------------------------------------------------------------------
// getItemsByAssignee
// ---------------------------------------------------------------------------

describe('SectionItemRepository.getItemsByAssignee', () => {
  beforeEach(() => {
    mockSend.mockResolvedValue({ Items: [] });
  });

  it('queries assignee-status-index with only assignee when no status', async () => {
    const repo = new SectionItemRepository(docClient);
    await repo.getItemsByAssignee('user-5');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.IndexName).toBe('assignee-status-index');
    expect(cmd.input.ExpressionAttributeValues[':a']).toBe('user-5');
    expect(cmd.input.ExpressionAttributeValues[':s']).toBeUndefined();
  });

  it('adds status condition when status is provided', async () => {
    const repo = new SectionItemRepository(docClient);
    await repo.getItemsByAssignee('user-5', 'open');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ExpressionAttributeValues[':s']).toBe('open');
    expect(cmd.input.KeyConditionExpression).toContain('#status');
  });
});

// ---------------------------------------------------------------------------
// getItemsForDocument
// ---------------------------------------------------------------------------

describe('SectionItemRepository.getItemsForDocument', () => {
  it('queries documentId-index GSI', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new SectionItemRepository(docClient);
    await repo.getItemsForDocument('doc-9');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.IndexName).toBe('documentId-index');
    expect(cmd.input.ExpressionAttributeValues[':d']).toBe('doc-9');
  });
});

// ---------------------------------------------------------------------------
// ackItem
// ---------------------------------------------------------------------------

describe('SectionItemRepository.ackItem', () => {
  it('sends UpdateCommand with SET ackedBy, ackedAt, updatedAt', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new SectionItemRepository(docClient);
    await repo.ackItem('doc-1', 'sec-1', 'item-1', 'user-mod', '2026-01-01T00:00:00Z');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toContain('ackedBy');
    expect(cmd.input.UpdateExpression).toContain('ackedAt');
    expect(cmd.input.UpdateExpression).toContain('updatedAt');
    expect(cmd.input.ExpressionAttributeValues[':ab']).toBe('user-mod');
    expect(cmd.input.ExpressionAttributeValues[':at']).toBe('2026-01-01T00:00:00Z');
  });

  it('uses composite sectionKey as part of the ack key', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new SectionItemRepository(docClient);
    await repo.ackItem('doc-2', 'sec-3', 'item-4', 'user-1', '2026-01-01T00:00:00Z');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toMatchObject({ sectionKey: 'doc-2:sec-3', itemId: 'item-4' });
  });
});
