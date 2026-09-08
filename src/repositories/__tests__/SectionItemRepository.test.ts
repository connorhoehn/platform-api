// social-api/src/repositories/__tests__/SectionItemRepository.test.ts
//
// Tests for SectionItemRepository: createItem (sectionKey composite, ULID id,
// status default, optional fields), getItemsForSection (limit), updateItemFields
// (dynamic SET aliases, empty guard, null-means-REMOVE), deleteItemById,
// getItemsAssignedTo (with/without status), getItemsForDocument, ackItem.

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

  it('stores the label and the identity as two separate fields', async () => {
    const repo = new SectionItemRepository(docClient);
    const result = await repo.createItem({
      ...BASE_INPUT,
      assignee: 'Alice Chen',
      assigneeId: 'dev-alice',
    });
    expect(result.assignee).toBe('Alice Chen');
    expect(result.assigneeId).toBe('dev-alice');
  });

  it('omits an empty assigneeId rather than writing "" into a GSI key', async () => {
    const repo = new SectionItemRepository(docClient);
    const result = await repo.createItem({ ...BASE_INPUT, assignee: 'Alice Chen', assigneeId: '' });
    expect(result.assignee).toBe('Alice Chen');
    expect(result).not.toHaveProperty('assigneeId');

    const written = mockSend.mock.calls[0][0].input.Item;
    expect(written).not.toHaveProperty('assigneeId');
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

  // Both assignee fields are GSI hash keys, and DynamoDB refuses an empty
  // string in one. Unassigning therefore has to REMOVE the attributes; writing
  // '' would have failed the whole update.
  it('REMOVEs an attribute passed as null instead of setting it', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new SectionItemRepository(docClient);
    await repo.updateItemFields('doc-1', 'sec-1', 'item-1', {
      assignee: null,
      assigneeId: null,
    });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toContain('REMOVE #assignee');
    expect(cmd.input.UpdateExpression).toContain('#assigneeId');
    expect(cmd.input.ExpressionAttributeValues[':assignee']).toBeUndefined();
    expect(cmd.input.ExpressionAttributeValues[':assigneeId']).toBeUndefined();
  });

  it('mixes SET and REMOVE in one expression', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new SectionItemRepository(docClient);
    await repo.updateItemFields('doc-1', 'sec-1', 'item-1', {
      text: 'still mine',
      assigneeId: null,
    });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.UpdateExpression).toMatch(/^SET .*REMOVE #assigneeId$/);
    expect(cmd.input.ExpressionAttributeValues[':text']).toBe('still mine');
  });

  it('writes both assignee fields when a person is picked', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new SectionItemRepository(docClient);
    await repo.updateItemFields('doc-1', 'sec-1', 'item-1', {
      assignee: 'Alice Chen',
      assigneeId: 'dev-alice',
    });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ExpressionAttributeValues[':assignee']).toBe('Alice Chen');
    expect(cmd.input.ExpressionAttributeValues[':assigneeId']).toBe('dev-alice');
    expect(cmd.input.UpdateExpression).not.toContain('REMOVE');
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
// getItemsAssignedTo
//
// The one query behind GET /api/items/mine. It is keyed on the IDENTITY
// (`assigneeId`), and it also reads the legacy label index for the SAME id —
// never for a display name. See the method's own comment for why both.
// ---------------------------------------------------------------------------

/** The index each captured QueryCommand was issued against. */
const indexesQueried = (): string[] =>
  mockSend.mock.calls.map((c: unknown[]) => (c[0] as { input: { IndexName: string } }).input.IndexName);

/** The captured QueryCommand input for one index. */
const queryOn = (index: string) =>
  (mockSend.mock.calls.find(
    (c: unknown[]) => (c[0] as { input: { IndexName: string } }).input.IndexName === index,
  )?.[0] as { input: Record<string, never> }).input as unknown as {
    IndexName: string;
    KeyConditionExpression: string;
    ExpressionAttributeNames: Record<string, string>;
    ExpressionAttributeValues: Record<string, string>;
  };

describe('SectionItemRepository.getItemsAssignedTo', () => {
  beforeEach(() => {
    mockSend.mockResolvedValue({ Items: [] });
  });

  it('queries the assigneeId index — not the display-name index — for the caller', async () => {
    const repo = new SectionItemRepository(docClient);
    await repo.getItemsAssignedTo('dev-alice');

    const byId = queryOn('assigneeId-status-index');
    expect(byId.ExpressionAttributeNames['#k']).toBe('assigneeId');
    expect(byId.ExpressionAttributeValues[':a']).toBe('dev-alice');
    expect(byId.ExpressionAttributeValues[':s']).toBeUndefined();
  });

  it('also reads the legacy assignee index for the SAME id, so pre-split rows survive', async () => {
    const repo = new SectionItemRepository(docClient);
    await repo.getItemsAssignedTo('dev-alice');

    expect(indexesQueried().sort()).toEqual([
      'assignee-status-index',
      'assigneeId-status-index',
    ]);
    const legacy = queryOn('assignee-status-index');
    expect(legacy.ExpressionAttributeNames['#k']).toBe('assignee');
    // The id, never a name. Asking the label index for a display name is the
    // bug this whole split exists to remove.
    expect(legacy.ExpressionAttributeValues[':a']).toBe('dev-alice');
  });

  it('adds the status condition to both index queries when status is provided', async () => {
    const repo = new SectionItemRepository(docClient);
    await repo.getItemsAssignedTo('dev-alice', 'open');

    for (const index of ['assigneeId-status-index', 'assignee-status-index']) {
      const q = queryOn(index);
      expect(q.ExpressionAttributeValues[':s']).toBe('open');
      expect(q.KeyConditionExpression).toContain('#status');
    }
  });

  it('never scans, and never returns the same row twice when both indexes hold it', async () => {
    const row = {
      sectionKey: 'doc-1:sec-1',
      itemId: 'item-1',
      assignee: 'dev-alice',
      assigneeId: 'dev-alice',
    };
    mockSend.mockResolvedValue({ Items: [row] });

    const repo = new SectionItemRepository(docClient);
    const items = await repo.getItemsAssignedTo('dev-alice');

    expect(items).toHaveLength(1);
    expect(indexesQueried().every((i) => typeof i === 'string' && i.length > 0)).toBe(true);
  });

  it('returns nothing without asking DynamoDB when there is no caller id', async () => {
    const repo = new SectionItemRepository(docClient);
    expect(await repo.getItemsAssignedTo('')).toEqual([]);
    expect(mockSend).not.toHaveBeenCalled();
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
