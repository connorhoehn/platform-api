// social-api/src/repositories/__tests__/DocumentTypeRepository.test.ts
//
// Unit tests for DocumentTypeRepository: CRUD commands, list limit clamping,
// and the update() method's version-bumping + previousVersions snapshot logic.

import {
  DynamoDBDocumentClient,
  DeleteCommand,
  GetCommand,
  PutCommand,
  ScanCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  DocumentTypeRepository,
  type DocumentTypeItem,
} from '../DocumentTypeRepository';

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

function baseItem(overrides: Partial<DocumentTypeItem> = {}): DocumentTypeItem {
  return {
    typeId: 'type-1',
    name: 'Contract',
    description: 'Legal contract',
    icon: '📄',
    fields: [],
    createdBy: 'user-1',
    createdAt: '2026-05-01T00:00:00.000Z',
    updatedAt: '2026-05-01T00:00:00.000Z',
    version: 1,
    ...overrides,
  };
}

beforeEach(() => {
  mockSend.mockReset();
});

// ---------------------------------------------------------------------------
// create
// ---------------------------------------------------------------------------

describe('DocumentTypeRepository — create', () => {
  it('issues a PutCommand with the full item', async () => {
    mockSend.mockResolvedValue({});
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.create(baseItem());

    expect(mockSend).toHaveBeenCalledTimes(1);
    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd).toBeInstanceOf(PutCommand);
    expect(cmd.input.TableName).toBe('test-document-types');
    expect(cmd.input.Item).toMatchObject({ typeId: 'type-1', name: 'Contract' });
  });
});

// ---------------------------------------------------------------------------
// get
// ---------------------------------------------------------------------------

describe('DocumentTypeRepository — get', () => {
  it('returns the item when found', async () => {
    const item = baseItem();
    mockSend.mockResolvedValue({ Item: item });
    const repo = new DocumentTypeRepository(mockDocClient);
    const result = await repo.get('type-1');

    const cmd = mockSend.mock.calls[0][0] as GetCommand;
    expect(cmd).toBeInstanceOf(GetCommand);
    expect(cmd.input.Key).toEqual({ typeId: 'type-1' });
    expect(result?.typeId).toBe('type-1');
  });

  it('returns null when item not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    const repo = new DocumentTypeRepository(mockDocClient);
    const result = await repo.get('missing');
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------

describe('DocumentTypeRepository — delete', () => {
  it('issues a DeleteCommand keyed by typeId', async () => {
    mockSend.mockResolvedValue({});
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.delete('type-99');

    const cmd = mockSend.mock.calls[0][0] as DeleteCommand;
    expect(cmd).toBeInstanceOf(DeleteCommand);
    expect(cmd.input.Key).toEqual({ typeId: 'type-99' });
  });
});

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

describe('DocumentTypeRepository — list', () => {
  it('uses ScanCommand against the document-types table', async () => {
    mockSend.mockResolvedValue({ Items: [baseItem()] });
    const repo = new DocumentTypeRepository(mockDocClient);
    const result = await repo.list();

    const cmd = mockSend.mock.calls[0][0] as ScanCommand;
    expect(cmd).toBeInstanceOf(ScanCommand);
    expect(cmd.input.TableName).toBe('test-document-types');
    expect(result).toHaveLength(1);
  });

  it('defaults limit to 50', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.list();

    const cmd = mockSend.mock.calls[0][0] as ScanCommand;
    expect(cmd.input.Limit).toBe(50);
  });

  it('accepts custom limit', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.list(10);

    const cmd = mockSend.mock.calls[0][0] as ScanCommand;
    expect(cmd.input.Limit).toBe(10);
  });

  it('clamps limit at 200', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.list(500);

    const cmd = mockSend.mock.calls[0][0] as ScanCommand;
    expect(cmd.input.Limit).toBe(200);
  });

  it('returns empty array when Items is undefined', async () => {
    mockSend.mockResolvedValue({ Items: undefined });
    const repo = new DocumentTypeRepository(mockDocClient);
    const result = await repo.list();
    expect(result).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// update — expression building
// ---------------------------------------------------------------------------

describe('DocumentTypeRepository — update expression building', () => {
  function captureUpdateCmd(): UpdateCommand {
    return mockSend.mock.calls[0][0] as UpdateCommand;
  }

  beforeEach(() => {
    mockSend.mockResolvedValue({ Attributes: baseItem({ version: 2 }) });
  });

  it('includes name in UpdateExpression when patched', async () => {
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.update('type-1', { name: 'New Name' });
    const cmd = captureUpdateCmd();
    expect(cmd.input.UpdateExpression).toContain('#name');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':name': 'New Name' });
  });

  it('uses ExpressionAttributeNames alias for reserved word "name"', async () => {
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.update('type-1', { name: 'Aliased' });
    const cmd = captureUpdateCmd();
    expect(cmd.input.ExpressionAttributeNames).toMatchObject({ '#name': 'name' });
  });

  it('includes description when patched', async () => {
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.update('type-1', { description: 'New desc' });
    const cmd = captureUpdateCmd();
    expect(cmd.input.UpdateExpression).toContain('description');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':description': 'New desc' });
  });

  it('includes icon when patched', async () => {
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.update('type-1', { icon: '🏷️' });
    const cmd = captureUpdateCmd();
    expect(cmd.input.UpdateExpression).toContain('icon');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':icon': '🏷️' });
  });

  it('includes fields with aliased name when patched', async () => {
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.update('type-1', { fields: [] });
    const cmd = captureUpdateCmd();
    expect(cmd.input.UpdateExpression).toContain('#fields');
    expect(cmd.input.ExpressionAttributeNames).toMatchObject({ '#fields': 'fields' });
  });

  it('always includes updatedAt in the expression', async () => {
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.update('type-1', { name: 'X' });
    const cmd = captureUpdateCmd();
    expect(cmd.input.UpdateExpression).toContain('updatedAt');
    expect(cmd.input.ExpressionAttributeValues?.[':updatedAt']).toBeDefined();
  });

  it('uses ReturnValues=ALL_NEW', async () => {
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.update('type-1', { name: 'Y' });
    const cmd = captureUpdateCmd();
    expect(cmd.input.ReturnValues).toBe('ALL_NEW');
  });
});

// ---------------------------------------------------------------------------
// update — version bumping
// ---------------------------------------------------------------------------

describe('DocumentTypeRepository — update version bumping', () => {
  it('bumps version from 1 to 2 when existingItem.version=1', async () => {
    mockSend.mockResolvedValue({ Attributes: baseItem({ version: 2 }) });
    const repo = new DocumentTypeRepository(mockDocClient);
    const existing = baseItem({ version: 1 });
    await repo.update('type-1', { name: 'v2' }, existing);

    const cmd = mockSend.mock.calls[0][0] as UpdateCommand;
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':version': 2 });
  });

  it('uses version 1 as nextVersion when existingItem has no version field', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new DocumentTypeRepository(mockDocClient);
    const existing = baseItem({ version: undefined });
    await repo.update('type-1', { name: 'first version' }, existing);

    const cmd = mockSend.mock.calls[0][0] as UpdateCommand;
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':version': 1 });
  });
});

// ---------------------------------------------------------------------------
// update — previousVersions snapshot
// ---------------------------------------------------------------------------

describe('DocumentTypeRepository — previousVersions snapshot', () => {
  it('appends a snapshot of the existing item to previousVersions', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new DocumentTypeRepository(mockDocClient);
    const existing = baseItem({ version: 1, name: 'Original', previousVersions: [] });
    await repo.update('type-1', { name: 'Updated' }, existing);

    const cmd = mockSend.mock.calls[0][0] as UpdateCommand;
    const snapshots = cmd.input.ExpressionAttributeValues?.[':previousVersions'] as unknown[];
    expect(snapshots).toHaveLength(1);
    expect(snapshots[0]).toMatchObject({ name: 'Original', version: 1 });
  });

  it('does not add a snapshot when existingItem is not provided', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new DocumentTypeRepository(mockDocClient);
    await repo.update('type-1', { name: 'No snapshot' });

    const cmd = mockSend.mock.calls[0][0] as UpdateCommand;
    expect(cmd.input.UpdateExpression).not.toContain('previousVersions');
  });

  it('trims history to at most 20 entries', async () => {
    mockSend.mockResolvedValue({ Attributes: {} });
    const repo = new DocumentTypeRepository(mockDocClient);
    const existingHistory = Array.from({ length: 20 }, (_, i) => ({
      version: i + 1,
      name: `v${i + 1}`,
      description: '',
      icon: '',
      fields: [],
      updatedAt: '2026-01-01T00:00:00.000Z',
    }));
    const existing = baseItem({ version: 20, previousVersions: existingHistory });
    await repo.update('type-1', { name: 'v21' }, existing);

    const cmd = mockSend.mock.calls[0][0] as UpdateCommand;
    const snapshots = cmd.input.ExpressionAttributeValues?.[':previousVersions'] as unknown[];
    // 19 trimmed from existing + 1 new snapshot = 20
    expect(snapshots).toHaveLength(20);
  });
});
