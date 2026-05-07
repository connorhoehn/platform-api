// social-api/src/pipeline/__tests__/definitions-repository.test.ts
//
// Unit tests for DefinitionsRepository: CRUD, list (QueryCommand),
// and listAll() pagination loop (handles multiple Scan pages).

import { DynamoDBDocumentClient, DeleteCommand, GetCommand, PutCommand, QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import {
  DefinitionsRepository,
  type PipelineDefinition,
} from '../definitions-repository';

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
}));

function def(overrides: Partial<PipelineDefinition> = {}): PipelineDefinition {
  return { id: 'pipeline-1', name: 'My Pipeline', ...overrides };
}

function item(userId: string, definition: PipelineDefinition) {
  return { userId, pipelineId: definition.id, definition, updatedAt: '2026-05-06T00:00:00.000Z' };
}

beforeEach(() => {
  mockSend.mockReset();
});

// ---------------------------------------------------------------------------
// get
// ---------------------------------------------------------------------------

describe('DefinitionsRepository — get', () => {
  it('fetches by composite userId+pipelineId key', async () => {
    mockSend.mockResolvedValue({ Item: item('user-1', def()) });
    const repo = new DefinitionsRepository(mockDocClient);
    const result = await repo.get('user-1', 'pipeline-1');

    const cmd = mockSend.mock.calls[0][0] as GetCommand;
    expect(cmd).toBeInstanceOf(GetCommand);
    expect(cmd.input.Key).toEqual({ userId: 'user-1', pipelineId: 'pipeline-1' });
    expect(result?.id).toBe('pipeline-1');
  });

  it('returns the definition (not the wrapper item)', async () => {
    mockSend.mockResolvedValue({ Item: item('user-1', def({ id: 'p-42', name: 'Special' })) });
    const repo = new DefinitionsRepository(mockDocClient);
    const result = await repo.get('user-1', 'p-42');
    expect(result?.name).toBe('Special');
    expect((result as Record<string, unknown>)['userId']).toBeUndefined();
  });

  it('returns null when item not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    const repo = new DefinitionsRepository(mockDocClient);
    const result = await repo.get('user-1', 'missing');
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

describe('DefinitionsRepository — list', () => {
  it('queries by userId using KeyConditionExpression', async () => {
    mockSend.mockResolvedValue({ Items: [item('user-1', def())] });
    const repo = new DefinitionsRepository(mockDocClient);
    await repo.list('user-1');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd).toBeInstanceOf(QueryCommand);
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':uid': 'user-1' });
  });

  it('returns only the definition objects', async () => {
    const d = def({ id: 'p-1', name: 'Pipeline A' });
    mockSend.mockResolvedValue({ Items: [item('user-1', d)] });
    const repo = new DefinitionsRepository(mockDocClient);
    const result = await repo.list('user-1');

    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Pipeline A');
    expect((result[0] as Record<string, unknown>)['userId']).toBeUndefined();
  });

  it('returns empty array when no items found', async () => {
    mockSend.mockResolvedValue({ Items: undefined });
    const repo = new DefinitionsRepository(mockDocClient);
    const result = await repo.list('user-1');
    expect(result).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// listAll — pagination
// ---------------------------------------------------------------------------

describe('DefinitionsRepository — listAll', () => {
  it('returns all definitions from a single page', async () => {
    const d1 = def({ id: 'p-1' });
    const d2 = def({ id: 'p-2' });
    mockSend.mockResolvedValueOnce({
      Items: [item('user-1', d1), item('user-2', d2)],
      LastEvaluatedKey: undefined,
    });

    const repo = new DefinitionsRepository(mockDocClient);
    const result = await repo.listAll();

    const cmd = mockSend.mock.calls[0][0] as ScanCommand;
    expect(cmd).toBeInstanceOf(ScanCommand);
    expect(result).toHaveLength(2);
    expect(result.map((r) => r.id)).toEqual(['p-1', 'p-2']);
  });

  it('follows LastEvaluatedKey across multiple pages', async () => {
    const d1 = def({ id: 'p-1' });
    const d2 = def({ id: 'p-2' });
    const lastKey = { userId: 'user-1', pipelineId: 'p-1' };

    mockSend
      .mockResolvedValueOnce({ Items: [item('user-1', d1)], LastEvaluatedKey: lastKey })
      .mockResolvedValueOnce({ Items: [item('user-2', d2)], LastEvaluatedKey: undefined });

    const repo = new DefinitionsRepository(mockDocClient);
    const result = await repo.listAll();

    expect(mockSend).toHaveBeenCalledTimes(2);
    // Second call should pass ExclusiveStartKey
    const cmd2 = mockSend.mock.calls[1][0] as ScanCommand;
    expect(cmd2.input.ExclusiveStartKey).toEqual(lastKey);
    expect(result).toHaveLength(2);
  });

  it('skips items that have no definition field', async () => {
    mockSend.mockResolvedValueOnce({
      Items: [
        item('user-1', def({ id: 'p-1' })),
        { userId: 'user-2', pipelineId: 'p-2' }, // no definition field
      ],
      LastEvaluatedKey: undefined,
    });

    const repo = new DefinitionsRepository(mockDocClient);
    const result = await repo.listAll();
    expect(result).toHaveLength(1);
    expect(result[0]!.id).toBe('p-1');
  });

  it('returns empty array when table is empty', async () => {
    mockSend.mockResolvedValueOnce({ Items: undefined, LastEvaluatedKey: undefined });
    const repo = new DefinitionsRepository(mockDocClient);
    const result = await repo.listAll();
    expect(result).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// put
// ---------------------------------------------------------------------------

describe('DefinitionsRepository — put', () => {
  it('issues a PutCommand with wrapped item including userId and pipelineId', async () => {
    mockSend.mockResolvedValue({});
    const repo = new DefinitionsRepository(mockDocClient);
    await repo.put('user-1', def({ id: 'p-99', name: 'My Pipeline' }));

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd).toBeInstanceOf(PutCommand);
    expect(cmd.input.Item).toMatchObject({
      userId: 'user-1',
      pipelineId: 'p-99',
    });
    expect((cmd.input.Item as Record<string, unknown>)['definition']).toMatchObject({
      id: 'p-99',
      name: 'My Pipeline',
    });
  });

  it('sets updatedAt timestamp on the item', async () => {
    mockSend.mockResolvedValue({});
    const repo = new DefinitionsRepository(mockDocClient);
    await repo.put('user-1', def());

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    const updatedAt = (cmd.input.Item as Record<string, unknown>)['updatedAt'] as string;
    expect(typeof updatedAt).toBe('string');
    expect(updatedAt.length).toBeGreaterThan(10);
  });
});

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------

describe('DefinitionsRepository — delete', () => {
  it('issues a DeleteCommand with composite key', async () => {
    mockSend.mockResolvedValue({});
    const repo = new DefinitionsRepository(mockDocClient);
    await repo.delete('user-1', 'p-99');

    const cmd = mockSend.mock.calls[0][0] as DeleteCommand;
    expect(cmd).toBeInstanceOf(DeleteCommand);
    expect(cmd.input.Key).toEqual({ userId: 'user-1', pipelineId: 'p-99' });
  });
});
