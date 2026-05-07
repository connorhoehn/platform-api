// social-api/src/repositories/__tests__/ApprovalRepository.test.ts
//
// Unit tests for ApprovalRepository: create (PutCommand), listByDocument
// (pk query), and listByStatus (GSI query with limit clamping + newest-first).

import { DynamoDBDocumentClient, PutCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import {
  ApprovalRepository,
  type ApprovalEntry,
  type ApprovalDecision,
} from '../ApprovalRepository';

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

function entry(overrides: Partial<ApprovalEntry> = {}): ApprovalEntry {
  return {
    documentId: 'doc-1',
    workflowId: 'wf-1',
    sectionId: 'sec-1',
    reviewerId: 'user-1',
    decision: 'approved',
    workflowStatus: 'approved',
    createdAt: '2026-05-06T09:00:00.000Z',
    ...overrides,
  };
}

beforeEach(() => {
  mockSend.mockReset();
});

// ---------------------------------------------------------------------------
// create
// ---------------------------------------------------------------------------

describe('ApprovalRepository — create', () => {
  it('issues a PutCommand with the full entry as Item', async () => {
    mockSend.mockResolvedValue({});
    const repo = new ApprovalRepository(mockDocClient);
    const e = entry();
    await repo.create(e);

    expect(mockSend).toHaveBeenCalledTimes(1);
    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd).toBeInstanceOf(PutCommand);
    expect(cmd.input.TableName).toBe('test-approval-workflows');
    expect(cmd.input.Item).toMatchObject({
      documentId: 'doc-1',
      workflowId: 'wf-1',
      workflowStatus: 'approved',
    });
  });
});

// ---------------------------------------------------------------------------
// listByDocument
// ---------------------------------------------------------------------------

describe('ApprovalRepository — listByDocument', () => {
  it('queries by documentId (pk KeyConditionExpression)', async () => {
    mockSend.mockResolvedValue({ Items: [entry()] });
    const repo = new ApprovalRepository(mockDocClient);
    const result = await repo.listByDocument('doc-42');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.KeyConditionExpression).toContain('documentId');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':did': 'doc-42' });
    expect(result).toHaveLength(1);
  });

  it('defaults limit to 100', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new ApprovalRepository(mockDocClient);
    await repo.listByDocument('doc-1');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(100);
  });

  it('accepts custom limit', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new ApprovalRepository(mockDocClient);
    await repo.listByDocument('doc-1', 25);

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(25);
  });

  it('clamps limit at 500', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new ApprovalRepository(mockDocClient);
    await repo.listByDocument('doc-1', 9999);

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(500);
  });

  it('returns empty array when Items is undefined', async () => {
    mockSend.mockResolvedValue({ Items: undefined });
    const repo = new ApprovalRepository(mockDocClient);
    const result = await repo.listByDocument('doc-1');
    expect(result).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// listByStatus
// ---------------------------------------------------------------------------

describe('ApprovalRepository — listByStatus', () => {
  const statuses: ApprovalDecision[] = ['approved', 'rejected', 'changes_requested'];

  it.each(statuses)('queries status-index GSI with workflowStatus="%s"', async (status) => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new ApprovalRepository(mockDocClient);
    await repo.listByStatus(status);

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('status-index');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':s': status });
  });

  it('sets ScanIndexForward=false (newest first)', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new ApprovalRepository(mockDocClient);
    await repo.listByStatus('pending' as ApprovalDecision);

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.ScanIndexForward).toBe(false);
  });

  it('clamps limit at 500', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new ApprovalRepository(mockDocClient);
    await repo.listByStatus('approved', 1000);

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(500);
  });

  it('returns the items from the DDB response', async () => {
    const item = entry({ decision: 'rejected', workflowStatus: 'rejected' });
    mockSend.mockResolvedValue({ Items: [item] });
    const repo = new ApprovalRepository(mockDocClient);
    const result = await repo.listByStatus('rejected');
    expect(result).toHaveLength(1);
    expect(result[0]!.decision).toBe('rejected');
  });
});
