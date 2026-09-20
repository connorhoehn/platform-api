// social-api/src/repositories/__tests__/DocumentCommentRepository.test.ts
//
// Tests for DocumentCommentRepository: createComment (ulid-generated id, optional
// parentCommentId), getCommentsForDocument/Section (query shape, limit, lastKey),
// resolveThread / unresolveThread (UpdateExpression), deleteComment.

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DocumentCommentRepository } from '../DocumentCommentRepository';

jest.mock('ulid', () => ({ ulid: jest.fn(() => 'MOCK-ULID-0001') }));

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const docClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

const BASE_COMMENT = {
  documentId: 'doc-1',
  sectionId: 'sec-1',
  text: 'Great work!',
  userId: 'user-1',
  displayName: 'Alice',
  color: '#ff0000',
  timestamp: '2026-01-01T00:00:00Z',
};

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// createComment
// ---------------------------------------------------------------------------

describe('DocumentCommentRepository.createComment', () => {
  it('generates a ULID commentId', async () => {
    const repo = new DocumentCommentRepository(docClient);
    const result = await repo.createComment(BASE_COMMENT);
    expect(result.commentId).toBe('MOCK-ULID-0001');
  });

  it('sends PutCommand with all fields including generated commentId', async () => {
    const repo = new DocumentCommentRepository(docClient);
    await repo.createComment(BASE_COMMENT);

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.TableName).toBe('test-document-comments');
    expect(cmd.input.Item).toMatchObject({
      documentId: 'doc-1',
      sectionId: 'sec-1',
      text: 'Great work!',
      userId: 'user-1',
      displayName: 'Alice',
      color: '#ff0000',
      commentId: 'MOCK-ULID-0001',
    });
  });

  it('includes parentCommentId when provided', async () => {
    const repo = new DocumentCommentRepository(docClient);
    const result = await repo.createComment({
      ...BASE_COMMENT,
      parentCommentId: 'parent-comment-xyz',
    });
    expect(result.parentCommentId).toBe('parent-comment-xyz');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Item.parentCommentId).toBe('parent-comment-xyz');
  });

  it('returns the comment object with all input fields', async () => {
    const repo = new DocumentCommentRepository(docClient);
    const result = await repo.createComment(BASE_COMMENT);
    expect(result.documentId).toBe('doc-1');
    expect(result.sectionId).toBe('sec-1');
    expect(result.text).toBe('Great work!');
  });
});

// ---------------------------------------------------------------------------
// getCommentsForDocument
// ---------------------------------------------------------------------------

describe('DocumentCommentRepository.getCommentsForDocument', () => {
  beforeEach(() => {
    mockSend.mockResolvedValue({ Items: [] });
  });

  it('queries by documentId with ScanIndexForward=true', async () => {
    const repo = new DocumentCommentRepository(docClient);
    await repo.getCommentsForDocument('doc-1');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.KeyConditionExpression).toContain('documentId');
    expect(cmd.input.ExpressionAttributeValues[':docId']).toBe('doc-1');
    expect(cmd.input.ScanIndexForward).toBe(true);
  });

  it('passes Limit when provided', async () => {
    const repo = new DocumentCommentRepository(docClient);
    await repo.getCommentsForDocument('doc-1', 25);

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Limit).toBe(25);
  });

  it('omits Limit when not provided', async () => {
    const repo = new DocumentCommentRepository(docClient);
    await repo.getCommentsForDocument('doc-1');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Limit).toBeUndefined();
  });

  it('passes ExclusiveStartKey when lastKey provided', async () => {
    const lastKey = { documentId: 'doc-1', commentId: 'cursor-abc' };
    const repo = new DocumentCommentRepository(docClient);
    await repo.getCommentsForDocument('doc-1', undefined, lastKey);

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ExclusiveStartKey).toEqual(lastKey);
  });
});

// ---------------------------------------------------------------------------
// getCommentsForSection
// ---------------------------------------------------------------------------

describe('DocumentCommentRepository.getCommentsForSection', () => {
  beforeEach(() => {
    mockSend.mockResolvedValue({ Items: [] });
  });

  it('queries the document partition and filters its section, never a global section GSI', async () => {
    const repo = new DocumentCommentRepository(docClient);
    await repo.getCommentsForSection('doc-1', 'sec-5');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.IndexName).toBeUndefined();
    expect(cmd.input.FilterExpression).toBe('sectionId = :sid');
    expect(cmd.input.KeyConditionExpression).toBe('documentId = :docId');
    expect(cmd.input.ExpressionAttributeValues[':docId']).toBe('doc-1');
    expect(cmd.input.ExpressionAttributeValues[':sid']).toBe('sec-5');
    expect(cmd.input.ScanIndexForward).toBe(true);
  });

  it('passes Limit when provided', async () => {
    const repo = new DocumentCommentRepository(docClient);
    await repo.getCommentsForSection('doc-1', 'sec-5', 10);

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Limit).toBe(10);
  });
});

// ---------------------------------------------------------------------------
// resolveThread
// ---------------------------------------------------------------------------

describe('DocumentCommentRepository.resolveThread', () => {
  it('sends UpdateCommand with SET resolved=true, resolvedBy, resolvedAt', async () => {
    const repo = new DocumentCommentRepository(docClient);
    await repo.resolveThread('doc-1', 'cmt-1', 'user-moderator', '2026-01-01T12:00:00Z');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ documentId: 'doc-1', commentId: 'cmt-1' });
    expect(cmd.input.UpdateExpression).toContain('SET');
    expect(cmd.input.ExpressionAttributeValues[':r']).toBe(true);
    expect(cmd.input.ExpressionAttributeValues[':by']).toBe('user-moderator');
    expect(cmd.input.ExpressionAttributeValues[':at']).toBe('2026-01-01T12:00:00Z');
  });
});

// ---------------------------------------------------------------------------
// unresolveThread
// ---------------------------------------------------------------------------

describe('DocumentCommentRepository.unresolveThread', () => {
  it('sends UpdateCommand with REMOVE resolved, resolvedBy, resolvedAt', async () => {
    const repo = new DocumentCommentRepository(docClient);
    await repo.unresolveThread('doc-1', 'cmt-1');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ documentId: 'doc-1', commentId: 'cmt-1' });
    expect(cmd.input.UpdateExpression).toContain('REMOVE');
    expect(cmd.input.UpdateExpression).toContain('resolved');
    expect(cmd.input.UpdateExpression).toContain('resolvedBy');
    expect(cmd.input.UpdateExpression).toContain('resolvedAt');
  });
});

// ---------------------------------------------------------------------------
// deleteComment
// ---------------------------------------------------------------------------

describe('DocumentCommentRepository.deleteComment', () => {
  it('sends DeleteCommand with composite key', async () => {
    const repo = new DocumentCommentRepository(docClient);
    await repo.deleteComment('doc-1', 'cmt-42');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ documentId: 'doc-1', commentId: 'cmt-42' });
    expect(cmd.input.TableName).toBe('test-document-comments');
  });
});
