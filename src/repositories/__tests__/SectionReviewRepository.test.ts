// social-api/src/repositories/__tests__/SectionReviewRepository.test.ts
//
// Tests for SectionReviewRepository: submitReview (composite key, return value),
// getReviewsForDocument, getReviewsForSection (begins_with prefix),
// getUserReviews (GSI), deleteReview.

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { SectionReviewRepository, type SectionReview } from '../SectionReviewRepository';

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

const mockSend = jest.fn();
const docClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

function review(overrides: Partial<SectionReview> = {}): SectionReview {
  return {
    documentId: 'doc-1',
    reviewKey: 'section-1:user-1',
    sectionId: 'section-1',
    userId: 'user-1',
    displayName: 'Alice',
    status: 'approved',
    timestamp: '2026-01-01T00:00:00Z',
    ...overrides,
  };
}

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// submitReview
// ---------------------------------------------------------------------------

describe('SectionReviewRepository.submitReview', () => {
  it('constructs reviewKey as sectionId:userId', async () => {
    const repo = new SectionReviewRepository(docClient);
    await repo.submitReview({
      documentId: 'doc-1',
      sectionId: 'sec-A',
      userId: 'u-42',
      displayName: 'Bob',
      status: 'pending',
      timestamp: '2026-01-01T00:00:00Z',
    });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Item.reviewKey).toBe('sec-A:u-42');
  });

  it('sends PutCommand with all review fields', async () => {
    const repo = new SectionReviewRepository(docClient);
    await repo.submitReview({
      documentId: 'doc-1',
      sectionId: 'sec-1',
      userId: 'usr-1',
      displayName: 'Alice',
      status: 'approved',
      timestamp: '2026-01-01T00:00:00Z',
      comment: 'Looks good',
    });

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.TableName).toBe('test-section-reviews');
    expect(cmd.input.Item).toMatchObject({
      documentId: 'doc-1',
      sectionId: 'sec-1',
      userId: 'usr-1',
      displayName: 'Alice',
      status: 'approved',
      comment: 'Looks good',
    });
  });

  it('returns the saved review including reviewKey', async () => {
    const repo = new SectionReviewRepository(docClient);
    const result = await repo.submitReview({
      documentId: 'doc-1',
      sectionId: 'sec-1',
      userId: 'usr-1',
      displayName: 'Alice',
      status: 'approved',
      timestamp: '2026-01-01T00:00:00Z',
    });

    expect(result.reviewKey).toBe('sec-1:usr-1');
    expect(result.documentId).toBe('doc-1');
  });

  it('includes optional comment when provided', async () => {
    const repo = new SectionReviewRepository(docClient);
    const result = await repo.submitReview({
      documentId: 'doc-1',
      sectionId: 'sec-1',
      userId: 'usr-1',
      displayName: 'Alice',
      status: 'approved',
      timestamp: '2026-01-01T00:00:00Z',
      comment: 'LGTM',
    });

    expect(result.comment).toBe('LGTM');
  });

  it('omits comment when not provided', async () => {
    const repo = new SectionReviewRepository(docClient);
    const result = await repo.submitReview({
      documentId: 'doc-1',
      sectionId: 'sec-1',
      userId: 'usr-1',
      displayName: 'Alice',
      status: 'approved',
      timestamp: '2026-01-01T00:00:00Z',
    });

    expect(result.comment).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// getReviewsForDocument
// ---------------------------------------------------------------------------

describe('SectionReviewRepository.getReviewsForDocument', () => {
  it('queries by documentId partition key', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new SectionReviewRepository(docClient);
    await repo.getReviewsForDocument('doc-99');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.KeyConditionExpression).toContain('documentId');
    expect(cmd.input.ExpressionAttributeValues[':docId']).toBe('doc-99');
  });

  it('returns array of reviews', async () => {
    const items = [review({ documentId: 'doc-1' }), review({ documentId: 'doc-1', reviewKey: 'sec-2:user-2' })];
    mockSend.mockResolvedValue({ Items: items });
    const repo = new SectionReviewRepository(docClient);
    const result = await repo.getReviewsForDocument('doc-1');
    expect(result).toHaveLength(2);
  });
});

// ---------------------------------------------------------------------------
// getReviewsForSection
// ---------------------------------------------------------------------------

describe('SectionReviewRepository.getReviewsForSection', () => {
  it('queries with begins_with prefix for the sectionId', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new SectionReviewRepository(docClient);
    await repo.getReviewsForSection('doc-1', 'sec-5');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ExpressionAttributeValues[':docId']).toBe('doc-1');
    expect(cmd.input.ExpressionAttributeValues[':prefix']).toBe('sec-5:');
    expect(cmd.input.KeyConditionExpression).toContain('begins_with');
  });
});

// ---------------------------------------------------------------------------
// getUserReviews
// ---------------------------------------------------------------------------

describe('SectionReviewRepository.getUserReviews', () => {
  it('queries the userId-documentId-index GSI', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new SectionReviewRepository(docClient);
    await repo.getUserReviews('user-77');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.IndexName).toBe('userId-documentId-index');
    expect(cmd.input.ExpressionAttributeValues[':uid']).toBe('user-77');
  });
});

// ---------------------------------------------------------------------------
// deleteReview
// ---------------------------------------------------------------------------

describe('SectionReviewRepository.deleteReview', () => {
  it('sends DeleteCommand with composite key', async () => {
    const repo = new SectionReviewRepository(docClient);
    await repo.deleteReview('doc-1', 'sec-1:user-1');

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.Key).toEqual({ documentId: 'doc-1', reviewKey: 'sec-1:user-1' });
  });
});
