// platform-api/src/repositories/__tests__/ChangeVerdictRepository.test.ts
//
// The cache is allowed to be unavailable. The local DDB substrate runs with no
// volume, so every pod restart wipes every table — a miss is a normal Tuesday,
// and a cache that can fail a verdict request would be worse than no cache at
// all. These tests pin that: nothing here throws, and a missing table gets
// created rather than reported.

const mockDdbSend = jest.fn();

jest.mock('../../lib/aws-clients', () => ({
  ddbClient: { send: (...args: unknown[]) => mockDdbSend(...args) },
  docClient: {},
}));

import { CreateTableCommand, DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { ChangeVerdictRepository, toVerdict, changeKey } from '../ChangeVerdictRepository';
import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

function missingTable(): Error {
  const err = new Error('Cannot do operations on a non-existent table');
  err.name = 'ResourceNotFoundException';
  return err;
}

function repo(send: jest.Mock): ChangeVerdictRepository {
  return new ChangeVerdictRepository({ send } as unknown as DynamoDBDocumentClient);
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('ChangeVerdictRepository', () => {
  it('creates the table when the substrate says it is not there', async () => {
    const send = jest.fn().mockRejectedValue(missingTable());
    mockDdbSend
      .mockRejectedValueOnce(missingTable()) // describe
      .mockResolvedValueOnce({}) // create
      .mockResolvedValueOnce({}); // ttl

    await expect(repo(send).get('doc-1', 'item-1', 'v1-abc')).resolves.toBeNull();

    expect(mockDdbSend.mock.calls[0]![0]).toBeInstanceOf(DescribeTableCommand);
    expect(mockDdbSend.mock.calls[1]![0]).toBeInstanceOf(CreateTableCommand);
  });

  it('describes the table at most once across many reads', async () => {
    const send = jest.fn().mockResolvedValue({ Item: undefined });
    mockDdbSend.mockResolvedValue({});
    const r = repo(send);
    await r.ensureTable();
    await r.ensureTable();
    expect(mockDdbSend.mock.calls.filter((c) => c[0] instanceof DescribeTableCommand)).toHaveLength(1);
  });

  it('reports any other read failure as a miss rather than throwing', async () => {
    const send = jest.fn().mockRejectedValue(new Error('substrate is having a day'));
    await expect(repo(send).get('doc-1', 'item-1', 'v1-abc')).resolves.toBeNull();
    await expect(repo(send).listForChange('doc-1', 'item-1')).resolves.toEqual([]);
  });

  it('never throws on a failed write — the verdict was already produced', async () => {
    const send = jest.fn().mockRejectedValue(new Error('nope'));
    await expect(
      repo(send).put({
        documentId: 'doc-1',
        itemId: 'item-1',
        forRevision: 'v1-abc',
        verdict: 'concern',
        headline: 'h',
        producedAt: 'now',
        contextDigest: 'c1-x',
        model: 'sonnet',
      }),
    ).resolves.toBeUndefined();
  });

  it('stamps a ttl and the composite key on write', async () => {
    const send = jest.fn().mockResolvedValue({});
    await repo(send).put({
      documentId: 'doc-1',
      itemId: 'item-1',
      forRevision: 'v1-abc',
      verdict: 'pass',
      headline: 'h',
      producedAt: 'now',
      contextDigest: 'c1-x',
      model: 'sonnet',
    });
    const item = (send.mock.calls[0]![0] as { input: { Item: Record<string, unknown> } }).input.Item;
    expect(item['changeKey']).toBe('doc-1#item-1');
    expect(item['forRevision']).toBe('v1-abc');
    expect(typeof item['ttl']).toBe('number');
  });

  it('orders history newest first — the sort key is a hash and carries no order', async () => {
    const send = jest.fn().mockResolvedValue({
      Items: [
        { forRevision: 'a', producedAt: '2026-01-01T00:00:00Z' },
        { forRevision: 'b', producedAt: '2026-06-01T00:00:00Z' },
      ],
    });
    const rows = await repo(send).listForChange('doc-1', 'item-1');
    expect(rows.map((r) => r.forRevision)).toEqual(['b', 'a']);
  });

  it('strips storage columns back down to the shared contract', () => {
    const v = toVerdict({
      changeKey: changeKey('doc-1', 'item-1'),
      forRevision: 'v1-abc',
      documentId: 'doc-1',
      itemId: 'item-1',
      verdict: 'fail',
      headline: 'h',
      producedAt: 'now',
      contextDigest: 'c1-x',
      model: 'sonnet',
      ttl: 1,
    });
    expect(Object.keys(v).sort()).toEqual(['forRevision', 'headline', 'producedAt', 'verdict']);
  });
});
