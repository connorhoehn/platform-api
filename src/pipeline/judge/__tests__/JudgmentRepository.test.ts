// platform-api/src/pipeline/judge/__tests__/JudgmentRepository.test.ts
//
// Unit tests for JudgmentRepository: put-then-get round-trip,
// rawText spillover guard at 32KB, GSI query shapes for the three
// listBy methods, cursor encode/decode.

import {
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
  GetCommand,
} from '@aws-sdk/lib-dynamodb';

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

jest.mock('../../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));
jest.mock('../../../lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
}));

import { JudgmentRepository } from '../JudgmentRepository';
import { InMemoryRawTextStore, rawTextKeyFor } from '../RawTextStore';
import type { JudgmentResult } from '../types';

function makeResult(overrides: Partial<JudgmentResult> = {}): JudgmentResult {
  return {
    judgmentId: 'jdg_1',
    runId: 'run_1',
    stepId: 'step_judge',
    pipelineId: 'pl_1',
    rubricId: 'rubric.helpfulness',
    rubricVersion: 1,
    rubricSnapshot: {
      id: 'rubric.helpfulness',
      version: 1,
      criteria: [{ id: 'a', prompt: 'p', scale: '1-5' }],
      overall: 'weighted-mean',
    },
    mode: 'absolute',
    overallScore: 0.82,
    overallVerdict: 'pass',
    overallPassed: true,
    confidence: 0.9,
    criteria: [
      { criterionId: 'a', score: 0.8, rawScore: 4, rationale: 'good', passed: true },
    ],
    rawModelText: '{"criteria":[...]}',
    model: 'claude-sonnet-4-6',
    tokensIn: 200,
    tokensOut: 50,
    latencyMs: 800,
    costUsd: 0.0015,
    cacheHit: false,
    judgedAt: '2026-06-02T10:00:00.000Z',
    ...overrides,
  };
}

beforeEach(() => {
  mockSend.mockReset();
});

// ---------------------------------------------------------------------------
// persistJudgment
// ---------------------------------------------------------------------------

describe('JudgmentRepository — persistJudgment', () => {
  it('issues a PutCommand to the pipeline-judgments table', async () => {
    mockSend.mockResolvedValue({});
    const repo = new JudgmentRepository(mockDocClient, { now: () => 1_700_000_000_000 });
    await repo.persistJudgment(makeResult());

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd).toBeInstanceOf(PutCommand);
    expect(cmd.input.TableName).toBe('test-pipeline-judgments');
    const item = cmd.input.Item as Record<string, unknown>;
    expect(item.judgmentId).toBe('jdg_1');
    expect(item.overallScore).toBe(0.82);
    expect(item.rawTextTruncated).toBeUndefined();
  });

  it('adds a ttlEpoch derived from now + ttlDays', async () => {
    mockSend.mockResolvedValue({});
    const fixedNow = 1_700_000_000_000;
    const repo = new JudgmentRepository(mockDocClient, {
      now: () => fixedNow,
      ttlDays: 30,
    });
    await repo.persistJudgment(makeResult());

    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as Record<string, unknown>;
    expect(item.ttlEpoch).toBe(Math.floor(fixedNow / 1000) + 30 * 24 * 60 * 60);
  });

  it('strips rawModelText and sets rawTextTruncated when ≥ 32KB', async () => {
    mockSend.mockResolvedValue({});
    const huge = 'x'.repeat(32 * 1024); // exactly the cap → truncate
    const repo = new JudgmentRepository(mockDocClient);
    await repo.persistJudgment(makeResult({ rawModelText: huge }));

    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as Record<string, unknown>;
    expect(item.rawTextTruncated).toBe(true);
    expect(item.rawModelText).toBeUndefined();
  });

  it('keeps rawModelText inline when smaller than the cap', async () => {
    mockSend.mockResolvedValue({});
    const small = 'x'.repeat(10 * 1024);
    const repo = new JudgmentRepository(mockDocClient);
    await repo.persistJudgment(makeResult({ rawModelText: small }));

    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as Record<string, unknown>;
    expect(item.rawModelText).toBe(small);
    expect(item.rawTextTruncated).toBeUndefined();
  });

  it('handles missing rawModelText without truncating', async () => {
    mockSend.mockResolvedValue({});
    const repo = new JudgmentRepository(mockDocClient);
    await repo.persistJudgment(makeResult({ rawModelText: undefined }));

    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as Record<string, unknown>;
    expect(item.rawTextTruncated).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// v2 — rawText spillover to RawTextStore
// ---------------------------------------------------------------------------

describe('JudgmentRepository — rawText spillover (v2)', () => {
  it('uploads oversized rawText to the store and stores the S3 key', async () => {
    mockSend.mockResolvedValue({});
    const store = new InMemoryRawTextStore();
    const repo = new JudgmentRepository(mockDocClient, { rawTextStore: store });
    const huge = 'x'.repeat(40 * 1024);

    await repo.persistJudgment(makeResult({ judgmentId: 'jdg_big', rawModelText: huge }));

    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as Record<string, unknown>;
    expect(item.rawTextTruncated).toBe(true);
    expect(item.rawModelText).toBeUndefined();
    expect(item.rawTextS3Key).toBe(rawTextKeyFor('jdg_big'));
    expect(await store.get(item.rawTextS3Key as string)).toBe(huge);
  });

  it('does not upload when rawText is under the cap (no store call)', async () => {
    mockSend.mockResolvedValue({});
    const store = new InMemoryRawTextStore();
    const putSpy = jest.spyOn(store, 'put');
    const repo = new JudgmentRepository(mockDocClient, { rawTextStore: store });

    await repo.persistJudgment(makeResult({ rawModelText: 'tiny' }));
    expect(putSpy).not.toHaveBeenCalled();
  });

  it('falls back to v1 behavior (drop body, set truncated flag) when store upload fails', async () => {
    mockSend.mockResolvedValue({});
    const store = new InMemoryRawTextStore();
    jest.spyOn(store, 'put').mockRejectedValue(new Error('S3 down'));
    const repo = new JudgmentRepository(mockDocClient, { rawTextStore: store });
    const huge = 'x'.repeat(40 * 1024);

    await repo.persistJudgment(makeResult({ rawModelText: huge }));

    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as Record<string, unknown>;
    expect(item.rawTextTruncated).toBe(true);
    expect(item.rawModelText).toBeUndefined();
    expect(item.rawTextS3Key).toBeUndefined();
  });

  it('with no store configured, oversized body is dropped without spillover', async () => {
    mockSend.mockResolvedValue({});
    const repo = new JudgmentRepository(mockDocClient); // no rawTextStore
    const huge = 'x'.repeat(40 * 1024);

    await repo.persistJudgment(makeResult({ rawModelText: huge }));

    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as Record<string, unknown>;
    expect(item.rawTextTruncated).toBe(true);
    expect(item.rawModelText).toBeUndefined();
    expect(item.rawTextS3Key).toBeUndefined();
  });
});

describe('JudgmentRepository — getRawText (v2)', () => {
  it('returns the inline body when the row was not truncated', async () => {
    mockSend.mockResolvedValue({
      Item: { judgmentId: 'jdg_1', rawModelText: 'inline body' },
    });
    const repo = new JudgmentRepository(mockDocClient);
    expect(await repo.getRawText('jdg_1')).toBe('inline body');
  });

  it('fetches from the store when rawTextS3Key is set', async () => {
    const store = new InMemoryRawTextStore();
    await store.put('jdg_big', 'recovered from S3');
    mockSend.mockResolvedValue({
      Item: { judgmentId: 'jdg_big', rawTextTruncated: true, rawTextS3Key: rawTextKeyFor('jdg_big') },
    });
    const repo = new JudgmentRepository(mockDocClient, { rawTextStore: store });
    expect(await repo.getRawText('jdg_big')).toBe('recovered from S3');
  });

  it('returns null when truncated and no store is configured', async () => {
    mockSend.mockResolvedValue({
      Item: { judgmentId: 'jdg_1', rawTextTruncated: true, rawTextS3Key: 'judgments/jdg_1.txt' },
    });
    const repo = new JudgmentRepository(mockDocClient); // no store
    expect(await repo.getRawText('jdg_1')).toBeNull();
  });

  it('returns null when the judgment row does not exist', async () => {
    mockSend.mockResolvedValue({});
    const repo = new JudgmentRepository(mockDocClient);
    expect(await repo.getRawText('jdg_missing')).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// getById
// ---------------------------------------------------------------------------

describe('JudgmentRepository — getById', () => {
  it('issues a GetCommand keyed by judgmentId', async () => {
    mockSend.mockResolvedValue({ Item: { judgmentId: 'jdg_1', overallScore: 0.5 } });
    const repo = new JudgmentRepository(mockDocClient);
    const got = await repo.getById('jdg_1');

    const cmd = mockSend.mock.calls[0][0] as GetCommand;
    expect(cmd).toBeInstanceOf(GetCommand);
    expect(cmd.input.Key).toEqual({ judgmentId: 'jdg_1' });
    expect(got?.overallScore).toBe(0.5);
  });

  it('returns null on a miss', async () => {
    mockSend.mockResolvedValue({});
    const repo = new JudgmentRepository(mockDocClient);
    expect(await repo.getById('missing')).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// list-by GSI queries
// ---------------------------------------------------------------------------

describe('JudgmentRepository — listByPipeline / listByRun / listByRubric', () => {
  it('listByPipeline queries pipeline-time-index', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new JudgmentRepository(mockDocClient);
    await repo.listByPipeline('pl_42');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('pipeline-time-index');
    expect(cmd.input.ExpressionAttributeValues?.[':pk']).toBe('pl_42');
    expect(cmd.input.ScanIndexForward).toBe(false);
  });

  it('listByRun queries run-time-index', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new JudgmentRepository(mockDocClient);
    await repo.listByRun('run_1');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('run-time-index');
    expect(cmd.input.ExpressionAttributeValues?.[':pk']).toBe('run_1');
  });

  it('listByRubric queries rubric-time-index', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new JudgmentRepository(mockDocClient);
    await repo.listByRubric('rubric.helpfulness');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('rubric-time-index');
    expect(cmd.input.ExpressionAttributeValues?.[':pk']).toBe('rubric.helpfulness');
  });

  it('passes since+until as a BETWEEN clause', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new JudgmentRepository(mockDocClient);
    await repo.listByPipeline('pl_1', {
      since: '2026-06-01T00:00:00.000Z',
      until: '2026-06-02T00:00:00.000Z',
    });

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.KeyConditionExpression).toMatch(/BETWEEN/);
  });

  it('emits a base64 cursor when DDB returns a LastEvaluatedKey', async () => {
    const lek = { pipelineId: 'pl_1', judgedAt: '2026-06-02T00:00:00.000Z', judgmentId: 'jdg_10' };
    mockSend.mockResolvedValue({ Items: [], LastEvaluatedKey: lek });
    const repo = new JudgmentRepository(mockDocClient);
    const result = await repo.listByPipeline('pl_1');

    expect(result.nextCursor).toBeDefined();
    const decoded = JSON.parse(Buffer.from(result.nextCursor!, 'base64').toString('utf8'));
    expect(decoded).toEqual(lek);
  });

  it('round-trips the cursor into ExclusiveStartKey on the next call', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new JudgmentRepository(mockDocClient);
    const cursor = Buffer.from(JSON.stringify({ pipelineId: 'pl_1', judgedAt: 't' }), 'utf8').toString('base64');
    await repo.listByPipeline('pl_1', { cursor });

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.ExclusiveStartKey).toEqual({ pipelineId: 'pl_1', judgedAt: 't' });
  });
});
