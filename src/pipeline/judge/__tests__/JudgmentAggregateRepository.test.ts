// platform-api/src/pipeline/judge/__tests__/JudgmentAggregateRepository.test.ts
//
// Unit tests for JudgmentAggregateRepository: UpdateItem shape,
// pass/fail counter routing, dayBucket derivation, GSI query shapes,
// best-effort min/max conditional-check swallow.

import {
  DynamoDBDocumentClient,
  UpdateCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

jest.mock('../../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));
jest.mock('../../../lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
}));

import {
  JudgmentAggregateRepository,
  buildAggregateKey,
  utcDayBucket,
} from '../JudgmentAggregateRepository';
import type { JudgmentResult } from '../types';

function makeResult(overrides: Partial<JudgmentResult> = {}): JudgmentResult {
  return {
    judgmentId: 'jdg_1',
    runId: 'run_1',
    stepId: 'step_judge',
    pipelineId: 'pl_42',
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
    criteria: [],
    model: 'claude-sonnet-4-6',
    tokensIn: 1200,
    tokensOut: 240,
    latencyMs: 1500,
    costUsd: 0.0072,
    cacheHit: false,
    judgedAt: '2026-06-03T12:00:00.000Z',
    ...overrides,
  };
}

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

describe('utcDayBucket / buildAggregateKey', () => {
  it('formats day as yyyy-mm-dd in UTC', () => {
    const ts = new Date('2026-06-03T15:00:00Z').getTime();
    expect(utcDayBucket(ts)).toBe('2026-06-03');
  });

  it('uses UTC boundary, not local', () => {
    // 2026-06-03 23:30Z → 2026-06-03 in UTC regardless of host TZ.
    const ts = new Date('2026-06-03T23:30:00Z').getTime();
    expect(utcDayBucket(ts)).toBe('2026-06-03');
  });

  it('aggregate key is `${pipelineId}#${rubricId}#${dayBucket}`', () => {
    expect(buildAggregateKey('pl_1', 'rubric.x', '2026-06-03')).toBe(
      'pl_1#rubric.x#2026-06-03',
    );
  });
});

// ---------------------------------------------------------------------------
// recordJudgment
// ---------------------------------------------------------------------------

describe('JudgmentAggregateRepository — recordJudgment', () => {
  it('issues an UpdateCommand to the aggregates table', async () => {
    const repo = new JudgmentAggregateRepository(mockDocClient);
    await repo.recordJudgment(makeResult());

    const cmd = mockSend.mock.calls[0][0] as UpdateCommand;
    expect(cmd).toBeInstanceOf(UpdateCommand);
    expect(cmd.input.TableName).toBe('test-pipeline-judgment-aggregates');
    expect(cmd.input.Key).toEqual({ aggregateKey: 'pl_42#rubric.helpfulness#2026-06-03' });
  });

  it('ADDs to counters in the correct buckets', async () => {
    const repo = new JudgmentAggregateRepository(mockDocClient);
    await repo.recordJudgment(makeResult({ overallPassed: true }));

    const cmd = mockSend.mock.calls[0][0] as UpdateCommand;
    const values = cmd.input.ExpressionAttributeValues!;
    expect(values[':one']).toBe(1);
    expect(values[':passed']).toBe(1);
    expect(values[':failed']).toBe(0);
    expect(values[':score']).toBe(0.82);
    expect(values[':tin']).toBe(1200);
    expect(values[':tout']).toBe(240);
    expect(values[':cost']).toBe(0.0072);
  });

  it('routes failed judgments to the numFailed bucket', async () => {
    const repo = new JudgmentAggregateRepository(mockDocClient);
    await repo.recordJudgment(makeResult({ overallPassed: false }));

    const values = (mockSend.mock.calls[0][0] as UpdateCommand).input.ExpressionAttributeValues!;
    expect(values[':passed']).toBe(0);
    expect(values[':failed']).toBe(1);
  });

  it('sets ttlEpoch derived from now + ttlDays', async () => {
    const fixedNow = 1_700_000_000_000;
    const repo = new JudgmentAggregateRepository(mockDocClient, {
      now: () => fixedNow,
      ttlDays: 30,
    });
    await repo.recordJudgment(makeResult());

    const values = (mockSend.mock.calls[0][0] as UpdateCommand).input.ExpressionAttributeValues!;
    expect(values[':ttl']).toBe(Math.floor(fixedNow / 1000) + 30 * 24 * 60 * 60);
  });

  it('derives dayBucket from judgedAt (not now)', async () => {
    // judgedAt in the past — aggregateKey uses judgedAt's day, not the
    // wall-clock day. Prevents back-fills from landing in today's row.
    const repo = new JudgmentAggregateRepository(mockDocClient, {
      now: () => new Date('2026-06-10T00:00:00Z').getTime(),
    });
    await repo.recordJudgment(makeResult({ judgedAt: '2026-06-01T03:00:00.000Z' }));

    expect((mockSend.mock.calls[0][0] as UpdateCommand).input.Key).toEqual({
      aggregateKey: 'pl_42#rubric.helpfulness#2026-06-01',
    });
  });

  it('falls back to now() when judgedAt is malformed', async () => {
    const repo = new JudgmentAggregateRepository(mockDocClient, {
      now: () => new Date('2026-06-10T03:00:00Z').getTime(),
    });
    await repo.recordJudgment(makeResult({ judgedAt: 'not-a-date' }));
    expect((mockSend.mock.calls[0][0] as UpdateCommand).input.Key).toEqual({
      aggregateKey: 'pl_42#rubric.helpfulness#2026-06-10',
    });
  });

  it('fires best-effort min/max updates as separate conditional writes', async () => {
    const repo = new JudgmentAggregateRepository(mockDocClient);
    await repo.recordJudgment(makeResult({ overallScore: 0.5 }));

    // 1 primary + 2 min/max = 3 calls
    expect(mockSend).toHaveBeenCalledTimes(3);
    const minCmd = mockSend.mock.calls[1][0] as UpdateCommand;
    const maxCmd = mockSend.mock.calls[2][0] as UpdateCommand;
    expect(minCmd.input.ConditionExpression).toMatch(/< #attr/);
    expect(maxCmd.input.ConditionExpression).toMatch(/> #attr/);
  });

  it('swallows ConditionalCheckFailedException on min/max (existing extreme already better)', async () => {
    mockSend
      .mockResolvedValueOnce({}) // primary update OK
      .mockRejectedValueOnce(Object.assign(new Error('cond fail'), { name: 'ConditionalCheckFailedException' }))
      .mockRejectedValueOnce(Object.assign(new Error('cond fail'), { name: 'ConditionalCheckFailedException' }));

    const repo = new JudgmentAggregateRepository(mockDocClient);
    await expect(repo.recordJudgment(makeResult())).resolves.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// list-by GSI queries
// ---------------------------------------------------------------------------

describe('JudgmentAggregateRepository — listByPipeline / listByRubric', () => {
  it('listByPipeline queries pipeline-day-index', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new JudgmentAggregateRepository(mockDocClient);
    await repo.listByPipeline('pl_42');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd).toBeInstanceOf(QueryCommand);
    expect(cmd.input.IndexName).toBe('pipeline-day-index');
    expect(cmd.input.ExpressionAttributeValues?.[':pk']).toBe('pl_42');
    expect(cmd.input.ScanIndexForward).toBe(false);
  });

  it('listByRubric queries rubric-day-index', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new JudgmentAggregateRepository(mockDocClient);
    await repo.listByRubric('rubric.helpfulness');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('rubric-day-index');
    expect(cmd.input.ExpressionAttributeValues?.[':pk']).toBe('rubric.helpfulness');
  });

  it('applies since+until BETWEEN clause', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new JudgmentAggregateRepository(mockDocClient);
    await repo.listByPipeline('pl_1', { since: '2026-06-01', until: '2026-06-30' });

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.KeyConditionExpression).toMatch(/BETWEEN/);
  });
});
