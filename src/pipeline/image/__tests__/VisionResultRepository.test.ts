// platform-api/src/pipeline/image/__tests__/VisionResultRepository.test.ts
//
// Round-trip + spillover + GSI query tests for VisionResultRepository.
// Code parity with JudgmentRepository.test.ts.

import {
  DynamoDBDocumentClient,
  PutCommand,
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

import { VisionResultRepository } from '../VisionResultRepository';
import { InMemoryRawTextStore } from '../../judge/RawTextStore';
import type { VisionResult } from '../types';

function makeResult(over: Partial<VisionResult> = {}): VisionResult {
  return {
    visionId: 'vis_1',
    runId: 'run_1',
    stepId: 'step_vision',
    pipelineId: 'pl_1',
    imageId: 'img_1',
    caption: 'A red apple.',
    extracted: { caption: 'A red apple.' },
    rawModelText: '{"caption":"A red apple."}',
    model: 'claude-sonnet-4-6',
    tokensIn: 100,
    tokensOut: 20,
    latencyMs: 500,
    costUsd: 0.001,
    cacheHit: false,
    analyzedAt: '2026-06-04T10:00:00.000Z',
    ...over,
  };
}

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

// ---------------------------------------------------------------------------
// persistVisionResult
// ---------------------------------------------------------------------------

describe('VisionResultRepository — persistVisionResult', () => {
  it('issues a PutCommand to the pipeline-vision-results table', async () => {
    const repo = new VisionResultRepository(mockDocClient, { now: () => 1_700_000_000_000 });
    await repo.persistVisionResult(makeResult());

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd).toBeInstanceOf(PutCommand);
    expect(cmd.input.TableName).toBe('test-pipeline-vision-results');
    const item = cmd.input.Item as Record<string, unknown>;
    expect(item.visionId).toBe('vis_1');
    expect(item.pipelineId).toBe('pl_1');
    expect(item.runId).toBe('run_1');
    expect(item.imageId).toBe('img_1');
    expect(item.caption).toBe('A red apple.');
  });

  it('writes ttlEpoch derived from now + ttlDays', async () => {
    const repo = new VisionResultRepository(mockDocClient, {
      now: () => 1_700_000_000_000,
      ttlDays: 7,
    });
    await repo.persistVisionResult(makeResult());
    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as { ttlEpoch: number };
    expect(item.ttlEpoch).toBe(Math.floor(1_700_000_000_000 / 1000) + 7 * 24 * 60 * 60);
  });

  it('keeps rawModelText inline when ≤32KB and no rawTextStore is wired', async () => {
    const repo = new VisionResultRepository(mockDocClient);
    await repo.persistVisionResult(makeResult({ rawModelText: 'small body' }));
    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as Record<string, unknown>;
    expect(item.rawModelText).toBe('small body');
    expect(item.rawTextTruncated).toBeUndefined();
    expect(item.rawTextKey).toBeUndefined();
  });

  it('strips rawModelText + flags truncated when >=32KB and no store is wired', async () => {
    const repo = new VisionResultRepository(mockDocClient);
    const big = 'a'.repeat(33 * 1024);
    await repo.persistVisionResult(makeResult({ rawModelText: big }));
    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as Record<string, unknown>;
    expect(item.rawModelText).toBeUndefined();
    expect(item.rawTextTruncated).toBe(true);
    expect(item.rawTextKey).toBeUndefined();
  });

  it('truncated item has rawModelText KEY fully absent (not undefined/null — DDB spread fragility fix)', async () => {
    const repo = new VisionResultRepository(mockDocClient);
    const big = 'x'.repeat(33 * 1024);
    await repo.persistVisionResult(makeResult({ rawModelText: big }));
    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as Record<string, unknown>;
    // The key must not exist at all — DDB marshallers may serialize
    // `undefined` as a NULL attribute if the key is present.
    expect(Object.prototype.hasOwnProperty.call(item, 'rawModelText')).toBe(false);
    expect(item.rawTextTruncated).toBe(true);
  });

  it('spills oversized rawText to the rawTextStore + records the key', async () => {
    const spillover = new InMemoryRawTextStore();
    const repo = new VisionResultRepository(mockDocClient, { rawTextStore: spillover });
    const big = 'b'.repeat(33 * 1024);
    await repo.persistVisionResult(makeResult({ rawModelText: big }));

    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as Record<string, unknown>;
    expect(item.rawModelText).toBeUndefined();
    expect(item.rawTextTruncated).toBe(true);
    expect(typeof item.rawTextKey).toBe('string');
    expect(await spillover.get(item.rawTextKey as string)).toBe(big);
  });

  it('does not fail the primary write when spillover throws', async () => {
    const flakyStore = {
      put: jest.fn().mockRejectedValue(new Error('spillover boom')),
      get: jest.fn(),
    };
    const repo = new VisionResultRepository(mockDocClient, {
      rawTextStore: flakyStore as never,
    });
    const big = 'c'.repeat(33 * 1024);
    await expect(
      repo.persistVisionResult(makeResult({ rawModelText: big })),
    ).resolves.toBeUndefined();
    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as Record<string, unknown>;
    expect(item.rawTextKey).toBeUndefined();
    expect(item.rawTextTruncated).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// getRawText
// ---------------------------------------------------------------------------

describe('VisionResultRepository — getRawText', () => {
  it('returns inline body when not truncated', async () => {
    mockSend.mockResolvedValueOnce({ Item: makeResult({ rawModelText: 'inline body' }) });
    const repo = new VisionResultRepository(mockDocClient);
    expect(await repo.getRawText('vis_1')).toBe('inline body');
  });

  it('returns null when visionId is missing', async () => {
    mockSend.mockResolvedValueOnce({});
    const repo = new VisionResultRepository(mockDocClient);
    expect(await repo.getRawText('vis_missing')).toBeNull();
  });

  it('hydrates from spillover when rawTextKey is set', async () => {
    const spillover = new InMemoryRawTextStore();
    const key = await spillover.put('vis_1', 'spilled body');
    mockSend.mockResolvedValueOnce({
      Item: {
        ...makeResult(),
        rawModelText: undefined,
        rawTextTruncated: true,
        rawTextKey: key,
      },
    });
    const repo = new VisionResultRepository(mockDocClient, { rawTextStore: spillover });
    expect(await repo.getRawText('vis_1')).toBe('spilled body');
  });

  it('returns null when rawText was truncated AND no store is wired', async () => {
    mockSend.mockResolvedValueOnce({
      Item: { ...makeResult(), rawModelText: undefined, rawTextTruncated: true },
    });
    const repo = new VisionResultRepository(mockDocClient);
    expect(await repo.getRawText('vis_1')).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// listByPipeline + listByRun
// ---------------------------------------------------------------------------

describe('VisionResultRepository — list', () => {
  it('listByPipeline queries the pipeline-time-index', async () => {
    mockSend.mockResolvedValueOnce({ Items: [], LastEvaluatedKey: undefined });
    const repo = new VisionResultRepository(mockDocClient);
    await repo.listByPipeline('pl_1');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd).toBeInstanceOf(QueryCommand);
    expect(cmd.input.TableName).toBe('test-pipeline-vision-results');
    expect(cmd.input.IndexName).toBe('pipeline-time-index');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':pk': 'pl_1' });
    expect(cmd.input.ScanIndexForward).toBe(false); // newest first
  });

  it('listByRun queries the run-time-index', async () => {
    mockSend.mockResolvedValueOnce({ Items: [], LastEvaluatedKey: undefined });
    const repo = new VisionResultRepository(mockDocClient);
    await repo.listByRun('run_1');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('run-time-index');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':pk': 'run_1' });
  });

  it('appends since BETWEEN until to the key condition when both supplied', async () => {
    mockSend.mockResolvedValueOnce({ Items: [], LastEvaluatedKey: undefined });
    const repo = new VisionResultRepository(mockDocClient);
    await repo.listByPipeline('pl_1', { since: '2026-06-01', until: '2026-06-04' });

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.KeyConditionExpression).toContain('BETWEEN');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({
      ':since': '2026-06-01',
      ':until': '2026-06-04',
    });
  });

  it('round-trips a cursor base64 → ExclusiveStartKey → nextCursor', async () => {
    const lastKey = { visionId: 'vis_99', pipelineId: 'pl_1', analyzedAt: '2026-06-03' };
    mockSend.mockResolvedValueOnce({ Items: [], LastEvaluatedKey: lastKey });
    const repo = new VisionResultRepository(mockDocClient);
    const result = await repo.listByPipeline('pl_1');
    expect(typeof result.nextCursor).toBe('string');

    mockSend.mockResolvedValueOnce({ Items: [] });
    await repo.listByPipeline('pl_1', { cursor: result.nextCursor });
    const secondCall = mockSend.mock.calls[1][0] as QueryCommand;
    expect(secondCall.input.ExclusiveStartKey).toEqual(lastKey);
  });
});
