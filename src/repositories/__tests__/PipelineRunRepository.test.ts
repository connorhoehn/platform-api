// social-api/src/repositories/__tests__/PipelineRunRepository.test.ts
//
// Unit tests for PipelineRunRepository (CRUD + GSI queries, limit clamping)
// and the readAttempts() backward-compat helper.

import { DynamoDBDocumentClient, DeleteCommand, GetCommand, PutCommand, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import {
  PipelineRunRepository,
  readAttempts,
  type PipelineRunItem,
  type StepSummary,
} from '../PipelineRunRepository';

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

function runItem(overrides: Partial<PipelineRunItem> = {}): PipelineRunItem {
  return {
    pipelineId: 'pipeline-1',
    runId: 'run-1',
    status: 'succeeded',
    triggeredBy: 'user-1',
    triggerType: 'manual',
    startedAt: '2026-05-06T09:00:00.000Z',
    ...overrides,
  };
}

beforeEach(() => {
  mockSend.mockReset();
});

// ---------------------------------------------------------------------------
// put
// ---------------------------------------------------------------------------

describe('PipelineRunRepository — put', () => {
  it('issues a PutCommand with the full item', async () => {
    mockSend.mockResolvedValue({});
    const repo = new PipelineRunRepository(mockDocClient);
    await repo.put(runItem());

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd).toBeInstanceOf(PutCommand);
    expect(cmd.input.TableName).toBe('test-pipeline-runs');
    expect(cmd.input.Item).toMatchObject({ pipelineId: 'pipeline-1', runId: 'run-1' });
  });

  // Runs used to accumulate forever: this ttl was declared but never written,
  // and pipeline-runs was never registered with ensureTableTTL. Both halves
  // are needed, so both are pinned — this one, and dynamo-setup's.
  it('stamps a ttl on a finished run so it eventually expires', async () => {
    mockSend.mockResolvedValue({});
    const repo = new PipelineRunRepository(mockDocClient);
    const before = Math.floor(Date.now() / 1000);
    await repo.put(runItem({ status: 'completed' }));

    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as { ttl?: number };
    expect(item.ttl).toBeGreaterThanOrEqual(before + 29 * 86_400);
    expect(item.ttl).toBeLessThanOrEqual(before + 31 * 86_400);
  });

  it('accepts the legacy `succeeded` spelling as terminal', async () => {
    mockSend.mockResolvedValue({});
    const repo = new PipelineRunRepository(mockDocClient);
    await repo.put(runItem({ status: 'succeeded' }));
    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as { ttl?: number };
    expect(item.ttl).toBeDefined();
  });

  // The one that would hurt: resumeInterruptedRuns reads exactly these rows at
  // boot, so expiring one destroys work that was going to be resumed.
  it.each(['pending', 'running', 'interrupted'] as const)(
    'never expires a %s run',
    async (status) => {
      mockSend.mockResolvedValue({});
      const repo = new PipelineRunRepository(mockDocClient);
      await repo.put(runItem({ status }));
      const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as { ttl?: number };
      expect(item.ttl).toBeUndefined();
    },
  );

  it('leaves an explicitly supplied ttl alone', async () => {
    mockSend.mockResolvedValue({});
    const repo = new PipelineRunRepository(mockDocClient);
    await repo.put(runItem({ status: 'completed', ttl: 42 }));
    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as { ttl?: number };
    expect(item.ttl).toBe(42);
  });

  it('honours PIPELINE_RUN_RETENTION_DAYS', async () => {
    const prev = process.env['PIPELINE_RUN_RETENTION_DAYS'];
    process.env['PIPELINE_RUN_RETENTION_DAYS'] = '2';
    try {
      mockSend.mockResolvedValue({});
      const repo = new PipelineRunRepository(mockDocClient);
      const before = Math.floor(Date.now() / 1000);
      await repo.put(runItem({ status: 'failed' }));
      const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as { ttl?: number };
      expect(item.ttl).toBeLessThanOrEqual(before + 3 * 86_400);
    } finally {
      if (prev === undefined) delete process.env['PIPELINE_RUN_RETENTION_DAYS'];
      else process.env['PIPELINE_RUN_RETENTION_DAYS'] = prev;
    }
  });
});

// ---------------------------------------------------------------------------
// get
// ---------------------------------------------------------------------------

describe('PipelineRunRepository — get', () => {
  it('fetches by composite pipelineId+runId key', async () => {
    mockSend.mockResolvedValue({ Item: runItem() });
    const repo = new PipelineRunRepository(mockDocClient);
    const result = await repo.get('pipeline-1', 'run-1');

    const cmd = mockSend.mock.calls[0][0] as GetCommand;
    expect(cmd).toBeInstanceOf(GetCommand);
    expect(cmd.input.Key).toEqual({ pipelineId: 'pipeline-1', runId: 'run-1' });
    expect(result?.runId).toBe('run-1');
  });

  it('returns null when item not found', async () => {
    mockSend.mockResolvedValue({ Item: undefined });
    const repo = new PipelineRunRepository(mockDocClient);
    const result = await repo.get('pipeline-1', 'missing');
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------

describe('PipelineRunRepository — delete', () => {
  it('issues a DeleteCommand with composite key', async () => {
    mockSend.mockResolvedValue({});
    const repo = new PipelineRunRepository(mockDocClient);
    await repo.delete('pipeline-1', 'run-99');

    const cmd = mockSend.mock.calls[0][0] as DeleteCommand;
    expect(cmd).toBeInstanceOf(DeleteCommand);
    expect(cmd.input.Key).toEqual({ pipelineId: 'pipeline-1', runId: 'run-99' });
  });
});

// ---------------------------------------------------------------------------
// listByPipeline
// ---------------------------------------------------------------------------

describe('PipelineRunRepository — listByPipeline', () => {
  it('queries by pipelineId, newest first', async () => {
    mockSend.mockResolvedValue({ Items: [runItem()] });
    const repo = new PipelineRunRepository(mockDocClient);
    await repo.listByPipeline('pipeline-1');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.KeyConditionExpression).toContain('pipelineId');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':pipelineId': 'pipeline-1' });
    expect(cmd.input.ScanIndexForward).toBe(false);
  });

  it('defaults limit to 50', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new PipelineRunRepository(mockDocClient);
    await repo.listByPipeline('pipeline-1');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(50);
  });

  it('accepts custom limit', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new PipelineRunRepository(mockDocClient);
    await repo.listByPipeline('pipeline-1', { limit: 20 });

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(20);
  });

  it('clamps limit at 200', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new PipelineRunRepository(mockDocClient);
    await repo.listByPipeline('pipeline-1', { limit: 500 });

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(200);
  });

  it('clamps limit minimum to 1', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new PipelineRunRepository(mockDocClient);
    await repo.listByPipeline('pipeline-1', { limit: 0 });

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// listByStatus
// ---------------------------------------------------------------------------

describe('PipelineRunRepository — listByStatus', () => {
  it('queries the status-startedAt-index GSI with #status alias', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new PipelineRunRepository(mockDocClient);
    await repo.listByStatus('running');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('status-startedAt-index');
    expect(cmd.input.ExpressionAttributeNames).toMatchObject({ '#status': 'status' });
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':status': 'running' });
    expect(cmd.input.ScanIndexForward).toBe(false);
  });

  it('clamps limit at 200', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new PipelineRunRepository(mockDocClient);
    await repo.listByStatus('failed', { limit: 999 });

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(200);
  });
});

// ---------------------------------------------------------------------------
// listByTriggeredBy
// ---------------------------------------------------------------------------

describe('PipelineRunRepository — listByTriggeredBy', () => {
  it('queries the triggeredBy-startedAt-index GSI', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new PipelineRunRepository(mockDocClient);
    await repo.listByTriggeredBy('user-42');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('triggeredBy-startedAt-index');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':triggeredBy': 'user-42' });
    expect(cmd.input.ScanIndexForward).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// readAttempts (backward-compat helper)
// ---------------------------------------------------------------------------

describe('readAttempts', () => {
  it('returns attempts[] when present and non-empty', () => {
    const summary: StepSummary = {
      attempts: [
        { attemptNumber: 1, startedAt: '2026-05-06T09:00:00.000Z', idempotencyKey: 'k1' },
        { attemptNumber: 2, startedAt: '2026-05-06T09:01:00.000Z', idempotencyKey: 'k2' },
      ],
    };
    const result = readAttempts(summary, 'run-1', 'step-1');
    expect(result).toHaveLength(2);
    expect(result[0]!.attemptNumber).toBe(1);
  });

  it('returns [] when summary is undefined', () => {
    expect(readAttempts(undefined, 'run-1', 'step-1')).toEqual([]);
  });

  it('synthesises a single attempt from top-level fields for legacy rows', () => {
    const summary: StepSummary = {
      status: 'completed',
      startedAt: '2026-05-06T09:00:00.000Z',
      completedAt: '2026-05-06T09:00:05.000Z',
      durationMs: 5000,
      output: { result: 42 },
    };
    const result = readAttempts(summary, 'run-X', 'node-A');
    expect(result).toHaveLength(1);
    expect(result[0]!.attemptNumber).toBe(1);
    expect(result[0]!.startedAt).toBe('2026-05-06T09:00:00.000Z');
    expect(result[0]!.completedAt).toBe('2026-05-06T09:00:05.000Z');
    expect(result[0]!.durationMs).toBe(5000);
  });

  it('synthetic attempt uses completedAt as startedAt when startedAt missing', () => {
    const summary: StepSummary = { completedAt: '2026-05-06T09:01:00.000Z' };
    const result = readAttempts(summary, 'run-1', 'node-1');
    expect(result).toHaveLength(1);
    expect(result[0]!.startedAt).toBe('2026-05-06T09:01:00.000Z');
  });

  it('returns [] for a summary with no recorded state (all fields undefined)', () => {
    const summary: StepSummary = {};
    const result = readAttempts(summary, 'run-1', 'node-1');
    expect(result).toEqual([]);
  });

  it('synthetic idempotencyKey includes runId and nodeId', () => {
    const summary: StepSummary = { startedAt: '2026-05-06T09:00:00.000Z' };
    const result = readAttempts(summary, 'run-abc', 'node-xyz');
    expect(result[0]!.idempotencyKey).toContain('run-abc');
    expect(result[0]!.idempotencyKey).toContain('node-xyz');
  });
});

// ---------------------------------------------------------------------------
// appendRedrivenBy — DLQ redrive lineage on the ORIGINAL run
// ---------------------------------------------------------------------------

describe('appendRedrivenBy', () => {
  it('appends to the list in place, creating it when absent, only if the row exists', async () => {
    mockSend.mockResolvedValue({});
    const repo = new PipelineRunRepository(mockDocClient);
    expect(await repo.appendRedrivenBy('pipeline-1', 'run-old', 'run-new')).toBe(true);

    const cmd = mockSend.mock.calls[0][0] as UpdateCommand;
    expect(cmd).toBeInstanceOf(UpdateCommand);
    expect(cmd.input.TableName).toBe('test-pipeline-runs');
    expect(cmd.input.Key).toEqual({ pipelineId: 'pipeline-1', runId: 'run-old' });
    expect(cmd.input.UpdateExpression).toBe('SET redrivenBy = list_append(if_not_exists(redrivenBy, :empty), :new)');
    expect(cmd.input.ConditionExpression).toBe('attribute_exists(runId)');
    expect(cmd.input.ExpressionAttributeValues).toEqual({ ':empty': [], ':new': ['run-new'] });
  });

  it('returns false instead of creating a phantom row when the original run is not stored', async () => {
    mockSend.mockRejectedValue(Object.assign(new Error('cond'), { name: 'ConditionalCheckFailedException' }));
    const repo = new PipelineRunRepository(mockDocClient);
    expect(await repo.appendRedrivenBy('pipeline-1', 'ghost', 'run-new')).toBe(false);
  });

  it('rethrows anything that is not the condition failing', async () => {
    mockSend.mockRejectedValue(new Error('throttled'));
    const repo = new PipelineRunRepository(mockDocClient);
    await expect(repo.appendRedrivenBy('pipeline-1', 'run-old', 'run-new')).rejects.toThrow('throttled');
  });
});
