// social-api/src/pipeline/__tests__/audit-repository.test.ts
//
// Unit tests for AuditRepository: record() ULID+timestamp+ttlEpoch generation,
// optional field handling, GSI query shapes for listByActor/listByPipeline/
// listByRun, and the sweepExpired() TTL fallback for envs (DDB-local) where
// native TTL doesn't fire.

import {
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
  ScanCommand,
  DeleteCommand,
} from '@aws-sdk/lib-dynamodb';

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

// Prevent module-level singleton from trying to import real AWS credentials.
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
}));

import { AuditRepository, type AuditEvent } from '../audit-repository';

function event(overrides: Partial<AuditEvent> = {}): AuditEvent {
  return {
    action: 'pipeline.trigger',
    actorUserId: 'user-1',
    pipelineId: 'pipeline-42',
    ...overrides,
  };
}

beforeEach(() => {
  mockSend.mockReset();
});

// ---------------------------------------------------------------------------
// record
// ---------------------------------------------------------------------------

describe('AuditRepository — record', () => {
  it('issues a PutCommand to the pipeline-audit table', async () => {
    mockSend.mockResolvedValue({});
    const repo = new AuditRepository(mockDocClient);
    await repo.record(event());

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd).toBeInstanceOf(PutCommand);
    expect(cmd.input.TableName).toBe('test-pipeline-audit');
  });

  it('generates a 26-char uppercase ULID as auditId', async () => {
    mockSend.mockResolvedValue({});
    const repo = new AuditRepository(mockDocClient);
    await repo.record(event());

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    const auditId = cmd.input.Item?.auditId as string;
    expect(typeof auditId).toBe('string');
    expect(auditId).toMatch(/^[0-9A-Z]{26}$/);
  });

  it('sets a non-empty ISO timestamp', async () => {
    mockSend.mockResolvedValue({});
    const repo = new AuditRepository(mockDocClient);
    await repo.record(event());

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    const ts = cmd.input.Item?.timestamp as string;
    expect(typeof ts).toBe('string');
    expect(ts.length).toBeGreaterThan(10);
    expect(() => new Date(ts).toISOString()).not.toThrow();
  });

  it('includes action, actorUserId, pipelineId in the persisted item', async () => {
    mockSend.mockResolvedValue({});
    const repo = new AuditRepository(mockDocClient);
    await repo.record(event({ action: 'pipeline.approve', actorUserId: 'user-99', pipelineId: 'pipe-1' }));

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd.input.Item).toMatchObject({
      action: 'pipeline.approve',
      actorUserId: 'user-99',
      pipelineId: 'pipe-1',
    });
  });

  it('includes optional runId when provided', async () => {
    mockSend.mockResolvedValue({});
    const repo = new AuditRepository(mockDocClient);
    await repo.record(event({ runId: 'run-77' }));

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd.input.Item?.runId).toBe('run-77');
  });

  it('omits runId from the item when not provided', async () => {
    mockSend.mockResolvedValue({});
    const repo = new AuditRepository(mockDocClient);
    await repo.record(event()); // no runId

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(Object.prototype.hasOwnProperty.call(cmd.input.Item, 'runId')).toBe(false);
  });

  it('includes optional decision when provided', async () => {
    mockSend.mockResolvedValue({});
    const repo = new AuditRepository(mockDocClient);
    await repo.record(event({ decision: 'approved' }));

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd.input.Item?.decision).toBe('approved');
  });

  it('omits decision from the item when not provided', async () => {
    mockSend.mockResolvedValue({});
    const repo = new AuditRepository(mockDocClient);
    await repo.record(event());

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(Object.prototype.hasOwnProperty.call(cmd.input.Item, 'decision')).toBe(false);
  });

  it('includes details when provided', async () => {
    mockSend.mockResolvedValue({});
    const repo = new AuditRepository(mockDocClient);
    await repo.record(event({ details: { source: 'webhook' } }));

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd.input.Item?.details).toEqual({ source: 'webhook' });
  });

  // hub#2224 Wave 2 — TTL + new optional field passthrough.

  it('sets ttlEpoch to now + 30 days (in seconds) by default', async () => {
    mockSend.mockResolvedValue({});
    const fixedNowMs = Date.UTC(2026, 4, 1, 12, 0, 0);
    const repo = new AuditRepository(mockDocClient, { now: () => fixedNowMs });
    await repo.record(event());

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    const ttl = cmd.input.Item?.ttlEpoch as number;
    expect(typeof ttl).toBe('number');
    expect(ttl).toBe(Math.floor(fixedNowMs / 1000) + 30 * 24 * 60 * 60);
  });

  it('honors a custom ttlDays in the constructor', async () => {
    mockSend.mockResolvedValue({});
    const fixedNowMs = Date.UTC(2026, 4, 1, 12, 0, 0);
    const repo = new AuditRepository(mockDocClient, {
      now: () => fixedNowMs,
      ttlDays: 7,
    });
    await repo.record(event());
    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd.input.Item?.ttlEpoch).toBe(
      Math.floor(fixedNowMs / 1000) + 7 * 24 * 60 * 60,
    );
  });

  it('persists outcome / before / after / trace when provided', async () => {
    mockSend.mockResolvedValue({});
    const repo = new AuditRepository(mockDocClient);
    await repo.record(
      event({
        outcome: 'success',
        before: { version: 1 },
        after: { version: 2 },
        trace: { stepId: 's1', durationMs: 50 },
      }),
    );

    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd.input.Item).toMatchObject({
      outcome: 'success',
      before: { version: 1 },
      after: { version: 2 },
      trace: { stepId: 's1', durationMs: 50 },
    });
  });

  it('omits outcome / before / after / trace when not provided', async () => {
    mockSend.mockResolvedValue({});
    const repo = new AuditRepository(mockDocClient);
    await repo.record(event());
    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    for (const k of ['outcome', 'before', 'after', 'trace']) {
      expect(Object.prototype.hasOwnProperty.call(cmd.input.Item, k)).toBe(false);
    }
  });
});

// ---------------------------------------------------------------------------
// listByActor
// ---------------------------------------------------------------------------

describe('AuditRepository — listByActor', () => {
  it('queries actor-time-index GSI by actorUserId, newest first', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new AuditRepository(mockDocClient);
    await repo.listByActor('user-42');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('actor-time-index');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':uid': 'user-42' });
    expect(cmd.input.ScanIndexForward).toBe(false);
  });

  it('defaults limit to 100', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new AuditRepository(mockDocClient);
    await repo.listByActor('user-1');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(100);
  });

  it('accepts a custom limit', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new AuditRepository(mockDocClient);
    await repo.listByActor('user-1', 25);

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(25);
  });

  it('returns empty array when Items is undefined', async () => {
    mockSend.mockResolvedValue({ Items: undefined });
    const repo = new AuditRepository(mockDocClient);
    const result = await repo.listByActor('user-1');
    expect(result).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// listByPipeline
// ---------------------------------------------------------------------------

describe('AuditRepository — listByPipeline', () => {
  it('queries pipeline-time-index GSI by pipelineId, newest first', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new AuditRepository(mockDocClient);
    await repo.listByPipeline('pipeline-7');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('pipeline-time-index');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':pid': 'pipeline-7' });
    expect(cmd.input.ScanIndexForward).toBe(false);
  });

  it('defaults limit to 100', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new AuditRepository(mockDocClient);
    await repo.listByPipeline('pipeline-1');

    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(100);
  });
});

// ---------------------------------------------------------------------------
// listByRun (hub#2224 Wave 2)
// ---------------------------------------------------------------------------

describe('AuditRepository — listByRun', () => {
  it('queries runId-time-index GSI by runId, newest first', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new AuditRepository(mockDocClient);
    await repo.listByRun('run-9');
    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.IndexName).toBe('runId-time-index');
    expect(cmd.input.ExpressionAttributeValues).toMatchObject({ ':rid': 'run-9' });
    expect(cmd.input.ScanIndexForward).toBe(false);
  });

  it('defaults limit to 100', async () => {
    mockSend.mockResolvedValue({ Items: [] });
    const repo = new AuditRepository(mockDocClient);
    await repo.listByRun('run-1');
    const cmd = mockSend.mock.calls[0][0] as QueryCommand;
    expect(cmd.input.Limit).toBe(100);
  });
});

// ---------------------------------------------------------------------------
// sweepExpired (hub#2224 Wave 2 — DDB-local TTL fallback)
// ---------------------------------------------------------------------------

describe('AuditRepository — sweepExpired', () => {
  it('scans with FilterExpression ttlEpoch < now and deletes returned items', async () => {
    const fixedNowMs = Date.UTC(2026, 4, 1, 12, 0, 0);
    const expectedNowSec = Math.floor(fixedNowMs / 1000);
    // First call: Scan returns 2 expired auditIds, no LastEvaluatedKey.
    mockSend
      .mockResolvedValueOnce({
        Items: [{ auditId: 'A1' }, { auditId: 'A2' }],
        LastEvaluatedKey: undefined,
      })
      // Two delete calls.
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({});

    const repo = new AuditRepository(mockDocClient, { now: () => fixedNowMs });
    const deleted = await repo.sweepExpired();

    expect(deleted).toBe(2);

    const scanCmd = mockSend.mock.calls[0][0] as ScanCommand;
    expect(scanCmd).toBeInstanceOf(ScanCommand);
    expect(scanCmd.input.FilterExpression).toBe('ttlEpoch < :now');
    expect(scanCmd.input.ExpressionAttributeValues).toMatchObject({
      ':now': expectedNowSec,
    });

    const del1 = mockSend.mock.calls[1][0] as DeleteCommand;
    expect(del1).toBeInstanceOf(DeleteCommand);
    expect(del1.input.Key).toEqual({ auditId: 'A1' });
    const del2 = mockSend.mock.calls[2][0] as DeleteCommand;
    expect(del2.input.Key).toEqual({ auditId: 'A2' });
  });

  it('returns 0 and issues no Delete calls when nothing is expired', async () => {
    mockSend.mockResolvedValueOnce({ Items: [], LastEvaluatedKey: undefined });
    const repo = new AuditRepository(mockDocClient);
    const deleted = await repo.sweepExpired();
    expect(deleted).toBe(0);
    // Only the Scan call, no Delete.
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it('caps deletions at maxItems', async () => {
    // Scan returns 5 items; cap at 3.
    mockSend
      .mockResolvedValueOnce({
        Items: [
          { auditId: 'A1' },
          { auditId: 'A2' },
          { auditId: 'A3' },
          { auditId: 'A4' },
          { auditId: 'A5' },
        ],
        LastEvaluatedKey: undefined,
      })
      .mockResolvedValue({}); // each delete

    const repo = new AuditRepository(mockDocClient);
    const deleted = await repo.sweepExpired(3);
    expect(deleted).toBe(3);
    // 1 Scan + 3 Deletes = 4 calls.
    expect(mockSend).toHaveBeenCalledTimes(4);
  });
});
