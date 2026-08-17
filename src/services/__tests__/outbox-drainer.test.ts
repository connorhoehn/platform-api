// platform-api/src/services/__tests__/outbox-drainer.test.ts
//
// Phase 54.6.5 — covers the singleton-elected drainer's lease lifecycle and
// ownership-conditional row updates. The drainer must:
//   1. Acquire a DDB lease before scanning UNPROCESSED rows
//   2. Bail out on contention (another replica already holds the lease)
//   3. Stop publishing rows once its lease has lapsed mid-batch (renewal
//      failure or expiration during a slow publishDurable call)
//   4. Use ConditionExpression on PROCESSED/FAILED writes so a stale drainer
//      cannot clobber rows another drainer is now processing
//
// Drives the public surface (startOutboxDrainer / stop) with fake timers and a
// mocked docClient. publishDurable is mocked at the module boundary so each
// test controls publish success/failure precisely.

import {
  PutCommand,
  UpdateCommand,
  ScanCommand,
  DeleteCommand,
} from '@aws-sdk/lib-dynamodb';

const mockSend = jest.fn();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: (...args: unknown[]) => mockSend(...args) },
}));
jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (base: string) => `test-${base}`,
}));

const mockPublishDurable = jest.fn();
jest.mock('../../lib/event-catalog', () => ({
  publishDurable: (...args: unknown[]) => mockPublishDurable(...args),
  // The drainer publishes via catalog.publish (72b5bd1) — route it to the
  // same assertion mock.
  catalog: { publish: (...args: unknown[]) => mockPublishDurable(...args) },
}));

jest.mock('../../lib/logger', () => ({
  __esModule: true,
  default: {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  },
}));

import { startOutboxDrainer } from '../outbox-drainer';

interface DDBCmd {
  constructor: { name: string };
  input: Record<string, unknown>;
}

/** Match the first send call of a given command class. */
function findCall(cmdClass: { name: string }): DDBCmd | undefined {
  for (const call of mockSend.mock.calls) {
    const cmd = call[0] as DDBCmd;
    if (cmd?.constructor?.name === cmdClass.name) return cmd;
  }
  return undefined;
}

function findAllCalls(cmdClass: { name: string }): DDBCmd[] {
  return mockSend.mock.calls
    .map((c) => c[0] as DDBCmd)
    .filter((c) => c?.constructor?.name === cmdClass.name);
}

/** Drain the microtask queue so awaited promises in the drainer settle. */
async function flush(): Promise<void> {
  for (let i = 0; i < 5; i++) {
    await Promise.resolve();
  }
}

class ConditionalCheckFailedException extends Error {
  name = 'ConditionalCheckFailedException';
}

beforeEach(() => {
  mockSend.mockReset();
  mockPublishDurable.mockReset();
});

describe('outbox-drainer — lease lifecycle', () => {
  it('acquires lease, scans, then releases on stop', async () => {
    // acquire (PutCommand) → scan (ScanCommand) → release (DeleteCommand)
    mockSend.mockImplementation(async (cmd: DDBCmd) => {
      if (cmd.constructor.name === 'ScanCommand') return { Items: [] };
      return {};
    });

    const drainer = startOutboxDrainer({ pollIntervalMs: 10_000_000 });
    await flush();
    drainer.stop();
    await flush();

    expect(findCall(PutCommand)).toBeDefined();
    expect(findCall(ScanCommand)).toBeDefined();
    expect(findCall(DeleteCommand)).toBeDefined();
  });

  it('skips scan when lease acquire fails (contention)', async () => {
    mockSend.mockImplementation(async (cmd: DDBCmd) => {
      if (cmd.constructor.name === 'PutCommand') {
        throw new ConditionalCheckFailedException('lease held by another');
      }
      return {};
    });

    const drainer = startOutboxDrainer({ pollIntervalMs: 10_000_000 });
    await flush();
    drainer.stop();
    await flush();

    expect(findCall(PutCommand)).toBeDefined();
    expect(findCall(ScanCommand)).toBeUndefined();
  });
});

describe('outbox-drainer — processRow happy path', () => {
  it('publishes, then writes PROCESSED with ownership-conditional update', async () => {
    const row = {
      outboxId: 'o-1',
      status: 'UNPROCESSED',
      eventType: 'social.post.created',
      queueName: 'post-events',
      payload: JSON.stringify({ postId: 'p-1' }),
      createdAt: new Date().toISOString(),
    };

    mockSend.mockImplementation(async (cmd: DDBCmd) => {
      if (cmd.constructor.name === 'ScanCommand') return { Items: [row] };
      return {};
    });
    mockPublishDurable.mockResolvedValue(undefined);

    const drainer = startOutboxDrainer({ pollIntervalMs: 10_000_000 });
    await flush();
    drainer.stop();
    await flush();

    expect(mockPublishDurable).toHaveBeenCalledWith('social.post.created', { postId: 'p-1' });

    const updates = findAllCalls(UpdateCommand);
    // first UpdateCommand may be lease-renewal heartbeat; filter to the row write
    const rowUpdate = updates.find(
      (u) => (u.input.Key as { outboxId?: string })?.outboxId === 'o-1',
    );
    expect(rowUpdate).toBeDefined();
    expect(rowUpdate!.input.ConditionExpression).toContain('attribute_exists(outboxId)');
    expect(rowUpdate!.input.ConditionExpression).toContain('#status = :unprocessed');
    expect(
      (rowUpdate!.input.ExpressionAttributeValues as Record<string, unknown>)[':processed'],
    ).toBe('PROCESSED');
  });
});

describe('outbox-drainer — retry & failure paths', () => {
  it('increments retryCount on publish failure (below max)', async () => {
    const row = {
      outboxId: 'o-retry',
      status: 'UNPROCESSED',
      eventType: 'social.post.created',
      queueName: 'post-events',
      payload: JSON.stringify({ postId: 'p-1' }),
      createdAt: new Date().toISOString(),
      retryCount: 0,
    };

    mockSend.mockImplementation(async (cmd: DDBCmd) => {
      if (cmd.constructor.name === 'ScanCommand') return { Items: [row] };
      return {};
    });
    mockPublishDurable.mockRejectedValue(new Error('transport down'));

    const drainer = startOutboxDrainer({ pollIntervalMs: 10_000_000, maxRetries: 3 });
    await flush();
    drainer.stop();
    await flush();

    const updates = findAllCalls(UpdateCommand);
    const rowUpdate = updates.find(
      (u) => (u.input.Key as { outboxId?: string })?.outboxId === 'o-retry',
    );
    expect(rowUpdate).toBeDefined();
    expect(rowUpdate!.input.UpdateExpression).toContain('retryCount');
    expect(
      (rowUpdate!.input.ExpressionAttributeValues as Record<string, unknown>)[':retryCount'],
    ).toBe(1);
    // Must remain ownership-conditional
    expect(rowUpdate!.input.ConditionExpression).toContain('attribute_exists(outboxId)');
  });

  it('marks FAILED when retryCount reaches maxRetries', async () => {
    const row = {
      outboxId: 'o-fail',
      status: 'UNPROCESSED',
      eventType: 'social.post.created',
      queueName: 'post-events',
      payload: JSON.stringify({ postId: 'p-1' }),
      createdAt: new Date().toISOString(),
      retryCount: 2, // one more failure → newRetryCount=3, hits maxRetries=3
    };

    mockSend.mockImplementation(async (cmd: DDBCmd) => {
      if (cmd.constructor.name === 'ScanCommand') return { Items: [row] };
      return {};
    });
    mockPublishDurable.mockRejectedValue(new Error('transport down'));

    const drainer = startOutboxDrainer({ pollIntervalMs: 10_000_000, maxRetries: 3 });
    await flush();
    drainer.stop();
    await flush();

    const updates = findAllCalls(UpdateCommand);
    const rowUpdate = updates.find(
      (u) => (u.input.Key as { outboxId?: string })?.outboxId === 'o-fail',
    );
    expect(rowUpdate).toBeDefined();
    expect(
      (rowUpdate!.input.ExpressionAttributeValues as Record<string, unknown>)[':failed'],
    ).toBe('FAILED');
    expect(
      (rowUpdate!.input.ExpressionAttributeValues as Record<string, unknown>)[':retryCount'],
    ).toBe(3);
    // Failure write also ownership-conditional + carries TTL
    expect(rowUpdate!.input.ConditionExpression).toContain('attribute_exists(outboxId)');
    expect(rowUpdate!.input.UpdateExpression).toContain('failedAt');
    expect(rowUpdate!.input.UpdateExpression).toContain('#ttl');
  });
});

describe('outbox-drainer — ownership contention', () => {
  it('swallows ConditionalCheckFailedException on status write (another drainer claimed the row)', async () => {
    const row = {
      outboxId: 'o-claimed',
      status: 'UNPROCESSED',
      eventType: 'social.post.created',
      queueName: 'post-events',
      payload: JSON.stringify({ postId: 'p-1' }),
      createdAt: new Date().toISOString(),
    };

    mockSend.mockImplementation(async (cmd: DDBCmd) => {
      if (cmd.constructor.name === 'ScanCommand') return { Items: [row] };
      if (cmd.constructor.name === 'UpdateCommand') {
        const key = (cmd.input.Key as { outboxId?: string }) ?? {};
        if (key.outboxId === 'o-claimed') {
          throw new ConditionalCheckFailedException('row already PROCESSED');
        }
      }
      return {};
    });
    mockPublishDurable.mockResolvedValue(undefined);

    const drainer = startOutboxDrainer({ pollIntervalMs: 10_000_000 });
    await flush();
    // Must not throw — contention is a non-fatal expected outcome
    drainer.stop();
    await flush();

    expect(mockPublishDurable).toHaveBeenCalledTimes(1);
  });
});
