// src/services/__tests__/activity-consumer.test.ts
//
// Phase 57b — verifies the activity.recorded durable consumer:
//   - subscribes via the catalog with the right event name
//   - writes a row keyed on userId + timestamp#envelopeId
//   - is idempotent for re-delivery of the same envelope
//   - skips anonymous events (no userId)
//   - re-throws DDB errors so the durable transport redelivers
//
// P1 #6 — error-replay coverage hardening:
//   - DDB rejection propagates the original Error instance (not a copy)
//   - Redelivered envelopes produce byte-identical Item shapes (full equality,
//     not just sort-key equality) so consumers never write divergent rows
//   - stop() is graceful under unsubscribe failure and logs a warning so
//     operators can detect transport teardown issues

const mockSend = jest.fn<Promise<unknown>, [unknown]>();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: mockSend },
}));

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (n: string) => n,
}));

jest.mock('../../lib/event-catalog', () => ({
  catalog: { subscribe: jest.fn() },
}));

const mockLogWarn = jest.fn();
const mockLogError = jest.fn();
jest.mock('../../lib/logger', () => {
  const logger = {
    info: jest.fn(),
    warn: (...args: unknown[]) => mockLogWarn(...args),
    error: (...args: unknown[]) => mockLogError(...args),
    debug: jest.fn(),
  };
  return {
    __esModule: true,
    default: logger,
    // Wave A: the consumer's import chain now reaches
    // pipeline-event-dispatcher → definitions-cache, which builds a
    // child logger at module load.
    withContext: () => logger,
  };
});

import { startActivityConsumer } from '../activity-consumer';
import type { EventEnvelope } from '@connorhoehn/event-catalog';

interface ActivityPayload {
  eventType: string;
  detail?: Record<string, unknown>;
  timestamp: string;
  userId?: string | null;
  displayName?: string;
}

function makeEnvelope(payload: ActivityPayload, id = 'evt-01'): EventEnvelope<ActivityPayload> {
  return {
    eventName: 'activity.recorded',
    payload,
    metadata: { id, producer: 'websocket-gateway', timestamp: Date.now() },
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  mockSend.mockResolvedValue({});
  mockLogWarn.mockReset();
  mockLogError.mockReset();
});

describe('startActivityConsumer (Phase 57b)', () => {
  it('subscribes via the supplied catalog to activity.recorded', () => {
    const subscribe = jest.fn().mockReturnValue({ unsubscribe: async () => {} });
    startActivityConsumer({ _catalog: { subscribe } });
    expect(subscribe).toHaveBeenCalledTimes(1);
    expect(subscribe.mock.calls[0][0]).toBe('activity.recorded');
  });

  it('writes a DDB row keyed on userId + timestamp#envelopeId', async () => {
    let handler!: (env: EventEnvelope<ActivityPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startActivityConsumer({ _catalog: { subscribe } });

    await handler(makeEnvelope({
      eventType: 'doc.created',
      detail: { docId: 'd-1' },
      timestamp: '2026-05-13T00:00:00.000Z',
      userId: 'u-7',
      displayName: 'Alice',
    }, 'evt-XYZ'));

    expect(mockSend).toHaveBeenCalledTimes(1);
    const cmd = mockSend.mock.calls[0][0] as { input: { TableName: string; Item: Record<string, unknown> } };
    expect(cmd.input.TableName).toBe('user-activity');
    expect(cmd.input.Item.userId).toBe('u-7');
    expect(cmd.input.Item.timestamp).toBe('2026-05-13T00:00:00.000Z#evt-XYZ');
    expect(cmd.input.Item.eventType).toBe('doc.created');
    expect(cmd.input.Item.displayName).toBe('Alice');
    expect(cmd.input.Item.detail).toBe('{"docId":"d-1"}');
  });

  // Wave A: activity events double as pipeline triggers.
  it('dispatches to the pipeline dispatcher AFTER a successful persist', async () => {
    let handler!: (env: EventEnvelope<ActivityPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    const dispatch = jest.fn().mockResolvedValue({ fired: [], failed: [] });
    startActivityConsumer({ _catalog: { subscribe }, _dispatch: dispatch });

    await handler(makeEnvelope({
      eventType: 'doc.finalize',
      detail: { documentId: 'doc-9', documentTitle: 'Q3 Retro' },
      timestamp: '2026-08-25T00:00:00.000Z',
      userId: 'u-7',
      displayName: 'Alice',
    }));

    expect(dispatch).toHaveBeenCalledTimes(1);
    expect(dispatch).toHaveBeenCalledWith('doc.finalize', expect.objectContaining({
      documentId: 'doc-9',
      documentTitle: 'Q3 Retro',
      userId: 'u-7',
      displayName: 'Alice',
    }));
  });

  it('does NOT dispatch when the DDB persist fails (redelivery would double-fire)', async () => {
    let handler!: (env: EventEnvelope<ActivityPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    const dispatch = jest.fn().mockResolvedValue({ fired: [], failed: [] });
    startActivityConsumer({ _catalog: { subscribe }, _dispatch: dispatch });
    mockSend.mockRejectedValueOnce(new Error('ddb down'));

    await expect(handler(makeEnvelope({
      eventType: 'doc.finalize',
      detail: {},
      timestamp: '2026-08-25T00:00:00.000Z',
      userId: 'u-7',
    }))).rejects.toThrow('ddb down');
    expect(dispatch).not.toHaveBeenCalled();
  });

  it('anonymous events still dispatch pipelines (no persist)', async () => {
    let handler!: (env: EventEnvelope<ActivityPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    const dispatch = jest.fn().mockResolvedValue({ fired: [], failed: [] });
    startActivityConsumer({ _catalog: { subscribe }, _dispatch: dispatch });

    await handler(makeEnvelope({
      eventType: 'doc.finalize',
      detail: { documentId: 'doc-9' },
      timestamp: '2026-08-25T00:00:00.000Z',
      userId: null,
    }));

    expect(mockSend).not.toHaveBeenCalled();
    expect(dispatch).toHaveBeenCalledTimes(1);
  });

  it('a dispatcher throw never poisons the activity drain', async () => {
    let handler!: (env: EventEnvelope<ActivityPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    const dispatch = jest.fn().mockRejectedValue(new Error('dispatcher bug'));
    startActivityConsumer({ _catalog: { subscribe }, _dispatch: dispatch });

    await expect(handler(makeEnvelope({
      eventType: 'doc.finalize',
      detail: {},
      timestamp: '2026-08-25T00:00:00.000Z',
      userId: 'u-7',
    }))).resolves.toBeUndefined();
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it('is idempotent for re-delivery: same envelope id → same sort key', async () => {
    let handler!: (env: EventEnvelope<ActivityPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startActivityConsumer({ _catalog: { subscribe } });
    const env = makeEnvelope({
      eventType: 'ping',
      timestamp: '2026-05-13T00:00:00.000Z',
      userId: 'u-1',
    }, 'same-id');
    await handler(env);
    await handler(env); // re-delivery
    const first = mockSend.mock.calls[0][0] as { input: { Item: { timestamp: string } } };
    const second = mockSend.mock.calls[1][0] as { input: { Item: { timestamp: string } } };
    expect(first.input.Item.timestamp).toBe(second.input.Item.timestamp);
  });

  it('skips events with no userId (anonymous — already in Redis history)', async () => {
    let handler!: (env: EventEnvelope<ActivityPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startActivityConsumer({ _catalog: { subscribe } });
    await handler(makeEnvelope({
      eventType: 'ping',
      timestamp: '2026-05-13T00:00:00.000Z',
      userId: null,
    }));
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('re-throws DDB errors so the durable transport redelivers', async () => {
    let handler!: (env: EventEnvelope<ActivityPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startActivityConsumer({ _catalog: { subscribe } });
    mockSend.mockRejectedValueOnce(new Error('ThrottlingException'));
    await expect(handler(makeEnvelope({
      eventType: 'ping',
      timestamp: '2026-05-13T00:00:00.000Z',
      userId: 'u-1',
    }))).rejects.toThrow('ThrottlingException');
  });

  it('stop() unsubscribes', async () => {
    const unsubscribe = jest.fn().mockResolvedValue(undefined);
    const subscribe = jest.fn().mockReturnValue({ unsubscribe });
    const handle = startActivityConsumer({ _catalog: { subscribe } });
    await handle.stop();
    expect(unsubscribe).toHaveBeenCalledTimes(1);
  });

  it('stop() tolerates unsubscribe failures', async () => {
    const unsubscribe = jest.fn().mockRejectedValue(new Error('boom'));
    const subscribe = jest.fn().mockReturnValue({ unsubscribe });
    const handle = startActivityConsumer({ _catalog: { subscribe } });
    await expect(handle.stop()).resolves.toBeUndefined();
  });

  // ---------------------------------------------------------------------
  // P1 #6 — error-replay hardening
  // ---------------------------------------------------------------------

  it('propagates the original Error instance (not just message) so the durable transport can inspect it', async () => {
    let handler!: (env: EventEnvelope<ActivityPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startActivityConsumer({ _catalog: { subscribe } });

    const original = new Error('ProvisionedThroughputExceededException');
    (original as Error & { name: string }).name = 'ProvisionedThroughputExceededException';
    mockSend.mockRejectedValueOnce(original);

    let caught: unknown;
    try {
      await handler(makeEnvelope({
        eventType: 'doc.created',
        timestamp: '2026-05-13T00:00:00.000Z',
        userId: 'u-1',
      }));
    } catch (err) {
      caught = err;
    }
    // Identity equality — the consumer must re-throw the SAME Error so
    // the durable transport's redelivery classifier sees the real name.
    expect(caught).toBe(original);
    expect(mockLogError).toHaveBeenCalledTimes(1);
  });

  it('writes byte-identical Item shapes on redelivery (full equality, not just sort key)', async () => {
    let handler!: (env: EventEnvelope<ActivityPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startActivityConsumer({ _catalog: { subscribe } });

    const env = makeEnvelope({
      eventType: 'doc.updated',
      detail: { docId: 'd-7', diff: { title: 'new' } },
      timestamp: '2026-05-13T00:00:00.000Z',
      userId: 'u-9',
      displayName: 'Bob',
    }, 'redeliver-1');

    await handler(env);
    await handler(env); // redelivery

    expect(mockSend).toHaveBeenCalledTimes(2);
    const first = mockSend.mock.calls[0][0] as { input: { Item: Record<string, unknown> } };
    const second = mockSend.mock.calls[1][0] as { input: { Item: Record<string, unknown> } };
    // Idempotency contract: every Item field must match exactly so DDB
    // PutItem on the same key produces a no-op overwrite.
    expect(second.input.Item).toEqual(first.input.Item);
  });

  it('stop() logs a warning when unsubscribe throws (operators need visibility on teardown failures)', async () => {
    const unsubscribe = jest.fn().mockRejectedValue(new Error('transport offline'));
    const subscribe = jest.fn().mockReturnValue({ unsubscribe });
    const handle = startActivityConsumer({ _catalog: { subscribe } });

    await expect(handle.stop()).resolves.toBeUndefined();
    expect(mockLogWarn).toHaveBeenCalledTimes(1);
    const [logCtx] = mockLogWarn.mock.calls[0] as [{ err?: string }];
    expect(logCtx.err).toBe('transport offline');
  });

  it('does not write to DDB or log errors when userId is undefined (distinct from null)', async () => {
    let handler!: (env: EventEnvelope<ActivityPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startActivityConsumer({ _catalog: { subscribe } });

    await handler(makeEnvelope({
      eventType: 'ping',
      timestamp: '2026-05-13T00:00:00.000Z',
      // userId omitted entirely
    }));

    expect(mockSend).not.toHaveBeenCalled();
    expect(mockLogError).not.toHaveBeenCalled();
  });

  it('one failed envelope does not stop the consumer from processing the next', async () => {
    // Failure mode contract: re-throw is fine because the durable transport
    // owns redelivery; the in-process handler reference must remain usable
    // for subsequent deliveries.
    let handler!: (env: EventEnvelope<ActivityPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startActivityConsumer({ _catalog: { subscribe } });

    mockSend.mockRejectedValueOnce(new Error('throttle'));
    mockSend.mockResolvedValueOnce({});

    await expect(
      handler(makeEnvelope({ eventType: 'a', timestamp: 't1', userId: 'u-1' }, 'e1')),
    ).rejects.toThrow('throttle');
    await expect(
      handler(makeEnvelope({ eventType: 'b', timestamp: 't2', userId: 'u-1' }, 'e2')),
    ).resolves.toBeUndefined();

    expect(mockSend).toHaveBeenCalledTimes(2);
  });
});
