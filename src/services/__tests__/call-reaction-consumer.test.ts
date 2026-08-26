// src/services/__tests__/call-reaction-consumer.test.ts
//
// Verifies the `call:reaction.recorded` durable consumer (mirrors
// activity-consumer.test.ts):
//   - subscribes via the catalog with the right event name
//   - writes the right row shape via the repository
//   - re-throws DDB errors so the durable transport redelivers
//   - stop() unsubscribes, tolerating failures

const mockPutReaction = jest.fn<Promise<void>, [unknown]>();
jest.mock('../../repositories', () => ({
  callReactionRepo: { putReaction: mockPutReaction },
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
    // Some transitive imports build child loggers at module load.
    withContext: () => logger,
  };
});

import { startCallReactionConsumer } from '../call-reaction-consumer';
import type { EventEnvelope } from '@connorhoehn/event-catalog';

interface CallReactionPayload {
  reactionId: string;
  channel: string;
  lobbyName: string;
  userId?: string | null;
  displayName?: string;
  emoji: string;
  effect?: string;
  at: string;
}

function makeEnvelope(
  payload: CallReactionPayload,
  id = 'evt-01',
): EventEnvelope<CallReactionPayload> {
  return {
    eventName: 'call:reaction.recorded',
    payload,
    metadata: { id, producer: 'websocket-gateway', timestamp: Date.now() },
  };
}

const BASE_PAYLOAD: CallReactionPayload = {
  reactionId: 'rx-1',
  channel: 'reactions:room:lounge',
  lobbyName: 'room:lounge',
  userId: 'u-connor',
  displayName: 'Connor',
  emoji: '👍',
  effect: 'burst',
  at: '2026-08-25T10:01:30.000Z',
};

beforeEach(() => {
  jest.clearAllMocks();
  mockPutReaction.mockResolvedValue(undefined);
});

describe('startCallReactionConsumer', () => {
  it('subscribes via the supplied catalog to call:reaction.recorded', () => {
    const subscribe = jest.fn().mockReturnValue({ unsubscribe: async () => {} });
    startCallReactionConsumer({ _catalog: { subscribe } });
    expect(subscribe).toHaveBeenCalledTimes(1);
    expect(subscribe.mock.calls[0][0]).toBe('call:reaction.recorded');
  });

  it('writes the reaction row through the repository with the payload shape', async () => {
    let handler!: (env: EventEnvelope<CallReactionPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startCallReactionConsumer({ _catalog: { subscribe } });

    await handler(makeEnvelope(BASE_PAYLOAD));

    expect(mockPutReaction).toHaveBeenCalledTimes(1);
    expect(mockPutReaction).toHaveBeenCalledWith({
      // A durable consumer has no request context, so the tenant rides the
      // event. The producer does not stamp orgId yet, so these land in the
      // reserved org-less namespace — isolated from every real org, NOT
      // pooled into a shared default (that pooling is the leak the tenant
      // convention exists to prevent).
      tenant: '~',
      reactionId: 'rx-1',
      channel: 'reactions:room:lounge',
      lobbyName: 'room:lounge',
      userId: 'u-connor',
      displayName: 'Connor',
      emoji: '👍',
      effect: 'burst',
      at: '2026-08-25T10:01:30.000Z',
    });
  });

  it('normalizes a missing userId to null (anonymous reactions still persist)', async () => {
    let handler!: (env: EventEnvelope<CallReactionPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startCallReactionConsumer({ _catalog: { subscribe } });

    const { userId: _omit, displayName: _omit2, ...anonymous } = BASE_PAYLOAD;
    await handler(makeEnvelope(anonymous as CallReactionPayload));

    expect(mockPutReaction).toHaveBeenCalledTimes(1);
    const row = mockPutReaction.mock.calls[0][0] as { userId: string | null };
    expect(row.userId).toBeNull();
  });

  it('is idempotent for re-delivery: same envelope → identical row both times', async () => {
    let handler!: (env: EventEnvelope<CallReactionPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startCallReactionConsumer({ _catalog: { subscribe } });

    const env = makeEnvelope(BASE_PAYLOAD, 'same-id');
    await handler(env);
    await handler(env); // re-delivery

    expect(mockPutReaction).toHaveBeenCalledTimes(2);
    expect(mockPutReaction.mock.calls[1][0]).toEqual(mockPutReaction.mock.calls[0][0]);
  });

  it('re-throws DDB errors so the durable transport redelivers', async () => {
    let handler!: (env: EventEnvelope<CallReactionPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startCallReactionConsumer({ _catalog: { subscribe } });

    const original = new Error('ThrottlingException');
    mockPutReaction.mockRejectedValueOnce(original);

    let caught: unknown;
    try {
      await handler(makeEnvelope(BASE_PAYLOAD));
    } catch (err) {
      caught = err;
    }
    // Identity equality — the same Error instance must propagate so the
    // durable transport's redelivery classifier sees the real name.
    expect(caught).toBe(original);
    expect(mockLogError).toHaveBeenCalledTimes(1);
  });

  it('one failed envelope does not stop the consumer from processing the next', async () => {
    let handler!: (env: EventEnvelope<CallReactionPayload>) => Promise<void>;
    const subscribe = jest.fn().mockImplementation((_name, h) => {
      handler = h;
      return { unsubscribe: async () => {} };
    });
    startCallReactionConsumer({ _catalog: { subscribe } });

    mockPutReaction.mockRejectedValueOnce(new Error('throttle'));
    mockPutReaction.mockResolvedValueOnce(undefined);

    await expect(handler(makeEnvelope(BASE_PAYLOAD, 'e1'))).rejects.toThrow('throttle');
    await expect(handler(makeEnvelope(BASE_PAYLOAD, 'e2'))).resolves.toBeUndefined();
    expect(mockPutReaction).toHaveBeenCalledTimes(2);
  });

  it('stop() unsubscribes', async () => {
    const unsubscribe = jest.fn().mockResolvedValue(undefined);
    const subscribe = jest.fn().mockReturnValue({ unsubscribe });
    const handle = startCallReactionConsumer({ _catalog: { subscribe } });
    await handle.stop();
    expect(unsubscribe).toHaveBeenCalledTimes(1);
  });

  it('stop() tolerates unsubscribe failures and logs a warning', async () => {
    const unsubscribe = jest.fn().mockRejectedValue(new Error('transport offline'));
    const subscribe = jest.fn().mockReturnValue({ unsubscribe });
    const handle = startCallReactionConsumer({ _catalog: { subscribe } });

    await expect(handle.stop()).resolves.toBeUndefined();
    expect(mockLogWarn).toHaveBeenCalledTimes(1);
    const [logCtx] = mockLogWarn.mock.calls[0] as [{ err?: string }];
    expect(logCtx.err).toBe('transport offline');
  });
});
