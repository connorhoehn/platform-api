// src/services/__tests__/activity-consumer.test.ts
//
// Phase 57b — verifies the activity.recorded durable consumer:
//   - subscribes via the catalog with the right event name
//   - writes a row keyed on userId + timestamp#envelopeId
//   - is idempotent for re-delivery of the same envelope
//   - skips anonymous events (no userId)
//   - re-throws DDB errors so the durable transport redelivers

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
});
