// src/lib/__tests__/event-catalog.test.ts
//
// Tests for event-catalog module:
//   - publishSignal: now a thin delegate to catalog.publishSignal()
//     (wave1-ec-3 — schema enforcement happens inside the library)
//   - publishDurable: writes to DDB outbox (covered by outbox-drainer test)
//   - lifecycle helpers (connectCatalog / disconnectCatalog)

const mockCatalogConnect = jest.fn().mockResolvedValue(undefined);
const mockCatalogDisconnect = jest.fn().mockResolvedValue(undefined);
const mockCatalogDeclare = jest.fn();
const mockCatalogDeclareAll = jest.fn();
const mockCatalogPublish = jest.fn().mockResolvedValue(undefined);
const mockCatalogPublishSignal = jest.fn().mockResolvedValue(0);
const mockGetSignalTransport = jest.fn();
const mockCheckAllSchemaFreshness = jest.fn().mockResolvedValue([]);

jest.mock('@connorhoehn/event-catalog', () => ({
  // Pure pass-through for envelope helpers (no state, no I/O).
  ...jest.requireActual('@connorhoehn/event-catalog'),
  EventCatalog: jest.fn().mockImplementation(() => ({
    connect: mockCatalogConnect,
    disconnect: mockCatalogDisconnect,
    declare: mockCatalogDeclare,
    declareAll: mockCatalogDeclareAll,
    publish: mockCatalogPublish,
    publishSignal: mockCatalogPublishSignal,
    getSignalTransport: mockGetSignalTransport,
    checkAllSchemaFreshness: mockCheckAllSchemaFreshness,
  })),
}));

beforeEach(() => {
  jest.clearAllMocks();
  mockCatalogPublishSignal.mockResolvedValue(0);
});

// We need to import after mocks are set up
import {
  publishSignal,
  publishDurable,
  connectCatalog,
  disconnectCatalog,
} from '../event-catalog';

// ---------------------------------------------------------------------------
// publishSignal — wave1-ec-3 routes through catalog.publishSignal()
// ---------------------------------------------------------------------------

describe('publishSignal (wave1-ec-3)', () => {
  it('delegates to catalog.publishSignal with channelId, eventName, payload', async () => {
    await publishSignal('room:abc', 'social:post', { content: 'hello' });

    expect(mockCatalogPublishSignal).toHaveBeenCalledTimes(1);
    expect(mockCatalogPublishSignal).toHaveBeenCalledWith(
      'room:abc',
      'social:post',
      { content: 'hello' },
    );
  });

  it('propagates errors from the catalog (e.g. schema validation failure)', async () => {
    mockCatalogPublishSignal.mockRejectedValueOnce(
      new Error('[event-catalog] publishSignal payload validation failed for "social:post": missing required'),
    );

    await expect(
      publishSignal('room:abc', 'social:post', {} as Record<string, unknown>),
    ).rejects.toThrow(/payload validation failed/);
  });

  it('returns void even when the catalog reports delivery count', async () => {
    mockCatalogPublishSignal.mockResolvedValueOnce(3);

    const result = await publishSignal('room:abc', 'social:post', { content: 'hi' });

    expect(result).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// publishDurable
// ---------------------------------------------------------------------------

// publishDurable was migrated to the outbox-drainer pattern: it writes
// to the DDB outbox table now, NOT directly to catalog.publish. The
// outbox-drainer is responsible for fanning out to the durable queue
// later. Exercising the full path requires AWS credentials + DDB, which
// is the integration-test layer. Marked skipped at the unit level; covered
// in services/__tests__/outbox-drainer.test.ts.
describe.skip('publishDurable (covered by outbox-drainer integration test)', () => {
  it('writes to the DDB outbox table — not catalog.publish directly', () => {
    expect(true).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// connectCatalog
// ---------------------------------------------------------------------------

describe('connectCatalog', () => {
  it('connects the catalog (broadcast Redis client is now owned by the library)', async () => {
    await connectCatalog();

    expect(mockCatalogConnect).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// disconnectCatalog
// ---------------------------------------------------------------------------

describe('disconnectCatalog', () => {
  it('disconnects the catalog (broadcast Redis client is now owned by the library)', async () => {
    await disconnectCatalog();

    expect(mockCatalogDisconnect).toHaveBeenCalledTimes(1);
  });
});
