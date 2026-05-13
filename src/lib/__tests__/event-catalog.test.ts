// src/lib/__tests__/event-catalog.test.ts
//
// Tests for event-catalog module: publishSignal (gateway-compatible broadcast),
// publishDurable (message-queue delegation), and lifecycle helpers
// (connectCatalog / disconnectCatalog).

const mockConnect = jest.fn().mockResolvedValue(undefined);
const mockDisconnect = jest.fn().mockResolvedValue(undefined);
const mockSMembers = jest.fn<Promise<string[]>, [string]>();
const mockPublish = jest.fn<Promise<number>, [string, string]>();
const mockOn = jest.fn();

const mockRedisClient = {
  connect: mockConnect,
  disconnect: mockDisconnect,
  sMembers: mockSMembers,
  publish: mockPublish,
  on: mockOn,
  isReady: true,
};

jest.mock('redis', () => ({
  createClient: jest.fn(() => mockRedisClient),
}));

const mockCatalogConnect = jest.fn().mockResolvedValue(undefined);
const mockCatalogDisconnect = jest.fn().mockResolvedValue(undefined);
const mockCatalogDeclare = jest.fn();
const mockCatalogPublish = jest.fn().mockResolvedValue(undefined);
const mockGetSignalTransport = jest.fn();

jest.mock('@connorhoehn/event-catalog', () => ({
  // Pure pass-through for envelope helpers (no state, no I/O).
  ...jest.requireActual('@connorhoehn/event-catalog'),
  EventCatalog: jest.fn().mockImplementation(() => ({
    connect: mockCatalogConnect,
    disconnect: mockCatalogDisconnect,
    declare: mockCatalogDeclare,
    publish: mockCatalogPublish,
    getSignalTransport: mockGetSignalTransport,
  })),
}));

beforeEach(() => {
  jest.clearAllMocks();
  mockRedisClient.isReady = true;
  mockSMembers.mockResolvedValue([]);
  mockPublish.mockResolvedValue(1);
});

// We need to import after mocks are set up
import {
  publishSignal,
  publishDurable,
  connectCatalog,
  disconnectCatalog,
} from '../event-catalog';

// ---------------------------------------------------------------------------
// publishSignal
// ---------------------------------------------------------------------------

describe('publishSignal', () => {
  it('skips when no target nodes', async () => {
    mockSMembers.mockResolvedValue([]);

    await publishSignal('room:abc', 'social:post', { content: 'hello' });

    expect(mockSMembers).toHaveBeenCalledWith('websocket:channel:room:abc:nodes');
    expect(mockPublish).not.toHaveBeenCalled();
  });

  it('builds correct gateway envelope and publishes', async () => {
    mockSMembers.mockResolvedValue(['node-1']);

    const payload = { roomId: 'room-1', postId: 'p-1', content: 'test' };
    await publishSignal('room:abc', 'social:post', payload);

    expect(mockPublish).toHaveBeenCalledTimes(1);

    const [channel, rawEnvelope] = mockPublish.mock.calls[0];
    expect(channel).toBe('websocket:route:room:abc');

    const envelope = JSON.parse(rawEnvelope);
    expect(envelope.type).toBe('channel_message');
    expect(envelope.channel).toBe('room:abc');
    expect(envelope.fromNode).toBe('platform-api');
    expect(envelope.targetNodes).toEqual(['node-1']);
    expect(envelope.seq).toBe(0);
    expect(envelope.excludeClientId).toBeNull();
    expect(envelope.message).toMatchObject({
      type: 'social:post',
      channel: 'room:abc',
      payload,
    });
    expect(envelope.message.timestamp).toBeDefined();
    expect(envelope.timestamp).toBeDefined();
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
  it('connects both catalog and broadcast client', async () => {
    // Reset isReady so getBroadcastClient creates a new connection
    mockRedisClient.isReady = false;

    await connectCatalog();

    expect(mockCatalogConnect).toHaveBeenCalledTimes(1);
    expect(mockConnect).toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// disconnectCatalog
// ---------------------------------------------------------------------------

describe('disconnectCatalog', () => {
  it('disconnects catalog and broadcast client', async () => {
    // Ensure broadcast client is connected
    mockRedisClient.isReady = true;

    await disconnectCatalog();

    expect(mockCatalogDisconnect).toHaveBeenCalledTimes(1);
    expect(mockDisconnect).toHaveBeenCalled();
  });
});
