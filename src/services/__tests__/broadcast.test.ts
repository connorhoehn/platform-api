// social-api/src/services/__tests__/broadcast.test.ts
//
// Tests for BroadcastService.emit(): Redis-unavailable no-op, empty targetNodes
// skip, successful publish envelope shape, and non-fatal emit error handling.
// Uses jest.mock('redis') to avoid real Redis connections.

import { describe, it, expect, jest, beforeEach } from '@jest/globals';

// ---------------------------------------------------------------------------
// Mock redis before importing the service
// ---------------------------------------------------------------------------

const mockPublish = jest.fn<() => Promise<number>>().mockResolvedValue(1);
const mockSMembers = jest.fn<() => Promise<string[]>>().mockResolvedValue([]);
const mockConnect = jest.fn<() => Promise<void>>().mockResolvedValue(undefined);
const mockIsReady = { value: true };

const mockRedisClient = {
  get isReady() { return mockIsReady.value; },
  connect: mockConnect,
  publish: mockPublish,
  sMembers: mockSMembers,
  on: jest.fn(),
};

const mockCreateClient = jest.fn().mockReturnValue(mockRedisClient);

jest.mock('redis', () => ({
  createClient: mockCreateClient,
}));

// ---------------------------------------------------------------------------
// Import the module under test AFTER mocks are set up.
// BroadcastService is a module-level singleton; we re-import a fresh copy
// per test by resetting module registry.
// ---------------------------------------------------------------------------

let broadcastService: { emit: (channelId: string, type: string, payload: Record<string, unknown>) => Promise<void> };

beforeEach(async () => {
  jest.resetAllMocks();

  // Restore default happy-path behaviour after reset
  mockConnect.mockResolvedValue(undefined);
  mockPublish.mockResolvedValue(1);
  mockSMembers.mockResolvedValue([]);
  mockIsReady.value = false; // starts disconnected → triggers connect()
  mockCreateClient.mockReturnValue(mockRedisClient);
  mockRedisClient.on.mockImplementation(() => mockRedisClient);

  // Re-import fresh singleton each test
  jest.resetModules();
  const mod = await import('../broadcast');
  broadcastService = mod.broadcastService;
});

// ---------------------------------------------------------------------------
// Redis unavailable — connect failure
// ---------------------------------------------------------------------------

describe('broadcastService.emit() — Redis unavailable', () => {
  it('returns without throwing when connect throws', async () => {
    mockConnect.mockRejectedValue(new Error('Connection refused'));

    await expect(broadcastService.emit('ch-1', 'social:post', { id: '1' })).resolves.toBeUndefined();
  });

  it('does not call publish when Redis is unavailable', async () => {
    mockConnect.mockRejectedValue(new Error('Connection refused'));

    await broadcastService.emit('ch-1', 'social:post', { id: '1' });
    expect(mockPublish).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Empty targetNodes — skip publish
// ---------------------------------------------------------------------------

describe('broadcastService.emit() — no subscribers', () => {
  it('does not publish when no gateway nodes are subscribed', async () => {
    mockIsReady.value = false;
    mockConnect.mockResolvedValue(undefined);
    mockSMembers.mockResolvedValue([]);

    // After connect, isReady becomes true
    mockConnect.mockImplementation(async () => { mockIsReady.value = true; });

    await broadcastService.emit('ch-empty', 'social:like', { itemId: 'x' });
    expect(mockPublish).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Successful publish — envelope shape
// ---------------------------------------------------------------------------

describe('broadcastService.emit() — successful publish', () => {
  beforeEach(() => {
    mockIsReady.value = false;
    mockConnect.mockImplementation(async () => { mockIsReady.value = true; });
    mockSMembers.mockResolvedValue(['node-1', 'node-2']);
    mockPublish.mockResolvedValue(2);
  });

  it('publishes to the correct Redis channel', async () => {
    await broadcastService.emit('room-42', 'social:post', { body: 'hi' });
    expect(mockPublish).toHaveBeenCalledTimes(1);
    const [channel] = mockPublish.mock.calls[0] as [string, string];
    expect(channel).toBe('websocket:route:room-42');
  });

  it('published envelope has type=channel_message', async () => {
    await broadcastService.emit('room-1', 'social:comment', {});
    const [, raw] = mockPublish.mock.calls[0] as [string, string];
    const envelope = JSON.parse(raw);
    expect(envelope.type).toBe('channel_message');
  });

  it('published envelope carries the correct event type in message', async () => {
    await broadcastService.emit('room-1', 'social:like', { count: 5 });
    const [, raw] = mockPublish.mock.calls[0] as [string, string];
    const envelope = JSON.parse(raw);
    expect(envelope.message.type).toBe('social:like');
  });

  it('published envelope includes targetNodes', async () => {
    await broadcastService.emit('room-1', 'social:post', {});
    const [, raw] = mockPublish.mock.calls[0] as [string, string];
    const envelope = JSON.parse(raw);
    expect(envelope.targetNodes).toEqual(['node-1', 'node-2']);
  });

  it('published envelope payload is passed through', async () => {
    await broadcastService.emit('room-1', 'social:post', { text: 'hello' });
    const [, raw] = mockPublish.mock.calls[0] as [string, string];
    const envelope = JSON.parse(raw);
    expect(envelope.message.payload).toEqual({ text: 'hello' });
  });

  it('looks up nodes using the correct Redis SET key', async () => {
    await broadcastService.emit('my-channel', 'social:post', {});
    expect(mockSMembers).toHaveBeenCalledWith('websocket:channel:my-channel:nodes');
  });
});

// ---------------------------------------------------------------------------
// Redis error event handler (lines 44-47)
// ---------------------------------------------------------------------------

describe('broadcastService.emit() — Redis error event handler', () => {
  it('marks service unavailable and does not publish after an error event fires', async () => {
    // Capture the 'error' handler registered via c.on('error', fn)
    let errorHandler: ((err: Error) => void) | null = null;
    mockRedisClient.on.mockImplementation((event: string, fn: (err: Error) => void) => {
      if (event === 'error') errorHandler = fn;
      return mockRedisClient;
    });

    // Connect succeeds initially
    mockIsReady.value = false;
    mockConnect.mockImplementation(async () => { mockIsReady.value = true; });
    mockSMembers.mockResolvedValue(['node-1']);
    mockPublish.mockResolvedValue(1);

    // First emit connects and publishes
    await broadcastService.emit('ch-1', 'ev:type', {});
    expect(mockPublish).toHaveBeenCalledTimes(1);

    // Simulate the Redis client firing an error event
    expect(errorHandler).not.toBeNull();
    errorHandler!(new Error('connection reset by peer'));

    // After the error event the service marks itself unavailable;
    // connect() won't be retried (unavailable guard), so publish won't be called
    mockPublish.mockClear();
    mockSMembers.mockClear();
    await broadcastService.emit('ch-1', 'ev:type', {});
    expect(mockPublish).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Non-fatal emit error
// ---------------------------------------------------------------------------

describe('broadcastService.emit() — non-fatal errors', () => {
  it('swallows publish errors without throwing', async () => {
    mockIsReady.value = false;
    mockConnect.mockImplementation(async () => { mockIsReady.value = true; });
    mockSMembers.mockResolvedValue(['node-1']);
    mockPublish.mockRejectedValue(new Error('Redis write failed'));

    await expect(broadcastService.emit('room-1', 'social:like', {})).resolves.toBeUndefined();
  });

  it('swallows sMembers errors without throwing', async () => {
    mockIsReady.value = false;
    mockConnect.mockImplementation(async () => { mockIsReady.value = true; });
    mockSMembers.mockRejectedValue(new Error('SET lookup failed'));

    await expect(broadcastService.emit('room-1', 'social:like', {})).resolves.toBeUndefined();
  });
});
