// social-api/src/lib/__tests__/redis-client.test.ts
//
// Tests for getRedisClient: singleton connection, throttle behaviour,
// error handling, and env-var-driven URL construction.
//
// The module holds three module-level singletons (client, connecting,
// lastAttempt). We use jest.resetModules() + re-require in each test for
// full isolation.

const mockConnect = jest.fn().mockResolvedValue(undefined);
const mockOn = jest.fn();
const mockCreateClient = jest.fn();

jest.mock('redis', () => ({
  createClient: mockCreateClient,
}));

function makeMockClient(isReady = false) {
  return {
    isReady,
    on: mockOn,
    connect: mockConnect,
  };
}

beforeEach(() => {
  jest.resetModules();
  jest.clearAllMocks();
  // Default: createClient returns a client whose connect resolves
  // and isReady flips to true after connect is awaited.
  mockConnect.mockResolvedValue(undefined);
  mockCreateClient.mockReturnValue(makeMockClient(false));
});

// ---------------------------------------------------------------------------
// 1. Returns client when connect succeeds
// ---------------------------------------------------------------------------

test('returns client when connect succeeds', async () => {
  const mockClient = makeMockClient(false);
  mockCreateClient.mockReturnValue(mockClient);

  const { getRedisClient } = require('../redis-client');
  const result = await getRedisClient();

  expect(result).toBe(mockClient);
  expect(mockConnect).toHaveBeenCalledTimes(1);
});

// ---------------------------------------------------------------------------
// 2. Returns null when connecting=true (concurrent calls)
// ---------------------------------------------------------------------------

test('returns null when a connection attempt is already in progress', async () => {
  // Make connect never resolve so the first call stays "connecting"
  let resolveConnect!: () => void;
  const neverResolves = new Promise<void>((res) => { resolveConnect = res; });
  mockConnect.mockReturnValue(neverResolves);

  const mockClient = makeMockClient(false);
  mockCreateClient.mockReturnValue(mockClient);

  const { getRedisClient } = require('../redis-client');

  // Fire first call — it sets connecting=true and then awaits connect
  const first = getRedisClient();

  // Second call arrives synchronously before connect resolves
  const second = await getRedisClient();
  expect(second).toBeNull();

  // Clean up by letting the first promise settle
  resolveConnect();
  await first;
});

// ---------------------------------------------------------------------------
// 3. Returns null within RETRY_INTERVAL_MS after a failed connect
// ---------------------------------------------------------------------------

test('returns null within RETRY_INTERVAL_MS after a failed connect', async () => {
  mockConnect.mockRejectedValueOnce(new Error('connect failed'));

  const { getRedisClient } = require('../redis-client');

  // First call fails
  const first = await getRedisClient();
  expect(first).toBeNull();

  // Immediate second call — lastAttempt was just set, retry interval not elapsed
  const second = await getRedisClient();
  expect(second).toBeNull();
  expect(mockConnect).toHaveBeenCalledTimes(1); // no second attempt
});

// ---------------------------------------------------------------------------
// 4. Returns existing client when isReady is true (no re-connect)
// ---------------------------------------------------------------------------

test('returns existing ready client without reconnecting', async () => {
  const mockClient = makeMockClient(false);
  mockCreateClient.mockReturnValue(mockClient);

  const { getRedisClient } = require('../redis-client');

  // First call — connects and stores client
  const first = await getRedisClient();
  expect(first).toBe(mockClient);

  // Simulate client now being ready
  mockClient.isReady = true;

  // Second call — should short-circuit on isReady check
  const second = await getRedisClient();
  expect(second).toBe(mockClient);

  // connect was only called once (on first attempt)
  expect(mockConnect).toHaveBeenCalledTimes(1);
});

// ---------------------------------------------------------------------------
// 5. Returns null when connect throws (catch path)
// ---------------------------------------------------------------------------

test('returns null when connect throws', async () => {
  mockConnect.mockRejectedValue(new Error('ECONNREFUSED'));

  const { getRedisClient } = require('../redis-client');
  const result = await getRedisClient();

  expect(result).toBeNull();
});

// ---------------------------------------------------------------------------
// 6. on('error', …) handler is registered
// ---------------------------------------------------------------------------

test('registers an error handler on the client', async () => {
  const { getRedisClient } = require('../redis-client');
  await getRedisClient();

  expect(mockOn).toHaveBeenCalledWith('error', expect.any(Function));
});

// ---------------------------------------------------------------------------
// 7. Uses REDIS_ENDPOINT env var in the URL
// ---------------------------------------------------------------------------

test('uses REDIS_ENDPOINT env var when building the Redis URL', async () => {
  process.env.REDIS_ENDPOINT = 'my-redis-host';
  delete process.env.REDIS_PORT;

  jest.resetModules();

  const { getRedisClient } = require('../redis-client');
  await getRedisClient();

  const [[callArg]] = mockCreateClient.mock.calls as [{ url: string }][];
  expect(callArg.url).toContain('my-redis-host');

  delete process.env.REDIS_ENDPOINT;
});

// ---------------------------------------------------------------------------
// 8. Uses REDIS_PORT env var in the URL
// ---------------------------------------------------------------------------

test('uses REDIS_PORT env var when building the Redis URL', async () => {
  delete process.env.REDIS_ENDPOINT;
  process.env.REDIS_PORT = '6380';

  jest.resetModules();

  const { getRedisClient } = require('../redis-client');
  await getRedisClient();

  const [[callArg]] = mockCreateClient.mock.calls as [{ url: string }][];
  expect(callArg.url).toContain('6380');

  delete process.env.REDIS_PORT;
});

test('shutdown does not initialize unused Redis connections', async () => {
  const { closeRedisClients } = require('../redis-client');
  await closeRedisClients();
  expect(mockCreateClient).not.toHaveBeenCalled();
});

test('shutdown still closes the cache when the primary disconnect fails', async () => {
  const oldCache = process.env.REDIS_CACHE_URL;
  process.env.REDIS_CACHE_URL = 'redis://owned-cache:6379';
  try {
    const primary = { ...makeMockClient(true), isOpen: true, destroy: jest.fn(() => { throw new Error('offline'); }) };
    const cache = { ...makeMockClient(true), isOpen: true, destroy: jest.fn() };
    mockCreateClient.mockReturnValueOnce(primary).mockReturnValueOnce(cache);
    const { getRedisClient, getRedisCacheClient, closeRedisClients } = require('../redis-client');
    await getRedisClient(); await getRedisCacheClient();
    await expect(closeRedisClients()).rejects.toThrow('Redis shutdown failed');
    expect(primary.destroy).toHaveBeenCalledTimes(1); expect(cache.destroy).toHaveBeenCalledTimes(1);
    await closeRedisClients();
    expect(mockCreateClient).toHaveBeenCalledTimes(2);
  } finally {
    if (oldCache === undefined) delete process.env.REDIS_CACHE_URL; else process.env.REDIS_CACHE_URL = oldCache;
  }
});

// Lane P1: a connect still retrying (no Redis) must not outlive shutdown —
// the unit suite used to leave one dialling `redis` once a second.
test('shutdown cancels a connect that is still retrying', async () => {
  let rejectConnect!: (err: Error) => void;
  const pending = {
    ...makeMockClient(false),
    isOpen: true,
    connect: jest.fn(() => new Promise((_resolve, reject) => { rejectConnect = reject; })),
    destroy: jest.fn(() => { pending.isOpen = false; rejectConnect(new Error('destroyed')); }),
  };
  mockCreateClient.mockReturnValueOnce(pending);
  const { getRedisClient, closeRedisClients } = require('../redis-client');
  const attempt = getRedisClient();
  await closeRedisClients();
  expect(pending.destroy).toHaveBeenCalledTimes(1);
  await expect(attempt).resolves.toBeNull();
});

// Lane P1: node-redis rejects connect() on the first socket error yet keeps
// the client reconnecting in the background. The failed client must be closed.
test('a failed connect closes the client it created', async () => {
  const failed = {
    ...makeMockClient(false),
    isOpen: true,
    connect: jest.fn().mockRejectedValue(new Error('getaddrinfo ENOTFOUND redis')),
    destroy: jest.fn(() => { failed.isOpen = false; }),
  };
  mockCreateClient.mockReturnValueOnce(failed);
  const { getRedisClient } = require('../redis-client');
  await expect(getRedisClient()).resolves.toBeNull();
  expect(failed.destroy).toHaveBeenCalledTimes(1);
});
