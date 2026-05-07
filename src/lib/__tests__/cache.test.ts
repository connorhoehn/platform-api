// social-api/src/lib/__tests__/cache.test.ts
//
// Tests for cache utilities: key helpers (pure), cache hit/miss (Redis available),
// Redis unavailable (null fallback), error swallowing, TTL values, and all
// three entity types (profile, room, group).

const mockRedisGet = jest.fn<Promise<string | null>, [string]>();
const mockRedisSet = jest.fn<Promise<unknown>, [string, string, { EX: number }]>();
const mockRedisDel = jest.fn<Promise<unknown>, [string]>();

jest.mock('../redis-client', () => ({
  getRedisClient: jest.fn().mockResolvedValue({
    get: mockRedisGet,
    set: mockRedisSet,
    del: mockRedisDel,
  }),
}));

jest.mock('../logger', () => ({
  withContext: jest.fn().mockReturnValue({
    warn: jest.fn(),
    info: jest.fn(),
  }),
}));

import { getRedisClient } from '../redis-client';
import {
  profileCacheKey,
  getCachedProfile,
  setCachedProfile,
  invalidateProfileCache,
  roomCacheKey,
  getCachedRoom,
  setCachedRoom,
  invalidateRoomCache,
  groupCacheKey,
  getCachedGroup,
  setCachedGroup,
  invalidateGroupCache,
} from '../cache';

const mockGetRedisClient = getRedisClient as jest.MockedFunction<typeof getRedisClient>;

beforeEach(() => {
  jest.clearAllMocks();
  // Default: Redis is available
  mockGetRedisClient.mockResolvedValue({
    get: mockRedisGet,
    set: mockRedisSet,
    del: mockRedisDel,
  } as unknown as ReturnType<typeof getRedisClient> extends Promise<infer T> ? T : never);
  mockRedisGet.mockResolvedValue(null);
  mockRedisSet.mockResolvedValue('OK');
  mockRedisDel.mockResolvedValue(1);
});

// ---------------------------------------------------------------------------
// Cache key helpers (pure functions)
// ---------------------------------------------------------------------------

describe('cache key helpers', () => {
  it('profileCacheKey returns social:profile:<userId>', () => {
    expect(profileCacheKey('user-42')).toBe('social:profile:user-42');
  });

  it('roomCacheKey returns social:room:<roomId>', () => {
    expect(roomCacheKey('room-7')).toBe('social:room:room-7');
  });

  it('groupCacheKey returns social:group:<groupId>', () => {
    expect(groupCacheKey('group-3')).toBe('social:group:group-3');
  });
});

// ---------------------------------------------------------------------------
// Profile cache
// ---------------------------------------------------------------------------

describe('getCachedProfile', () => {
  it('returns parsed value on cache hit', async () => {
    const profile = { userId: 'u-1', displayName: 'Alice' };
    mockRedisGet.mockResolvedValue(JSON.stringify(profile));
    const result = await getCachedProfile<typeof profile>('u-1');
    expect(result).toEqual(profile);
    expect(mockRedisGet).toHaveBeenCalledWith('social:profile:u-1');
  });

  it('returns null on cache miss', async () => {
    mockRedisGet.mockResolvedValue(null);
    const result = await getCachedProfile('u-1');
    expect(result).toBeNull();
  });

  it('returns null when Redis is unavailable', async () => {
    mockGetRedisClient.mockResolvedValue(null);
    const result = await getCachedProfile('u-1');
    expect(result).toBeNull();
    expect(mockRedisGet).not.toHaveBeenCalled();
  });

  it('returns null and swallows error when Redis throws', async () => {
    mockRedisGet.mockRejectedValue(new Error('ECONNREFUSED'));
    await expect(getCachedProfile('u-1')).resolves.toBeNull();
  });
});

describe('setCachedProfile', () => {
  it('serializes value and sets with 300s TTL', async () => {
    const profile = { userId: 'u-1', displayName: 'Alice' };
    await setCachedProfile('u-1', profile);
    expect(mockRedisSet).toHaveBeenCalledWith(
      'social:profile:u-1',
      JSON.stringify(profile),
      { EX: 300 },
    );
  });

  it('does not call redis.set when Redis is unavailable', async () => {
    mockGetRedisClient.mockResolvedValue(null);
    await setCachedProfile('u-1', { userId: 'u-1' });
    expect(mockRedisSet).not.toHaveBeenCalled();
  });

  it('swallows errors from redis.set', async () => {
    mockRedisSet.mockRejectedValue(new Error('write error'));
    await expect(setCachedProfile('u-1', {})).resolves.toBeUndefined();
  });
});

describe('invalidateProfileCache', () => {
  it('deletes the profile cache key', async () => {
    await invalidateProfileCache('u-1');
    expect(mockRedisDel).toHaveBeenCalledWith('social:profile:u-1');
  });
});

// ---------------------------------------------------------------------------
// Room cache
// ---------------------------------------------------------------------------

describe('getCachedRoom', () => {
  it('returns parsed value on cache hit', async () => {
    const room = { roomId: 'r-1', name: 'General' };
    mockRedisGet.mockResolvedValue(JSON.stringify(room));
    const result = await getCachedRoom<typeof room>('r-1');
    expect(result).toEqual(room);
    expect(mockRedisGet).toHaveBeenCalledWith('social:room:r-1');
  });

  it('returns null on cache miss', async () => {
    const result = await getCachedRoom('r-1');
    expect(result).toBeNull();
  });
});

describe('setCachedRoom', () => {
  it('sets with 120s TTL', async () => {
    await setCachedRoom('r-1', { roomId: 'r-1' });
    expect(mockRedisSet).toHaveBeenCalledWith(
      'social:room:r-1',
      expect.any(String),
      { EX: 120 },
    );
  });
});

describe('invalidateRoomCache', () => {
  it('deletes the room cache key', async () => {
    await invalidateRoomCache('r-1');
    expect(mockRedisDel).toHaveBeenCalledWith('social:room:r-1');
  });
});

// ---------------------------------------------------------------------------
// Group cache
// ---------------------------------------------------------------------------

describe('getCachedGroup', () => {
  it('returns parsed value on cache hit', async () => {
    const group = { groupId: 'g-1', name: 'Devs' };
    mockRedisGet.mockResolvedValue(JSON.stringify(group));
    const result = await getCachedGroup<typeof group>('g-1');
    expect(result).toEqual(group);
    expect(mockRedisGet).toHaveBeenCalledWith('social:group:g-1');
  });
});

describe('setCachedGroup', () => {
  it('sets with 120s TTL', async () => {
    await setCachedGroup('g-1', { groupId: 'g-1' });
    expect(mockRedisSet).toHaveBeenCalledWith(
      'social:group:g-1',
      expect.any(String),
      { EX: 120 },
    );
  });
});

describe('invalidateGroupCache', () => {
  it('deletes the group cache key', async () => {
    await invalidateGroupCache('g-1');
    expect(mockRedisDel).toHaveBeenCalledWith('social:group:g-1');
  });
});
