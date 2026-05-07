/**
 * Tests for social-api RoomBroadcaster.
 *
 * Mocks the cache module, the roomRepo, and broadcastService. Verifies that:
 *  - Cache hit → repo is NOT queried, broadcast fires with cached channelId
 *  - Cache miss → repo.getRoom is called, cache is warmed, broadcast fires
 *  - Missing room → no-op (no throw, no broadcast)
 */

jest.mock('../src/lib/cache', () => ({
  getCachedRoom: jest.fn(),
  setCachedRoom: jest.fn(() => Promise.resolve()),
}));

jest.mock('../src/repositories', () => ({
  roomRepo: {
    getRoom: jest.fn(),
  },
}));

jest.mock('../src/services/broadcast', () => ({
  broadcastService: {
    emit: jest.fn(() => Promise.resolve()),
  },
}));

import { broadcastToRoom } from '../src/services/room-broadcaster';
import { getCachedRoom, setCachedRoom } from '../src/lib/cache';
import { roomRepo } from '../src/repositories';
import { broadcastService } from '../src/services/broadcast';

const getCachedRoomMock = getCachedRoom as jest.MockedFunction<typeof getCachedRoom>;
const setCachedRoomMock = setCachedRoom as jest.MockedFunction<typeof setCachedRoom>;
const getRoomMock = roomRepo.getRoom as jest.MockedFunction<typeof roomRepo.getRoom>;
const emitMock = broadcastService.emit as jest.MockedFunction<typeof broadcastService.emit>;

describe('broadcastToRoom', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('cache hit → does NOT call repo, emits with cached channelId', async () => {
    getCachedRoomMock.mockResolvedValueOnce({ channelId: 'chan-cached' } as any);

    await broadcastToRoom('room-1', 'social:post', { foo: 'bar' });

    expect(getCachedRoomMock).toHaveBeenCalledWith('room-1');
    expect(getRoomMock).not.toHaveBeenCalled();
    expect(setCachedRoomMock).not.toHaveBeenCalled();
    expect(emitMock).toHaveBeenCalledWith('chan-cached', 'social:post', { foo: 'bar' });
  });

  test('cache miss + repo hit → warms cache, emits with repo channelId', async () => {
    getCachedRoomMock.mockResolvedValueOnce(null);
    getRoomMock.mockResolvedValueOnce({
      roomId: 'room-2',
      channelId: 'chan-from-repo',
    } as any);

    await broadcastToRoom('room-2', 'social:comment', { text: 'hi' });

    expect(getCachedRoomMock).toHaveBeenCalledWith('room-2');
    expect(getRoomMock).toHaveBeenCalledWith('room-2');
    expect(setCachedRoomMock).toHaveBeenCalledWith(
      'room-2',
      expect.objectContaining({ roomId: 'room-2', channelId: 'chan-from-repo' }),
    );
    expect(emitMock).toHaveBeenCalledWith('chan-from-repo', 'social:comment', { text: 'hi' });
  });

  test('cache miss + room missing → no broadcast, no throw', async () => {
    getCachedRoomMock.mockResolvedValueOnce(null);
    getRoomMock.mockResolvedValueOnce(null);

    await expect(
      broadcastToRoom('room-gone', 'social:like', { userId: 'u1' }),
    ).resolves.toBeUndefined();

    expect(setCachedRoomMock).not.toHaveBeenCalled();
    expect(emitMock).not.toHaveBeenCalled();
  });
});
