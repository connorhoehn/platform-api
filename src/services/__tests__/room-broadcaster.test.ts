const mockEmit = jest.fn();
jest.mock('../broadcast', () => ({
  broadcastService: { emit: (...args: unknown[]) => mockEmit(...args) },
}));

const mockGetCachedRoom = jest.fn();
const mockSetCachedRoom = jest.fn();
jest.mock('../../lib/cache', () => ({
  getCachedRoom: (...args: unknown[]) => mockGetCachedRoom(...args),
  setCachedRoom: (...args: unknown[]) => mockSetCachedRoom(...args),
}));

const mockGetRoom = jest.fn();
jest.mock('../../repositories', () => ({
  roomRepo: { getRoom: (...args: unknown[]) => mockGetRoom(...args) },
}));

import { broadcastToRoom } from '../room-broadcaster';

beforeEach(() => {
  mockEmit.mockReset();
  mockGetCachedRoom.mockReset();
  mockSetCachedRoom.mockReset();
  mockGetRoom.mockReset();
});

describe('broadcastToRoom', () => {
  it('uses cached room and broadcasts to channelId', async () => {
    mockGetCachedRoom.mockResolvedValue({ channelId: 'ch-1' });
    await broadcastToRoom('room-1', 'social:post', { postId: 'p-1' });
    expect(mockGetCachedRoom).toHaveBeenCalledWith('room-1');
    expect(mockGetRoom).not.toHaveBeenCalled();
    expect(mockEmit).toHaveBeenCalledWith('ch-1', 'social:post', { postId: 'p-1' });
  });

  it('falls back to repo when cache misses and warms cache', async () => {
    mockGetCachedRoom.mockResolvedValue(null);
    mockGetRoom.mockResolvedValue({ channelId: 'ch-2', roomId: 'room-2' });
    await broadcastToRoom('room-2', 'social:comment', { commentId: 'c-1' });
    expect(mockGetRoom).toHaveBeenCalledWith('room-2');
    expect(mockSetCachedRoom).toHaveBeenCalledWith('room-2', { channelId: 'ch-2', roomId: 'room-2' });
    expect(mockEmit).toHaveBeenCalledWith('ch-2', 'social:comment', { commentId: 'c-1' });
  });

  it('no-ops silently when room not found in cache or repo', async () => {
    mockGetCachedRoom.mockResolvedValue(null);
    mockGetRoom.mockResolvedValue(null);
    await broadcastToRoom('missing', 'social:like', { likeId: 'l-1' });
    expect(mockEmit).not.toHaveBeenCalled();
    expect(mockSetCachedRoom).not.toHaveBeenCalled();
  });

  it('does not cache when repo returns null', async () => {
    mockGetCachedRoom.mockResolvedValue(null);
    mockGetRoom.mockResolvedValue(null);
    await broadcastToRoom('gone', 'social:member_left', { userId: 'u-1' });
    expect(mockSetCachedRoom).not.toHaveBeenCalled();
    expect(mockEmit).not.toHaveBeenCalled();
  });
});
