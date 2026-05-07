/**
 * RoomBroadcaster — wraps the cache+repo+broadcast composition used by ~22 route
 * handlers that need to fan out a social event to a room's channel.
 *
 * Replaces this inline pattern:
 *
 *   let roomData = await getCachedRoom<{ channelId: string }>(roomId);
 *   if (!roomData) {
 *     const room = await roomRepo.getRoom(roomId);
 *     if (room) {
 *       roomData = room as { channelId: string };
 *       void setCachedRoom(roomId, room);
 *     }
 *   }
 *   if (roomData) {
 *     void broadcastService.emit(roomData.channelId, eventType, payload);
 *   }
 *
 * No-ops silently if the room can't be found — matches existing behavior
 * (routes do not return 404 when broadcast fan-out fails to locate the room).
 */
import { broadcastService } from './broadcast';
import { getCachedRoom, setCachedRoom } from '../lib/cache';
import { roomRepo } from '../repositories';

type SocialEventType = Parameters<typeof broadcastService.emit>[1];

interface RoomForBroadcast {
  channelId: string;
}

/**
 * Look up a room's channelId (cache-first, DynamoDB fallback) and fan out an
 * event to that channel. Silently returns if the room is missing.
 *
 * This function does NOT await the broadcast emit — emission failures never
 * affect HTTP responses. The lookup itself is awaited so cache warming is
 * coherent for the next caller.
 */
export async function broadcastToRoom(
  roomId: string,
  eventType: SocialEventType,
  payload: Record<string, unknown>,
): Promise<void> {
  let roomData = await getCachedRoom<RoomForBroadcast>(roomId);
  if (!roomData) {
    const room = await roomRepo.getRoom(roomId);
    if (room) {
      roomData = room as RoomForBroadcast;
      void setCachedRoom(roomId, room);
    }
  }
  if (roomData) {
    void broadcastService.emit(roomData.channelId, eventType, payload);
  }
}
