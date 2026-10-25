import type { HangoutRoom } from '../repositories/HangoutRoomRepository';
import type { VideoSessionRecord } from '../repositories/VideoSessionStore';
import { AppError } from '../middleware/error-handler';

/** One stable hangout can have many main and breakout media sessions. */
export async function resolveHangoutMedia(lobby: string, read: (orgId: string, slug: string) => Promise<HangoutRoom | null>): Promise<VideoSessionRecord['hangout']> {
  const parsed = /^([^:]+):room:([A-Za-z0-9_-]+)(?:~breakout~([A-Za-z0-9_-]{1,64}))?$/.exec(lobby);
  if (!parsed) return undefined;
  const [, orgId, slug, breakoutRoomId] = parsed;
  const room = await read(orgId, slug);
  if (!room || room.state !== 'active') throw new AppError(404, 'Hangout not available');
  return { orgId, roomId: room.roomId, slug,
    roomName: breakoutRoomId ? room.plan?.rooms.find(r => r.id === breakoutRoomId)?.name ?? 'Temporary room' : 'Main call',
    ...(breakoutRoomId ? { breakoutRoomId } : {}), audioOnly: room.settings.audioOnly, recordingEnabled: room.settings.recordingEnabled };
}
