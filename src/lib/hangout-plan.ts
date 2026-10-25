import { ValidationError } from '../middleware/error-handler';

/** Saved layout only. Live membership, presence and media sessions never live here. */
export interface HangoutPlan {
  version: number;
  mode: 'assigned' | 'self-select';
  allowReturnToMain: boolean;
  rooms: Array<{ id: string; name: string; members: string[] }>;
  updatedAt: string;
  updatedBy: string;
}

export function readHangoutPlan(input: unknown, userId: string): HangoutPlan {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new ValidationError('plan must be an object');
  const raw = input as Record<string, unknown>;
  if (!Number.isSafeInteger(raw.expectedVersion) || Number(raw.expectedVersion) < 0) throw new ValidationError('plan.expectedVersion is required');
  if (raw.mode !== 'assigned' && raw.mode !== 'self-select') throw new ValidationError('plan.mode must be assigned or self-select');
  if (typeof raw.allowReturnToMain !== 'boolean') throw new ValidationError('plan.allowReturnToMain must be a boolean');
  if (!Array.isArray(raw.rooms) || raw.rooms.length > 50) throw new ValidationError('A saved layout supports up to 50 rooms');
  const ids = new Set<string>(), members = new Set<string>();
  const rooms = raw.rooms.map((value: unknown) => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new ValidationError('Invalid room');
    const room = value as Record<string, unknown>;
    if (typeof room.id !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/.test(room.id) || ids.has(room.id)) throw new ValidationError('Room ids must be unique');
    if (typeof room.name !== 'string' || !room.name.trim() || room.name.trim().length > 80) throw new ValidationError('Room names must be 1–80 characters');
    if (!Array.isArray(room.members)) throw new ValidationError('Room members must be an array');
    ids.add(room.id);
    const assigned = room.members.map((id: unknown) => {
      if (typeof id !== 'string' || !/^[A-Za-z0-9:_@.+-]{1,128}$/.test(id) || members.has(id)) throw new ValidationError('Each person can be assigned to one saved room');
      members.add(id);
      if (members.size > 500) throw new ValidationError('A layout supports up to 500 assignments');
      return id;
    });
    return { id: room.id, name: room.name.trim(), members: assigned };
  });
  return { version: Number(raw.expectedVersion) + 1, mode: raw.mode, allowReturnToMain: raw.allowReturnToMain,
    rooms, updatedAt: new Date().toISOString(), updatedBy: userId };
}
