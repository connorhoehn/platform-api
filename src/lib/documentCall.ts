// In-document calls — request parsing and response shaping for the
// `document-review` fields on a video session row.
//
// Contract: realtime-examples docs/design/document-calls/SPEC.md §4.1 / §5.2.
// Kept out of routes/videoSessions.ts so that file only gains call sites.

import { ValidationError } from '../middleware/error-handler';
import type { DocumentCallFieldsPatch, VideoSessionRecord } from '../repositories/VideoSessionStore';

export const MAX_TITLE_LENGTH = 200;
export const MAX_REVIEW_DOCUMENTS = 50;

function parseTitle(raw: unknown): string | undefined {
  if (raw === undefined || raw === null) return undefined;
  if (typeof raw !== 'string') throw new ValidationError('title must be a string');
  const title = raw.trim();
  if (!title) throw new ValidationError('title must not be empty');
  if (title.length > MAX_TITLE_LENGTH) {
    throw new ValidationError(`title must be at most ${MAX_TITLE_LENGTH} characters`);
  }
  return title;
}

/**
 * The review list, host document first, duplicates removed, order kept.
 * The host is prepended when the caller left it out: the call belongs to it,
 * and the lobby (recording ACLs, call cards) is keyed on it.
 */
function parseDocumentIds(raw: unknown, hostDocumentId: string): string[] | undefined {
  if (raw === undefined || raw === null) return undefined;
  if (!Array.isArray(raw) || raw.some((d) => typeof d !== 'string' || d.length === 0)) {
    throw new ValidationError('documentIds must be an array of non-empty strings');
  }
  const seen = new Set<string>([hostDocumentId]);
  const out = [hostDocumentId];
  for (const id of raw as string[]) {
    if (seen.has(id)) continue;
    seen.add(id);
    out.push(id);
  }
  if (out.length > MAX_REVIEW_DOCUMENTS) {
    throw new ValidationError(`documentIds must hold at most ${MAX_REVIEW_DOCUMENTS} documents`);
  }
  return out;
}

/**
 * `POST /sessions` body → the fields to store. Returns null for every call
 * that is not a document review, so other lobbies' rows are untouched.
 */
export function parseCreateFields(
  body: Record<string, unknown>,
  lobbyName: string,
  userId: string,
): DocumentCallFieldsPatch | null {
  if (body.kind === undefined) return null;
  if (body.kind !== 'document-review') {
    throw new ValidationError(`unknown call kind: '${String(body.kind)}'`);
  }
  const media = body.media ?? 'video';
  if (media !== 'video' && media !== 'audio') {
    throw new ValidationError("media must be 'video' or 'audio'");
  }
  return {
    kind: 'document-review',
    title: parseTitle(body.title),
    documentIds: parseDocumentIds(body.documentIds, lobbyName) ?? [lobbyName],
    media,
    hostUserId: userId,
  };
}

/** `PATCH /sessions/:id` body → `{ title?, documentIds? }`; at least one. */
export function parsePatchFields(
  body: Record<string, unknown>,
  hostDocumentId: string,
): { title?: string; documentIds?: string[] } {
  const title = parseTitle(body.title);
  const documentIds = parseDocumentIds(body.documentIds, hostDocumentId);
  if (title === undefined && documentIds === undefined) {
    throw new ValidationError('nothing to change: send title and/or documentIds');
  }
  return {
    ...(title !== undefined ? { title } : {}),
    ...(documentIds !== undefined ? { documentIds } : {}),
  };
}

export function hostOf(session: VideoSessionRecord): string | undefined {
  return session.hostUserId ?? session.startedBy;
}

/** Someone in the call's join list, or its host. */
export function isCallParticipant(session: VideoSessionRecord, userId: string): boolean {
  if (hostOf(session) === userId) return true;
  return (session.participants ?? []).some((p) => p.userId === userId);
}

/**
 * People in the call right now, from the join mirror: distinct users, so
 * two tabs of one person count once. 0 once the row has ended.
 *
 * This is the mirror, not the SFU, so it can run high when a client never
 * says goodbye (the stale-session reaper ends such rows). The live dock uses
 * the gateway roster instead; this number is for list rows and the incoming
 * popover ("3 in call"), where one stale head is tolerable and an SFU hop per
 * row is not.
 */
export function participantCountOf(session: VideoSessionRecord): number {
  if (session.status !== 'active') return 0;
  return new Set((session.participants ?? []).map((p) => p.userId).filter(Boolean)).size;
}

/** The row as the API returns it: stored fields plus `participantCount`. */
export function withCallFields<T extends VideoSessionRecord>(session: T): T & { participantCount: number } {
  return { ...session, participantCount: participantCountOf(session) };
}
