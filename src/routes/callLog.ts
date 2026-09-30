// /api/calls/log — the call history a Teams user expects: what rang, who
// answered, how long, and what was missed. See CallLogRepository for the model.
import express, { Router, Request, Response } from 'express';
import { randomUUID } from 'crypto';
import { requireAuth } from '../middleware/auth';
import { asyncHandler, ForbiddenError, NotFoundError, ValidationError } from '../middleware/error-handler';
import { lobbyAccess } from '../lib/lobbyAccess';
import { dmLobbyParticipants } from '../lib/authorize';
import { callLogRepo, voicemailRepo } from '../repositories';
import { VOICEMAIL_MAX_BYTES, VOICEMAIL_MAX_DURATION_MS } from '../repositories/VoicemailRepository';
import type { CallEvent, CallLogFilter } from '../repositories/CallLogRepository';

// `voicemail` is not reportable here — it is applied by POST /voicemail, which owns the bytes.
// `joined` IS reportable: it is how a call you walked into (a room, the shared
// hangout) reaches the log, since a place broadcasts no ring for the frame
// reporter to overhear. It only ever writes the reporter's own row.
const ACTIONS = new Set(['invite', 'accepted', 'declined', 'cancelled', 'ended', 'missed', 'joined']);
const FILTERS = new Set<CallLogFilter>(['all', 'missed', 'incoming', 'outgoing', 'voicemail']);

function parseEvent(body: unknown, reporterId: string): CallEvent {
  const b = (body ?? {}) as Record<string, unknown>;
  if (typeof b.callId !== 'string' || !b.callId) throw new ValidationError('callId is required');
  if (typeof b.action !== 'string' || !ACTIONS.has(b.action)) throw new ValidationError(`action must be one of ${[...ACTIONS].join(', ')}`);
  if (typeof b.callerId !== 'string' || !b.callerId) throw new ValidationError('callerId is required');
  if (typeof b.lobbyName !== 'string' || !b.lobbyName) throw new ValidationError('lobbyName is required');
  const targets = Array.isArray(b.targetUserIds) ? b.targetUserIds.filter((t): t is string => typeof t === 'string' && t.length > 0) : [];
  if (targets.length > 50) throw new ValidationError('too many targets');
  const at = typeof b.at === 'string' && !Number.isNaN(Date.parse(b.at)) ? new Date(b.at).toISOString() : undefined;
  return {
    callId: b.callId,
    action: b.action as CallEvent['action'],
    // A join is a claim about YOURSELF. Taking the caller id from the token
    // rather than the body means no client can write attendance into somebody
    // else's history; every other action is a frame both parties saw, so the
    // body's caller id is the shared fact and stands.
    callerId: b.action === 'joined' ? reporterId : b.callerId,
    ...(typeof b.callerName === 'string' ? { callerName: b.callerName } : {}),
    targetUserIds: targets,
    lobbyName: b.lobbyName,
    ...(typeof b.audioOnly === 'boolean' ? { audioOnly: b.audioOnly } : {}),
    ...(b.kind === 'knock' || b.kind === 'invite' ? { kind: b.kind } : {}),
    ...(typeof b.sessionId === 'string' ? { sessionId: b.sessionId } : {}),
    ...(at ? { at } : {}),
    reporterId,
  };
}

export const callLogRouter = Router();

// POST /api/calls/log/events — idempotent; report as often as you like.
callLogRouter.post('/log/events', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const event = parseEvent(req.body, req.user!.sub);
  // NFR #175: a report writes rows into the caller's AND the targets' histories,
  // so the reporter must be one of the parties — the caller or a target — and,
  // for a DM lobby, one of the two people it names. Anyone else could forge
  // calls into strangers' logs.
  // A guest of the DM's live call (a third person let in or invited —
  // lib/lobbyAccess.ts) is one of its parties too, or the ring and the join
  // they report are refused and their history never shows the call.
  const reporter = req.user!.sub;
  const dm = typeof event.lobbyName === 'string' ? dmLobbyParticipants(event.lobbyName) : null;
  const dmParty = !dm || dm.includes(reporter)
    || (await lobbyAccess().canUseLobby(req, event.lobbyName as string).catch(() => false));
  if ((event.callerId !== reporter && !event.targetUserIds.includes(reporter)) || !dmParty) {
    throw new ForbiddenError('You can only report calls you are part of');
  }
  const rows = await callLogRepo.applyEvent(event);
  const mine = rows.find((r) => r.userId === req.user!.sub) ?? null;
  res.status(200).json({ applied: rows.length, row: mine });
}));

// GET /api/calls/log?filter=all|missed|incoming|outgoing&limit=
callLogRouter.get('/log', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const filter = (typeof req.query.filter === 'string' && FILTERS.has(req.query.filter as CallLogFilter) ? req.query.filter : 'all') as CallLogFilter;
  const limit = Math.min(Math.max(Number(req.query.limit) || 100, 1), 500);
  const rows = await callLogRepo.listForUser(req.user!.sub, filter, limit);
  res.status(200).json({ calls: rows });
}));

// GET /api/calls/log/unseen — the badge.
callLogRouter.get('/log/unseen', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  res.status(200).json({ missed: await callLogRepo.unseenMissed(req.user!.sub) });
}));

// POST /api/calls/log/seen — the person looked at their calls.
callLogRouter.post('/log/seen', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  res.status(200).json({ marked: await callLogRepo.markSeen(req.user!.sub) });
}));

// ---------------------------------------------------------------------------
// Voicemail. Raw audio in the body (Content-Type audio/*), the call it belongs
// to in the query — the global JSON parser skips non-JSON bodies, so this
// route parses its own, with a limit the JSON one would refuse.
// ---------------------------------------------------------------------------

const audioBody = express.raw({ type: ['audio/*', 'video/webm', 'application/octet-stream'], limit: VOICEMAIL_MAX_BYTES + 1024 });

function q(req: Request, key: string): string | undefined {
  const v = req.query[key];
  return typeof v === 'string' && v.length > 0 ? v : undefined;
}

// POST /api/calls/voicemail?toUserId=&callId=&lobbyName=&durationMs=&fromName=
callLogRouter.post('/voicemail', requireAuth, audioBody, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const fromUserId = req.user!.sub;
  const toUserId = q(req, 'toUserId');
  const callId = q(req, 'callId');
  const lobbyName = q(req, 'lobbyName');
  const durationMs = Number(q(req, 'durationMs'));
  if (!toUserId) throw new ValidationError('toUserId is required');
  if (toUserId === fromUserId) throw new ValidationError('you cannot leave yourself a voicemail');
  if (!callId) throw new ValidationError('callId is required');
  if (!lobbyName) throw new ValidationError('lobbyName is required');
  if (!Number.isFinite(durationMs) || durationMs <= 0 || durationMs > VOICEMAIL_MAX_DURATION_MS) throw new ValidationError(`durationMs must be 1..${VOICEMAIL_MAX_DURATION_MS}`);
  const audio = Buffer.isBuffer(req.body) ? (req.body as Buffer) : null;
  if (!audio || audio.length === 0) throw new ValidationError('an audio body is required');
  if (audio.length > VOICEMAIL_MAX_BYTES) throw new ValidationError(`voicemail is too large (max ${VOICEMAIL_MAX_BYTES} bytes)`);
  const mimeType = String(req.headers['content-type'] || 'audio/webm').split(';')[0].trim();
  const voicemailId = `vm-${Date.now()}-${randomUUID().slice(0, 8)}`;
  const createdAt = new Date().toISOString();
  const fromName = q(req, 'fromName');
  await voicemailRepo.save({
    userId: toUserId, voicemailId, fromUserId, ...(fromName ? { fromName } : {}), callId, lobbyName, mimeType,
    durationMs: Math.round(durationMs), sizeBytes: audio.length, createdAt, audioB64: audio.toString('base64'),
  });
  // The row is how the message is FOUND: it lands in the Voicemail lane and
  // the badge through the same call-log query as a missed call.
  await callLogRepo.applyEvent({
    callId, action: 'voicemail', callerId: fromUserId, ...(fromName ? { callerName: fromName } : {}), targetUserIds: [toUserId], lobbyName,
    at: createdAt, reporterId: fromUserId, voicemailId, voicemailDurationMs: Math.round(durationMs),
  });
  res.status(201).json({ voicemailId, createdAt, durationMs: Math.round(durationMs), sizeBytes: audio.length, mimeType });
}));

// GET /api/calls/voicemail — mine, newest first, no audio.
callLogRouter.get('/voicemail', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  res.status(200).json({ voicemails: await voicemailRepo.listForUser(req.user!.sub) });
}));

// GET /api/calls/voicemail/:id/audio — the bytes, for the recipient only.
callLogRouter.get('/voicemail/:id/audio', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const item = await voicemailRepo.get(req.user!.sub, String(req.params.id));
  if (!item || !item.audioB64) throw new NotFoundError('Voicemail not found');
  const bytes = Buffer.from(item.audioB64, 'base64');
  res.status(200).set({ 'Content-Type': item.mimeType, 'Content-Length': String(bytes.length), 'Cache-Control': 'private, max-age=3600' }).send(bytes);
}));

// POST /api/calls/voicemail/:id/listened
callLogRouter.post('/voicemail/:id/listened', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const item = await voicemailRepo.get(req.user!.sub, String(req.params.id));
  if (!item) throw new NotFoundError('Voicemail not found');
  await voicemailRepo.markListened(req.user!.sub, item.voicemailId);
  res.status(200).json({ ok: true });
}));
