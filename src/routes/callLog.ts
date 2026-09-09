// /api/calls/log — the call history a Teams user expects: what rang, who
// answered, how long, and what was missed. See CallLogRepository for the model.
import { Router, Request, Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import { callLogRepo } from '../repositories';
import type { CallEvent, CallLogFilter } from '../repositories/CallLogRepository';

const ACTIONS = new Set(['invite', 'accepted', 'declined', 'cancelled', 'ended', 'missed']);
const FILTERS = new Set<CallLogFilter>(['all', 'missed', 'incoming', 'outgoing']);

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
    callerId: b.callerId,
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
