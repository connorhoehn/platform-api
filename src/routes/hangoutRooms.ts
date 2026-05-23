/**
 * hangoutRoomsRouter — first-class persistent hangout rooms.
 *
 * Mounted at `/api/rooms` AFTER requireAuth in app.ts. All endpoints require
 * a valid Cognito JWT (or SKIP_AUTH=true → dev-user in local dev).
 *
 * Surface (see .planning/HANGOUT-ROOMS-DESIGN.md for the full spec):
 *
 *   GET    /api/rooms                       — list user-visible rooms
 *   GET    /api/rooms/:slug                 — find by slug
 *   POST   /api/rooms                       — create (auto-slug from name)
 *   PATCH  /api/rooms/:slug                 — partial update (owner only)
 *   POST   /api/rooms/:slug/archive         — soft-delete (owner only)
 *   POST   /api/rooms/:slug/members         — add member (owner only)
 *   DELETE /api/rooms/:slug/members/:userId — remove member (owner or self)
 *   POST   /api/rooms/:slug/join            — bridge to /api/video/sessions
 *   POST   /api/rooms/:slug/leave           — end session participation
 *
 * Bridge endpoints (/join, /leave) do an internal HTTP call to the same
 * platform-api process at /api/video/sessions — they forward the caller's
 * Authorization header so SKIP_AUTH and Cognito both work. This keeps the
 * video-session lifecycle owned by videoSessions.ts; rooms.ts is purely
 * the persistent-metadata layer on top.
 */

import { Router, Request, Response } from 'express';
import {
  hangoutRoomRepo,
  hangoutRoomMemberRepo,
  RoomSlugConflictError,
  RoomNotFoundError,
  RoomAccessDeniedError,
  RoomArchivedError,
  type HangoutRoom,
  type HangoutRoomSettings,
  type HangoutRoomVisibility,
  type HangoutRoomState,
} from '../repositories';
import {
  asyncHandler,
  ValidationError,
  AppError,
} from '../middleware/error-handler';
import log from '../lib/logger';

export const hangoutRoomsRouter = Router();

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DEFAULT_ORG_ID = 'default-org';
const DEFAULT_SOFT_CAP = 25;
const SLUG_MAX_LEN = 48;
const SLUG_RETRY_LIMIT = 3;

/**
 * Resolve the orgId for the caller. In dev (SKIP_AUTH=true) defaults to
 * 'default-org' when the JWT doesn't carry one — keeps local-dev frictionless
 * without compromising prod isolation.
 */
function resolveOrgId(req: Request): string {
  const user = req.user as unknown as { sub: string; orgId?: string } | undefined;
  if (user?.orgId) return user.orgId;
  if (process.env.SKIP_AUTH === 'true') return DEFAULT_ORG_ID;
  throw new ValidationError('orgId is required (missing from auth context)');
}

function getUserId(req: Request): string {
  const sub = req.user?.sub;
  if (!sub) throw new ValidationError('userId is required');
  return sub;
}

/**
 * Slug-ify a free-text name into a URL-safe slug. Lowercases, collapses
 * non-alnum runs to single hyphens, strips leading/trailing hyphens, caps
 * length at 48 chars. Always yields a non-empty string — falls back to
 * `'room'` if the input scrubs to nothing.
 */
export function slugify(name: string): string {
  return (
    name
      .toLowerCase()
      .trim()
      .replace(/[^a-z0-9-]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, SLUG_MAX_LEN) || 'room'
  );
}

/**
 * Coerce arbitrary settings input into a fully-populated HangoutRoomSettings.
 * Applies sensible defaults so callers can omit fields.
 */
function normalizeSettings(input: unknown): HangoutRoomSettings {
  const raw = (input ?? {}) as Partial<HangoutRoomSettings>;
  return {
    audioOnly: raw.audioOnly === true,
    recordingEnabled: raw.recordingEnabled === true,
    softCap:
      typeof raw.softCap === 'number' && raw.softCap > 0
        ? Math.floor(raw.softCap)
        : DEFAULT_SOFT_CAP,
  };
}

/**
 * Owner-or-admin gate. Admin is a placeholder — for now anyone with
 * createdBy === userId qualifies; an explicit admin role can layer on top
 * via the existing requireAdmin middleware if needed later.
 */
function assertOwner(room: HangoutRoom, userId: string): void {
  if (room.createdBy !== userId) {
    throw new RoomAccessDeniedError(room.slug);
  }
}

/**
 * Visibility gate: org-rooms are visible to anyone in the org; private
 * rooms require a membership row.
 */
async function assertCanAccess(room: HangoutRoom, userId: string, orgId: string): Promise<void> {
  if (room.orgId !== orgId) throw new RoomAccessDeniedError(room.slug);
  if (room.visibility === 'org') return;
  if (room.createdBy === userId) return;
  const isMember = await hangoutRoomMemberRepo.isMember(room.roomId, userId);
  if (!isMember) throw new RoomAccessDeniedError(room.slug);
}

/**
 * Look up a room by slug + assert it exists and isn't archived. Centralizes
 * the 404/410 split so route handlers stay short.
 */
async function loadActiveRoom(orgId: string, slug: string): Promise<HangoutRoom> {
  const room = await hangoutRoomRepo.getRoomBySlug(orgId, slug);
  if (!room) throw new RoomNotFoundError(slug);
  if (room.state === 'archived') throw new RoomArchivedError(slug);
  return room;
}

/**
 * Internal HTTP bridge to /api/video/sessions. Forwards the caller's
 * Authorization header so SKIP_AUTH dev-user + real Cognito both work.
 * Hits localhost so we stay within the process boundary.
 */
async function callSelf(
  req: Request,
  path: string,
  init: { method: 'GET' | 'POST'; body?: unknown },
): Promise<{ status: number; body: unknown }> {
  const port = process.env.PORT ?? '3001';
  const baseUrl =
    process.env.PLATFORM_API_INTERNAL_URL ?? `http://127.0.0.1:${port}`;
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };
  const auth = req.headers.authorization;
  if (auth) headers['Authorization'] = auth;

  const res = await fetch(`${baseUrl}${path}`, {
    method: init.method,
    headers,
    ...(init.body !== undefined ? { body: JSON.stringify(init.body) } : {}),
  });
  const text = await res.text();
  let parsed: unknown = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = text;
  }
  return { status: res.status, body: parsed };
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/**
 * GET /api/rooms — list rooms the user can see.
 *
 * Composition: (a) all org-visible rooms in the user's org +
 * (b) any private rooms the user is a member of. We page from the
 * org-recency GSI first; private rooms are merged in best-effort by
 * looking them up individually if they're not already in (a).
 */
hangoutRoomsRouter.get(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const state = ((req.query.state as string) ?? 'active') as HangoutRoomState;
    const limitParam = Number(req.query.limit ?? '50');
    const limit = Number.isFinite(limitParam) && limitParam > 0 ? Math.min(limitParam, 200) : 50;
    const cursor = typeof req.query.cursor === 'string' ? req.query.cursor : undefined;

    const orgPage = await hangoutRoomRepo.listRoomsByOrg(orgId, {
      state,
      limit,
      ...(cursor ? { cursor } : {}),
    });

    // Filter to rooms the user can see (org-visibility passes; private
    // requires membership). We don't fetch missing private rooms in this
    // pass — the dedicated GET /api/rooms/:slug handles deep links.
    const visible: HangoutRoom[] = [];
    for (const r of orgPage.items) {
      if (r.visibility === 'org' || r.createdBy === userId) {
        visible.push(r);
        continue;
      }
      const isMember = await hangoutRoomMemberRepo.isMember(r.roomId, userId);
      if (isMember) visible.push(r);
    }

    res.status(200).json({
      rooms: visible,
      ...(orgPage.cursor ? { cursor: orgPage.cursor } : {}),
    });
  }),
);

/**
 * GET /api/rooms/:slug — find a room by slug. Enforces access on private
 * rooms; 410 on archived; 404 if missing.
 */
hangoutRoomsRouter.get(
  '/:slug',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const { slug } = req.params;

    const room = await loadActiveRoom(orgId, slug);
    await assertCanAccess(room, userId, orgId);

    res.status(200).json({ room });
  }),
);

/**
 * POST /api/rooms — create a room. Slug is auto-derived from the name;
 * on collision we retry with `-2`, `-3` suffix (up to 3 tries) before
 * giving up with 409. Tradeoff: the FE never has to think about slugs.
 */
hangoutRoomsRouter.post(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const body = (req.body ?? {}) as {
      name?: string;
      description?: string;
      visibility?: HangoutRoomVisibility;
      settings?: Partial<HangoutRoomSettings>;
    };

    const name = typeof body.name === 'string' ? body.name.trim() : '';
    if (!name || name.length > 100) {
      throw new ValidationError('name is required (1–100 chars)');
    }

    const visibility: HangoutRoomVisibility =
      body.visibility === 'private' ? 'private' : 'org';
    const settings = normalizeSettings(body.settings);
    const baseSlug = slugify(name);

    let lastErr: unknown;
    for (let attempt = 1; attempt <= SLUG_RETRY_LIMIT; attempt++) {
      const slug = attempt === 1 ? baseSlug : `${baseSlug}-${attempt}`;
      try {
        const room = await hangoutRoomRepo.createRoom({
          orgId,
          slug,
          name,
          ...(body.description !== undefined ? { description: body.description } : {}),
          visibility,
          settings,
          createdBy: userId,
        });

        // Private rooms auto-enroll the creator as owner so visibility
        // checks pass on the very first GET. Org rooms don't need this
        // since the visibility check short-circuits.
        if (visibility === 'private') {
          await hangoutRoomMemberRepo.addMember(room.roomId, userId, 'owner', userId);
        }

        res.status(201).json({ room });
        return;
      } catch (err) {
        if (err instanceof RoomSlugConflictError) {
          lastErr = err;
          continue;
        }
        throw err;
      }
    }
    throw lastErr ?? new RoomSlugConflictError(baseSlug);
  }),
);

/**
 * PATCH /api/rooms/:slug — partial update. Owner-only.
 */
hangoutRoomsRouter.patch(
  '/:slug',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const { slug } = req.params;

    const room = await loadActiveRoom(orgId, slug);
    assertOwner(room, userId);

    const body = (req.body ?? {}) as {
      name?: string;
      description?: string;
      visibility?: HangoutRoomVisibility;
      settings?: Partial<HangoutRoomSettings>;
    };

    const partial: Parameters<typeof hangoutRoomRepo.updateRoom>[2] = {};
    if (typeof body.name === 'string') {
      const n = body.name.trim();
      if (!n || n.length > 100) {
        throw new ValidationError('name must be 1–100 chars');
      }
      partial.name = n;
    }
    if (body.description !== undefined) partial.description = body.description;
    if (body.visibility !== undefined) {
      partial.visibility = body.visibility === 'private' ? 'private' : 'org';
    }
    if (body.settings !== undefined) partial.settings = normalizeSettings(body.settings);

    const updated = await hangoutRoomRepo.updateRoom(orgId, room.roomId, partial);
    res.status(200).json({ room: updated });
  }),
);

/**
 * POST /api/rooms/:slug/archive — soft-delete. Owner-only.
 */
hangoutRoomsRouter.post(
  '/:slug/archive',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const { slug } = req.params;

    const room = await loadActiveRoom(orgId, slug);
    assertOwner(room, userId);

    await hangoutRoomRepo.archiveRoom(orgId, room.roomId);
    res.status(200).json({ ok: true, slug });
  }),
);

/**
 * POST /api/rooms/:slug/members — add a member to a private room.
 */
hangoutRoomsRouter.post(
  '/:slug/members',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const { slug } = req.params;
    const body = (req.body ?? {}) as { userId?: string; role?: string };

    if (!body.userId) throw new ValidationError('userId is required');
    const role = body.role === 'owner' ? 'owner' : 'member';

    const room = await loadActiveRoom(orgId, slug);
    assertOwner(room, userId);

    const member = await hangoutRoomMemberRepo.addMember(
      room.roomId,
      body.userId,
      role,
      userId,
    );
    void hangoutRoomRepo.touchLastActive(orgId, room.roomId);
    res.status(201).json({ member });
  }),
);

/**
 * DELETE /api/rooms/:slug/members/:userId — remove a member. The caller
 * can remove themselves OR the owner can remove anyone.
 */
hangoutRoomsRouter.delete(
  '/:slug/members/:userId',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const callerId = getUserId(req);
    const { slug, userId: targetUserId } = req.params;

    const room = await loadActiveRoom(orgId, slug);
    if (targetUserId !== callerId && room.createdBy !== callerId) {
      throw new RoomAccessDeniedError(slug);
    }

    await hangoutRoomMemberRepo.removeMember(room.roomId, targetUserId);
    void hangoutRoomRepo.touchLastActive(orgId, room.roomId);
    res.status(204).end();
  }),
);

/**
 * POST /api/rooms/:slug/join — bridge to /api/video/sessions.
 *
 * Wire-up:
 *   1. find-or-create a VideoSession with lobbyName='room:'+slug
 *   2. mint a participant token via /sessions/:id/join
 *   3. return { room, sessionId, token, participantId }
 *
 * TODO(agent-6): also fire a service-auth POST to the gateway's room-event
 * sink so subscribed clients see the join in real-time. For now we just
 * log a structured "room.member-joined" line for visibility.
 */
hangoutRoomsRouter.post(
  '/:slug/join',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const { slug } = req.params;

    const room = await loadActiveRoom(orgId, slug);
    await assertCanAccess(room, userId, orgId);

    const lobbyName = `room:${slug}`;

    // Step 1 — find-or-create the live VideoSession.
    const createBody: Record<string, unknown> = {
      lobbyName,
      findOrCreate: true,
    };
    if (room.settings.recordingEnabled) {
      createBody['recordingConfiguration'] = { enabled: true, destination: 'local' };
    }
    const create = await callSelf(req, '/api/video/sessions', {
      method: 'POST',
      body: createBody,
    });
    if (create.status >= 400) {
      throw new AppError(create.status, `Failed to create video session: ${JSON.stringify(create.body)}`);
    }
    const sessionId = (create.body as { sessionId?: string }).sessionId;
    if (!sessionId) {
      throw new AppError(502, 'Video session response missing sessionId');
    }

    // Step 2 — mint a participant token.
    const join = await callSelf(req, `/api/video/sessions/${encodeURIComponent(sessionId)}/join`, {
      method: 'POST',
      body: { lobbyName },
    });
    if (join.status >= 400) {
      throw new AppError(join.status, `Failed to join video session: ${JSON.stringify(join.body)}`);
    }
    const { token, participantId } = join.body as { token?: string; participantId?: string };

    // Bump recency + fire the visibility log line.
    void hangoutRoomRepo.touchLastActive(orgId, room.roomId);
    log.info(
      { slug, userId, sessionId, participantId },
      'room.member-joined',
    );

    res.status(200).json({
      room,
      sessionId,
      token,
      participantId,
    });
  }),
);

/**
 * POST /api/rooms/:slug/leave — end participation in the room's live session.
 */
hangoutRoomsRouter.post(
  '/:slug/leave',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const { slug } = req.params;
    const body = (req.body ?? {}) as { sessionId?: string; participantId?: string };

    if (!body.sessionId) throw new ValidationError('sessionId is required');

    const room = await loadActiveRoom(orgId, slug);
    await assertCanAccess(room, userId, orgId);

    const lobbyName = `room:${slug}`;
    const end = await callSelf(req, `/api/video/sessions/${encodeURIComponent(body.sessionId)}/end`, {
      method: 'POST',
      body: {
        lobbyName,
        ...(body.participantId ? { participantId: body.participantId } : {}),
      },
    });

    void hangoutRoomRepo.touchLastActive(orgId, room.roomId);
    log.info(
      { slug, userId, sessionId: body.sessionId, participantId: body.participantId },
      'room.member-left',
    );

    res.status(end.status).json(end.body ?? {});
  }),
);
