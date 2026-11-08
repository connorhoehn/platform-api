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

import { skipAuthEnabled } from '../lib/insecureDevMode';
import { mayUsePartition, requestedPartition } from '../lib/partition';
import { Router, Request, Response } from 'express';
import {
  hangoutRoomRepo,
  videoSessionRepo,
  callRecordingRepo,
  hangoutRoomMemberRepo,
  RoomSlugConflictError,
  RoomNotFoundError,
  RoomAccessDeniedError,
  RoomArchivedError,
  type HangoutRoom,
  type HangoutRoomSettings,
  type HangoutRoomVisibility,
  type HangoutRoomState, profileRepo } from '../repositories';
import {
  asyncHandler,
  ValidationError,
  AppError, ForbiddenError } from '../middleware/error-handler';
import { canCreateRoom } from '../middleware/canCreateRoom';
import log from '../lib/logger';
import { readHangoutPlan } from '../lib/hangout-plan';
import { authorize, principalOf } from '../lib/authorize';
import { tenantOf } from '../lib/tenant';
import { mirrorRoomMembers, mirrorRoomMemberRemoval, syncRoomOnce } from '../gateway/chatMembership';

/**
 * A person's name for the conversation's system line ("Eve added Carol"),
 * from their profile. Undefined when there is none: the line then falls
 * back to the id, which is honest, and never blocks the membership mirror.
 */
async function displayNameOf(userId: string): Promise<string | undefined> {
  try {
    const profile = await profileRepo.getProfile(userId);
    return profile?.displayName || undefined;
  } catch {
    return undefined;
  }
}

export const hangoutRoomsRouter = Router();

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DEFAULT_ORG_ID = 'default-org';
const DEFAULT_SOFT_CAP = 25;
const SLUG_MAX_LEN = 48;
// E2E suites churn many rooms with similar names and archive is a
// soft-delete (slug stays reserved on the row), so the default cap of 3
// retries trips 409 within a few rerun cycles. Make the cap env-overridable.
const SLUG_RETRY_LIMIT = Number(process.env.ROOM_SLUG_RETRY_LIMIT ?? '3');

// Per-user create rate limit: 5 creates / sliding-1h-window / userId.
// In-memory by design — abuse mitigation, not a hard quota. Multi-replica
// deployments would each maintain their own bucket; tighten via Redis if
// that becomes a real concern (the existing `createRateLimiter` in
// middleware/rateLimit.ts is Redis-backed but is token-bucket rather than
// sliding-window, so it doesn't fit this spec's "5 per rolling hour" shape).
// Default: 5 creates per rolling hour. Override via env so e2e suites
// (which legitimately need to churn many rooms) don't false-429.
const CREATE_RATE_LIMIT_MAX = Number(
  process.env.ROOM_CREATE_RATE_LIMIT_MAX ?? '5',
);
const CREATE_RATE_LIMIT_WINDOW_MS = Number(
  process.env.ROOM_CREATE_RATE_LIMIT_WINDOW_MS ?? String(60 * 60 * 1000),
); // 1 hour
const createAttemptsByUser = new Map<string, number[]>();

/**
 * Test-only: clear the in-memory create-rate-limit table so suites don't
 * leak attempt counts across tests.
 */
export function _resetCreateRateLimit(): void {
  createAttemptsByUser.clear();
}

/**
 * Record one create attempt for the user. Returns false when the user has
 * already hit the cap within the rolling window; true otherwise. Sliding
 * window — old timestamps are pruned before each check.
 */
function recordCreateAttempt(userId: string): boolean {
  const now = Date.now();
  const cutoff = now - CREATE_RATE_LIMIT_WINDOW_MS;
  const prior = (createAttemptsByUser.get(userId) ?? []).filter((t) => t > cutoff);
  if (prior.length >= CREATE_RATE_LIMIT_MAX) {
    // Persist the pruned list so the next denial check is honest.
    createAttemptsByUser.set(userId, prior);
    return false;
  }
  prior.push(now);
  createAttemptsByUser.set(userId, prior);
  return true;
}

/**
 * Parse the comma-separated ROOM_ADMIN_USER_IDS env var. Empty / unset
 * means "no admins" — only the owner can archive. We re-parse on every
 * call rather than caching so tests + ops can flip the var at runtime.
 */
function getRoomAdminUserIds(): Set<string> {
  const raw = process.env.ROOM_ADMIN_USER_IDS ?? '';
  return new Set(
    raw
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s.length > 0),
  );
}

/**
 * Returns true if the caller is the room owner OR a configured org-admin.
 */
function isRoomOwnerOrAdmin(room: HangoutRoom, userId: string): boolean {
  if (room.createdBy === userId) return true;
  return getRoomAdminUserIds().has(userId);
}

/**
 * Resolve the room partition (the rooms' `orgId`) for the request. A request
 * may name one (`?tenant=` / body `tenant`, lib/partition.ts): allowed when
 * the caller may use it — any tenant in its token's `tenants` claim, else its
 * own org — and refused with the same 403 a room outside the caller's org
 * gets from `assertCanAccess`. Naming none keeps the caller's org. In dev
 * (SKIP_AUTH=true) defaults to 'default-org' when the JWT doesn't carry one —
 * keeps local-dev frictionless without compromising prod isolation.
 */
function resolveOrgId(req: Request): string {
  const user = req.user as unknown as { sub: string; orgId?: string; tenants?: string[] } | undefined;
  const named = requestedPartition(req);
  if (named) {
    if (!mayUsePartition(user, named)) throw new RoomAccessDeniedError(req.params?.slug ?? named);
    return named;
  }
  if (user?.orgId) return user.orgId;
  if (skipAuthEnabled()) return DEFAULT_ORG_ID;
  throw new ValidationError('orgId is required (missing from auth context)');
}

/**
 * The call lobby for a room. An OrgIQ caller's lobbies are always
 * tenant-prefixed (lib/lobbyAccess.ts refuses an unprefixed one), so its room
 * call is `<partition>:room:<slug>` — the partition keeps two tenants' rooms
 * with one slug in separate calls. Every other caller keeps `room:<slug>`.
 */
function roomLobbyName(req: Request, orgId: string, slug: string): string {
  return req.user?.authKind === 'orgiq-hs256' ? `${orgId}:room:${slug}` : `room:${slug}`;
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
  if (input !== undefined && (!input || typeof input !== 'object' || Array.isArray(input))) throw new ValidationError('settings must be an object');
  const raw = (input ?? {}) as Partial<HangoutRoomSettings>;
  for (const key of ['audioOnly', 'recordingEnabled'] as const) if (raw[key] !== undefined && typeof raw[key] !== 'boolean') throw new ValidationError(`${key} must be a boolean`);
  if (raw.softCap !== undefined && (!Number.isSafeInteger(raw.softCap) || raw.softCap < 1 || raw.softCap > 1000)) throw new ValidationError('softCap must be an integer between 1 and 1000');
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
 *
 * Query params:
 *   ?state=active|archived  (default: active) — archived rooms include
 *       archivedAt + ttl fields populated by the repo's archiveRoom().
 *   ?q=<text>               — case-insensitive substring match against
 *       name + slug. When present, response is capped at 100 rows and
 *       the cursor is omitted (search is single-page by design). Empty
 *       string is treated as no filter.
 *   ?limit=<n>              — page size for the unfiltered list (1..200).
 *   ?cursor=<opaque>        — pagination cursor from a prior page.
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
    const qRaw = typeof req.query.q === 'string' ? req.query.q.trim() : '';

    // Search path — server-side filter against recency-ordered rows.
    // Capped at 100 rows + no cursor (search is single-page by design).
    if (qRaw.length > 0) {
      const candidates = await hangoutRoomRepo.searchRooms(orgId, {
        q: qRaw,
        state,
        limit: 100,
      });
      const visible: HangoutRoom[] = [];
      for (const r of candidates) {
        if (r.visibility === 'org' || r.createdBy === userId) {
          visible.push(r);
          continue;
        }
        const isMember = await hangoutRoomMemberRepo.isMember(r.roomId, userId);
        if (isMember) visible.push(r);
      }
      res.status(200).json({ rooms: visible });
      return;
    }

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
  '/:slug/activity',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req), userId = getUserId(req);
    const room = await loadActiveRoom(orgId, req.params.slug);
    await assertCanAccess(room, userId, orgId);
    const page = await hangoutRoomRepo.listSessionPointers(orgId, room.roomId, typeof req.query.cursor === 'string' ? req.query.cursor : undefined);
    const sessions: Array<Record<string, unknown>> = [];
    // Bounded page (20); each session is read by its exact key, never a table scan.
    for (const pointer of page.items) {
      const session = await videoSessionRepo.getSession(pointer.documentId, pointer.sessionId);
      if (!session || session.hangout?.roomId !== room.roomId || session.hangout.orgId !== orgId
        || (session.ttl && session.ttl <= Math.floor(Date.now() / 1000))) continue;
      const attendance = Object.values(session.participantHistory ?? {});
      const people = [...new Set(attendance.map(p => p.userId))].map(id => ({ userId: id,
        displayName: attendance.find(p => p.userId === id)?.displayName || id,
        joinedAt: attendance.filter(p => p.userId === id).map(p => p.joinedAt).sort()[0],
        // An open media leg is not proof of live presence. The call supplies that separately.
        leftAt: attendance.filter(p => p.userId === id).every(p => !!p.leftAt)
          ? attendance.filter(p => p.userId === id).map(p => p.leftAt!).sort().at(-1) : null,
      }));
      const summaries: Array<{ recordingId: string; status: string; text?: string }> = [];
      if (attendance.some(p => p.userId === userId)) {
        const callIds = [...new Set(attendance.flatMap(p => p.callBinding ? [p.callBinding.callId] : []))];
        for (const callId of callIds) for (const recording of await callRecordingRepo.listByCall(callId)) {
          if (recording.lobbyName !== session.documentId || recording.sessionId !== session.sessionId) continue;
          // Attendance is historical. The current recording fan-out is the
          // authority, so an untagged participant cannot recover removed access
          // through the hangout's activity view.
          const resolved = await callRecordingRepo.resolveForUser(userId, recording.recordingId);
          if (!resolved || !(await authorize({ principal: principalOf(req.user, tenantOf(req.user?.orgId)),
            action: 'recording:read', resource: { type: 'recording', id: recording.recordingId,
              attrs: { isParticipant: resolved.isParticipant } } })).allow) continue;
          const detail = resolved.detail;
          if (detail.lobbyName !== session.documentId || detail.sessionId !== session.sessionId) continue;
          summaries.push({ recordingId: detail.recordingId, status: detail.summaryStatus ?? 'unavailable',
            ...(detail.summaryStatus === 'ready' && detail.summaryMarkdown ? { text: detail.summaryMarkdown } : {}) });
        }
      }
      sessions.push({ sessionId: session.sessionId, roomId: session.hangout.breakoutRoomId ?? null, roomName: session.hangout.roomName,
        startedAt: session.startedAt, endedAt: session.endedAt ?? null, people, summaries,
        recordingEnabled: session.recording?.enabled ?? null });
    }
    res.json({ sessions, ...(page.cursor ? { cursor: page.cursor } : {}) });
  }),
);

hangoutRoomsRouter.get(
  '/:slug',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const { slug } = req.params;

    const room = await loadActiveRoom(orgId, slug);
    await assertCanAccess(room, userId, orgId);

    // A private room's conversation carries the room's members. Converge
    // here, lazily, so rooms that predate the mirror need no backfill —
    // and since `listMembers` is the room's complete, authoritative
    // roster (unlike the single-member lists the add-member routes pass
    // to `mirrorRoomMembers`), this is also where GAP-12 evicts anyone
    // the gateway still has from a removal whose mirror call failed.
    if (room.visibility === 'private') {
      syncRoomOnce(slug, async () => ({
        slug,
        ownerId: room.createdBy,
        members: await Promise.all((await hangoutRoomMemberRepo.listMembers(room.roomId)).map(async (m) => ({ userId: m.userId, displayName: await displayNameOf(m.userId) }))),
        authoritative: true,
      }));
    }

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

    // Permission gate — pluggable hook (default: open). See
    // middleware/canCreateRoom.ts for wiring + failure-mode semantics.
    const allowed = await canCreateRoom(userId, orgId);
    if (!allowed) {
      res.status(403).json({ error: { code: 'ROOM_CREATE_DENIED' } });
      return;
    }

    // Rate limit — 5 creates / rolling 1h / user. Sliding window in-memory.
    if (!recordCreateAttempt(userId)) {
      res.status(429).json({
        error: {
          code: 'ROOM_CREATE_RATE_LIMITED',
          message: `Room create limit reached (max ${CREATE_RATE_LIMIT_MAX} per hour).`,
        },
      });
      return;
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
 *
 * Covers renaming a room (`{ name }`), the display-name-only rename Teams
 * calls "rename channel". Owner-only: same gate as archive and add-member —
 * a room's identity-ish fields (name, visibility, settings) are structural
 * changes we restrict to the creator, not something any member can do.
 * There's no separate "admin" concept here beyond `ROOM_ADMIN_USER_IDS`
 * (used for archive); we didn't extend that to rename since it isn't a
 * destructive/moderation action and the existing PATCH gate already covers
 * the owner.
 *
 * IMPORTANT: renaming changes `name` only. `slug` is the room's identity —
 * it's the DDB partition key component, the gateway's `room:<slug>`
 * channel key, and every stored message/membership/subscription is keyed
 * off it. `updateRoom` below is keyed by `roomId` (not slug) and its
 * UpdateExpression never includes `slug`, so there is no code path here
 * that can change it — a rename can never orphan existing data.
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
    const planInput = (req.body as Record<string, unknown> | undefined)?.plan;
    if (planInput !== undefined) {
      partial.plan = readHangoutPlan(planInput, userId);
      if (partial.plan.version !== (room.plan?.version ?? 0) + 1) throw new AppError(409, 'This layout changed. Reload it before saving.');
      // Assignment never grants access to a private hangout.
      if (room.visibility === 'private') for (const member of partial.plan.rooms.flatMap(r => r.members)) {
        if (member !== room.createdBy && !await hangoutRoomMemberRepo.isMember(room.roomId, member)) throw new ValidationError('Assigned people must already have access to this private hangout');
      }
    }
    if (body.name !== undefined) {
      // Present but wrong type is a caller bug, not a no-op — reject it
      // rather than silently dropping the field (matches the CREATE path's
      // strictness on `name`).
      if (typeof body.name !== 'string') {
        throw new ValidationError('name must be a string');
      }
      // Trim + same 1–100 bound the CREATE path enforces, so a name that's
      // valid on create is always valid on rename and vice versa.
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
    if (body.settings !== undefined) {
      normalizeSettings(body.settings); // Validate before spreading: null and strings are not patches.
      partial.settings = normalizeSettings({ ...room.settings, ...body.settings });
    }

    let updated: HangoutRoom;
    try { updated = await hangoutRoomRepo.updateRoom(orgId, room.roomId, partial); }
    catch (error) {
      if (partial.plan && error instanceof Error && error.name === 'ConditionalCheckFailedException') throw new AppError(409, 'This layout changed. Reload it before saving.');
      throw error;
    }
    res.status(200).json({ room: updated });
  }),
);

/**
 * POST /api/rooms/:slug/archive — soft-delete. Owner OR org-admin (the
 * `ROOM_ADMIN_USER_IDS` env var, comma-separated). Everyone else gets 403.
 */
hangoutRoomsRouter.post(
  '/:slug/archive',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const { slug } = req.params;

    const room = await loadActiveRoom(orgId, slug);
    if (!isRoomOwnerOrAdmin(room, userId)) {
      throw new RoomAccessDeniedError(slug);
    }

    await hangoutRoomRepo.archiveRoom(orgId, room.roomId);
    res.status(200).json({ ok: true, slug });
  }),
);

/**
 * GET /api/rooms/:slug/members — who is in a private room. The owner and
 * members may look; the room admin page lists them here (it used to ask
 * for a route that did not exist and showed "Not Found" over an empty
 * list). An org-visible room has no roster and answers an empty list.
 */
hangoutRoomsRouter.get(
  '/:slug/members',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const slug = req.params.slug;
    const room = await loadActiveRoom(orgId, slug);
    const members = await hangoutRoomMemberRepo.listMembers(room.roomId);
    const mine = room.createdBy === userId || members.some((m) => m.userId === userId);
    if (room.visibility === 'private' && !mine) throw new ForbiddenError('Not a member of this room');
    res.json({ members });
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
    const body = (req.body ?? {}) as { userId?: string; role?: string; displayName?: string; byName?: string };

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
    // The room's conversation follows: the new member can read it, with its
    // history — a room member is entitled to the room's past.
    mirrorRoomMembers({
      slug,
      ownerId: room.createdBy,
      // The client knows the names (its people directory is presence-fed);
      // profiles are the fallback, ids the last resort.
      ownerName: (typeof body.byName === 'string' && body.byName.slice(0, 80)) || await displayNameOf(room.createdBy),
      members: [{ userId: body.userId, displayName: (typeof body.displayName === 'string' && body.displayName.slice(0, 80)) || await displayNameOf(body.userId) }],
    });
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
    // Out of the room, out of its conversation — the gateway evicts their
    // live connections too.
    mirrorRoomMemberRemoval({
      slug,
      userId: targetUserId,
      name: (typeof req.query.name === 'string' && req.query.name.slice(0, 80)) || await displayNameOf(targetUserId),
      byOwnerId: room.createdBy,
      byOwnerName: (typeof req.query.byName === 'string' && req.query.byName.slice(0, 80)) || await displayNameOf(room.createdBy),
    });
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

    const lobbyName = roomLobbyName(req, orgId, slug);

    // Per-tab key, forwarded to the video join so this bridge and the call
    // widget that follows it are recognised as the SAME client.
    //
    // Entering a room joins the video session TWICE: once here, and again
    // when HangoutOverlay calls useVideoCall.startCall against the same
    // lobby (the client deliberately drops the token this route returns —
    // see HangoutDomainProvider.joinRoomBySlug). Leaving hangs up once. That
    // net +1 participant per call is why sessions never end. videoSessions'
    // /join now collapses repeat joins from one client, and when the client
    // sends a `clientId` it must reach BOTH joins or they look like two
    // different tabs and the leak comes straight back. With no clientId the
    // collapse falls back to userId, which covers today's clients.
    const clientId =
      typeof (req.body as { clientId?: unknown } | undefined)?.clientId === 'string'
        ? ((req.body as { clientId: string }).clientId)
        : undefined;

    // Step 1 — find-or-create the live VideoSession. Pass the room's
    // recording setting EXPLICITLY (both on + off) so videoSessions.ts
    // honors per-room intent. Default-on behavior in videoSessions only
    // applies when no field is sent at all (DM/global calls); rooms
    // ALWAYS send the explicit choice so an opt-out actually disables
    // recording downstream.
    const createBody: Record<string, unknown> = {
      lobbyName,
      findOrCreate: true,
      recordingConfiguration: {
        enabled: room.settings.recordingEnabled === true,
        // The decision's reason travels with it, so the session row and the
        // call card can say "off for #room" rather than just "off".
        reason: room.settings.recordingEnabled === true ? 'room-setting-on' : 'room-setting-off',
        destination: 'local',
      },
    };
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
      body: { lobbyName, ...(clientId ? { clientId } : {}) },
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
    const body = (req.body ?? {}) as {
      sessionId?: string;
      participantId?: string;
      clientId?: string;
    };

    if (!body.sessionId) throw new ValidationError('sessionId is required');

    const room = await loadActiveRoom(orgId, slug);
    await assertCanAccess(room, userId, orgId);

    const lobbyName = roomLobbyName(req, orgId, slug);
    const end = await callSelf(req, `/api/video/sessions/${encodeURIComponent(body.sessionId)}/end`, {
      method: 'POST',
      body: {
        lobbyName,
        ...(body.participantId ? { participantId: body.participantId } : {}),
        // Second chance at naming the right entry when the caller lost its
        // participantId — see the /end route's clientId fallback.
        ...(body.clientId ? { clientId: body.clientId } : {}),
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
