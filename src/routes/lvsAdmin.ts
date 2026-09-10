/**
 * LVS admin / observability surface — W5.
 *
 * Operator endpoints for debugging the LVS recording pipeline:
 *
 *   GET /api/admin/recordings/health
 *     Returns the in-memory ring buffer of recent webhook deliveries
 *     (see `getRecentDeliveries` in routes/lvsWebhooks.ts) plus a
 *     small aggregate summary (totals, hmac/persist failure rates,
 *     lastReceivedAt). Process-local; lost on restart.
 *
 *   GET /api/admin/sessions
 *     Lists recent VideoSession rows (DDB Scan, capped at 50) along
 *     with whether the matching `video:arn-index:<arn>` Redis key
 *     exists — useful when triaging "recording came in but landed
 *     under unknown-callId" complaints (root cause is usually the
 *     arn-index entry not having been written or having expired).
 *
 *   GET /api/admin/bindings
 *     Dumps recent rows from the gateway-owned `call-session-bindings`
 *     table (via `ddbRaw` + `unmarshall`). Useful when debugging the
 *     callId ↔ lobbyName mapping that the webhook handler relies on
 *     to produce a non-`unknown-…` callId.
 *
 * All endpoints are admin-token gated via `requireAdmin`. None of them
 * mutate state.
 */

import { Router, Request, Response } from 'express';
import { DynamoDBClient, ScanCommand } from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { ScanCommand as DocScanCommand } from '@aws-sdk/lib-dynamodb';
import { requireAdmin } from '../middleware/requireAdmin';
import { asyncHandler } from '../middleware/error-handler';
import { getRecentDeliveries } from './lvsWebhooks';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import { VIDEO_SESSIONS_TABLE } from '../repositories/VideoSessionRepository';
import {
  HANGOUT_ROOMS_TABLE,
  type HangoutRoom,
} from '../repositories/HangoutRoomRepository';
import { hangoutRoomMemberRepo, videoSessionRepo } from '../repositories';
import log from '../lib/logger';

// Cap on cross-org room listing — admin endpoint only, but we still
// don't want a `/api/admin/rooms` call to drag back every row of a
// 10k-row table. Tracks the spec's "Cap at 200 rows for list".
const ADMIN_ROOMS_LIST_MAX = 200;

export const lvsAdminRouter = Router();

// Raw DDB client — needed for the bindings endpoint where we read the
// gateway-owned `call-session-bindings` table directly and want the
// AttributeValue map (passed through `unmarshall`) so the response
// mirrors the on-disk shape. Same pattern as lvsWebhooks.ts.
const ddbRaw = new DynamoDBClient({});

// --------------------------------------------------------------------
// GET /api/admin/video/recording-decision?lobbyName=<lobby>
//
// The recording decision of a lobby's most recent call — read by the
// gateway when a call ends, so the card it posts into the thread can say
// whether the call was recorded and, if not, why. The gateway has no user
// token at that moment (the participants have hung up), only the lobby
// name, so this sits behind the admin secret like the other
// service-to-service reads here. Absent decision → `recording: null`,
// which the gateway turns into "say nothing".
// --------------------------------------------------------------------
lvsAdminRouter.get(
  '/video/recording-decision',
  requireAdmin,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const lobbyName = typeof req.query.lobbyName === 'string' ? req.query.lobbyName : '';
    if (!lobbyName) {
      res.status(400).json({ error: 'lobbyName is required' });
      return;
    }
    const sessions = await videoSessionRepo.getSessionsByDocument(lobbyName);
    // Most recent first: at call end the row that just ended is the newest.
    const latest = sessions
      .slice()
      .sort((a, b) => String(b.startedAt ?? '').localeCompare(String(a.startedAt ?? '')))[0];
    if (!latest) {
      res.status(200).json({ lobbyName, sessionId: null, recording: null });
      return;
    }
    res.status(200).json({
      lobbyName,
      sessionId: latest.sessionId,
      status: latest.status,
      startedAt: latest.startedAt,
      recording: latest.recording ?? null,
    });
  }),
);

// --------------------------------------------------------------------
// GET /api/admin/recordings/health
// --------------------------------------------------------------------
lvsAdminRouter.get(
  '/recordings/health',
  requireAdmin,
  asyncHandler(async (_req: Request, res: Response): Promise<void> => {
    const all = getRecentDeliveries();
    // Newest first for operator legibility.
    const deliveries = all.slice().reverse();

    const totalReceived = deliveries.length;
    const hmacFailures = deliveries.filter((d) => !d.hmacOk).length;
    // "persistFailures" counted against deliveries that PASSED HMAC and
    // were `recording.completed` events — i.e., where persistence was
    // actually attempted. We approximate by counting hmacOk + non-ignored
    // entries that didn't persist.
    const persistAttempts = deliveries.filter(
      (d) =>
        d.hmacOk &&
        !!d.eventType &&
        d.eventType.toLowerCase().includes('completed'),
    );
    const persistFailures = persistAttempts.filter((d) => !d.persisted).length;
    const lastReceivedAt = deliveries[0]?.timestamp ?? null;

    res.json({
      deliveries,
      aggregate: {
        totalReceived,
        hmacFailRate: totalReceived ? hmacFailures / totalReceived : 0,
        persistFailRate: persistAttempts.length
          ? persistFailures / persistAttempts.length
          : 0,
        lastReceivedAt,
      },
    });
  }),
);

// --------------------------------------------------------------------
// GET /api/admin/sessions
// --------------------------------------------------------------------
lvsAdminRouter.get(
  '/sessions',
  requireAdmin,
  asyncHandler(async (_req: Request, res: Response): Promise<void> => {
    const limit = 50;

    // Scan the video-sessions table. DDB Scan returns un-ordered rows,
    // so we sort by startedAt desc in memory.
    let items: Array<Record<string, unknown>> = [];
    try {
      const out = await docClient.send(
        new DocScanCommand({
          TableName: VIDEO_SESSIONS_TABLE,
          Limit: Math.min(limit * 4, 200), // over-fetch to compensate for sort
        }),
      );
      items = (out.Items ?? []) as Array<Record<string, unknown>>;
    } catch (e: any) {
      log.warn({ err: e?.message ?? e }, '[lvs-admin] video-sessions scan failed');
    }

    items.sort((a, b) => {
      const av = String(a.startedAt ?? '');
      const bv = String(b.startedAt ?? '');
      return bv.localeCompare(av);
    });
    const recent = items.slice(0, limit);

    // For each session, check whether the Redis arn-index key still exists.
    // Redis may be unavailable — treat that as `arnIndexExists: false`
    // rather than failing the whole response.
    let redis: any = null;
    try {
      const { getRedisClient } = await import('../lib/redis-client.js');
      redis = await getRedisClient();
    } catch (e: any) {
      log.warn({ err: e?.message ?? e }, '[lvs-admin] redis client failed');
    }

    const results = [];
    for (const item of recent) {
      const channelArn = String(item.channelArn ?? '');
      let arnIndexExists = false;
      if (redis && channelArn) {
        try {
          const exists = await redis.exists(`video:arn-index:${channelArn}`);
          arnIndexExists = !!exists;
        } catch {
          // ignore individual lookup failures
        }
      }
      const participants = Array.isArray(item.participants)
        ? (item.participants as unknown[]).length
        : 0;
      results.push({
        sessionId: item.sessionId ?? null,
        lobbyName: item.documentId ?? null,
        channelArn: channelArn || null,
        status: item.status ?? null,
        startedAt: item.startedAt ?? null,
        participants,
        arnIndexExists,
      });
    }

    res.json({ sessions: results, count: results.length });
  }),
);

// --------------------------------------------------------------------
// GET /api/admin/bindings
//
// Reads recent rows from `call-session-bindings`, the gateway-owned
// table that maps callId ↔ lobbyName. We don't own this table, so we
// scan it via the raw AttributeValue API + unmarshall — same path the
// LVS webhook handler uses for the reverse lookup. Scan is acceptable
// here because the table is small in dev/MVP and this is an admin-only
// surface.
// --------------------------------------------------------------------
lvsAdminRouter.get(
  '/bindings',
  requireAdmin,
  asyncHandler(async (_req: Request, res: Response): Promise<void> => {
    const limit = 50;
    const table = tableName('call-session-bindings');
    let items: Array<Record<string, unknown>> = [];
    try {
      const out = await ddbRaw.send(
        new ScanCommand({
          TableName: table,
          Limit: Math.min(limit, 200),
        }),
      );
      items = (out.Items ?? []).map((row) => unmarshall(row));
    } catch (e: any) {
      log.warn(
        { err: e?.message ?? e, table },
        '[lvs-admin] call-session-bindings scan failed',
      );
    }
    res.json({ bindings: items, count: items.length });
  }),
);

// --------------------------------------------------------------------
// GET /api/admin/rooms
//
// Cross-org listing of hangout rooms for ops dashboards. The
// HangoutRoomRepository.listRoomsByOrg helper is org-scoped (its
// underlying GSI is keyed on orgId) — for an all-orgs view we Scan the
// base table directly. Capped at ADMIN_ROOMS_LIST_MAX to keep the
// response bounded even in worst-case fleet sizes.
//
// Returns {rooms, totalCount} so the dashboard can render "showing N of
// M" hints; totalCount is the number returned (i.e. equals rooms.length
// today; we don't pre-COUNT because that's a second Scan).
// --------------------------------------------------------------------
lvsAdminRouter.get(
  '/rooms',
  requireAdmin,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    log.info(
      { path: req.path, ip: req.ip },
      '[lvs-admin] GET /api/admin/rooms',
    );
    let items: HangoutRoom[] = [];
    try {
      const out = await docClient.send(
        new DocScanCommand({
          TableName: HANGOUT_ROOMS_TABLE,
          Limit: ADMIN_ROOMS_LIST_MAX,
        }),
      );
      items = (out.Items ?? []) as HangoutRoom[];
    } catch (e: any) {
      log.warn(
        { err: e?.message ?? e, table: HANGOUT_ROOMS_TABLE },
        '[lvs-admin] hangout-rooms scan failed',
      );
    }

    // Sort newest-active-first to match the sidebar's listRoomsByOrg
    // ordering — admins eyeballing the dashboard expect "recent" at top.
    items.sort((a, b) => {
      const av = String(a.lastActiveAt ?? a.createdAt ?? '');
      const bv = String(b.lastActiveAt ?? b.createdAt ?? '');
      return bv.localeCompare(av);
    });

    const rooms = items.slice(0, ADMIN_ROOMS_LIST_MAX);
    res.json({ rooms, totalCount: rooms.length });
  }),
);

// --------------------------------------------------------------------
// GET /api/admin/rooms/:slug
//
// Full detail view for a single room: the HangoutRoom row, its current
// member list (HangoutRoomMember rows by roomId), and basic stats.
// "Recent activity" — the spec mentions occupancy-delta history, which
// the gateway broadcasts ephemerally over WebSocket and does not
// persist; for now we surface the static stats (createdAt /
// lastActiveAt / archivedAt / member count) instead. If the gateway
// later writes a per-room activity log, this endpoint is the natural
// place to attach it.
//
// Slug → roomId resolution requires the orgId on the GSI; we Scan with
// a FilterExpression on slug because admins don't know which org owns
// a slug. Bounded by HANGOUT_ROOMS_TABLE's row count (small in MVP).
// --------------------------------------------------------------------
lvsAdminRouter.get(
  '/rooms/:slug',
  requireAdmin,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const slug = String(req.params.slug ?? '').trim();
    log.info(
      { path: req.path, slug, ip: req.ip },
      '[lvs-admin] GET /api/admin/rooms/:slug',
    );
    if (!slug) {
      res.status(400).json({ error: 'slug is required' });
      return;
    }

    // Locate the room by slug. We don't know the orgId here, so Scan
    // with a FilterExpression rather than Query. Capped by the table
    // size; OK for the admin-only surface.
    let room: HangoutRoom | null = null;
    try {
      const out = await docClient.send(
        new DocScanCommand({
          TableName: HANGOUT_ROOMS_TABLE,
          FilterExpression: 'slug = :s',
          ExpressionAttributeValues: { ':s': slug },
          Limit: ADMIN_ROOMS_LIST_MAX,
        }),
      );
      const found = (out.Items ?? []) as HangoutRoom[];
      room = found[0] ?? null;
    } catch (e: any) {
      log.warn(
        { err: e?.message ?? e, slug },
        '[lvs-admin] hangout-rooms slug lookup failed',
      );
    }

    if (!room) {
      res.status(404).json({ error: `Room not found: ${slug}` });
      return;
    }

    // Member list — HangoutRoomMemberRepository.listMembers keys on
    // roomId so we can answer for both `org` and `private` rooms
    // uniformly (for org rooms the table will be empty, which is fine).
    let members: unknown[] = [];
    try {
      members = await hangoutRoomMemberRepo.listMembers(room.roomId);
    } catch (e: any) {
      log.warn(
        { err: e?.message ?? e, roomId: room.roomId },
        '[lvs-admin] member listMembers failed',
      );
    }

    res.json({
      room,
      members,
      stats: {
        memberCount: members.length,
        createdAt: room.createdAt,
        lastActiveAt: room.lastActiveAt,
        archivedAt: room.archivedAt ?? null,
        state: room.state,
      },
      // Recent occupancy activity is broadcast ephemerally by the
      // gateway and not persisted; the dashboard should subscribe to
      // the rooms-index WS feed for live deltas. We return an empty
      // array here for a stable shape — future change can plug in a
      // gateway-side ring buffer if/when one is added.
      recentActivity: [],
    });
  }),
);
