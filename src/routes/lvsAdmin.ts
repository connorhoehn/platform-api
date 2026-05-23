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
import log from '../lib/logger';

export const lvsAdminRouter = Router();

// Raw DDB client — needed for the bindings endpoint where we read the
// gateway-owned `call-session-bindings` table directly and want the
// AttributeValue map (passed through `unmarshall`) so the response
// mirrors the on-disk shape. Same pattern as lvsWebhooks.ts.
const ddbRaw = new DynamoDBClient({});

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
