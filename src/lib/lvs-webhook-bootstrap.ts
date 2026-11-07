/**
 * lvs-webhook-bootstrap — at platform-api startup, register a global
 * webhook subscription with LVS so `recording.completed` events land at
 * /hooks/lvs/recording. Without this, recordings finish silently and
 * the DDB table stays empty.
 *
 * Idempotency story:
 *   LVS subscription state is in-memory (per the upstream design), so
 *   restarting LVS forgets all subscriptions. Restarting platform-api
 *   would otherwise accumulate duplicate subs in LVS. To avoid that we:
 *     1. List existing subs first (GET /api/event-subscriptions).
 *     2. If one matching our (url, filter) already exists, no-op.
 *     3. Otherwise POST a new one.
 *   Best-effort — failures log + retry on next boot.
 *
 * Env contract:
 *   VIDEO_API_URL              — LVS base URL (already used by videoSessions)
 *   LVS_WEBHOOK_BOOTSTRAP_URL  — Where LVS should POST events. Skip if unset.
 *   LVS_WEBHOOK_SECRET         — HMAC secret (passed through to LVS for
 *                                 outbound signing).
 *   VIDEO_ADMIN_TOKEN /
 *   LVS_ADMIN_TOKEN            — Auth for /api/event-subscriptions (LVS
 *                                 admin route), first one set wins. No
 *                                 default: unset skips the bootstrap.
 *   LVS_WEBHOOK_REBOOTSTRAP_MS — If >0, re-run the bootstrap on this
 *                                 interval. Default 30000 (30s). LVS
 *                                 keeps subscriptions in memory only —
 *                                 if LVS restarts after platform-api,
 *                                 our subscription is silently dropped
 *                                 and `recording.completed` events never
 *                                 land in DDB. Periodic re-bootstrap
 *                                 self-heals that. Set to 0 to disable.
 */

import log from './logger';
import { lvsAdminToken } from './lvsAdminToken';

const FILTER = 'IVS Recording State Change';

interface ExistingSubscription {
  id?: string;
  url?: string;
  filter?: string;
}

export async function bootstrapLvsWebhook(): Promise<void> {
  const lvsUrl = process.env.VIDEO_API_URL;
  const receiverUrl = process.env.LVS_WEBHOOK_BOOTSTRAP_URL;
  if (!lvsUrl || !receiverUrl) {
    log.info(
      { lvsUrl: !!lvsUrl, receiverUrl: !!receiverUrl },
      '[lvs-webhook-bootstrap] skipping — VIDEO_API_URL or LVS_WEBHOOK_BOOTSTRAP_URL not set',
    );
    return;
  }
  // Fail closed: without a configured admin token there is nothing to
  // register with — never fall back to a guessable literal.
  const adminToken = lvsAdminToken(['VIDEO_ADMIN_TOKEN', 'LVS_ADMIN_TOKEN']);
  if (!adminToken) {
    log.error('[lvs-webhook-bootstrap] skipping — VIDEO_ADMIN_TOKEN / LVS_ADMIN_TOKEN not set');
    return;
  }
  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${adminToken}`,
  };

  // Step 1: list existing subs. Skip if already registered with same
  // (url, filter). Otherwise we'd accumulate duplicates on every
  // platform-api restart.
  try {
    const listRes = await fetch(`${lvsUrl}/api/event-subscriptions`, { headers });
    if (listRes.ok) {
      // LVS returns a BARE ARRAY from GET /api/event-subscriptions; the old
      // { subscriptions: [...] } read always yielded [] so the dedup check
      // never matched and every boot POSTed another duplicate (2,356 subs
      // accumulated by 2026-08-21 — each recording event then fanned out
      // into thousands of webhook deliveries and froze active calls).
      const body = (await listRes.json()) as
        | ExistingSubscription[]
        | { subscriptions?: ExistingSubscription[] };
      const subs = Array.isArray(body)
        ? body
        : Array.isArray(body?.subscriptions) ? body.subscriptions : [];
      const match = subs.find(
        (s) => s.url === receiverUrl && (s.filter === FILTER || s.filter === '*'),
      );
      if (match) {
        log.info({ subId: match.id }, '[lvs-webhook-bootstrap] existing subscription found — no-op');
        return;
      }
    } else if (listRes.status === 404) {
      // LVS instance doesn't expose the list endpoint — fall through
      // to POST. Worst case is a duplicate sub, which is benign
      // because LVS dispatches once per URL match.
      log.warn('[lvs-webhook-bootstrap] /api/event-subscriptions GET returned 404 — will POST anyway');
    } else {
      log.warn(
        { status: listRes.status },
        '[lvs-webhook-bootstrap] list call failed — will attempt POST anyway',
      );
    }
  } catch (e: any) {
    log.warn(
      { err: e?.message ?? e },
      '[lvs-webhook-bootstrap] list call threw — will attempt POST anyway',
    );
  }

  // Step 2: POST a fresh subscription.
  try {
    const postRes = await fetch(`${lvsUrl}/api/event-subscriptions`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ url: receiverUrl, filter: FILTER }),
    });
    if (!postRes.ok) {
      const text = await postRes.text().catch(() => '');
      log.error(
        { status: postRes.status, body: text.slice(0, 200) },
        '[lvs-webhook-bootstrap] subscription POST failed',
      );
      return;
    }
    const body = (await postRes.json().catch(() => ({}))) as { id?: string };
    log.info(
      { subId: body.id, url: receiverUrl, filter: FILTER },
      '[lvs-webhook-bootstrap] subscription registered',
    );
  } catch (e: any) {
    log.error(
      { err: e?.message ?? e },
      '[lvs-webhook-bootstrap] subscription POST threw',
    );
  }
}

/**
 * Start a recurring re-bootstrap to self-heal after LVS restarts.
 * LVS keeps `_globalSubs[]` in memory only, so a SFU bounce silently drops
 * our subscription and `recording.completed` events stop reaching
 * /hooks/lvs/recording. The list-then-POST is idempotent (matching subs
 * are no-ops), so the only steady-state cost is one GET per tick.
 *
 * Returns a stop function for tests / graceful shutdown.
 */
export function startLvsWebhookReconciler(): () => void {
  const raw = Number(process.env.LVS_WEBHOOK_REBOOTSTRAP_MS);
  const intervalMs = Number.isFinite(raw) && raw >= 0 ? raw : 30_000;
  if (intervalMs === 0) {
    log.info('[lvs-webhook-bootstrap] periodic reconciler disabled (LVS_WEBHOOK_REBOOTSTRAP_MS=0)');
    return () => { /* noop */ };
  }
  const timer = setInterval(() => {
    void bootstrapLvsWebhook().catch((e: unknown) =>
      log.warn({ err: (e as Error)?.message ?? String(e) }, '[lvs-webhook-bootstrap] periodic re-bootstrap failed'),
    );
  }, intervalMs);
  // Don't keep the event loop alive just for this timer.
  if (typeof timer.unref === 'function') timer.unref();
  log.info({ intervalMs }, '[lvs-webhook-bootstrap] periodic reconciler started');
  return () => clearInterval(timer);
}
