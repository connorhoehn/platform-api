/**
 * PipelineEventMqRelay — live pipeline progress onto the message-queue queue
 * the gateway already consumes (NFR #35).
 *
 * The gateway long-polls MQ queue `pipeline-events` (event-catalog
 * `MQ_QUEUE_NAMES.PIPELINE_EVENTS`) whenever MESSAGE_QUEUE_URL /
 * PIPELINE_EVENT_BUS_URL is set, and re-emits each body as a BusEvent — see
 * realtime-examples `src/pipeline-bridge/event-bus-subscriber.ts`. Until this
 * existed nothing published there: live events reached the gateway only over
 * Redis (the bus's own channel, `event-redis-relay.ts`, `deckReviseEvents.ts`),
 * so a deployment without the Redis path saw no run/step progress at all.
 *
 * WHAT: run and step lifecycle, approvals, and the deck-revise phases — the
 * progress a person watches. Not token streams, heartbeats or checkpoints
 * (high rate or large context; they stay on the Redis path).
 *
 * ONE DELIVERY: the body is the BusEvent unchanged, `id` included. When a
 * gateway also runs its Redis relay it drops the second copy by `id`
 * (realtime-examples `pipeline-bridge/event-dedupe.ts`). The MQ queue is a
 * work queue, so with N gateway replicas exactly one receives each event and
 * broadcasts it cluster-wide — the Redis pattern hands every replica a copy.
 *
 * ORDER + BACKPRESSURE: one sender, in publish order; at most MAX_BACKLOG
 * events wait (oldest dropped and counted) so an MQ outage costs bounded
 * memory and never blocks or fails a run.
 */
import type { BusEvent } from 'distributed-core';
import { MQ_QUEUE_NAMES } from '@connorhoehn/event-catalog';
import { createServiceRequestHeaders } from '@connorhoehn/service-runtime';
import log from '../lib/logger';

const relayLog = log.child({ component: 'pipeline-event-mq-relay' });

export const LIVE_MQ_QUEUE: string = MQ_QUEUE_NAMES.PIPELINE_EVENTS;
const MAX_BACKLOG = 1_000;
/** MQ's default max message is 256 KiB; leave room for the envelope. */
const MAX_BODY_BYTES = 200 * 1024;
/** Live progress is worthless after an hour; do not let a dead consumer hoard it. */
const RETENTION_SECONDS = 3_600;

const EXCLUDED = new Set([
  'pipeline:run:checkpoint',
  'pipeline:step:heartbeat',
]);

/** True for the progress events that go to MQ. */
export function isLiveMqEventType(type: unknown): boolean {
  if (typeof type !== 'string' || EXCLUDED.has(type)) return false;
  if (type.startsWith('pipeline:step:context.')) return false;
  return type.startsWith('pipeline:run:')
    || type.startsWith('pipeline:step:')
    || type.startsWith('pipeline:approval:')
    || type.startsWith('pipeline:deck:revise:');
}

export interface LiveMqConfig {
  enabled: boolean;
  url?: string;
  reason: string;
}

/**
 * `PIPELINE_LIVE_EVENTS_MQ=on|off`. Unset: on when the deployment names a
 * message queue (PIPELINE_EVENT_BUS_URL or MESSAGE_QUEUE_URL), off otherwise.
 */
export function resolveLiveMqConfig(env: NodeJS.ProcessEnv = process.env): LiveMqConfig {
  const url = (env.PIPELINE_EVENT_BUS_URL ?? '').trim() || (env.MESSAGE_QUEUE_URL ?? '').trim() || undefined;
  const flag = (env.PIPELINE_LIVE_EVENTS_MQ ?? '').trim().toLowerCase();
  if (flag === 'off' || flag === 'false' || flag === '0') return { enabled: false, reason: 'PIPELINE_LIVE_EVENTS_MQ=off' };
  if (!url) return { enabled: false, reason: 'no PIPELINE_EVENT_BUS_URL / MESSAGE_QUEUE_URL' };
  return { enabled: true, url: url.replace(/\/+$/, ''), reason: flag ? `PIPELINE_LIVE_EVENTS_MQ=${flag}` : 'message queue configured' };
}

export interface PipelineEventMqRelay {
  /** Queue one event (already filtered or not; non-progress types are ignored). */
  publish(event: { id?: unknown; type?: unknown }): void;
  readonly stats: { sent: number; failed: number; dropped: number; skippedLarge: number };
  /** Resolves when the backlog is empty (tests, shutdown). */
  flush(): Promise<void>;
  stop(): Promise<void>;
}

interface StartOpts {
  url: string;
  eventBus?: {
    subscribeAll(handler: (event: BusEvent) => Promise<void>): string;
    unsubscribe(subscriptionId: string): void;
  };
  fetchImpl?: typeof fetch;
  serviceAuthSecret?: string;
}

export function startPipelineEventMqRelay(opts: StartOpts): PipelineEventMqRelay {
  const fetchImpl = opts.fetchImpl ?? fetch;
  const queueUrl = `${opts.url}/api/v1/queues/${encodeURIComponent(LIVE_MQ_QUEUE)}`;
  const secret = opts.serviceAuthSecret ?? process.env.MQ_SERVICE_AUTH_SECRET;
  const sign = secret?.trim()
    ? createServiceRequestHeaders({ secret, serviceId: 'platform-api', audience: 'message-queue' })
    : undefined;
  const headers = (url: string, method: string, body: string): Record<string, string> => ({
    'content-type': 'application/json',
    ...(sign?.({ url, method, body }) ?? {}),
  });

  const stats = { sent: 0, failed: 0, dropped: 0, skippedLarge: 0 };
  const backlog: string[] = [];
  let pumping: Promise<void> | null = null;
  let stopped = false;
  let queueReady = false;

  let ensuring: Promise<void> | null = null;
  const ensureQueue = (): Promise<void> => {
    if (queueReady) return Promise.resolve();
    ensuring ??= putQueue().finally(() => { ensuring = null; });
    return ensuring;
  };
  const putQueue = async (): Promise<void> => {
    const body = JSON.stringify({ configuration: { message_retention_period_seconds: RETENTION_SECONDS } });
    const response = await fetchImpl(queueUrl, { method: 'PUT', headers: headers(queueUrl, 'PUT', body), body, redirect: 'error' });
    // 409: it exists with another configuration — it exists, which is all we need.
    if (!response.ok && response.status !== 409) throw new Error(`ensure queue got HTTP ${response.status}`);
    queueReady = true;
  };

  const send = async (eventJson: string): Promise<void> => {
    await ensureQueue();
    const url = `${queueUrl}/messages`;
    const body = JSON.stringify({ message_body: eventJson });
    const response = await fetchImpl(url, { method: 'POST', headers: headers(url, 'POST', body), body, redirect: 'error' });
    if (response.status === 404 || (response.status === 400 && !queueReady)) queueReady = false;
    if (!response.ok) throw new Error(`send got HTTP ${response.status}`);
  };

  const pump = async (): Promise<void> => {
    while (backlog.length > 0 && !stopped) {
      const next = backlog.shift()!;
      try {
        await send(next);
        stats.sent += 1;
      } catch (err) {
        stats.failed += 1;
        if (stats.failed === 1 || stats.failed % 100 === 0) {
          relayLog.warn({ err: err instanceof Error ? err.message : String(err), failed: stats.failed }, 'live pipeline event not sent to MQ');
        }
      }
    }
  };

  const publish = (event: { id?: unknown; type?: unknown }): void => {
    if (stopped || !isLiveMqEventType(event?.type)) return;
    let json: string;
    try { json = JSON.stringify(event); } catch { return; }
    if (Buffer.byteLength(json) > MAX_BODY_BYTES) { stats.skippedLarge += 1; return; }
    if (backlog.length >= MAX_BACKLOG) { backlog.shift(); stats.dropped += 1; }
    backlog.push(json);
    kick();
  };

  function kick(): void {
    if (pumping) return;
    pumping = pump().finally(() => {
      pumping = null;
      // An event pushed between the loop's last check and here.
      if (backlog.length > 0 && !stopped) kick();
    });
  }

  // Create the queue up front so a gateway polling for it attaches before the
  // first run, not up to its 30 s backoff after it.
  ensureQueue().catch((err: unknown) => {
    relayLog.warn({ err: err instanceof Error ? err.message : String(err) }, 'pipeline-events queue not ensured at start; retried on first send');
  });
  const subscriptionId = opts.eventBus?.subscribeAll(async (event) => { publish(event); });
  relayLog.info({ queue: LIVE_MQ_QUEUE, url: opts.url }, 'live pipeline events → MQ');

  return {
    publish,
    stats,
    async flush() {
      while (pumping) await pumping;
    },
    async stop() {
      if (subscriptionId && opts.eventBus) {
        try { opts.eventBus.unsubscribe(subscriptionId); } catch { /* already gone */ }
      }
      await this.flush();
      stopped = true;
    },
  };
}
