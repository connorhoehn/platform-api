// realtime-examples NFR #197: how long the pipeline bus's `publish` holds the
// executor, and where that time goes.
//
// Wraps `eventBus.publish` (outermost, installed after the runSeq/artifact
// stamps and the MQ relay tap) and the bus's pub-sub `publish` for the bus
// topic. Phases, per event:
//   * total   — the executor's `await eventBus.publish(...)`;
//   * wal     — from the call to the pub-sub hand-off (the WAL append + fsync,
//               queued behind every earlier append);
//   * pubsub  — the pub-sub hand-off itself (Redis PUBLISH in the cluster).
// Kept in small ring buffers and served as percentiles on
// `GET /internal/bus-publish-timing` (`?reset=1` clears), and observed into
// `pipeline_operation_duration_ms{operation="bus.publish.<phase>"}`.

import { monitorEventLoopDelay, performance } from 'perf_hooks';
import { recordPipelineOperationDuration } from '../observability/metrics';

// Event-loop delay over the same window: a phase that is mostly waiting for
// the loop (not for disk or Redis) shows up here, not in its own work.
const loopDelay = monitorEventLoopDelay({ resolution: 10 });
loopDelay.enable();
// Event-loop utilization since the last reset (NFR #202).
let eluMark = performance.eventLoopUtilization();

const RING = 2000;

type Phase = 'total' | 'wal' | 'pubsub';

class Ring {
  private values: number[] = [];
  private next = 0;
  push(v: number): void {
    if (this.values.length < RING) this.values.push(v);
    else this.values[this.next] = v;
    this.next = (this.next + 1) % RING;
  }
  reset(): void { this.values = []; this.next = 0; }
  summary(): { n: number; p50: number | null; p95: number | null; p99: number | null; max: number | null } {
    const s = [...this.values].sort((a, b) => a - b);
    const q = (p: number) => (s.length ? Math.round(s[Math.min(s.length - 1, Math.ceil(p * s.length) - 1)] * 10) / 10 : null);
    return { n: s.length, p50: q(0.5), p95: q(0.95), p99: q(0.99), max: s.length ? Math.round(s[s.length - 1] * 10) / 10 : null };
  }
}

const rings: Record<Phase, Ring> = { total: new Ring(), wal: new Ring(), pubsub: new Ring() };

function observe(phase: Phase, ms: number): void {
  rings[phase].push(ms);
  recordPipelineOperationDuration(`bus.publish.${phase}`, ms);
}

export function busPublishTimingSummary(reset = false): Record<Phase, ReturnType<Ring['summary']>> & {
  eventLoopDelayMs: { p50: number; p95: number; p99: number; max: number };
  eventLoopUtilization: number;
} {
  const ms = (ns: number) => Math.round(ns / 1e5) / 10;
  const out = {
    total: rings.total.summary(),
    wal: rings.wal.summary(),
    pubsub: rings.pubsub.summary(),
    eventLoopDelayMs: {
      p50: ms(loopDelay.percentile(50)),
      p95: ms(loopDelay.percentile(95)),
      p99: ms(loopDelay.percentile(99)),
      max: ms(loopDelay.max),
    },
    eventLoopUtilization: Math.round(performance.eventLoopUtilization(eluMark).utilization * 1000) / 1000,
  };
  if (reset) {
    for (const r of Object.values(rings)) r.reset();
    loopDelay.reset();
    eluMark = performance.eventLoopUtilization();
  }
  return out;
}

interface TimableBus {
  publish: (type: string, payload: unknown, ...rest: unknown[]) => Promise<unknown>;
  pubsub?: { publish?: (topic: string, payload: unknown) => Promise<unknown> };
  config?: { topic?: string };
  _versionCounter?: number;
  __publishTimed?: boolean;
}

/**
 * Wraps `eventBus.publish` (and its pub-sub hand-off) in place; idempotent.
 * The dc bus numbers an event synchronously inside `publish` (its
 * `_versionCounter`, +1 per call), so a call is paired with the event pub-sub
 * later sees by that version.
 */
export function timeBusPublish<T>(eventBus: T): T {
  const bus = eventBus as unknown as TimableBus;
  if (!bus || typeof bus.publish !== 'function' || bus.__publishTimed) return eventBus;
  const original = bus.publish;
  const startByVersion = new Map<number, number>();
  const canPair = typeof bus._versionCounter === 'number';
  bus.publish = function timedPublish(this: unknown, ...args: [string, unknown, ...unknown[]]) {
    const t0 = performance.now();
    if (canPair) {
      startByVersion.set((bus._versionCounter ?? 0) + 1, t0);
      if (startByVersion.size > RING) startByVersion.delete(startByVersion.keys().next().value as number);
    }
    return Promise.resolve(original.apply(this ?? bus, args)).finally(() => observe('total', performance.now() - t0));
  } as TimableBus['publish'];
  const pubsub = bus.pubsub;
  const topic = bus.config?.topic;
  if (canPair && pubsub && typeof pubsub.publish === 'function' && topic) {
    const pubsubPublish = pubsub.publish.bind(pubsub);
    pubsub.publish = (t: string, payload: unknown) => {
      const v = t === topic ? (payload as { version?: unknown } | null)?.version : undefined;
      const started = typeof v === 'number' ? startByVersion.get(v) : undefined;
      if (started === undefined) return pubsubPublish(t, payload);
      startByVersion.delete(v as number);
      const tp = performance.now();
      observe('wal', tp - started);
      return Promise.resolve(pubsubPublish(t, payload)).finally(() => observe('pubsub', performance.now() - tp));
    };
  }
  bus.__publishTimed = true;
  return eventBus;
}
