// NFR #170 (realtime-examples): a per-run sequence number on the run's
// progress events.
//
// The gateway reads the MQ `pipeline-events` queue as a work queue: with two
// gateway replicas each takes some of a run's events and forwards them to the
// other over the cluster, so a client can see `pipeline:step:started` before
// the `pipeline:run:started` that precedes it (Loop 28 (ppt), #169). The bus
// `version` is per process and shared by every run (gaps), so a consumer
// cannot tell "late" from "never coming". `runSeq` is contiguous per run —
// 1 for the first progress event, +1 for each after — so a consumer can put a
// run's frames back in order and knows when one is missing.
//
// Only the progress events (event-catalog `isPipelineMqLaneEvent`: run, step
// and approval lifecycle) are numbered. Tokens, checkpoints and heartbeats
// ride the Redis hot lane and may be dropped under backpressure; numbering
// them would leave gaps a consumer would wait on.
//
// Wraps the module EventBus's `publish` once, before the event is built, so
// every lane (dc pub-sub, the Redis relay, the MQ relay) carries the same
// number. The counter is per process: the executor running a run publishes
// all of its events from one node. A run resumed on another pod starts again
// at 1 — a consumer treats a number it has passed as late, not a duplicate.

import { isPipelineMqLaneEvent } from '@connorhoehn/event-catalog';

const MAX_TRACKED_RUNS = 2000;

type Publish = (type: string, payload: unknown, ...rest: unknown[]) => Promise<unknown>;

interface StampableBus {
  publish: Publish;
  __runSeqStamped?: boolean;
}

/** Wraps `eventBus.publish` in place; idempotent. Returns the bus. */
export function stampPipelineRunSeq<T>(eventBus: T): T {
  const bus = eventBus as unknown as StampableBus;
  if (!bus || typeof bus.publish !== 'function' || bus.__runSeqStamped) return eventBus;
  const original = bus.publish.bind(bus);
  const byRun = new Map<string, number>();

  bus.publish = (type: string, payload: unknown, ...rest: unknown[]) => {
    const runId = (payload as { runId?: unknown } | undefined)?.runId;
    if (typeof runId !== 'string' || !runId || !isPipelineMqLaneEvent(type)) return original(type, payload, ...rest);
    if (typeof (payload as { runSeq?: unknown }).runSeq === 'number') return original(type, payload, ...rest);
    // Numbered synchronously, in call order — the order the executor emitted.
    const runSeq = (byRun.get(runId) ?? 0) + 1;
    byRun.delete(runId); // re-insert: Map order is least recently touched first
    byRun.set(runId, runSeq);
    while (byRun.size > MAX_TRACKED_RUNS) byRun.delete(byRun.keys().next().value as string);
    return original(type, { ...(payload as Record<string, unknown>), runSeq }, ...rest);
  };
  bus.__runSeqStamped = true;
  return eventBus;
}
