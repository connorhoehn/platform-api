/**
 * In-process wake-up for the outbox drainers (realtime-examples NFR #133).
 *
 * Both outboxes are drained on a 2 s poll, so an event written just after a
 * poll waited up to 2 s before anything published it — and a new run crosses
 * TWO outboxes before a gateway hears of it (its source event on the durable
 * catalog outbox, then the work-graph projection's change row). Measured on
 * 2026-09-24: 1.1–1.9 s of the 3.2–4.8 s it took a run to reach Active Now
 * was the first outbox's poll alone.
 *
 * A writer that has just committed a row calls `wakeOutbox(name)`; the drainer
 * running in this process (if any) polls now instead of at its next tick. It
 * changes nothing about correctness: the drainer's lease and row conditions
 * still decide who publishes, the timer still runs, and a replica that does
 * not hold the lease simply finds nothing to do.
 */
export type OutboxName = 'platform-events' | 'work-graph';

const wakers = new Map<OutboxName, () => void>();

/** A drainer registers how to wake it; the returned function unregisters. */
export function registerOutboxWake(name: OutboxName, wake: () => void): () => void {
  wakers.set(name, wake);
  return () => { if (wakers.get(name) === wake) wakers.delete(name); };
}

/** Best effort: never throws, never waits. */
export function wakeOutbox(name: OutboxName): void {
  try { wakers.get(name)?.(); } catch { /* the timer still drains it */ }
}

