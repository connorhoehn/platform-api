// The durable side of the pipeline dead-letter queue.
//
// The DLQ operators read and redrive is `InMemoryDeadLetterQueue` — a Map.
// Every entry in it (a bus dead letter, or a failed run filed by
// failedRunDeadLetters.ts) is also written to the `pipeline-dlq` table, but
// until now nothing ever read the table back: a restart emptied the queue
// and the rows sat there until their TTL. This module is the read side and
// the bookkeeping that keeps the two in step:
//
//   rehydrateDeadLetters  — at boot, put every persisted row back into memory
//   forgetDeadLetters     — after a redrive or a purge, drop the rows too
//
// The store is exposed through a module singleton (like getPipelineBridge)
// so the DLQ route can forget purged entries without bootstrap having to
// thread the repository through Express.

import { wrap } from 'distributed-core/messaging-queue';
import type { BusEvent } from 'distributed-core';
import type { DeadLetterQueue, Envelope } from 'distributed-core/messaging-queue';
import type { DLQEntry, DLQRepository } from '../repositories/DLQRepository';
import baseLog from '../lib/logger';

const log = baseLog.child({ service: 'dead-letter-store' });

/** The slice of DLQRepository this module needs; tests hand in a fake. */
export type DeadLetterStore = Pick<DLQRepository, 'create' | 'delete' | 'list'>;

let store: DeadLetterStore | null = null;

export function setDeadLetterStore(s: DeadLetterStore | null): void {
  store = s;
}

export function getDeadLetterStore(): DeadLetterStore | null {
  return store;
}

/** The in-memory DLQ's write surface — `put` is all rehydration needs. */
export type DeadLetterSink = Pick<DeadLetterQueue<BusEvent>, 'put'>;

export const REHYDRATE_CAP = 500;
const PAGE_SIZE = 100;

/** Build the envelope a persisted row stands for, dated when it failed, not now. */
export function envelopeFromStoredEntry(entry: DLQEntry): { env: Envelope<BusEvent>; failedAtMs: number } {
  const parsed = Date.parse(entry.failedAt);
  const failedAtMs = Number.isFinite(parsed) ? parsed : Date.now();
  const event = {
    id: entry.messageId,
    type: entry.eventType,
    payload: entry.eventPayload,
    timestamp: failedAtMs,
    sourceNodeId: 'dead-letter-store',
  } as unknown as BusEvent;
  const env = wrap<BusEvent>(event, {
    id: entry.messageId,
    now: () => failedAtMs,
    attemptCount: entry.totalAttempts,
  });
  return { env, failedAtMs };
}

/**
 * Put every persisted dead letter back into the in-memory queue. Pages
 * through the store newest-first and stops at `cap` entries — an operator
 * who has let thousands pile up gets the most recent 500, not a slow boot.
 * The put is idempotent on messageId, so running this against a queue that
 * already holds some of the rows is harmless. Returns how many were put.
 */
export async function rehydrateDeadLetters(
  dlq: DeadLetterSink,
  from: Pick<DeadLetterStore, 'list'>,
  opts: { cap?: number } = {},
): Promise<number> {
  const cap = opts.cap ?? REHYDRATE_CAP;
  let put = 0;
  let exclusiveStartKey: Record<string, unknown> | undefined;
  while (put < cap) {
    const page = await from.list({ limit: Math.min(PAGE_SIZE, cap - put), exclusiveStartKey });
    for (const entry of page.entries) {
      if (put >= cap) break;
      const { env, failedAtMs } = envelopeFromStoredEntry(entry);
      await dlq.put(env, { lastError: entry.lastError, failedAtMs, totalAttempts: entry.totalAttempts });
      put += 1;
    }
    if (!page.lastEvaluatedKey || page.entries.length === 0) break;
    exclusiveStartKey = page.lastEvaluatedKey;
  }
  if (put > 0) log.info({ count: put, cap }, 'dead letters rehydrated from the store');
  return put;
}

/** Drop rows for entries that left the in-memory queue. Never throws; a miss is logged. */
export async function forgetDeadLetters(from: Pick<DeadLetterStore, 'delete'> | null, ids: ReadonlyArray<string>): Promise<void> {
  if (!from) return;
  await Promise.all(ids.map(async (id) => {
    try {
      await from.delete(id);
    } catch (err) {
      log.warn({ id, err: err instanceof Error ? err.message : String(err) }, 'could not delete the dead letter from the store');
    }
  }));
}
