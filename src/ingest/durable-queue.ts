// platform-api/src/ingest/durable-queue.ts
//
// Minimal write-ahead-log (WAL) durability layer for the ingest queues.
//
// Three components:
//   1. QueueStore — minimal interface (subset of MQ's MessageStore) that
//      only covers the message-body persistence operations needed by
//      DurableIngestQueue. Queue-config persistence is not needed here
//      (bootstrap owns queue creation).
//   2. InMemoryQueueStore — no-op; preserves byte-identical behaviour
//      when INGEST_QUEUE_DURABLE is not set.
//   3. WalQueueStore — persists envelopes to a JSON lines file so pending
//      items survive process crashes. Suitable for single-node posture.
//      File is created on first write; truncated to zero on init if the
//      flush-on-complete path cleaned up correctly.
//   4. DurableIngestQueue<T> — extends InMemoryBatchedQueue; persist-before-
//      enqueue on enqueueBatch, delete-after-complete on completeBatch,
//      nack-no-retry deletes, hydrateFromStore() for boot recovery.
//
// Usage in bootstrapIngest:
//   INGEST_QUEUE_DURABLE=true  → WalQueueStore backed queue
//   unset / false              → InMemoryQueueStore (legacy behaviour)

import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {
  InMemoryBatchedQueue,
  type BatchCompleteResult,
  type BatchEnqueueResult,
  type BatchLease,
  type BatchLeaseOptions,
  type BatchNackOptions,
} from 'distributed-core/messaging-queue';
import type { Envelope } from 'distributed-core/messaging-queue';

// ---------------------------------------------------------------------------
// QueueStore interface
// ---------------------------------------------------------------------------

/**
 * Minimal durable store for queue message bodies. Only covers the subset
 * of MessageStore operations required for bootstrap-level WAL durability.
 */
export interface QueueStore {
  /** One-time async init (create file, open connection, etc). */
  init(queueName: string): Promise<void>;
  /** Optional teardown. */
  shutdown(): Promise<void>;
  /** Persist a batch of envelopes (called before in-memory enqueue). */
  putBatch(queueName: string, envs: ReadonlyArray<Envelope<unknown>>): Promise<void>;
  /** Delete one persisted envelope by id (called after successful complete). */
  delete(queueName: string, id: string): Promise<void>;
  /** Load all persisted envelopes for a queue (called at boot for hydration). */
  loadPending(queueName: string): Promise<Envelope<unknown>[]>;
}

// ---------------------------------------------------------------------------
// InMemoryQueueStore — no-op
// ---------------------------------------------------------------------------

/** No-op store — byte-identical to plain InMemoryBatchedQueue behaviour. */
export class InMemoryQueueStore implements QueueStore {
  async init(_queueName: string): Promise<void> { /* no-op */ }
  async shutdown(): Promise<void> { /* no-op */ }
  async putBatch(_queueName: string, _envs: ReadonlyArray<Envelope<unknown>>): Promise<void> { /* no-op */ }
  async delete(_queueName: string, _id: string): Promise<void> { /* no-op */ }
  async loadPending(_queueName: string): Promise<Envelope<unknown>[]> { return []; }
}

// ---------------------------------------------------------------------------
// WalQueueStore — JSON-lines file-backed WAL
// ---------------------------------------------------------------------------

/**
 * JSON-lines WAL store. Each persisted envelope is one line of JSON.
 * Completed (acked) envelopes are removed by rewriting the file without
 * the completed ids.
 *
 * Crash semantics:
 *   - enqueueBatch appends BEFORE in-memory push → crash after append but
 *     before push → envelope replayed on next boot (at-least-once).
 *   - completeBatch rewrites file AFTER in-memory complete → crash after
 *     in-memory complete but before rewrite → envelope replayed on boot
 *     (benign: worker re-processes an already-completed item, idempotency
 *     guards on the consumer side handle it).
 *
 * Not suitable for high-throughput (full-file rewrite on each complete).
 * Single-node ingest load (hundreds/day) is well within tolerance.
 */
export class WalQueueStore implements QueueStore {
  private readonly dir: string;

  /**
   * @param dir Directory where WAL files are stored. Defaults to
   *            `$TMPDIR/platform-api-ingest-wal` so tests stay sandboxed.
   *            Override with `INGEST_QUEUE_WAL_DIR` env var.
   */
  constructor(dir?: string) {
    this.dir = dir ?? process.env.INGEST_QUEUE_WAL_DIR ?? path.join(os.tmpdir(), 'platform-api-ingest-wal');
  }

  private walPath(queueName: string): string {
    // Sanitize queue name to a safe filename.
    const safe = queueName.replace(/[^a-zA-Z0-9_-]/g, '_');
    return path.join(this.dir, `${safe}.wal`);
  }

  async init(queueName: string): Promise<void> {
    fs.mkdirSync(this.dir, { recursive: true });
    const p = this.walPath(queueName);
    if (!fs.existsSync(p)) {
      fs.writeFileSync(p, '', 'utf-8');
    }
  }

  async shutdown(): Promise<void> { /* no-op */ }

  async putBatch(queueName: string, envs: ReadonlyArray<Envelope<unknown>>): Promise<void> {
    const p = this.walPath(queueName);
    const lines = envs.map((e) => JSON.stringify(e) + '\n').join('');
    fs.appendFileSync(p, lines, 'utf-8');
  }

  async delete(queueName: string, id: string): Promise<void> {
    const p = this.walPath(queueName);
    if (!fs.existsSync(p)) return;
    const content = fs.readFileSync(p, 'utf-8');
    const kept = content
      .split('\n')
      .filter((line) => {
        if (!line.trim()) return false;
        try {
          const env = JSON.parse(line) as Envelope<unknown>;
          return env.id !== id;
        } catch {
          return true; // keep malformed lines (don't silently lose data)
        }
      })
      .join('\n');
    fs.writeFileSync(p, kept ? kept + '\n' : '', 'utf-8');
  }

  async loadPending(queueName: string): Promise<Envelope<unknown>[]> {
    const p = this.walPath(queueName);
    if (!fs.existsSync(p)) return [];
    const content = fs.readFileSync(p, 'utf-8');
    const envelopes: Envelope<unknown>[] = [];
    for (const line of content.split('\n')) {
      if (!line.trim()) continue;
      try {
        envelopes.push(JSON.parse(line) as Envelope<unknown>);
      } catch {
        // Skip corrupt lines — log is best-effort WAL, not audit trail.
      }
    }
    return envelopes;
  }
}

// ---------------------------------------------------------------------------
// DurableIngestQueue<T>
// ---------------------------------------------------------------------------

export interface DurableIngestQueueOptions {
  store: QueueStore;
  queueName: string;
  /** Skip durable persistence during boot hydration replay. */
  skipPersistOnEnqueue?: boolean;
}

/**
 * InMemoryBatchedQueue extended with durable persistence via QueueStore.
 *
 *   - enqueueBatch: store.putBatch THEN super.enqueueBatch
 *   - completeBatch: super.completeBatch THEN store.delete for each acked id
 *   - nackBatch(retry=false): super.nackBatch THEN store.delete (drop path)
 *   - hydrateFromStore(): load WAL → replay into super (skipping persist)
 */
export class DurableIngestQueue<T> extends InMemoryBatchedQueue<T> {
  private readonly store: QueueStore;
  private readonly queueName: string;
  private suspendPersist = false;

  constructor(opts: DurableIngestQueueOptions) {
    super();
    this.store = opts.store;
    this.queueName = opts.queueName;
    this.suspendPersist = opts.skipPersistOnEnqueue ?? false;
  }

  /** Load WAL → replay into in-memory pending. Call once at boot. */
  async hydrateFromStore(): Promise<number> {
    const pending = await this.store.loadPending(this.queueName);
    if (pending.length === 0) return 0;
    this.suspendPersist = true;
    try {
      await super.enqueueBatch(pending as ReadonlyArray<Envelope<T>>);
    } finally {
      this.suspendPersist = false;
    }
    return pending.length;
  }

  override async enqueueBatch(
    envs: ReadonlyArray<Envelope<T>>,
  ): Promise<BatchEnqueueResult> {
    if (!this.suspendPersist) {
      await this.store.putBatch(
        this.queueName,
        envs as ReadonlyArray<Envelope<unknown>>,
      );
    }
    return super.enqueueBatch(envs);
  }

  override async completeBatch(
    leases: ReadonlyArray<BatchLease<T>>,
  ): Promise<BatchCompleteResult> {
    const result = await super.completeBatch(leases);
    for (const id of result.completed) {
      await this.store.delete(this.queueName, id).catch(() => {
        // WAL delete failure is non-fatal — TTL / manual reap handles stale lines.
      });
    }
    return result;
  }

  override async nackBatch(
    leases: ReadonlyArray<BatchLease<T>>,
    opts?: BatchNackOptions,
  ): Promise<BatchCompleteResult> {
    const retry = opts?.retry ?? true;
    const result = await super.nackBatch(leases, opts);
    if (!retry) {
      // Caller routing to DLQ — remove from WAL so it doesn't resurrect on boot.
      for (const id of result.completed) {
        await this.store.delete(this.queueName, id).catch(() => { /* non-fatal */ });
      }
    }
    return result;
  }

  // Lease is in-memory only (same Phase-1 posture as MQ service).
  override async leaseBatch(
    workerId: string,
    opts: BatchLeaseOptions,
  ): Promise<ReadonlyArray<BatchLease<T>>> {
    return super.leaseBatch(workerId, opts);
  }
}
