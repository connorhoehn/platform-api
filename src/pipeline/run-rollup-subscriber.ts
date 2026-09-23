// platform-api/src/pipeline/run-rollup-subscriber.ts
//
// Keeps `pipeline-run-rollups` fresh from the executor's run lifecycle events
// — the same PipelineModule EventBus the audit subscriber listens to — so the
// pipelines page never has to list runs to draw a status pill, and never
// polls.
//
//   pipeline:run:started    → running       (carries pipelineId: indexed)
//   pipeline:run:resumed    → running       (carries pipelineId: indexed)
//   pipeline:run:completed  → completed | rejected (event.status)
//   pipeline:run:failed     → failed
//   pipeline:run:cancelled  → cancelled
//   pipeline:run:orphaned   → interrupted
//   pipeline:run:stuck      → stuck
//
// Terminal events name only the run, so the subscriber remembers runId →
// pipelineId from `started`/`resumed`. A run it never saw start (the process
// restarted, or the run executes on the other replica and only its orphan
// transition fires here) is looked up on the `runId-index` GSI of
// `pipeline-runs`; a run found nowhere is logged and skipped — the read-side
// backfill heals it on the next list.
//
// Exactly-once, with two replicas:
//   - a bus event id is remembered (bounded) so a redelivery in this process is
//     dropped before any read;
//   - the merge itself is keyed by runId (`applyRunEvent`), so a duplicate that
//     slips through — the other replica's copy, a replayed outbox delivery —
//     finds its run and changes nothing;
//   - the write is compare-and-set on the row's `updatedAt`, retried on
//     conflict, and events for one pipeline are applied one at a time inside
//     the process, so two replicas cannot overwrite each other's merge.
//
// No lease and no leader: every replica applies what it observes, and the
// rules above make the result the same whichever replica writes last.
// Nothing here touches runPersistence.ts (the work-graph projection lives
// there and is somebody else's file).

import type { EventBus } from 'distributed-core';
import type { PipelineEventMap } from 'distributed-core/applications/pipeline';
import { QueryCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import { withContext } from '../lib/logger';
import {
  pipelineRunRollupRepo,
  RollupVersionConflict,
  type PipelineRunRollupRepository,
} from '../repositories/PipelineRunRollupRepository';
import { applyRunEvent, type PipelineRunRollup, type RollupRunStatus, type RunRollupEvent } from './run-rollup';
import { runOverridesFrom } from './runOverrides';

const log = withContext({ service: 'run-rollup-subscriber' });

const MAX_RUN_INDEX = 10_000;
const MAX_SEEN_EVENTS = 10_000;
const CAS_ATTEMPTS = 4;
const PIPELINE_RUNS_TABLE = tableName('pipeline-runs');
const RUN_ID_INDEX = 'runId-index';

type RollupStore = Pick<PipelineRunRollupRepository, 'get' | 'putIfUnchanged'>;

export interface RunRollupSubscriberOptions {
  eventBus: EventBus<PipelineEventMap>;
  /** `startRunRollupSubscriber` only: false skips it (no run table wired, so no rollups). Default true. */
  enabled?: boolean;
  repo?: RollupStore;
  /** Resolves a run the process never saw start. Default: the `runId-index` GSI. */
  lookupPipelineId?: (runId: string) => Promise<string | null>;
}

interface BusEventLike<T> { id?: string; payload: T }

/** One Query on the runId GSI; null when the row or the index is missing. */
export async function lookupPipelineIdByRunId(runId: string, client: DynamoDBDocumentClient = docClient): Promise<string | null> {
  try {
    const out = await client.send(new QueryCommand({
      TableName: PIPELINE_RUNS_TABLE,
      IndexName: RUN_ID_INDEX,
      KeyConditionExpression: 'runId = :r',
      ExpressionAttributeValues: { ':r': runId },
      ProjectionExpression: 'pipelineId, startedAt',
      Limit: 2,
    }));
    const rows = (out.Items ?? []) as Array<{ pipelineId?: unknown }>;
    if (rows.length !== 1 || typeof rows[0].pipelineId !== 'string') return null;
    return rows[0].pipelineId;
  } catch (err) {
    log.warn({ runId, err: String(err) }, 'runId lookup failed; rollup event skipped');
    return null;
  }
}

export class RunRollupSubscriber {
  private readonly eventBus: EventBus<PipelineEventMap>;
  private readonly repo: RollupStore;
  private readonly lookup: (runId: string) => Promise<string | null>;
  private readonly runIndex = new Map<string, { pipelineId: string; startedAt: string }>();
  private readonly seen = new Set<string>();
  private readonly lanes = new Map<string, Promise<void>>();
  private inFlight: Promise<void>[] = [];
  private subscriptionIds: string[] = [];
  private active = false;
  /** Counters a test or a health line can read. */
  readonly stats = { applied: 0, unchanged: 0, duplicates: 0, unresolved: 0, conflicts: 0, errors: 0 };

  constructor(options: RunRollupSubscriberOptions) {
    this.eventBus = options.eventBus;
    this.repo = options.repo ?? pipelineRunRollupRepo;
    this.lookup = options.lookupPipelineId ?? ((runId) => lookupPipelineIdByRunId(runId));
  }

  /** Idempotent. */
  start(): void {
    if (this.active) return;
    this.active = true;
    const on = <K extends keyof PipelineEventMap>(name: K, handler: (event: BusEventLike<PipelineEventMap[K]>) => Promise<void>) => {
      // Fire-and-forget at the bus, like the audit subscriber: a slow or
      // failing rollup write must never hold up the executor or the other
      // listeners on the same event. `idle()` waits for what is in flight.
      this.subscriptionIds.push(this.eventBus.subscribe(name, async (event) => {
        const p = handler(event as BusEventLike<PipelineEventMap[K]>).catch((err) => {
          this.stats.errors += 1;
          log.error({ event: name, err: String(err) }, 'rollup event failed');
        });
        this.track(p);
      }));
    };

    on('pipeline:run:started', async (event) => {
      const { runId, pipelineId, at } = event.payload;
      if (typeof runId !== 'string' || typeof pipelineId !== 'string') return;
      this.indexRun(runId, pipelineId, at);
      const model = runOverridesFrom((event.payload as { triggeredBy?: { payload?: Record<string, unknown> } }).triggeredBy?.payload)?.model;
      await this.apply(event.id, { pipelineId, runId, status: 'running', at, startedAt: at, ...(model ? { model } : {}) });
    });
    on('pipeline:run:resumed', async (event) => {
      const { runId, pipelineId, at } = event.payload;
      if (typeof runId !== 'string' || typeof pipelineId !== 'string') return;
      this.indexRun(runId, pipelineId, at);
      await this.apply(event.id, { pipelineId, runId, status: 'running', at });
    });
    on('pipeline:run:completed', async (event) => {
      const status: RollupRunStatus = event.payload.status === 'rejected' ? 'rejected' : 'completed';
      await this.applyTerminal(event.id, event.payload.runId, status, event.payload.at);
    });
    on('pipeline:run:failed', async (event) => {
      await this.applyTerminal(event.id, event.payload.runId, 'failed', event.payload.at);
    });
    on('pipeline:run:cancelled', async (event) => {
      await this.applyTerminal(event.id, event.payload.runId, 'cancelled', event.payload.at);
    });
    on('pipeline:run:orphaned', async (event) => {
      await this.applyLive(event.id, event.payload.runId, 'interrupted', event.payload.at);
    });
    on('pipeline:run:stuck', async (event) => {
      await this.applyLive(event.id, event.payload.runId, 'stuck', event.payload.at);
    });
    // Approval gates: the executor's queue is process memory on the replica
    // that runs the pipeline, so the count on the row is what every replica
    // answers (NFR loop 18). A gate is keyed (runId, stepId) on the row.
    on('pipeline:approval:requested', async (event) => {
      const { runId, stepId, at } = event.payload;
      if (typeof stepId !== 'string') return;
      await this.applyLive(event.id, runId, 'awaiting_approval', at, { stepId, pending: true });
    });
    on('pipeline:approval:recorded', async (event) => {
      const { runId, stepId, at } = event.payload;
      if (typeof stepId !== 'string') return;
      await this.applyLive(event.id, runId, 'running', at, { stepId, pending: false });
    });
  }

  stop(): void {
    if (!this.active) return;
    this.active = false;
    if (current === this) current = null;
    for (const id of this.subscriptionIds) {
      try { this.eventBus.unsubscribe(id); } catch { /* already gone */ }
    }
    this.subscriptionIds = [];
  }

  /** Resolves once every event received so far has been applied (tests, shutdown). */
  async idle(): Promise<void> {
    while (this.inFlight.length > 0) await Promise.allSettled(this.inFlight.splice(0));
  }

  private track(p: Promise<void>): void {
    this.inFlight.push(p);
    void p.finally(() => { this.inFlight = this.inFlight.filter((x) => x !== p); });
  }

  private indexRun(runId: string, pipelineId: string, startedAt: string): void {
    if (!this.runIndex.has(runId) && this.runIndex.size >= MAX_RUN_INDEX) {
      const oldest = this.runIndex.keys().next().value as string | undefined;
      if (oldest !== undefined) this.runIndex.delete(oldest);
    }
    const prev = this.runIndex.get(runId);
    this.runIndex.set(runId, { pipelineId, startedAt: prev && prev.startedAt < startedAt ? prev.startedAt : startedAt });
  }

  private remember(eventId: string | undefined): boolean {
    if (!eventId) return true;
    if (this.seen.has(eventId)) return false;
    if (this.seen.size >= MAX_SEEN_EVENTS) {
      const oldest = this.seen.values().next().value as string | undefined;
      if (oldest !== undefined) this.seen.delete(oldest);
    }
    this.seen.add(eventId);
    return true;
  }

  /** The pipeline a run belongs to: this process's index first, then the run table. */
  async resolvePipelineId(runId: string): Promise<string | null> {
    return (await this.resolve(runId))?.pipelineId ?? null;
  }

  private async resolve(runId: string): Promise<{ pipelineId: string; startedAt?: string } | null> {
    const known = this.runIndex.get(runId);
    if (known) return known;
    const pipelineId = await this.lookup(runId);
    if (!pipelineId) return null;
    return { pipelineId };
  }

  private async applyTerminal(eventId: string | undefined, runId: string, status: RollupRunStatus, at: string): Promise<void> {
    if (typeof runId !== 'string' || !runId) return;
    if (!this.remember(eventId)) { this.stats.duplicates += 1; return; }
    const found = await this.resolve(runId);
    if (!found) { this.stats.unresolved += 1; log.warn({ runId, status }, 'run unknown to this process and not in the run table; rollup left to the read-side backfill'); return; }
    // The index entry stays (bounded FIFO eviction) so a late copy of this
    // terminal event — the outbox replay, the other replica's delivery — is a
    // cheap no-op merge and never a GSI lookup.
    await this.applyResolved({ pipelineId: found.pipelineId, runId, status, at, completedAt: at, ...(found.startedAt ? { startedAt: found.startedAt } : {}) });
  }

  private async applyLive(eventId: string | undefined, runId: string, status: RollupRunStatus, at: string, approval?: RunRollupEvent['approval']): Promise<void> {
    if (typeof runId !== 'string' || !runId) return;
    if (!this.remember(eventId)) { this.stats.duplicates += 1; return; }
    const found = await this.resolve(runId);
    if (!found) { this.stats.unresolved += 1; log.warn({ runId, status }, 'run unknown to this process and not in the run table; rollup left to the read-side backfill'); return; }
    await this.applyResolved({ pipelineId: found.pipelineId, runId, status, at, ...(found.startedAt ? { startedAt: found.startedAt } : {}), ...(approval ? { approval } : {}) });
  }

  private async apply(eventId: string | undefined, event: RunRollupEvent): Promise<void> {
    if (!this.remember(eventId)) { this.stats.duplicates += 1; return; }
    await this.applyResolved(event);
  }

  /**
   * Merge one already-resolved event. Public: the orphan scan and the stuck
   * detector call it with the pipelineId they read off the run row (the
   * leader replica never saw that run start), and tests drive it directly.
   * The run is indexed here so the bus copy of the same transition resolves
   * locally instead of querying the run table.
   */
  async applyResolved(event: RunRollupEvent): Promise<PipelineRunRollup | null> {
    this.indexRun(event.runId, event.pipelineId, event.startedAt ?? event.at);
    const prev = this.lanes.get(event.pipelineId) ?? Promise.resolve();
    let result: PipelineRunRollup | null = null;
    const next = prev.then(async () => { result = await this.merge(event); }, async () => { result = await this.merge(event); });
    this.lanes.set(event.pipelineId, next);
    await next;
    if (this.lanes.get(event.pipelineId) === next) this.lanes.delete(event.pipelineId);
    return result;
  }

  private async merge(event: RunRollupEvent): Promise<PipelineRunRollup | null> {
    for (let attempt = 1; attempt <= CAS_ATTEMPTS; attempt++) {
      const seen = await this.repo.get(event.pipelineId);
      const next = applyRunEvent(seen, event);
      if (next === seen) { this.stats.unchanged += 1; return seen; }
      try {
        await this.repo.putIfUnchanged(next, seen);
        this.stats.applied += 1;
        return next;
      } catch (err) {
        if (!(err instanceof RollupVersionConflict)) throw err;
        this.stats.conflicts += 1;
        // Visible evidence of the CAS doing its job (the other replica, or the
        // read-side backfill, wrote the row between our read and our put).
        log.info({ pipelineId: event.pipelineId, runId: event.runId, status: event.status, attempt }, 'rollup CAS conflict; re-reading');
        if (attempt === CAS_ATTEMPTS) throw err;
      }
    }
    return null;
  }
}

let current: RunRollupSubscriber | null = null;

/** The process-wide subscriber `startRunRollupSubscriber` installed, or null (disabled / not started). */
export function getRunRollupSubscriber(): RunRollupSubscriber | null { return current; }

/** Tests: install or clear the process-wide instance. */
export function setRunRollupSubscriber(subscriber: RunRollupSubscriber | null): void { current = subscriber; }

/**
 * The one line bootstrap.ts calls. Opt-out via PIPELINE_RUN_ROLLUP_ENABLED=false.
 */
export function startRunRollupSubscriber(options: RunRollupSubscriberOptions): RunRollupSubscriber | null {
  const enabled = (process.env.PIPELINE_RUN_ROLLUP_ENABLED ?? 'true').trim().toLowerCase() !== 'false';
  if (!enabled) { log.info('run rollup subscriber DISABLED via PIPELINE_RUN_ROLLUP_ENABLED=false'); return null; }
  if (options.enabled === false) { log.info('run rollup subscriber DISABLED: no run table wired'); return null; }
  const subscriber = new RunRollupSubscriber(options);
  subscriber.start();
  current = subscriber;
  log.info('run rollup subscriber ENABLED');
  return subscriber;
}
