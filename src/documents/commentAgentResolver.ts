// platform-api/src/documents/commentAgentResolver.ts
//
// Settles comments handed to the document agent (commentAgentRequests.ts) from
// the executor's run lifecycle — the same PipelineModule EventBus the run
// rollup and the audit subscriber read. Every replica runs one:
//
//   pipeline:step:completed (apply)  → remember what the apply step landed
//   pipeline:run:completed           → addressed (edit landed) | refused (0 ops, or rejected at approval)
//   pipeline:run:failed / cancelled  → failed
//
// The run → comment link is looked up in `idempotency-claims` on the terminal
// event (cached per process), so the replica that finishes a run it never saw
// start — a resume after the owner died — still finds the comment. The write
// itself is the compare-and-set in `settleCommentAgentRun`; a second replica or
// a redelivery finds the request settled and writes nothing.

import type { EventBus } from 'distributed-core';
import type { PipelineEventMap } from 'distributed-core/applications/pipeline';
import { withContext } from '../lib/logger';
import {
  commentForRun,
  settleCommentAgentRun,
  type ApplyOutcome,
  type CommentRunLink,
  type RunSettlement,
} from './commentAgentRequests';

const log = withContext({ service: 'comment-agent-resolver' });
const MAX_TRACKED = 5_000;

export interface CommentAgentResolverOptions {
  eventBus: EventBus<PipelineEventMap>;
  lookup?: (runId: string) => Promise<CommentRunLink | null>;
  settle?: typeof settleCommentAgentRun;
}

interface BusEventLike<T> { payload: T }

export class CommentAgentResolver {
  private readonly eventBus: EventBus<PipelineEventMap>;
  private readonly lookup: (runId: string) => Promise<CommentRunLink | null>;
  private readonly settle: typeof settleCommentAgentRun;
  /** runId → link, or null for "not a comment's run" (so ordinary runs cost one read). */
  private readonly links = new Map<string, CommentRunLink | null>();
  private readonly outcomes = new Map<string, ApplyOutcome>();
  private subscriptionIds: string[] = [];
  private inFlight = new Set<Promise<unknown>>();
  readonly stats = { settled: 0, skipped: 0, errors: 0 };

  constructor(options: CommentAgentResolverOptions) {
    this.eventBus = options.eventBus;
    this.lookup = options.lookup ?? commentForRun;
    this.settle = options.settle ?? settleCommentAgentRun;
  }

  /** The route knows the link the moment it triggers; no read needed on this replica. */
  remember(runId: string, link: CommentRunLink): void {
    this.bound(this.links).set(runId, link);
  }

  start(): void {
    const on = <K extends keyof PipelineEventMap>(name: K, handler: (event: BusEventLike<PipelineEventMap[K]>) => Promise<void> | void) => {
      this.subscriptionIds.push(this.eventBus.subscribe(name, async (event) => {
        const p = Promise.resolve(handler(event as BusEventLike<PipelineEventMap[K]>)).catch((err) => {
          this.stats.errors += 1;
          log.error({ event: name, err: String(err) }, 'comment agent settle failed');
        });
        this.inFlight.add(p);
        void p.finally(() => this.inFlight.delete(p));
      }));
    };
    on('pipeline:step:completed', (event) => {
      const { runId, stepId, output } = event.payload;
      if (stepId !== 'apply' || !output || typeof output !== 'object') return;
      // Kept for every run with an apply step (bounded, dropped at the terminal
      // event): a resumed run's link is only looked up at the end.
      if (this.links.get(runId) === null) return;
      this.bound(this.outcomes).set(runId, output as ApplyOutcome);
    });
    on('pipeline:run:completed', async (event) => {
      const { runId, status, rejection } = event.payload;
      await this.finish(runId, status === 'rejected'
        ? { kind: 'rejected', ...(rejection?.comment ? { reason: rejection.comment } : {}) }
        : { kind: 'completed', outcome: this.outcomes.get(runId) ?? null });
    });
    on('pipeline:run:failed', async (event) => {
      await this.finish(event.payload.runId, { kind: 'failed', reason: event.payload.error?.message ?? '' });
    });
    on('pipeline:run:cancelled', async (event) => {
      await this.finish(event.payload.runId, { kind: 'cancelled' });
    });
  }

  private async finish(runId: string, settlement: RunSettlement): Promise<void> {
    if (typeof runId !== 'string' || !runId) return;
    let link = this.links.get(runId);
    if (link === undefined) {
      link = await this.lookup(runId);
      this.bound(this.links).set(runId, link);
    }
    this.outcomes.delete(runId);
    if (!link) return;
    const result = await this.settle(link, runId, settlement);
    if (result === 'skipped') this.stats.skipped += 1; else this.stats.settled += 1;
  }

  private bound<V>(map: Map<string, V>): Map<string, V> {
    if (map.size >= MAX_TRACKED) {
      const oldest = map.keys().next();
      if (!oldest.done) map.delete(oldest.value);
    }
    return map;
  }

  /** Test seam: wait for queued handlers. */
  async drain(): Promise<void> { await Promise.all([...this.inFlight]); }

  stop(): void {
    for (const id of this.subscriptionIds) this.eventBus.unsubscribe(id);
    this.subscriptionIds = [];
  }
}

let current: CommentAgentResolver | null = null;

export function startCommentAgentResolver(options: CommentAgentResolverOptions): CommentAgentResolver {
  current?.stop();
  current = new CommentAgentResolver(options);
  current.start();
  log.info('comment agent resolver ENABLED');
  return current;
}

export function getCommentAgentResolver(): CommentAgentResolver | null { return current; }
