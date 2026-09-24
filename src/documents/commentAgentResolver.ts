// platform-api/src/documents/commentAgentResolver.ts
//
// Settles comments handed to the document agent (commentAgentRequests.ts) from
// the executor's run lifecycle — the same PipelineModule EventBus the run
// rollup and the audit subscriber read. Every replica runs one:
//
//   pipeline:step:completed (apply)  → remember what the apply step landed
//   pipeline:run:completed           → addressed (edit landed) | refused (0 ops, or rejected at approval)
//   pipeline:run:failed / cancelled  → failed
//   pipeline:run:reviewed            → a SUGGESTING run's change was accepted
//                                      (resolve, when asked) or rejected
//                                      ("Suggestion rejected by <name>", open)
//
// `pipeline:run:reviewed` is published by POST /pipelines/:id/runs/:runId/review
// — the margin's Accept / Reject and the run card both land there — on the same
// bus, so it rides the same lanes (dc pub-sub, the Redis relay, MQ) as the
// terminal events. The verdict's write is its own compare-and-set on
// (runId, suggested|pending), so two replicas hearing it write one reply.
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
  settleCommentSuggestionReview,
  type RunSettlement,
  type SuggestionVerdict,
} from './commentAgentRequests';

/** Published by routes/pipelineRunReview.ts (not in dc's PipelineEventMap). */
export const RUN_REVIEWED = 'pipeline:run:reviewed';
interface RunReviewedPayload { runId?: unknown; decision?: unknown; by?: unknown; byName?: unknown }

const log = withContext({ service: 'comment-agent-resolver' });
const MAX_TRACKED = 5_000;

export interface CommentAgentResolverOptions {
  eventBus: EventBus<PipelineEventMap>;
  lookup?: (runId: string) => Promise<CommentRunLink | null>;
  settle?: typeof settleCommentAgentRun;
  review?: typeof settleCommentSuggestionReview;
}

interface BusEventLike<T> { payload: T }

export class CommentAgentResolver {
  private readonly eventBus: EventBus<PipelineEventMap>;
  private readonly lookup: (runId: string) => Promise<CommentRunLink | null>;
  private readonly settle: typeof settleCommentAgentRun;
  private readonly review: typeof settleCommentSuggestionReview;
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
    this.review = options.review ?? settleCommentSuggestionReview;
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
    on(RUN_REVIEWED as keyof PipelineEventMap, async (event) => {
      const p = (event.payload ?? {}) as RunReviewedPayload;
      if (typeof p.runId !== 'string' || !p.runId) return;
      if (p.decision !== 'accept' && p.decision !== 'reject') return;
      if (typeof p.by !== 'string') return;
      await this.reviewed(p.runId, { decision: p.decision, by: p.by, ...(typeof p.byName === 'string' ? { byName: p.byName } : {}) });
    });
  }

  private async linkFor(runId: string): Promise<CommentRunLink | null> {
    let link = this.links.get(runId);
    if (link === undefined) {
      link = await this.lookup(runId);
      this.bound(this.links).set(runId, link);
    }
    return link;
  }

  private async reviewed(runId: string, verdict: SuggestionVerdict): Promise<void> {
    const link = await this.linkFor(runId);
    if (!link) return;
    const result = await this.review(link, runId, verdict);
    if (result === 'skipped') this.stats.skipped += 1; else this.stats.settled += 1;
  }

  private async finish(runId: string, settlement: RunSettlement): Promise<void> {
    if (typeof runId !== 'string' || !runId) return;
    const link = await this.linkFor(runId);
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
