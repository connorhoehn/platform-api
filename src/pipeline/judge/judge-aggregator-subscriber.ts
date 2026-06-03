// platform-api/src/pipeline/judge/judge-aggregator-subscriber.ts
//
// v2 — subscribes to `pipeline:judge:scored` on the PipelineModule's
// EventBus and rolls the JudgmentResult into the per-pipeline/per-rubric/
// per-day aggregate table. Mirrors the architecture of the
// executor-audit-subscriber (in-process EventBus listener; fire-and-forget
// writes; idempotent start/stop).
//
// Why in-process instead of a real DDB Streams subscriber:
//   - The judge step lives in this service. The EventBus already carries
//     the payload we need, with no Streams round-trip.
//   - Streams would require either a real Lambda (operator overhead) or
//     a kinesis-shards consumer (also operator overhead). The aggregator
//     is small and the rollup math is simple — running it in-process
//     here is the lowest-friction path.
//   - At-most-once semantics are fine: the per-judgment row in
//     pipeline-judgments is the source of truth; aggregates are
//     derivable but kept hot for fast UI rendering. If a single rollup
//     write is dropped (process crash, DDB transient), the next
//     judgment's rollup auto-corrects the count direction (counters are
//     monotonically increasing on the steady-state path).
//
// We deliberately do NOT subscribe to `pipeline:judge:failed` or
// `pipeline:judge:budget-exceeded` — those have no `result` to fold.
// The aggregate row is judgment-centric.
//
// Persistence is fire-and-forget at this layer for the same reason the
// audit subscriber is: the executor must not be slowed down by DDB
// hiccups, and a missed aggregate row never warrants stalling pipeline
// progress.

import type { EventBus } from 'distributed-core';
import type { PipelineEventMap } from 'distributed-core/applications/pipeline';
import { withContext } from '../../lib/logger';
import type { JudgmentAggregateRepository } from './JudgmentAggregateRepository';
import type { JudgmentResult } from './types';

const log = withContext({ service: 'judge-aggregator-subscriber' });

export interface JudgeAggregatorSubscriberOptions {
  eventBus: EventBus<PipelineEventMap>;
  aggregateRepo: Pick<JudgmentAggregateRepository, 'recordJudgment'>;
}

export class JudgeAggregatorSubscriber {
  private readonly eventBus: EventBus<PipelineEventMap>;
  private readonly aggregateRepo: Pick<JudgmentAggregateRepository, 'recordJudgment'>;
  private subscriptionIds: string[] = [];
  private active = false;

  constructor(options: JudgeAggregatorSubscriberOptions) {
    this.eventBus = options.eventBus;
    this.aggregateRepo = options.aggregateRepo;
  }

  /** Begin subscribing. Idempotent — calling twice is a no-op. */
  start(): void {
    if (this.active) return;
    this.active = true;

    // `pipeline:judge:scored` isn't typed in DC's PipelineEventMap (see
    // JudgeStepHandler.ts comment — DC's TS map hasn't been bumped to
    // include the wave 3 judge events). The EventBus accepts arbitrary
    // string topics at runtime; we cast to bridge the TS gap.
    //
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const bus = this.eventBus as any;
    const subId = bus.subscribe(
      'pipeline:judge:scored',
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      async (event: { payload: any }) => {
        const result = event?.payload?.result as JudgmentResult | undefined;
        if (!isJudgmentResult(result)) return;
        try {
          await this.aggregateRepo.recordJudgment(result);
        } catch (err) {
          log.warn(
            {
              err: err instanceof Error ? err.message : String(err),
              judgmentId: result.judgmentId,
              pipelineId: result.pipelineId,
              rubricId: result.rubricId,
            },
            'judge aggregate write failed (best-effort, swallowed)',
          );
        }
      },
    );
    this.subscriptionIds.push(subId as string);
  }

  /** Unsubscribe and clear state. Safe to call when not started. */
  async stop(): Promise<void> {
    if (!this.active) return;
    this.active = false;
    for (const id of this.subscriptionIds) {
      try {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (this.eventBus as any).unsubscribe(id);
      } catch (err) {
        log.warn(
          { id, err: err instanceof Error ? err.message : String(err) },
          'judge aggregator unsubscribe failed',
        );
      }
    }
    this.subscriptionIds = [];
  }
}

/**
 * Narrow check that the payload looks like a JudgmentResult — guards
 * against malformed events from misbehaving emitters. We only require
 * the fields the aggregator reads.
 */
function isJudgmentResult(value: unknown): value is JudgmentResult {
  if (!value || typeof value !== 'object') return false;
  const v = value as Record<string, unknown>;
  return (
    typeof v.judgmentId === 'string' &&
    typeof v.pipelineId === 'string' &&
    typeof v.rubricId === 'string' &&
    typeof v.overallScore === 'number' &&
    typeof v.overallPassed === 'boolean' &&
    typeof v.tokensIn === 'number' &&
    typeof v.tokensOut === 'number' &&
    typeof v.costUsd === 'number' &&
    typeof v.judgedAt === 'string'
  );
}
