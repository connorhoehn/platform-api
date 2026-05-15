// platform-api/src/pipeline/executor-audit-subscriber.ts
//
// Wave 2 of hub#2224 — subscribes to PipelineModule's EventBus and persists
// every executor lifecycle event into the pipeline-audit table so operators
// can answer "what happened to run X 6 hours ago" without trawling Pino logs.
//
// Subscribed events (event names + AuditAction mapping):
//
//   pipeline:run:started      → tracked locally for runId→pipelineId lookup
//                                (NOT recorded as audit — the route's
//                                pipeline.trigger row already covers this).
//   pipeline:step:started     → pipeline.step.started
//   pipeline:step:completed   → pipeline.step.completed
//   pipeline:step:failed      → pipeline.step.failed
//   pipeline:step:breakpoint  → pipeline.step.breakpoint.hit
//   pipeline:run:checkpoint   → pipeline.run.checkpoint   (sampled 1-of-N,
//                                see CHECKPOINT_SAMPLE_RATE)
//   pipeline:run:completed    → pipeline.run.completed
//   pipeline:run:failed       → pipeline.run.failed
//   pipeline:llm:prompt       → pipeline.llm.prompt
//                                (carries the FULL prompt text in trace)
//   pipeline:llm:response     → pipeline.llm.response
//                                (carries the FULL response text + token counts)
//   pipeline:llm:stream:opened→ pipeline.llm.stream.opened
//                                (first-token latency marker)
//
// We deliberately do NOT subscribe to `pipeline:llm:token` — that fires once
// per token and would write thousands of audit rows for a long generation.
// The opened/prompt/response trio gives operators "what was sent + what came
// back + how long until first byte" without per-token row inflation.
//
// All persisted as `actorUserId='system'` — no operator initiates these,
// they're emitted by the executor. The route audits already cover the
// operator-driven origin events (pipeline.trigger / pipeline.cancel /
// pipeline.approve). The lifecycle audit gives the AFTER-the-fact replay.
//
// Sampling: checkpoints can fire on every step completion (worst case at
// `checkpointEveryN=1`). To prevent table bloat we sample 1-of-5 checkpoint
// events. The first checkpoint of any run (`sequenceNumber === 1`) is
// always recorded so the run's audit trail has at least one checkpoint
// row even if the run is short.
//
// All writes are fire-and-forget at this layer for the same reason as the
// route handlers — the executor must not be slowed down by audit-table
// hiccups, and a missed audit row never warrants stalling pipeline progress.

import type { EventBus, PipelineEventMap } from 'distributed-core';
import type { AuditEvent, AuditRepository } from './audit-repository';
import { withContext } from '../lib/logger';

const log = withContext({ service: 'executor-audit-subscriber' });

/** Sample 1-of-N checkpoint events. Tunable via env in bootstrap. */
const DEFAULT_CHECKPOINT_SAMPLE_RATE = 5;

/** Bounded LRU for runId → pipelineId. We only need the lookup until a
 *  run reaches a terminal state — after `pipeline:run:completed` /
 *  `pipeline:run:failed` we evict. Cap at 10k entries as a defensive
 *  upper bound for very long-running deployments without terminal events. */
const MAX_RUN_INDEX = 10_000;

export interface ExecutorAuditSubscriberOptions {
  eventBus: EventBus<PipelineEventMap>;
  auditRepo: Pick<AuditRepository, 'record'>;
  /** Override 1-of-N checkpoint sampling rate. Default 5. */
  checkpointSampleRate?: number;
}

export class ExecutorAuditSubscriber {
  private readonly eventBus: EventBus<PipelineEventMap>;
  private readonly auditRepo: Pick<AuditRepository, 'record'>;
  private readonly checkpointSampleRate: number;
  private readonly runIndex = new Map<string, string>(); // runId → pipelineId
  private subscriptionIds: string[] = [];
  private active = false;

  constructor(options: ExecutorAuditSubscriberOptions) {
    this.eventBus = options.eventBus;
    this.auditRepo = options.auditRepo;
    this.checkpointSampleRate =
      options.checkpointSampleRate ?? DEFAULT_CHECKPOINT_SAMPLE_RATE;
  }

  /**
   * Begin subscribing. Idempotent — calling twice is a no-op.
   */
  start(): void {
    if (this.active) return;
    this.active = true;

    // run:started — track runId→pipelineId for the lifecycle of the run.
    // We deliberately do NOT write an audit row here; the trigger route
    // already wrote `pipeline.trigger` and we'd just be duplicating it
    // (bridge.trigger emits this AFTER the route's audit call lands).
    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:run:started', async (event) => {
        const { runId, pipelineId } = event.payload;
        if (typeof runId === 'string' && typeof pipelineId === 'string') {
          this.indexRun(runId, pipelineId);
        }
      }),
    );

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:step:started', async (event) => {
        this.fireAndForget({
          action: 'pipeline.step.started',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'success',
          trace: {
            stepId: event.payload.stepId,
            nodeType: event.payload.nodeType,
            at: event.payload.at,
          },
        });
      }),
    );

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:step:completed', async (event) => {
        this.fireAndForget({
          action: 'pipeline.step.completed',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'success',
          trace: {
            stepId: event.payload.stepId,
            durationMs: event.payload.durationMs,
            at: event.payload.at,
          },
        });
      }),
    );

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:step:failed', async (event) => {
        this.fireAndForget({
          action: 'pipeline.step.failed',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'error',
          decision: 'error',
          trace: {
            stepId: event.payload.stepId,
            error: event.payload.error,
            at: event.payload.at,
          },
        });
      }),
    );

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:step:breakpoint', async (event) => {
        this.fireAndForget({
          action: 'pipeline.step.breakpoint.hit',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'success',
          trace: {
            stepId: event.payload.stepId,
            at: event.payload.at,
          },
        });
      }),
    );

    // Checkpoint sampling: always keep the first checkpoint (so very short
    // runs still have at least one), then sample 1-of-N. Without sampling
    // a `checkpointEveryN=1` run would write one audit row PER step on top
    // of the pipeline.step.completed row.
    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:run:checkpoint', async (event) => {
        const seq = event.payload.sequenceNumber;
        const sampled =
          seq === 1 || seq % this.checkpointSampleRate === 0;
        if (!sampled) return;
        this.fireAndForget({
          action: 'pipeline.run.checkpoint',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'success',
          trace: {
            sequenceNumber: seq,
            currentStepIds: event.payload.currentStepIds,
            at: event.payload.at,
          },
        });
      }),
    );

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:run:completed', async (event) => {
        const pipelineId = this.lookupPipelineId(event.payload.runId);
        this.fireAndForget({
          action: 'pipeline.run.completed',
          actorUserId: 'system',
          pipelineId,
          runId: event.payload.runId,
          outcome: 'success',
          decision: 'completed',
          trace: {
            durationMs: event.payload.durationMs,
            at: event.payload.at,
          },
        });
        // Run is terminal — drop the runId→pipelineId index entry.
        this.runIndex.delete(event.payload.runId);
      }),
    );

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:run:failed', async (event) => {
        const pipelineId = this.lookupPipelineId(event.payload.runId);
        const errorMessage =
          typeof event.payload.error === 'object' && event.payload.error
            ? (event.payload.error as { message?: string }).message ?? String(event.payload.error)
            : String(event.payload.error);
        this.fireAndForget({
          action: 'pipeline.run.failed',
          actorUserId: 'system',
          pipelineId,
          runId: event.payload.runId,
          outcome: 'error',
          decision: 'failed',
          trace: {
            error: errorMessage,
            at: event.payload.at,
          },
        });
        this.runIndex.delete(event.payload.runId);
      }),
    );

    // -----------------------------------------------------------------------
    // LLM observability — captures the FULL prompt and response text on the
    // audit row's `trace` field so operators can answer "what did the model
    // see / what did it say" for any historical run. Length is intentional:
    // without the full text the operator-debug story collapses to "step took
    // 3.5s" again. If the audit table ever grows past comfort, the right
    // fix is a separate prompt/response store with a pointer here, NOT
    // truncation at write time.
    // -----------------------------------------------------------------------

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:llm:prompt', async (event) => {
        this.fireAndForget({
          action: 'pipeline.llm.prompt',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'success',
          trace: {
            stepId: event.payload.stepId,
            model: event.payload.model,
            // FULL prompt text — see comment above.
            prompt: event.payload.prompt,
            at: event.payload.at,
          },
        });
      }),
    );

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:llm:response', async (event) => {
        this.fireAndForget({
          action: 'pipeline.llm.response',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'success',
          trace: {
            stepId: event.payload.stepId,
            // FULL response text — see comment above.
            response: event.payload.response,
            tokensIn: event.payload.tokensIn,
            tokensOut: event.payload.tokensOut,
            at: event.payload.at,
          },
        });
      }),
    );

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:llm:stream:opened', async (event) => {
        this.fireAndForget({
          action: 'pipeline.llm.stream.opened',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'success',
          trace: {
            stepId: event.payload.stepId,
            model: event.payload.model,
            // distributed-core uses `openedAt` (not `at`) for this event; we
            // mirror it verbatim so operators can correlate with the prompt
            // event's `at`.
            openedAt: event.payload.openedAt,
          },
        });
      }),
    );

    // -----------------------------------------------------------------------
    // P1 run-lifecycle — cancellation and stuck detection (wave19-c)
    // -----------------------------------------------------------------------

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:run:cancelled', async (event) => {
        this.fireAndForget({
          action: 'pipeline.run.cancelled',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'success',
          decision: 'cancelled',
          trace: {
            at: event.payload.at,
          },
        });
        // Run is terminal — drop the runId→pipelineId index entry.
        this.runIndex.delete(event.payload.runId);
      }),
    );

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:run:stuck', async (event) => {
        this.fireAndForget({
          action: 'pipeline.run.stuck',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'error',
          trace: {
            lastHeartbeatAt: event.payload.lastHeartbeatAt,
            at: event.payload.at,
          },
        });
      }),
    );

    // -----------------------------------------------------------------------
    // P1 context-patch mutations — edit-and-continue at breakpoints (wave19-c)
    // -----------------------------------------------------------------------

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:step:context.patched', async (event) => {
        this.fireAndForget({
          action: 'pipeline.step.context.patched',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'success',
          trace: {
            stepId: event.payload.stepId,
            patchId: event.payload.patchId,
            mode: event.payload.mode,
            at: event.payload.at,
          },
        });
      }),
    );

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:step:context.patched-unvalidated', async (event) => {
        log.warn(
          {
            runId: event.payload.runId,
            stepId: event.payload.stepId,
            patchId: event.payload.patchId,
          },
          'context patch applied without schema validation (no ContextValidator registered for node type)',
        );
        this.fireAndForget({
          action: 'pipeline.step.context.patched-unvalidated',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'success',
          trace: {
            stepId: event.payload.stepId,
            patchId: event.payload.patchId,
            mode: event.payload.mode,
            at: event.payload.at,
          },
        });
      }),
    );

    this.subscriptionIds.push(
      this.eventBus.subscribe('pipeline:step:context.patch-reverted', async (event) => {
        this.fireAndForget({
          action: 'pipeline.step.context.patch-reverted',
          actorUserId: 'system',
          pipelineId: this.lookupPipelineId(event.payload.runId),
          runId: event.payload.runId,
          outcome: 'success',
          trace: {
            stepId: event.payload.stepId,
            patchId: event.payload.patchId,
            at: event.payload.at,
          },
        });
      }),
    );

    log.info(
      { checkpointSampleRate: this.checkpointSampleRate },
      'executor audit subscriber started',
    );
  }

  /**
   * Unsubscribe and clear in-memory state. Safe to call when not started.
   */
  async stop(): Promise<void> {
    if (!this.active) return;
    this.active = false;
    for (const id of this.subscriptionIds) {
      try {
        this.eventBus.unsubscribe(id);
      } catch (err) {
        log.warn(
          { id, err: err instanceof Error ? err.message : String(err) },
          'executor audit subscriber unsubscribe failed',
        );
      }
    }
    this.subscriptionIds = [];
    this.runIndex.clear();
  }

  /** Test/operator hook — current size of the runId index. */
  indexSize(): number {
    return this.runIndex.size;
  }

  private indexRun(runId: string, pipelineId: string): void {
    if (this.runIndex.size >= MAX_RUN_INDEX) {
      // Drop the oldest entry (insertion order). Worst case for an entry
      // we drop here is a missing pipelineId on a downstream lifecycle
      // audit — we still record `pipelineId='unknown'` rather than fail.
      const firstKey = this.runIndex.keys().next().value;
      if (firstKey) this.runIndex.delete(firstKey);
    }
    this.runIndex.set(runId, pipelineId);
  }

  private lookupPipelineId(runId: string): string {
    return this.runIndex.get(runId) ?? 'unknown';
  }

  private fireAndForget(event: AuditEvent): void {
    this.auditRepo.record(event).catch((err: unknown) => {
      const message = err instanceof Error ? err.message : String(err);
      log.warn(
        {
          action: event.action,
          runId: event.runId,
          pipelineId: event.pipelineId,
          err: message,
        },
        'executor lifecycle audit write failed',
      );
    });
  }
}
