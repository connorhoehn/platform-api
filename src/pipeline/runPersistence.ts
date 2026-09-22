// platform-api/src/pipeline/runPersistence.ts
//
// Pipeline run persistence service — subscribes to pipeline events and persists
// full run state to DynamoDB for historical queries and checkpoint-based resume.
//
// Task hub#974 — durable storage for pipeline runs.
//
// Subscribes to:
//   - pipeline:run:started        → create run record
//   - pipeline:step:completed     → update step outputs
//   - pipeline:run:completed      → mark terminal
//   - pipeline:run:failed         → mark terminal with error
//   - pipeline:run:cancelled      → mark terminal
//
// Storage:
//   - Summary metadata in PipelineRunRepository (status, timestamps, trigger)
//   - Full step outputs stored inline in stepsSummary JSONB column
//   - EventBus WAL remains source of truth for replay
//
// Design decisions:
//   - Fire-and-forget persistence: DynamoDB failures logged but don't block execution
//   - Single-table design with GSI for querying by status/user
//   - TTL on completed runs (configurable, default 90 days)
//   - Full step outputs stored for replay, but limited to 100KB per step

import type { EventBus } from 'distributed-core';
import { retryWithBackoff } from 'distributed-core';
import type { PipelineEventMap, RunRejection } from 'distributed-core/applications/pipeline';
import {
  PipelineRunRepository,
  type PersistedCheckpoint,
  type PipelineRunItem,
  type RunItemStatus,
  type StepAttempt,
  type RunReview,
  type StepSummary,
  type TokenUsageByModel,
} from '../repositories/PipelineRunRepository';
import { withContext } from '../lib/logger';
import { computeCost } from './llm-pricing';
import { platformWorkGraphSourcePublisher } from '../work-graph/sources/platformPublisher';
import { pipelineStepResourceId } from '../work-graph/sources/mapPipeline';
import { recallRunLab } from './agent/DevboxAgentClient';
import { parseRunResult } from './runResult';
import type { RunWorkGraphPlan } from './workGraphPlan';

const log = withContext({ service: 'runPersistence' });

/**
 * How often a still-running step re-announces itself. The freshness lease it
 * feeds is five minutes, so a minute is frequent enough to keep a twenty
 * minute agent step live and infrequent enough not to flood the projection.
 */
const WORK_GRAPH_STEP_REFRESH_MS = 60_000;

export interface RunPersistenceOptions {
  /** EventBus instance to subscribe to. */
  eventBus: EventBus<PipelineEventMap>;
  /** Repository for persisting run state. */
  repository: PipelineRunRepository;
  /** TTL in seconds for completed runs (default: 90 days). */
  ttlSeconds?: number;
  /** Maximum size of step output to persist (default: 100KB). */
  maxStepOutputBytes?: number;
  /**
   * DC-25: size budget for the persisted checkpoint blob (default: 100KB —
   * same discipline as maxStepOutputBytes). Above it, the largest
   * `context.steps` entries are replaced with `{truncated:true,size}`
   * markers; if even that can't get under budget, the previous checkpoint
   * is kept and the oversized one is dropped with an error log.
   */
  maxCheckpointBytes?: number;
}

/**
 * In-memory cache of run state before persistence. Accumulates step outputs
 * across step:completed events until run reaches terminal state, then flushes
 * to DynamoDB.
 */
interface RunState {
  pipelineId: string;
  runId: string;
  status: RunItemStatus;
  triggeredBy: string;
  triggerType: string;
  /** Verified JWT organization captured by the authenticated trigger path. */
  organizationId?: string;
  startedAt: string;
  completedAt?: string;
  steps: Record<string, StepSummary>;
  error?: string;
  /** Set when an approval gate on this run was rejected (drives `status: 'rejected'`). */
  rejection?: RunRejection;
  /** Per-model token usage accumulated across LLM steps. */
  tokenUsage: Record<string, TokenUsageByModel>;
  /** Estimated total cost in USD. */
  estimatedCostUsd: number;
  /** Maps stepId → model for correlating llm:prompt with llm:response. */
  stepModelMap: Record<string, string>;
  /**
   * How many completions per stepId were already priced before a resume.
   *
   * Cost is read as money, so it must not double-count. A retried step only
   * emits `step:completed` for the attempt that succeeded, and two fork
   * branches of one node are two real executions that should both be charged —
   * those are right by construction. The case that is not is a RESUMED run
   * replaying a step it finished in its previous life: its tokens are already
   * inside the `tokenUsage` restored from the row.
   *
   * Keying that on the suffixed step key does not work, because the replayed
   * completion collides with the pre-crash summary and is filed as
   * `summarize:2` — a key nothing has priced. So this counts CREDITS per raw
   * stepId instead: a resume seeds one per pre-crash completion, each replayed
   * completion spends one, and anything beyond them is new work and is charged.
   */
  replayCredits: Record<string, number>;
  /** Transient input snapshot per stepId; captured from llm:prompt; cleared after step:completed. */
  stepInputMap: Record<string, unknown>;
  /** In-flight attempt history per stepId; populated by step:attempt:* events (maxAttempts > 1). */
  stepAttempts: Record<string, StepAttempt[]>;
  /** ISO timestamp of the most recent event observed for this run. */
  lastEventAt: string;
  /** What the graph shows for this run. Absent for a run nobody declared one for. */
  workGraph?: RunWorkGraphPlan;
  /** The lifecycle each step was last projected at, so it only moves forward. */
  stepLifecycle: Record<string, string>;
  /** When that projection happened, for the running step's lease refresh. */
  stepLifecycleAt: Record<string, string>;
  /** DC-25: latest (compacted) checkpoint — persisted onto the run row. */
  checkpoint?: PersistedCheckpoint;
  /** DC-25: definition snapshot captured from the first checkpoint event. */
  definitionSnapshot?: unknown;
  /** DC-25 resume bookkeeping — carried so full-row puts don't wipe the claim. */
  interruptedAt?: string;
  resumedAt?: string;
  resumeCount?: number;
  resumeClaimedBy?: string;
  /** DLQ redrive lineage — see PipelineRunItem. Carried so full-row puts keep them. */
  redriveOf?: string;
  redrivenBy?: string[];
  /** A person's verdict on the run's suggestions — carried so a late full-row put keeps it. */
  review?: RunReview;
}

/** A redrive marks its trigger payload with the run it replaces (failedRunDeadLetters.ts). */
function redriveOfFromPayload(payload: unknown): string | undefined {
  if (!payload || typeof payload !== 'object') return undefined;
  const v = (payload as { redriveOf?: unknown }).redriveOf;
  return typeof v === 'string' && v ? v : undefined;
}

/**
 * One replay credit per completion already recorded on a stored run.
 *
 * `stepsSummary` keys a repeated node as `id`, `id:2`, `id:3` — the same
 * suffixing handleStepCompleted applies — so the credit for a forked node is
 * its branch count, and a resumed run can replay all of them without being
 * charged twice.
 */
function creditsFromSummary(summary: Record<string, unknown> | undefined): Record<string, number> {
  const out: Record<string, number> = {};
  for (const key of Object.keys(summary ?? {})) {
    const rawId = key.includes(':') ? key.slice(0, key.lastIndexOf(':')) : key;
    const id = /^\d+$/.test(key.slice(key.lastIndexOf(':') + 1)) ? rawId : key;
    out[id] = (out[id] ?? 0) + 1;
  }
  return out;
}

export class RunPersistenceService {
  private readonly eventBus: EventBus<PipelineEventMap>;
  private readonly repository: PipelineRunRepository;
  private readonly ttlSeconds: number;
  private readonly maxStepOutputBytes: number;
  private readonly runCache = new Map<string, RunState>();
  private subscriptionActive = false;

  private readonly maxCheckpointBytes: number;

  constructor(options: RunPersistenceOptions) {
    this.eventBus = options.eventBus;
    this.repository = options.repository;
    this.ttlSeconds = options.ttlSeconds ?? 90 * 24 * 60 * 60; // 90 days
    this.maxStepOutputBytes = options.maxStepOutputBytes ?? 100 * 1024; // 100KB
    this.maxCheckpointBytes = options.maxCheckpointBytes ?? 100 * 1024; // 100KB
  }

  /**
   * Start subscribing to pipeline events and persisting run state.
   * Safe to call multiple times (idempotent).
   */
  start(): void {
    if (this.subscriptionActive) return;
    this.subscriptionActive = true;

    // Subscribe to run lifecycle events
    this.eventBus.subscribe('pipeline:run:started', async (event) => {
      await this.handleRunStarted(event.payload).catch((err) => {
        log.warn({ runId: event.payload.runId, err: String(err) }, 'failed to persist run:started');
      });
    });

    // A step's own start. It is the earliest honest moment to say a step is
    // running, and unlike `step:attempt:started` it fires for every step
    // rather than only for ones with a retry policy.
    this.eventBus.subscribe('pipeline:step:started', async (event) => {
      await this.markWorkGraphStep(event.payload.runId, event.payload.stepId, 'running', event.payload.at);
    });

    this.eventBus.subscribe('pipeline:step:completed', async (event) => {
      // AFTER the summary is written: the step's own output is what names the
      // document it produced, and that is not on the bus event.
      await this.handleStepCompleted(event.payload).catch((err) => {
        log.warn(
          { runId: event.payload.runId, stepId: event.payload.stepId, err: String(err) },
          'failed to persist step:completed',
        );
      });
      await this.markWorkGraphStep(event.payload.runId, event.payload.stepId, 'completed', event.payload.at);
    });

    this.eventBus.subscribe('pipeline:step:failed', async (event) => {
      await this.handleStepFailed(event.payload).catch((err) => {
        log.warn(
          { runId: event.payload.runId, stepId: event.payload.stepId, err: String(err) },
          'failed to persist step:failed',
        );
      });
      await this.markWorkGraphStep(event.payload.runId, event.payload.stepId, 'failed', event.payload.at);
    });

    this.eventBus.subscribe('pipeline:run:completed', async (event) => {
      await this.handleRunCompleted(event.payload).catch((err) => {
        log.warn({ runId: event.payload.runId, err: String(err) }, 'failed to persist run:completed');
      });
    });

    this.eventBus.subscribe('pipeline:run:failed', async (event) => {
      await this.handleRunFailed(event.payload).catch((err) => {
        log.warn({ runId: event.payload.runId, err: String(err) }, 'failed to persist run:failed');
      });
    });

    this.eventBus.subscribe('pipeline:run:cancelled', async (event) => {
      await this.handleRunCancelled(event.payload).catch((err) => {
        log.warn({ runId: event.payload.runId, err: String(err) }, 'failed to persist run:cancelled');
      });
    });

    // DC-25: durable resume. Every checkpoint the executor emits is
    // compacted and persisted onto the run row so an in-flight run's
    // frontier survives pod replacement (the file WAL does not — it lives
    // on the pod's ephemeral filesystem).
    this.eventBus.subscribe('pipeline:run:checkpoint', async (event) => {
      await this.handleCheckpoint(event.payload).catch((err) => {
        log.warn({ runId: event.payload.runId, err: String(err) }, 'failed to persist run:checkpoint');
      });
    });

    // Per-attempt retry events (DC-15 — only emitted when maxAttempts > 1)
    this.eventBus.subscribe('pipeline:step:attempt:started', async (event) => {
      this.handleStepAttemptStarted(event.payload);
      await this.markWorkGraphStep(event.payload.runId, event.payload.stepId, 'running', event.payload.startedAt);
    });

    this.eventBus.subscribe('pipeline:step:attempt:failed', async (event) => {
      this.handleStepAttemptFailed(event.payload);
    });

    // The agent loop's own turn counter. It is the only evidence a devbox
    // step is still alive between its start and its completion, so it both
    // records what the step has spent and refreshes the step node's lease.
    (this.eventBus as unknown as {
      subscribe(name: string, handler: (event: { payload: Record<string, unknown> }) => Promise<void>): string;
    }).subscribe('pipeline:step:agent:iteration', async (event) => {
      await this.handleAgentIteration(event.payload).catch((err) => {
        log.warn({ err: String(err) }, 'failed to record step:agent:iteration');
      });
    });

    // LLM cost tracking — correlate prompt (has model) with response (has tokens)
    this.eventBus.subscribe('pipeline:llm:prompt', async (event) => {
      this.handleLLMPrompt(event.payload);
    });

    this.eventBus.subscribe('pipeline:llm:response', async (event) => {
      await this.handleLLMResponse(event.payload).catch((err) => {
        log.warn(
          { runId: event.payload.runId, stepId: event.payload.stepId, err: String(err) },
          'failed to persist llm:response spend',
        );
      });
    });

    log.info('run persistence service started');
  }

  /**
   * Stop subscribing to events and flush any pending state.
   * Safe to call multiple times.
   */
  async stop(): Promise<void> {
    if (!this.subscriptionActive) return;
    this.subscriptionActive = false;

    // Flush any cached runs that haven't reached terminal state
    const pending = Array.from(this.runCache.values());
    for (const run of pending) {
      try {
        await this.persistRun(run);
      } catch (err) {
        log.warn({ runId: run.runId, err: String(err) }, 'failed to flush run on stop');
      }
    }

    this.runCache.clear();
    log.info('run persistence service stopped');
  }

  private async handleRunStarted(event: {
    runId: string;
    pipelineId: string;
    triggeredBy: { triggerType: string; userId?: string; payload?: Record<string, unknown> };
    at: string;
  }): Promise<void> {
    const userId = event.triggeredBy.userId ?? 'system';
    // The executor puts the trigger payload on `started`, so a redrive is
    // recognisable here for every caller — including ones that never call
    // registerTrigger. A registerTrigger that already named it wins.
    const redriveOf = this.runCache.get(event.runId)?.redriveOf ?? redriveOfFromPayload(event.triggeredBy.payload);
    const registered = this.runCache.get(event.runId);
    // "Agent loop · create a meeting with the X team": the pipeline's own name
    // and the instruction the trigger carried. The instruction is what the
    // person typed, so it is the half that identifies this run among others.
    const instruction = typeof event.triggeredBy.payload?.instruction === 'string'
      ? event.triggeredBy.payload.instruction.trim().replace(/\s+/g, ' ')
      : '';
    const planTitle = registered?.workGraph?.title;
    const runTitle = instruction
      ? `${planTitle ? `${planTitle} · ` : ''}${instruction}`.slice(0, 160)
      : planTitle;
    const state: RunState = {
      pipelineId: event.pipelineId,
      runId: event.runId,
      status: 'running',
      triggeredBy: userId,
      triggerType: event.triggeredBy.triggerType,
      startedAt: event.at,
      steps: {},
      tokenUsage: {},
      estimatedCostUsd: 0,
      replayCredits: {},
      stepModelMap: {},
      stepInputMap: {},
      stepAttempts: {},
      stepLifecycle: {},
      stepLifecycleAt: {},
      lastEventAt: event.at,
      ...(registered?.organizationId ? { organizationId: registered.organizationId } : {}),
      ...(registered?.workGraph
        ? { workGraph: { ...registered.workGraph, ...(runTitle ? { title: runTitle } : {}) } }
        : {}),
      ...(redriveOf ? { redriveOf } : {}),
    };

    this.runCache.set(event.runId, state);
    await this.persistRun(state);
    await this.publishWorkGraphLifecycle(state, event.at);
    // The run's shape is visible from the start: every planned step, waiting.
    await this.publishWorkGraphStepPlan(state, event.at);
  }

  private touchLastEvent(runId: string, at: string): void {
    const state = this.runCache.get(runId);
    if (state) state.lastEventAt = at;
  }

  private async handleStepCompleted(event: {
    runId: string;
    stepId: string;
    output?: unknown;
    durationMs: number;
    at: string;
  }): Promise<void> {
    this.touchLastEvent(event.runId, event.at);
    const state = this.runCache.get(event.runId);
    if (!state) {
      log.warn({ runId: event.runId }, 'step:completed for unknown run');
      return;
    }

    // Truncate large outputs
    let output = event.output;
    const outputStr = JSON.stringify(output ?? {});
    if (outputStr.length > this.maxStepOutputBytes) {
      output = { truncated: true, size: outputStr.length };
      log.warn(
        { runId: event.runId, stepId: event.stepId, size: outputStr.length },
        'step output truncated',
      );
    }

    // Use a unique key when the same node executes multiple times (fork branches)
    let stepKey = event.stepId;
    if (state.steps[stepKey]) {
      let suffix = 2;
      while (state.steps[`${event.stepId}:${suffix}`]) suffix++;
      stepKey = `${event.stepId}:${suffix}`;
    }

    // Price the step from its own output when it reported model usage. An agent
    // step runs its own turn loop and returns the totals rather than emitting
    // `llm:*` events, so this is the only place its tokens surface. Guarded on
    // the resolved stepKey, so a fork branch counts once per branch and a
    // resumed run does not re-count what it already paid for.
    const credit = state.replayCredits[event.stepId] ?? 0;
    if (credit > 0) {
      // A step this run already finished and already paid for, before it was
      // interrupted. Spend the credit and charge nothing.
      state.replayCredits[event.stepId] = credit - 1;
    } else {
      const reported = this.tokensFromStepOutput(event.output);
      if (reported) {
        this.addTokenUsage(state, reported.model, reported.tokensIn, reported.tokensOut);
      }
    }

    // Capture the resolved input from stepInputMap (populated by llm:prompt for LLM nodes).
    // Truncate inputs with the same budget as outputs.
    let input: unknown = state.stepInputMap[event.stepId];
    delete state.stepInputMap[event.stepId];
    if (input !== undefined) {
      const inputStr = JSON.stringify(input);
      if (inputStr.length > this.maxStepOutputBytes) {
        input = { truncated: true, size: inputStr.length };
      }
    }

    // Hub#1281: when step:attempt:* events were emitted (maxAttempts > 1), the
    // final successful attempt is already tracked in stepAttempts[]. Close it
    // with the completion data from this event. Otherwise synthesise a single
    // attempt from the step:completed payload (maxAttempts = 1, default path).
    let attempts: StepAttempt[];
    const accumulated = state.stepAttempts[event.stepId];
    if (accumulated && accumulated.length > 0) {
      const last = accumulated[accumulated.length - 1];
      last.completedAt = event.at;
      last.durationMs = event.durationMs;
      last.output = output;
      if (input !== undefined && last.input === undefined) last.input = input;
      attempts = accumulated;
      delete state.stepAttempts[event.stepId];
    } else {
      // Hub#1280/1283: synthetic single attempt (no retry events emitted)
      const attempt: StepAttempt = {
        attemptNumber: 1,
        startedAt: event.at, // step:completed lacks startedAt; approximate
        completedAt: event.at,
        durationMs: event.durationMs,
        input,
        output,
        idempotencyKey: `${event.runId}:${event.stepId}:1`,
      };
      attempts = [attempt];
    }

    state.steps[stepKey] = {
      status: 'completed',
      output,
      durationMs: event.durationMs,
      completedAt: event.at,
      attempts,
    };

    // Persist every 10 steps or on terminal event
    if (Object.keys(state.steps).length % 10 === 0) {
      await this.persistRun(state);
    }
  }

  /**
   * Record a FAILED step into the run's step summary. A failed step WAS
   * executed — until this existed, stepsSummary contained only completed
   * steps, so the retry route's "was this node executed in run X" guard
   * 400'd on exactly the node the UI renders the "Retry from here" pill on
   * (a failed one), and the feature could never work.
   */
  private async handleStepFailed(event: {
    runId: string;
    stepId: string;
    error: string;
    durationMs?: number;
    at: string;
  }): Promise<void> {
    this.touchLastEvent(event.runId, event.at);
    const state = this.runCache.get(event.runId);
    if (!state) {
      log.warn({ runId: event.runId }, 'step:failed for unknown run');
      return;
    }
    this.recordFailedStep(state, event.stepId, event.error, event.at, event.durationMs);
  }

  /**
   * Shared by handleStepFailed and the handleRunFailed backfill (the two
   * events travel on independently-ordered channels, so either can arrive
   * first — and older executors emit only run:failed).
   */
  private recordFailedStep(
    state: RunState,
    stepId: string,
    error: string,
    at: string,
    durationMs?: number,
  ): void {
    // Same fork-branch key uniquing as handleStepCompleted.
    let stepKey = stepId;
    if (state.steps[stepKey]) {
      let suffix = 2;
      while (state.steps[`${stepId}:${suffix}`]) suffix++;
      stepKey = `${stepId}:${suffix}`;
    }

    // Close out any in-flight attempt history (maxAttempts > 1 path), or
    // synthesise a single failed attempt.
    let attempts: StepAttempt[];
    const accumulated = state.stepAttempts[stepId];
    if (accumulated && accumulated.length > 0) {
      const last = accumulated[accumulated.length - 1];
      last.completedAt = at;
      last.error = error;
      if (last.durationMs === undefined && last.startedAt) {
        last.durationMs = Date.parse(at) - Date.parse(last.startedAt);
      }
      attempts = accumulated;
      delete state.stepAttempts[stepId];
    } else {
      attempts = [
        {
          attemptNumber: 1,
          startedAt: at, // step:failed lacks startedAt; approximate
          completedAt: at,
          durationMs,
          error,
          idempotencyKey: `${state.runId}:${stepId}:1`,
        },
      ];
    }

    state.steps[stepKey] = {
      status: 'failed',
      error,
      durationMs,
      completedAt: at,
      attempts,
    };
  }

  /**
   * DC-25: fold a `pipeline:run:checkpoint` event onto the run row.
   *
   * The raw payload duplicates step outputs twice (steps[].output and
   * context.steps) and can carry LLM transcripts — persisting it wholesale
   * would blow both the 100KB discipline and the 400KB DDB item ceiling.
   * Compaction, in order:
   *   1. Step records are ALWAYS slimmed to resume-critical fields
   *      (status/timestamps/sourceHandle/approvals/error). Outputs are not
   *      lost — they live in `context.steps` (execution truth) and in
   *      `stepsSummary` (display truth, per-step-truncated already).
   *   2. If the serialized checkpoint still exceeds `maxCheckpointBytes`,
   *      the largest `context.steps` values are replaced with
   *      `{truncated:true,size}` markers (largest-first) and recorded in
   *      `droppedContextStepKeys` — a resumed run sees truncation markers
   *      for those outputs rather than resurrecting megabytes.
   *   3. If even that cannot fit, the checkpoint is dropped (previous one
   *      kept) and an error is logged — stale-but-valid beats truncated-
   *      wholesale, which is how oversized items got silently mangled
   *      elsewhere in this codebase.
   */
  private async handleCheckpoint(event: {
    runId: string;
    context: Record<string, unknown>;
    currentStepIds: string[];
    sequenceNumber: number;
    at: string;
    steps?: Record<string, unknown>;
    joinStates?: Record<string, unknown[]>;
    firedJoins?: string[];
    completedStepCount?: number;
    definition?: unknown;
  }): Promise<void> {
    this.touchLastEvent(event.runId, event.at);
    const state = this.runCache.get(event.runId);
    if (!state) {
      log.warn({ runId: event.runId }, 'run:checkpoint for unknown run');
      return;
    }

    // Definition snapshot rides only the FIRST checkpoint — capture once.
    if (event.definition && state.definitionSnapshot === undefined) {
      state.definitionSnapshot = event.definition;
    }

    // (1) Slim the step records.
    const slimSteps: Record<string, unknown> = {};
    for (const [stepId, raw] of Object.entries(event.steps ?? {})) {
      const s = (raw ?? {}) as Record<string, unknown>;
      slimSteps[stepId] = {
        nodeId: s.nodeId,
        status: s.status,
        ...(s.startedAt !== undefined ? { startedAt: s.startedAt } : {}),
        ...(s.completedAt !== undefined ? { completedAt: s.completedAt } : {}),
        ...(s.durationMs !== undefined ? { durationMs: s.durationMs } : {}),
        ...(s.sourceHandle !== undefined ? { sourceHandle: s.sourceHandle } : {}),
        ...(s.error !== undefined ? { error: s.error } : {}),
        ...(s.approvals !== undefined ? { approvals: s.approvals } : {}),
      };
    }

    const candidate: PersistedCheckpoint = {
      sequenceNumber: event.sequenceNumber,
      at: event.at,
      context: JSON.parse(JSON.stringify(event.context)) as Record<string, unknown>,
      currentStepIds: event.currentStepIds,
      steps: slimSteps,
      ...(event.joinStates !== undefined ? { joinStates: event.joinStates } : {}),
      ...(event.firedJoins !== undefined ? { firedJoins: event.firedJoins } : {}),
      ...(event.completedStepCount !== undefined
        ? { completedStepCount: event.completedStepCount }
        : {}),
    };

    // (2) Largest-first truncation of context.steps values.
    let size = JSON.stringify(candidate).length;
    if (size > this.maxCheckpointBytes) {
      const ctxSteps = candidate.context.steps as Record<string, unknown> | undefined;
      if (ctxSteps && typeof ctxSteps === 'object') {
        const bySize = Object.entries(ctxSteps)
          .map(([k, v]) => ({ k, bytes: JSON.stringify(v ?? null).length }))
          .sort((a, b) => b.bytes - a.bytes);
        const dropped: string[] = [];
        for (const { k, bytes } of bySize) {
          if (size <= this.maxCheckpointBytes) break;
          ctxSteps[k] = { truncated: true, size: bytes };
          dropped.push(k);
          size = JSON.stringify(candidate).length;
        }
        if (dropped.length > 0) {
          candidate.droppedContextStepKeys = dropped;
          log.warn(
            { runId: event.runId, sequenceNumber: event.sequenceNumber, dropped, size },
            'checkpoint context.steps truncated to fit size budget',
          );
        }
      }
    }

    // (3) Still oversized (huge non-steps context) — keep the previous one.
    if (size > this.maxCheckpointBytes) {
      log.error(
        { runId: event.runId, sequenceNumber: event.sequenceNumber, size, budget: this.maxCheckpointBytes },
        'checkpoint exceeds size budget even after compaction — keeping previous checkpoint',
      );
      return;
    }

    state.checkpoint = candidate;
    await this.persistRun(state);
  }

  /**
   * DC-25: rebuild the in-memory run cache for a run this process is about
   * to RESUME (boot-time recovery). The resumed executor emits step events
   * under the original runId; without a seeded cache entry those events
   * would hit the 'unknown run' path and — worse — a later terminal event
   * would synthesize a stub state that wipes the pre-crash stepsSummary.
   * Seeding from the DDB row keeps pre-crash steps (original timestamps
   * intact — the resume proof relies on this) and the resume bookkeeping
   * fields, so subsequent full-row puts don't clobber the claim.
   */
  seedResumedRun(item: PipelineRunItem): void {
    this.runCache.set(item.runId, {
      pipelineId: item.pipelineId,
      runId: item.runId,
      status: 'running',
      triggeredBy: item.triggeredBy,
      organizationId: item.organizationId,
      triggerType: item.triggerType,
      startedAt: item.startedAt,
      steps: { ...(item.stepsSummary ?? {}) },
      tokenUsage: { ...(item.tokenUsage ?? {}) },
      estimatedCostUsd: item.estimatedCostUsd ?? 0,
      // The steps that finished before the crash are already inside the
      // `tokenUsage` restored above. One credit each, so a replayed completion
      // is recognised and not charged for twice.
      replayCredits: creditsFromSummary(item.stepsSummary),
      stepModelMap: {},
      stepInputMap: {},
      stepAttempts: {},
      stepLifecycle: {},
      stepLifecycleAt: {},
      lastEventAt: new Date().toISOString(),
      checkpoint: item.checkpoint,
      definitionSnapshot: item.definitionSnapshot,
      interruptedAt: item.interruptedAt,
      resumedAt: item.resumedAt,
      resumeCount: item.resumeCount,
      resumeClaimedBy: item.resumeClaimedBy,
      redriveOf: item.redriveOf,
      redrivenBy: item.redrivenBy,
    });
  }

  /**
   * Pre-populate the cache from the trigger handler before the framework
   * fires `pipeline:run:started`. Some pipeline shapes reach a terminal
   * state without ever publishing `started` (e.g. validation failures
   * like "No trigger node in definition") — without this priming the
   * terminal handler would have no pipelineId/triggeredBy to attribute
   * the failed record to. Idempotent: a real `started` event later in
   * the lifecycle overwrites the placeholder fields it cares about.
   */
  registerTrigger(args: {
    runId: string;
    pipelineId: string;
    userId: string;
    triggerType: string;
    at: string;
    /** Captured from verified request auth; absent system triggers fail closed. */
    organizationId?: string;
    /** The failed run this one replaces, when the trigger is a DLQ redrive. */
    redriveOf?: string;
    /** What the work graph shows for this run: its origin and its steps. */
    workGraph?: RunWorkGraphPlan;
  }): void {
    if (this.runCache.has(args.runId)) return;
    this.runCache.set(args.runId, {
      pipelineId: args.pipelineId,
      runId: args.runId,
      status: 'running',
      triggeredBy: args.userId,
      triggerType: args.triggerType,
      startedAt: args.at,
      steps: {},
      tokenUsage: {},
      estimatedCostUsd: 0,
      replayCredits: {},
      stepModelMap: {},
      stepInputMap: {},
      stepAttempts: {},
      stepLifecycle: {},
      stepLifecycleAt: {},
      lastEventAt: args.at,
      ...(args.organizationId ? { organizationId: args.organizationId } : {}),
      ...(args.redriveOf ? { redriveOf: args.redriveOf } : {}),
      ...(args.workGraph ? { workGraph: args.workGraph } : {}),
    });
  }

  /**
   * A review usually arrives after the run is terminal and evicted; when it is
   * still cached (a late event in flight) the next full-row put must not wipe
   * what the route just wrote.
   */
  setReview(runId: string, review: RunReview): void {
    const state = this.runCache.get(runId);
    if (state) state.review = review;
  }

  /**
   * The bus the executor publishes on — for a route that has something to say
   * about a run after the executor is done with it (a review verdict). Every
   * `pipeline:*` event on it reaches the gateway through the Redis relay.
   */
  getEventBus(): EventBus<PipelineEventMap> {
    return this.eventBus;
  }

  /**
   * Re-enqueue a durable projection transition from an authoritative stored
   * run. This repairs a prior outbox/catalog outage without accepting an
   * external replay payload. The projection event key is deterministic, so
   * normal startup reconciliation is idempotent.
   */
  async reconcileWorkGraphRun(item: PipelineRunItem): Promise<void> {
    if (!item.organizationId || item.triggeredBy === 'system') return;
    await this.publishWorkGraphLifecycle({
      pipelineId: item.pipelineId,
      runId: item.runId,
      status: item.status,
      triggeredBy: item.triggeredBy,
      triggerType: item.triggerType,
      organizationId: item.organizationId,
      startedAt: item.startedAt,
      completedAt: item.completedAt,
      steps: item.stepsSummary ?? {},
      tokenUsage: {}, estimatedCostUsd: 0, replayCredits: {}, stepModelMap: {},
      stepInputMap: {}, stepAttempts: {}, stepLifecycle: {}, stepLifecycleAt: {}, lastEventAt: item.lastEventAt ?? item.completedAt ?? item.startedAt,
    }, item.lastEventAt ?? item.completedAt ?? item.startedAt);
  }

  /**
   * Resolve a run's cache entry, tolerating the cross-topic race where a
   * terminal event arrives before run:started has populated the cache.
   * `started` and `failed` travel on different Redis pub/sub channels with
   * independent ordering, so a fast-failing pipeline can publish both
   * within a few ms. Polls every 25ms for up to ~500ms.
   */
  private async waitForCachedRun(runId: string): Promise<RunState | null> {
    const cached = this.runCache.get(runId);
    if (cached) return cached;
    for (let i = 0; i < 20; i++) {
      await new Promise((r) => setTimeout(r, 25));
      const s = this.runCache.get(runId);
      if (s) return s;
    }
    return null;
  }

  /**
   * Construct a stub RunState from a terminal event when no run:started was
   * ever observed. Some pipeline shapes (e.g. validation failures on empty
   * definitions) reach a terminal state without a corresponding started
   * event; without this fallback the run is invisible to the UI. We cannot
   * recover the original pipelineId/triggeredBy/startedAt from the terminal
   * event alone — those fields are best-effort placeholders so the row at
   * least exists and the operator can see "something failed."
   */
  private synthesizeStateFromTerminal(runId: string, terminalAt: string): RunState {
    return {
      pipelineId: 'unknown',
      runId,
      status: 'running',
      triggeredBy: 'system',
      triggerType: 'unknown',
      startedAt: terminalAt,
      steps: {},
      tokenUsage: {},
      estimatedCostUsd: 0,
      replayCredits: {},
      stepModelMap: {},
      stepInputMap: {},
      stepAttempts: {},
      stepLifecycle: {},
      stepLifecycleAt: {},
      lastEventAt: terminalAt,
    };
  }

  private async handleRunCompleted(event: {
    runId: string;
    durationMs: number;
    at: string;
    status?: 'completed' | 'rejected';
    rejection?: RunRejection;
  }): Promise<void> {
    this.touchLastEvent(event.runId, event.at);
    const state =
      (await this.waitForCachedRun(event.runId)) ??
      this.synthesizeStateFromTerminal(event.runId, event.at);

    // Canonical spelling (matches PipelineExecutor and the UI). This used to
    // write the legacy `succeeded` alias, which made the runs *list* endpoint
    // (backed by this table) disagree with the runs *detail* endpoint (backed
    // by the live executor) for the same run.
    //
    // A run whose approval gate was REJECTED also arrives on this event — the
    // traversal finished without throwing — but must not be listed as a
    // success. The executor tells us which it was via `status`; older
    // executors omit the field, which still means `completed`.
    state.status = event.status === 'rejected' ? 'rejected' : 'completed';
    if (event.rejection) state.rejection = event.rejection;
    state.completedAt = event.at;
    await this.persistRun(state);
    await this.publishWorkGraphLifecycle(state, event.at);
    // A finished run leaves no step queueing work it will never do.
    await this.closeWorkGraphSteps(state, event.at);
    this.runCache.delete(event.runId);
  }

  private async handleRunFailed(event: {
    runId: string;
    error: { nodeId: string; message: string; stack?: string };
    at: string;
  }): Promise<void> {
    this.touchLastEvent(event.runId, event.at);
    const state =
      (await this.waitForCachedRun(event.runId)) ??
      this.synthesizeStateFromTerminal(event.runId, event.at);

    state.status = 'failed';
    state.completedAt = event.at;
    const failedNodeId = event.error.nodeId;
    const model = state.stepModelMap[failedNodeId];
    const modelSuffix = model ? ` (model: ${model})` : '';
    state.error = `${failedNodeId}: ${event.error.message}${modelSuffix}`;
    // Backfill: step:failed and run:failed arrive on independently-ordered
    // channels, and pre-durationMs executors may not emit step:failed at all
    // for the terminal node. The failed step must exist in stepsSummary or
    // "Retry from here" 400s on it.
    if (failedNodeId && !state.steps[failedNodeId]) {
      this.recordFailedStep(state, failedNodeId, event.error.message, event.at);
    }
    await this.persistRun(state);
    await this.publishWorkGraphLifecycle(state, event.at);
    // A finished run leaves no step queueing work it will never do.
    await this.closeWorkGraphSteps(state, event.at);
    this.runCache.delete(event.runId);
  }

  private async handleRunCancelled(event: { runId: string; at: string }): Promise<void> {
    this.touchLastEvent(event.runId, event.at);
    const state =
      (await this.waitForCachedRun(event.runId)) ??
      this.synthesizeStateFromTerminal(event.runId, event.at);

    // Canonical spelling — see handleRunCompleted. The US-spelled `canceled`
    // was the same class of divergence.
    state.status = 'cancelled';
    state.completedAt = event.at;
    await this.persistRun(state);
    await this.publishWorkGraphLifecycle(state, event.at);
    // A finished run leaves no step queueing work it will never do.
    await this.closeWorkGraphSteps(state, event.at);
    this.runCache.delete(event.runId);
  }

  // Hub#1281: accumulate per-attempt history from DC-15 events (maxAttempts > 1)
  private handleStepAttemptStarted(event: {
    runId: string;
    stepId: string;
    attemptNumber: number;
    startedAt: string;
  }): void {
    this.touchLastEvent(event.runId, event.startedAt);
    const state = this.runCache.get(event.runId);
    if (!state) return;
    if (!state.stepAttempts[event.stepId]) state.stepAttempts[event.stepId] = [];
    state.stepAttempts[event.stepId].push({
      attemptNumber: event.attemptNumber,
      startedAt: event.startedAt,
      idempotencyKey: `${event.runId}:${event.stepId}:${event.attemptNumber}`,
    });
  }

  /**
   * One turn of an agent step's loop. It is the step's own report of what it
   * has spent — model, context against its budget, turns taken — so it is
   * recorded verbatim onto the step summary and never extrapolated. It also
   * refreshes the step node's freshness lease, which is the only thing that
   * distinguishes a step still working from one that stopped answering.
   */
  private async handleAgentIteration(payload: Record<string, unknown>): Promise<void> {
    const runId = typeof payload.runId === 'string' ? payload.runId : '';
    const stepId = typeof payload.stepId === 'string' ? payload.stepId : '';
    if (!runId || !stepId) return;
    const at = typeof payload.at === 'string' ? payload.at : new Date().toISOString();
    this.touchLastEvent(runId, at);
    const state = this.runCache.get(runId);
    if (!state) return;
    const numeric = (value: unknown): number | undefined =>
      (typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : undefined);
    const agent = {
      ...(typeof payload.model === 'string' ? { model: payload.model } : {}),
      ...(numeric(payload.iteration) !== undefined ? { iterations: numeric(payload.iteration) } : {}),
      ...(numeric(payload.contextTokens) !== undefined ? { contextTokens: numeric(payload.contextTokens) } : {}),
      ...(numeric(payload.contextBudgetTokens) !== undefined
        ? { contextBudgetTokens: numeric(payload.contextBudgetTokens) } : {}),
      ...(typeof payload.sessionId === 'string' ? { sessionId: payload.sessionId, runtime: 'devbox' } : {}),
      ...(typeof payload.boxId === 'string' ? { boxId: payload.boxId } : {}),
    };
    const summary = state.steps[stepId] ?? { status: 'running' as const, startedAt: at };
    state.steps[stepId] = { ...summary, status: 'running', agent: { ...(summary.agent ?? {}), ...agent } };
    await this.persistRun(state).catch((err) => {
      log.warn({ runId, stepId, err: String(err) }, 'failed to persist agent iteration');
    });
    await this.markWorkGraphStep(runId, stepId, 'running', at);
  }

  private handleStepAttemptFailed(event: {
    runId: string;
    stepId: string;
    attemptNumber: number;
    error: string;
    nextBackoffMs: number;
    willRetry: boolean;
  }): void {
    const failedAt = new Date().toISOString();
    this.touchLastEvent(event.runId, failedAt);
    const state = this.runCache.get(event.runId);
    if (!state) return;
    const attempts = state.stepAttempts[event.stepId];
    if (!attempts || attempts.length === 0) return;
    const last = attempts[attempts.length - 1];
    last.completedAt = failedAt;
    last.error = event.error;
    if (event.nextBackoffMs > 0) last.retryAfterMs = event.nextBackoffMs;
    if (last.startedAt) {
      last.durationMs = Date.parse(failedAt) - Date.parse(last.startedAt);
    }
  }

  private handleLLMPrompt(event: { runId: string; stepId: string; model: string; prompt: string }): void {
    const state = this.runCache.get(event.runId);
    if (!state) return;
    // Store the model for this step so we can look it up when the response arrives
    state.stepModelMap[event.stepId] = event.model;
    // Hub#1283: snapshot the resolved prompt as this step's input for replay determinism.
    state.stepInputMap[event.stepId] = { prompt: event.prompt, model: event.model };
  }

  /**
   * GAP-11: persist spend the moment it is incurred. `pipeline:llm:response`
   * fires once per completed LLM call (after the stream finishes) — token-
   * level updates go through the separate `pipeline:llm:token` event, which
   * this service does not subscribe to — so this is the same granularity as
   * `step:completed` or a checkpoint, not a hot per-token path. Without this,
   * tokens were only durable as a side effect of the next `step:completed`
   * or checkpoint write: a process death between "LLM call returned" and
   * "step finished" paid for tokens that DDB never learned about, and a
   * resume re-paid for them from zero. A full `persistRun` here matches the
   * existing idiom (`handleCheckpoint` already does the same on every
   * checkpoint, which fires at comparable frequency), so this doesn't
   * introduce a new write pattern — it closes the one gap in coverage.
   */
  private async handleLLMResponse(event: {
    runId: string;
    stepId: string;
    tokensIn: number;
    tokensOut: number;
  }): Promise<void> {
    const state = this.runCache.get(event.runId);
    if (!state) return;

    // Resolve model from the prompt event; fall back to 'unknown' if prompt was missed
    const model = state.stepModelMap[event.stepId] ?? 'unknown';

    this.addTokenUsage(state, model, event.tokensIn, event.tokensOut);
    await this.persistRun(state);
  }

  /**
   * Fold one model call's tokens into a run's usage and re-price the run.
   *
   * Shared by the `llm:*` event path and the agent-step path below. They are
   * two ways the same thing happens — a model was called and produced tokens —
   * and having one place to add them is what keeps the two from disagreeing
   * about what a run cost.
   */
  private addTokenUsage(
    state: RunState,
    model: string,
    tokensIn: number,
    tokensOut: number,
  ): void {
    if (!state.tokenUsage[model]) {
      state.tokenUsage[model] = { tokensIn: 0, tokensOut: 0 };
    }
    state.tokenUsage[model].tokensIn += tokensIn;
    state.tokenUsage[model].tokensOut += tokensOut;

    let totalCost = 0;
    for (const [m, usage] of Object.entries(state.tokenUsage)) {
      totalCost += computeCost(m, usage.tokensIn, usage.tokensOut);
    }
    state.estimatedCostUsd = Math.round(totalCost * 1_000_000) / 1_000_000; // 6 decimal places
  }

  /**
   * Tokens reported by a step's own output, rather than by `llm:*` events.
   *
   * Agent steps never emitted `pipeline:llm:prompt` / `:response` — they run
   * their own multi-turn loop and put the totals in their output — so no run
   * built from agent steps ever had a cost. Since nothing in this deployment
   * uses a plain `llm` node, that was every run.
   *
   * Returns null when the output is not a step reporting model usage, which is
   * most of them: a server step's output is whatever that handler returns.
   */
  private tokensFromStepOutput(
    output: unknown,
  ): { model: string; tokensIn: number; tokensOut: number } | null {
    if (!output || typeof output !== 'object') return null;
    const o = output as { model?: unknown; tokensIn?: unknown; tokensOut?: unknown };
    if (typeof o.model !== 'string' || o.model.length === 0) return null;
    if (typeof o.tokensIn !== 'number' || typeof o.tokensOut !== 'number') return null;
    if (!Number.isFinite(o.tokensIn) || !Number.isFinite(o.tokensOut)) return null;
    if (o.tokensIn < 0 || o.tokensOut < 0) return null;
    if (o.tokensIn === 0 && o.tokensOut === 0) return null;
    return { model: o.model, tokensIn: o.tokensIn, tokensOut: o.tokensOut };
  }

  /**
   * Returns run IDs (with pipelineId) whose lastEventAt is before cutoffMs.
   * Used by StuckRunDetector to find in-flight runs that have gone silent.
   */
  /**
   * The attempt history of every step of a run this process is watching —
   * closed attempts from the step records and the in-flight ones the
   * step:attempt:* events are still accumulating. The live run snapshot the
   * detail route serves comes from the executor, which does not carry
   * attempts; this is where "attempt 2 of 3" comes from while a step retries.
   */
  getStepAttempts(runId: string): Record<string, StepAttempt[]> {
    const state = this.runCache.get(runId);
    if (!state) return {};
    const out: Record<string, StepAttempt[]> = {};
    for (const [stepKey, step] of Object.entries(state.steps)) {
      if (step.attempts && step.attempts.length > 0) out[stepKey] = step.attempts.map((a) => ({ ...a }));
    }
    for (const [stepId, attempts] of Object.entries(state.stepAttempts)) {
      if (attempts.length > 0) out[stepId] = attempts.map((a) => ({ ...a }));
    }
    return out;
  }

  getStaleRunIds(cutoffMs: number): Array<{ runId: string; pipelineId: string }> {
    const result: Array<{ runId: string; pipelineId: string }> = [];
    for (const [runId, state] of this.runCache) {
      if (Date.parse(state.lastEventAt) < cutoffMs) {
        result.push({ runId, pipelineId: state.pipelineId });
      }
    }
    return result;
  }

  private async persistRun(state: RunState): Promise<void> {
    const ttl = state.completedAt
      ? Math.floor(Date.parse(state.completedAt) / 1000) + this.ttlSeconds
      : undefined;

    const item: PipelineRunItem = {
      pipelineId: state.pipelineId,
      runId: state.runId,
      status: state.status,
      triggeredBy: state.triggeredBy,
      organizationId: state.organizationId,
      triggerType: state.triggerType,
      startedAt: state.startedAt,
      completedAt: state.completedAt,
      stepsSummary: state.steps,
      error: state.error,
      rejection: state.rejection,
      tokenUsage: Object.keys(state.tokenUsage).length > 0 ? state.tokenUsage : undefined,
      estimatedCostUsd: state.estimatedCostUsd > 0 ? state.estimatedCostUsd : undefined,
      lastEventAt: state.lastEventAt,
      ttl,
      // DC-25: full-row puts must carry the durable-resume fields, or a
      // routine step/terminal persist would wipe the checkpoint + claim.
      checkpoint: state.checkpoint,
      definitionSnapshot: state.definitionSnapshot,
      interruptedAt: state.interruptedAt,
      resumedAt: state.resumedAt,
      resumeCount: state.resumeCount,
      resumeClaimedBy: state.resumeClaimedBy,
      redriveOf: state.redriveOf,
      redrivenBy: state.redrivenBy,
      review: state.review,
    };

    await this.putWithRetry(item, state);
  }

  /**
   * Projection is deliberately downstream of successful authoritative run
   * persistence. Missing organization/source-account joins deny publication;
   * they never turn a system or webhook trigger into a person-owned event.
   */
  private async publishWorkGraphLifecycle(state: RunState, occurredAt: string): Promise<void> {
    if (!state.organizationId || state.triggeredBy === 'system') return;
    const result = await platformWorkGraphSourcePublisher.pipeline({
      kind: 'pipeline-run',
      run: {
        runId: state.runId,
        pipelineId: state.pipelineId,
        status: state.status,
        startedAt: state.startedAt,
        ...(state.completedAt ? { completedAt: state.completedAt } : {}),
      } as never,
      attributedSubject: state.triggeredBy,
      occurredAt,
      sourceSequence: `${state.status}:${occurredAt}`,
      // The conversation the command was typed in is evidence this run
      // consumed, and the only thing in the graph it belongs to. It is
      // declared, never inferred: a run with no chat origin declares nothing.
      ...(state.workGraph?.conversationId
        ? { inputRefs: [{ source: 'conversation' as const, resourceId: state.workGraph.conversationId }] }
        : {}),
      // The run has to be nameable: it is what an effort anchors on when the
      // conversation it came from is not in this person's graph.
      ...(state.workGraph?.title ? { safeLabel: state.workGraph.title } : {}),
    }, { organizationId: state.organizationId, sourceSubject: state.triggeredBy });
    if (!result.published) {
      log.warn({ runId: state.runId, reason: result.reason }, 'work graph pipeline lifecycle was not published');
    }
  }

  /**
   * One generated step of the run, projected as its own node so the graph
   * shows the steps a person was shown on the card and in the editor.
   *
   * The step plan comes from the definition that was actually triggered, so a
   * planner fallback that produced ONE step yields ONE node — the graph never
   * shows steps that were not planned.
   */
  private async publishWorkGraphStep(
    state: RunState,
    stepId: string,
    status: string,
    occurredAt: string,
  ): Promise<void> {
    if (!state.organizationId || state.triggeredBy === 'system') return;
    const planned = state.workGraph?.steps.find((step) => step.stepId === stepId);
    if (!planned) return;
    const result = await platformWorkGraphSourcePublisher.pipeline({
      kind: 'pipeline-step',
      run: {
        runId: state.runId,
        pipelineId: state.pipelineId,
        status: state.status,
        startedAt: state.startedAt,
      } as never,
      step: { stepId, status },
      attributedSubject: state.triggeredBy,
      occurredAt,
      sourceSequence: `${stepId}:${status}:${occurredAt}`,
      safeLabel: planned.title,
      // Where this step is executing. The live session map is authoritative
      // while the run is in flight; what the step itself reported is the
      // fallback once the run's session has been torn down.
      ...(this.workspaceFor(state, stepId) ? { workspace: this.workspaceFor(state, stepId) as { resourceId: string } } : {}),
      // What it is writing, named while it is still writing it.
      ...(this.producesFor(state, stepId).length ? { producesRefs: this.producesFor(state, stepId) } : {}),
      // The run it belongs to, then the steps it waits on. Both are already
      // projected pipeline resources, so the chain reads in dependsOn order.
      inputRefs: [
        { source: 'pipeline' as const, resourceId: state.runId },
        ...planned.dependsOn.map((dependency) => ({
          source: 'pipeline' as const,
          resourceId: pipelineStepResourceId(state.runId, dependency),
        })),
      ],
    }, { organizationId: state.organizationId, sourceSubject: state.triggeredBy });
    if (!result.published) {
      log.warn({ runId: state.runId, stepId, reason: result.reason }, 'work graph pipeline step was not published');
    }
  }

  /**
   * The cloud workspace a step is executing in. `recallRunLab` is the live
   * map the devbox client keeps for the run; a step that already finished
   * reports its own session on its summary, which is what survives teardown.
   * A local step has no workspace and declares none.
   */
  private workspaceFor(state: RunState, stepId: string): { resourceId: string; safeLabel?: string } | undefined {
    const live = recallRunLab(state.runId);
    const recorded = state.steps[stepId]?.agent;
    const sessionId = live?.sessionId ?? recorded?.sessionId;
    if (!sessionId) return undefined;
    const boxId = live?.boxId ?? recorded?.boxId;
    return {
      resourceId: sessionId,
      // The box is what a person recognises the session by; without one the
      // node stays a placeholder rather than repeating the opaque id.
      ...(boxId ? { safeLabel: `Cloud workspace · ${boxId}` } : {}),
    };
  }

  /**
   * The documents a step said it produced, from the step's OWN result — the
   * `{ kind: 'document', documentId }` convention every command already uses.
   * Nothing is inferred from the run's context or from a neighbouring step.
   */
  private producesFor(state: RunState, stepId: string): Array<{ source: 'document'; resourceId: string }> {
    const output = state.steps[stepId]?.output as { result?: unknown } | undefined;
    const result = parseRunResult(output?.result);
    return result?.kind === 'document' ? [{ source: 'document', resourceId: result.documentId }] : [];
  }

  /**
   * Every planned step at once, in dependsOn order, so the run's shape is
   * visible from the moment it starts rather than appearing a step at a time.
   * A step already past `status` keeps what it has — the projection only moves
   * forward, and `waiting` must never overwrite a step that is running.
   */
  private async publishWorkGraphStepPlan(state: RunState, occurredAt: string): Promise<void> {
    const steps = state.workGraph?.steps ?? [];
    for (const step of steps) {
      if (state.stepLifecycle[step.stepId]) continue;
      state.stepLifecycle[step.stepId] = 'pending';
      await this.publishWorkGraphStep(state, step.stepId, 'pending', occurredAt)
        .catch((err) => log.warn({ runId: state.runId, stepId: step.stepId, err: String(err) }, 'step projection failed'));
    }
  }

  /**
   * Records a step transition and projects it. Repeated `running` observations
   * are how a long step keeps its freshness lease alive, so they are published
   * again — but no more than once a minute, since the lease outlives that by
   * five.
   */
  private async markWorkGraphStep(
    runId: string,
    stepId: string,
    status: 'running' | 'completed' | 'failed' | 'cancelled',
    occurredAt: string,
  ): Promise<void> {
    const state = this.runCache.get(runId);
    if (!state || !state.workGraph) return;
    const previous = state.stepLifecycle[stepId];
    if (previous === status && status === 'running') {
      const since = Date.parse(occurredAt) - Date.parse(state.stepLifecycleAt[stepId] ?? '');
      if (Number.isFinite(since) && since < WORK_GRAPH_STEP_REFRESH_MS) return;
    } else if (previous === status) {
      return;
    }
    state.stepLifecycle[stepId] = status;
    state.stepLifecycleAt[stepId] = occurredAt;
    await this.publishWorkGraphStep(state, stepId, status, occurredAt)
      .catch((err) => log.warn({ runId, stepId, err: String(err) }, 'step projection failed'));
  }

  /**
   * A terminal run closes out every step that never reached one of its own.
   * Leaving them `waiting` would show a finished run still queueing work.
   */
  private async closeWorkGraphSteps(state: RunState, occurredAt: string): Promise<void> {
    for (const step of state.workGraph?.steps ?? []) {
      const current = state.stepLifecycle[step.stepId];
      if (current === 'completed' || current === 'failed' || current === 'cancelled') continue;
      state.stepLifecycle[step.stepId] = 'cancelled';
      await this.publishWorkGraphStep(state, step.stepId, 'cancelled', occurredAt)
        .catch((err) => log.warn({ runId: state.runId, stepId: step.stepId, err: String(err) }, 'step projection failed'));
    }
  }

  /**
   * Exponential-backoff retry around `repository.put`. Previously the DDB
   * write was wrapped in a bare `.catch()` at the subscriber level, so a
   * single transient blip silently orphaned the run: the in-memory cache
   * had the entry but no DDB row was ever written, leaving the run
   * un-queryable after a restart.
   *
   * Now: up to 3 retries with delays of 100ms, 400ms, 1600ms (4 total
   * attempts). On exhaustion we log at error level with the run identifiers
   * so the failure is visibly surfaced — operators have something to grep
   * for instead of a silent loss.
   *
   * Stays fire-and-forget from the caller's perspective: the wrapping
   * `.catch()` on each subscribe handler still swallows the final rejection
   * (after our retry budget is spent and the error has been logged).
   */
  private async putWithRetry(item: PipelineRunItem, state: RunState): Promise<void> {
    // Backoff ladder: 100ms, 400ms, 1600ms (3 sleeps between 4 total attempts).
    // Equivalent to the historical [100, 400, 1600] array via multiplier=4:
    //   sleep(n) = min(100 * 4^(n-1), 1600) → 100, 400, 1600.
    try {
      await retryWithBackoff(() => this.repository.put(item), {
        maxAttempts: 4,
        initialDelayMs: 100,
        multiplier: 4,
        maxDelayMs: 1600,
        onRetry: (_err, attempt) => {
          // Logged after the first failure, before the next sleep — matches the
          // spirit of the prior "succeeded after retry" log (which fired only on
          // success). We keep telemetry visible at info level either way.
          log.info(
            { runId: state.runId, pipelineId: state.pipelineId, attempt: attempt + 1 },
            'run persistence retry scheduled',
          );
        },
      });
    } catch (err) {
      // All attempts exhausted — surface visibly at error level.
      log.error(
        {
          runId: state.runId,
          pipelineId: state.pipelineId,
          status: state.status,
          attempts: 4,
          err: String(err),
        },
        'run persistence failed after all retries — DDB row will not exist for this run',
      );
      throw err;
    }
  }
}

// Singleton for cross-module access (parallel to setPipelineBridge in
// bridge-types.ts). The trigger route uses this to call registerTrigger()
// before the framework fires events. Stays null when run persistence is
// disabled (PIPELINE_RUN_PERSISTENCE_ENABLED=false).
let runPersistenceSingleton: RunPersistenceService | null = null;

export function setRunPersistenceService(s: RunPersistenceService | null): void {
  runPersistenceSingleton = s;
}

export function getRunPersistenceService(): RunPersistenceService | null {
  return runPersistenceSingleton;
}
