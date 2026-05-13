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

import type { EventBus, PipelineEventMap } from 'distributed-core';
import {
  PipelineRunRepository,
  type PipelineRunItem,
  type StepAttempt,
  type StepSummary,
  type TokenUsageByModel,
} from '../repositories/PipelineRunRepository';
import { withContext } from '../lib/logger';
import { computeCost } from './llm-pricing';

const log = withContext({ service: 'runPersistence' });

export interface RunPersistenceOptions {
  /** EventBus instance to subscribe to. */
  eventBus: EventBus<PipelineEventMap>;
  /** Repository for persisting run state. */
  repository: PipelineRunRepository;
  /** TTL in seconds for completed runs (default: 90 days). */
  ttlSeconds?: number;
  /** Maximum size of step output to persist (default: 100KB). */
  maxStepOutputBytes?: number;
}

/**
 * In-memory cache of run state before persistence. Accumulates step outputs
 * across step:completed events until run reaches terminal state, then flushes
 * to DynamoDB.
 */
interface RunState {
  pipelineId: string;
  runId: string;
  status: string;
  triggeredBy: string;
  triggerType: string;
  startedAt: string;
  completedAt?: string;
  steps: Record<string, StepSummary>;
  error?: string;
  /** Per-model token usage accumulated across LLM steps. */
  tokenUsage: Record<string, TokenUsageByModel>;
  /** Estimated total cost in USD. */
  estimatedCostUsd: number;
  /** Maps stepId → model for correlating llm:prompt with llm:response. */
  stepModelMap: Record<string, string>;
  /** Transient input snapshot per stepId; captured from llm:prompt; cleared after step:completed. */
  stepInputMap: Record<string, unknown>;
  /** In-flight attempt history per stepId; populated by step:attempt:* events (maxAttempts > 1). */
  stepAttempts: Record<string, StepAttempt[]>;
  /** ISO timestamp of the most recent event observed for this run. */
  lastEventAt: string;
}

export class RunPersistenceService {
  private readonly eventBus: EventBus<PipelineEventMap>;
  private readonly repository: PipelineRunRepository;
  private readonly ttlSeconds: number;
  private readonly maxStepOutputBytes: number;
  private readonly runCache = new Map<string, RunState>();
  private subscriptionActive = false;

  constructor(options: RunPersistenceOptions) {
    this.eventBus = options.eventBus;
    this.repository = options.repository;
    this.ttlSeconds = options.ttlSeconds ?? 90 * 24 * 60 * 60; // 90 days
    this.maxStepOutputBytes = options.maxStepOutputBytes ?? 100 * 1024; // 100KB
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

    this.eventBus.subscribe('pipeline:step:completed', async (event) => {
      await this.handleStepCompleted(event.payload).catch((err) => {
        log.warn(
          { runId: event.payload.runId, stepId: event.payload.stepId, err: String(err) },
          'failed to persist step:completed',
        );
      });
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

    // Per-attempt retry events (DC-15 — only emitted when maxAttempts > 1)
    this.eventBus.subscribe('pipeline:step:attempt:started', async (event) => {
      this.handleStepAttemptStarted(event.payload);
    });

    this.eventBus.subscribe('pipeline:step:attempt:failed', async (event) => {
      this.handleStepAttemptFailed(event.payload);
    });

    // LLM cost tracking — correlate prompt (has model) with response (has tokens)
    this.eventBus.subscribe('pipeline:llm:prompt', async (event) => {
      this.handleLLMPrompt(event.payload);
    });

    this.eventBus.subscribe('pipeline:llm:response', async (event) => {
      this.handleLLMResponse(event.payload);
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
    triggeredBy: { triggerType: string; userId?: string };
    at: string;
  }): Promise<void> {
    const userId = event.triggeredBy.userId ?? 'system';
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
      stepModelMap: {},
      stepInputMap: {},
      stepAttempts: {},
      lastEventAt: event.at,
    };

    this.runCache.set(event.runId, state);
    await this.persistRun(state);
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
      stepModelMap: {},
      stepInputMap: {},
      stepAttempts: {},
      lastEventAt: args.at,
    });
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
      stepModelMap: {},
      stepInputMap: {},
      stepAttempts: {},
      lastEventAt: terminalAt,
    };
  }

  private async handleRunCompleted(event: { runId: string; durationMs: number; at: string }): Promise<void> {
    this.touchLastEvent(event.runId, event.at);
    const state =
      (await this.waitForCachedRun(event.runId)) ??
      this.synthesizeStateFromTerminal(event.runId, event.at);

    state.status = 'succeeded';
    state.completedAt = event.at;
    await this.persistRun(state);
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
    await this.persistRun(state);
    this.runCache.delete(event.runId);
  }

  private async handleRunCancelled(event: { runId: string; at: string }): Promise<void> {
    this.touchLastEvent(event.runId, event.at);
    const state =
      (await this.waitForCachedRun(event.runId)) ??
      this.synthesizeStateFromTerminal(event.runId, event.at);

    state.status = 'canceled';
    state.completedAt = event.at;
    await this.persistRun(state);
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

  private handleLLMResponse(event: {
    runId: string;
    stepId: string;
    tokensIn: number;
    tokensOut: number;
  }): void {
    const state = this.runCache.get(event.runId);
    if (!state) return;

    // Resolve model from the prompt event; fall back to 'unknown' if prompt was missed
    const model = state.stepModelMap[event.stepId] ?? 'unknown';

    // Accumulate token usage for this model
    if (!state.tokenUsage[model]) {
      state.tokenUsage[model] = { tokensIn: 0, tokensOut: 0 };
    }
    state.tokenUsage[model].tokensIn += event.tokensIn;
    state.tokenUsage[model].tokensOut += event.tokensOut;

    // Recompute total estimated cost across all models
    let totalCost = 0;
    for (const [m, usage] of Object.entries(state.tokenUsage)) {
      totalCost += computeCost(m, usage.tokensIn, usage.tokensOut);
    }
    state.estimatedCostUsd = Math.round(totalCost * 1_000_000) / 1_000_000; // 6 decimal places
  }

  /**
   * Returns run IDs (with pipelineId) whose lastEventAt is before cutoffMs.
   * Used by StuckRunDetector to find in-flight runs that have gone silent.
   */
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
      triggerType: state.triggerType,
      startedAt: state.startedAt,
      completedAt: state.completedAt,
      stepsSummary: state.steps,
      error: state.error,
      tokenUsage: Object.keys(state.tokenUsage).length > 0 ? state.tokenUsage : undefined,
      estimatedCostUsd: state.estimatedCostUsd > 0 ? state.estimatedCostUsd : undefined,
      lastEventAt: state.lastEventAt,
      ttl,
    };

    await this.putWithRetry(item, state);
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
    const delays = [100, 400, 1600]; // ms — exponential-ish (×4)
    let lastErr: unknown;
    for (let attempt = 0; attempt <= delays.length; attempt++) {
      try {
        await this.repository.put(item);
        if (attempt > 0) {
          log.info(
            { runId: state.runId, pipelineId: state.pipelineId, attempt: attempt + 1 },
            'run persistence succeeded after retry',
          );
        }
        return;
      } catch (err) {
        lastErr = err;
        if (attempt < delays.length) {
          await new Promise((r) => setTimeout(r, delays[attempt]));
          continue;
        }
        // All attempts exhausted — surface visibly at error level.
        log.error(
          {
            runId: state.runId,
            pipelineId: state.pipelineId,
            status: state.status,
            attempts: attempt + 1,
            err: String(err),
          },
          'run persistence failed after all retries — DDB row will not exist for this run',
        );
        throw lastErr;
      }
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
