// A run that fails lands in the dead-letter queue, and redriving it runs it again.
//
// The EventBus DLQ used to hold only bus dead letters (a subscriber threw, a
// publish failed). A run that exhausted its retries — compose never came back,
// the model never answered, the document could not be read — ended `failed`
// in the run store and NOWHERE an operator looks for things to redrive. This
// makes a failed run a dead letter of its own: the entry carries what the
// trigger carried, so a redrive re-triggers the same pipeline with the same
// payload after the operator has fixed whatever was wrong.
//
// It watches the executor's own events rather than the run store: `run:failed`
// names only the run, so the pipeline id comes from `run:started` and the
// trigger payload from the first step to complete (the trigger node is always
// first; its output IS the trigger payload).

import { wrap } from 'distributed-core/messaging-queue';
import type { BusEvent } from 'distributed-core';
import type { DeadLetterQueue } from 'distributed-core/messaging-queue';
import type { DLQEntry } from '../repositories/DLQRepository';
import baseLog from '../lib/logger';

const log = baseLog.child({ service: 'failed-run-dlq' });

/** The synthetic event type a failed run is filed under. Redrive re-triggers it. */
export const RUN_RETRIGGER_EVENT = 'pipeline:run:retrigger';

export interface RunRetriggerPayload {
  pipelineId: string;
  failedRunId: string;
  triggerPayload: Record<string, unknown>;
  triggeredBy?: { userId?: string; triggerType?: string };
  error: { nodeId: string; message: string };
}

interface TrackedRun {
  pipelineId: string;
  triggeredBy?: { userId?: string; triggerType?: string };
  triggerPayload?: Record<string, unknown>;
}

/** The slice of the pipeline EventBus this needs. Typed loosely so tests can hand a stub. */
export interface RunEventSource {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  subscribe(type: any, handler: (event: { payload: any }) => void | Promise<void>): string;
  unsubscribe?(subscriptionId: string): void;
}

/** How long a persisted failed run stays in the table before DDB expires it. */
export const FAILED_RUN_DLQ_TTL_SECONDS = 30 * 24 * 60 * 60;

export interface FailedRunDeadLetterOptions {
  eventBus: RunEventSource;
  dlq: DeadLetterQueue<BusEvent>;
  /**
   * Durable copy of each entry, written alongside `dlq.put` so the run is
   * still there to redrive after a restart (deadLetterStore.ts reads them
   * back at boot). Optional: without it the entry lives in memory only.
   */
  persist?: (entry: DLQEntry) => Promise<void>;
  now?: () => number;
}

/** Subscribe. Returns a detach function. */
export function attachFailedRunDeadLetters(opts: FailedRunDeadLetterOptions): () => void {
  const { eventBus, dlq } = opts;
  const now = opts.now ?? (() => Date.now());
  const runs = new Map<string, TrackedRun>();
  const subs: string[] = [];

  subs.push(eventBus.subscribe('pipeline:run:started', (e) => {
    const p = e.payload as { runId: string; pipelineId: string; triggeredBy?: { userId?: string; triggerType?: string; payload?: Record<string, unknown> } };
    runs.set(p.runId, {
      pipelineId: p.pipelineId,
      ...(p.triggeredBy ? { triggeredBy: { userId: p.triggeredBy.userId, triggerType: p.triggeredBy.triggerType } } : {}),
      // Some executors put the payload on the trigger; the step output below wins when it arrives.
      ...(p.triggeredBy?.payload && Object.keys(p.triggeredBy.payload).length > 0 ? { triggerPayload: p.triggeredBy.payload } : {}),
    });
  }));

  subs.push(eventBus.subscribe('pipeline:step:completed', (e) => {
    const p = e.payload as { runId: string; stepId: string; output?: unknown };
    const run = runs.get(p.runId);
    if (!run || run.triggerPayload) return;
    if (p.output && typeof p.output === 'object' && !Array.isArray(p.output)) {
      run.triggerPayload = p.output as Record<string, unknown>;
    }
  }));

  const forget = (e: { payload: { runId: string } }) => { runs.delete(e.payload.runId); };
  subs.push(eventBus.subscribe('pipeline:run:completed', forget));
  subs.push(eventBus.subscribe('pipeline:run:cancelled', forget));

  subs.push(eventBus.subscribe('pipeline:run:failed', async (e) => {
    const p = e.payload as { runId: string; error: { nodeId: string; message: string }; at?: string };
    const run = runs.get(p.runId);
    runs.delete(p.runId);
    if (!run) {
      // A run this process never saw start (resumed from a checkpoint by a
      // sibling, or older than the map). Nothing to re-trigger with.
      log.warn({ runId: p.runId }, 'failed run had no tracked trigger; not dead-lettered');
      return;
    }
    const failedAtMs = now();
    const payload: RunRetriggerPayload = {
      pipelineId: run.pipelineId,
      failedRunId: p.runId,
      triggerPayload: run.triggerPayload ?? {},
      ...(run.triggeredBy ? { triggeredBy: run.triggeredBy } : {}),
      error: { nodeId: p.error?.nodeId ?? '(unknown)', message: p.error?.message ?? 'run failed' },
    };
    const messageId = `run-failed:${p.runId}`;
    const event = {
      id: messageId,
      type: RUN_RETRIGGER_EVENT,
      payload,
      timestamp: failedAtMs,
      sourceNodeId: 'pipeline-executor',
    } as unknown as BusEvent;
    // `<Class>: <message>` so the route's ?errorKind= filter can match.
    const lastError = `PipelineRunFailed: [${payload.error.nodeId}] ${payload.error.message}`;
    try {
      await dlq.put(wrap(event, { id: messageId, now: () => failedAtMs, attemptCount: 0 }), {
        lastError,
        failedAtMs,
        totalAttempts: 1,
      });
      log.info({ runId: p.runId, pipelineId: run.pipelineId, nodeId: payload.error.nodeId }, 'failed run dead-lettered');
    } catch (err) {
      log.error({ runId: p.runId, err: err instanceof Error ? err.message : String(err) }, 'could not dead-letter the failed run');
      return;
    }
    if (!opts.persist) return;
    try {
      await opts.persist({
        messageId,
        eventType: RUN_RETRIGGER_EVENT,
        eventPayload: payload,
        lastError,
        failedAt: new Date(failedAtMs).toISOString(),
        totalAttempts: 1,
        ttl: Math.floor(failedAtMs / 1000) + FAILED_RUN_DLQ_TTL_SECONDS,
      });
    } catch (err) {
      // Memory has it; only the restart-survival copy is missing.
      log.error({ runId: p.runId, err: err instanceof Error ? err.message : String(err) }, 'could not persist the failed run dead letter');
    }
  }));

  return () => {
    for (const id of subs) eventBus.unsubscribe?.(id);
    runs.clear();
  };
}

/** True when a DLQ envelope body is a failed run (vs. a bus dead letter). */
export function isRunRetrigger(event: { type?: unknown }): boolean {
  return event?.type === RUN_RETRIGGER_EVENT;
}

export interface RetriggerDeps {
  /** Definition lookup: system registry first, then the owner's saved definitions. */
  resolveDefinition: (pipelineId: string, userId?: string) => Promise<Record<string, unknown> | null>;
  trigger: (input: { pipelineId: string; definition: Record<string, unknown>; triggerPayload: Record<string, unknown>; triggeredBy: { userId: string } }) => Promise<{ runId: string }>;
  /**
   * Record on the ORIGINAL run that `newRunId` replaced it (its `redrivenBy`).
   * The new run learns of the original through `redriveOf` in its trigger
   * payload; this is the other direction. Failures are logged, not thrown —
   * the redrive already happened.
   */
  linkRedrive?: (link: { pipelineId: string; failedRunId: string; newRunId: string }) => Promise<void>;
}

/** Redrive: run the failed run's pipeline again with the same trigger payload. */
export async function retriggerFromDeadLetter(payload: RunRetriggerPayload, deps: RetriggerDeps): Promise<{ runId: string }> {
  const userId = payload.triggeredBy?.userId ?? 'system';
  const definition = await deps.resolveDefinition(payload.pipelineId, userId);
  if (!definition) throw new Error(`no definition for pipeline ${payload.pipelineId}; cannot re-trigger run ${payload.failedRunId}`);
  const out = await deps.trigger({ pipelineId: payload.pipelineId, definition, triggerPayload: { ...payload.triggerPayload, redriveOf: payload.failedRunId }, triggeredBy: { userId } });
  log.info({ failedRunId: payload.failedRunId, runId: out.runId, pipelineId: payload.pipelineId }, 'failed run re-triggered from the DLQ');
  if (deps.linkRedrive) {
    try {
      await deps.linkRedrive({ pipelineId: payload.pipelineId, failedRunId: payload.failedRunId, newRunId: out.runId });
    } catch (err) {
      log.warn({ failedRunId: payload.failedRunId, runId: out.runId, err: err instanceof Error ? err.message : String(err) }, 'could not record redrivenBy on the original run');
    }
  }
  return out;
}
