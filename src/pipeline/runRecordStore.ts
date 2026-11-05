// platform-api/src/pipeline/runRecordStore.ts
//
// The seam between platform-api's run table (PipelineRunItem rows behind
// RunRecordRepository) and pipeline-runtime's `/durable` pieces, which speak
// `DurableRunRecord` and `RunStore`.
//
//   - `toDurableRunRecord` maps a row (plus the definition it runs) onto the
//     shared record shape, so `resumeStateFromRecord` builds its resume state.
//   - `resumeStateForRow` is that resume state for a row: the boot resume and
//     POST …/resume?fromNodeId (after a restart) both continue from it.
//   - `RunRecordRecoveryStore` is a RunStore over a set of rows already vetted
//     for boot resume, whose `claim` is the run table's conditional
//     `claimForResume`. It is only what `recoverInterruptedRuns` needs: rows
//     are written by RunPersistenceService from executor events, not through
//     RunStore transactions, so `transact` / `commandRun` are refused.

import type {
  PipelineDefinition,
  PipelineResumeState,
  ResumeJoinArrival,
} from 'distributed-core/applications/pipeline';
import {
  resumeStateFromRecord,
  type CheckpointStep,
  type DurableCheckpoint,
  type DurableRunRecord,
  type DurableRunStatus,
  type RunFilter,
  type RunStore,
  type TransactResult,
} from '@connorhoehn/pipeline-runtime/durable';
import type { PipelineRunItem, RunRecordRepository } from '../repositories/PipelineRunRepository';
import { normalizeRunStatus } from './run-rollup';

/** What `toDurableRunRecord` keeps in `record.meta`. */
export interface RunRowMeta {
  /** The row the record was mapped from. */
  item: PipelineRunItem;
  [key: string]: unknown;
}

/** The row a record was mapped from (see `toDurableRunRecord`). */
export function rowOf(record: DurableRunRecord): PipelineRunItem {
  return (record.meta as RunRowMeta).item;
}

/**
 * The row's checkpoint in the shared shape. Checkpoint step records are
 * persisted without outputs (execution truth travels in `context.steps`);
 * the display output is re-attached from `stepsSummary` where it is still
 * there, as the resume has always done.
 */
function checkpointOf(item: PipelineRunItem): DurableCheckpoint | null {
  const cp = item.checkpoint;
  if (!cp) return null;
  const steps: Record<string, CheckpointStep> = {};
  for (const [stepId, raw] of Object.entries(cp.steps ?? {})) {
    const rec = { ...(raw as Record<string, unknown>) };
    const summary = item.stepsSummary?.[stepId];
    if (rec['output'] === undefined && summary?.output !== undefined) rec['output'] = summary.output;
    steps[stepId] = rec as unknown as CheckpointStep;
  }
  return {
    sequenceNumber: cp.sequenceNumber,
    at: cp.at,
    currentStepIds: cp.currentStepIds,
    completedStepCount: cp.completedStepCount ?? 0,
    context: cp.context,
    steps,
    joinStates: (cp.joinStates ?? {}) as Record<string, ResumeJoinArrival[]>,
    firedJoins: cp.firedJoins ?? [],
    ...(cp.droppedContextStepKeys ? { droppedContextStepKeys: cp.droppedContextStepKeys } : {}),
  };
}

/** A row's status in the shared vocabulary (`succeeded` → `completed`, `stuck` → `running`, …). */
function durableStatusOf(item: PipelineRunItem): DurableRunStatus {
  const status = normalizeRunStatus(item.status);
  return status === null || status === 'stuck' ? 'running' : status;
}

/**
 * A run-table row as a `DurableRunRecord`. `definition` is the graph the run
 * executes (the row's `definitionSnapshot`, or the caller's fallback).
 * `replays` is the executor's `replayAttempts`; the boot resume has always
 * passed the row's `resumeCount`. Step records are left empty: approval
 * decisions live in the checkpoint here, so `resumeStateFromRecord` has
 * nothing to merge from them.
 */
export function toDurableRunRecord(
  item: PipelineRunItem,
  definition: PipelineDefinition,
  opts: { replays?: number; owner?: string } = {},
): DurableRunRecord {
  const userId = item.triggeredBy && item.triggeredBy !== 'system' ? item.triggeredBy : undefined;
  return {
    id: item.runId,
    pipelineId: item.pipelineId,
    status: durableStatusOf(item),
    startedAt: item.startedAt,
    updatedAt: item.interruptedAt ?? item.lastEventAt ?? item.completedAt ?? item.startedAt,
    completedAt: item.completedAt ?? null,
    replays: opts.replays ?? item.resumeCount ?? 0,
    epoch: item.resumeCount ?? 0,
    runSeq: item.checkpoint?.sequenceNumber ?? 0,
    revision: 0,
    definition,
    steps: {},
    checkpoint: checkpointOf(item),
    triggeredBy: { ...(userId ? { userId } : {}), triggerType: item.triggerType ?? 'manual' },
    ...(opts.owner ?? item.resumeClaimedBy ?? item.ownerNodeId
      ? { owner: opts.owner ?? item.resumeClaimedBy ?? item.ownerNodeId }
      : {}),
    meta: { item } satisfies RunRowMeta,
  };
}

/**
 * The executor resume state for a row: its checkpoint (with display outputs
 * re-attached), its trigger, and `replays` as `replayAttempts`. A row with
 * no checkpoint has no frontier to continue from; callers check that first.
 */
export function resumeStateForRow(
  item: PipelineRunItem,
  definition: PipelineDefinition,
  opts: { replays?: number } = {},
): PipelineResumeState {
  return resumeStateFromRecord(toDurableRunRecord(item, definition, opts));
}

/**
 * RunStore over rows vetted for boot resume (in the window, a checkpoint and
 * a definition each). `claim` is the run table's conditional
 * `interrupted → running` write; a claim that throws is remembered in
 * `claimErrors` (the caller counts those as failures, not lost races) and
 * rethrown, which `recoverInterruptedRuns` reports as a lost claim.
 */
export class RunRecordRecoveryStore implements RunStore {
  private readonly records = new Map<string, DurableRunRecord>();
  /** runId → error text, for claims that threw (not lost races). */
  readonly claimErrors = new Map<string, string>();

  constructor(
    private readonly repository: Pick<RunRecordRepository, 'claimForResume'>,
    records: readonly DurableRunRecord[],
    private readonly now: () => number = Date.now,
  ) {
    for (const r of records) this.records.set(r.id, r);
  }

  async get(id: string): Promise<DurableRunRecord | null> {
    return this.records.get(id) ?? null;
  }

  async list(filter: RunFilter = {}): Promise<DurableRunRecord[]> {
    return [...this.records.values()].filter(
      (r) =>
        (filter.pipelineId === undefined || r.pipelineId === filter.pipelineId) &&
        (!filter.statuses || filter.statuses.includes(r.status)),
    );
  }

  listByStatus(statuses: readonly DurableRunStatus[]): Promise<DurableRunRecord[]> {
    return this.list({ statuses });
  }

  async claim(runId: string, owner: string): Promise<DurableRunRecord | null> {
    const record = this.records.get(runId);
    if (!record) return null;
    let claimed: PipelineRunItem | null;
    try {
      claimed = await this.repository.claimForResume(
        record.pipelineId,
        runId,
        owner,
        new Date(this.now()).toISOString(),
      );
    } catch (err) {
      this.claimErrors.set(runId, String(err));
      throw err;
    }
    if (!claimed) return null;
    // The frontier, trigger and display outputs come from the vetted row;
    // the claim contributes the resume bookkeeping and is what gets seeded.
    const next: DurableRunRecord = {
      ...toDurableRunRecord(rowOf(record), record.definition, { owner }),
      status: 'running',
      replays: claimed.resumeCount ?? 0,
      epoch: claimed.resumeCount ?? record.epoch + 1,
      meta: { item: rowOf(record), claimed },
    };
    this.records.set(runId, next);
    return next;
  }

  transact(): Promise<TransactResult> {
    return Promise.reject(new Error('RunRecordRecoveryStore: run rows are written by RunPersistenceService, not RunStore transactions'));
  }

  commandRun(): Promise<string | undefined> {
    return Promise.resolve(undefined);
  }
}

/** The claimed row a `RunRecordRecoveryStore.claim` record carries. */
export function claimedRowOf(record: DurableRunRecord): PipelineRunItem {
  return (record.meta as { claimed?: PipelineRunItem }).claimed ?? rowOf(record);
}
