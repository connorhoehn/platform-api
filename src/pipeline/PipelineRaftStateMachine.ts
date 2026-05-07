// social-api/src/pipeline/PipelineRaftStateMachine.ts
//
// Raft-replicated state machine for pipeline-run lifecycle. Implements
// `IRaftStateMachine<TCommand, TResult>` from distributed-core's Raft surface
// (cluster/raft/state/IRaftStateMachine.d.ts and RaftStateMachineBase.d.ts).
//
// Why an extra state machine on top of distributed-core's Raft surface?
//   - The Cluster facade's `registry: { type: 'raft', raftConfig }` path
//     hardwires its OWN `RaftStateMachine` (an entity-record state machine in
//     distributed-core/cluster/raft/state/RaftStateMachine.js — used by
//     `RaftEntityRegistry`). Pipeline-run state is a DIFFERENT shape — it's
//     append-mostly with terminal states ('started' → 'completed'/'failed'/
//     'cancelled'), so we keep our own minimal SM here and let the
//     entity-record SM stay in its lane.
//   - This SM is the slot a multi-Raft setup (MultiRaftCoordinator) would
//     plug into when an operator wants pipeline-run state itself replicated
//     through consensus. Single-process bootstrap doesn't currently host a
//     RaftNode against this SM — see raft-bootstrap.test.ts for the smoke
//     test we DO exercise (apply / snapshot / restore).
//
// State shape — `runId → { status, lastEvent, checkpoint }`:
//   - `status` is the canonical RunStatus from distributed-core's pipeline
//     module (pending / running / awaiting_approval / completed / failed /
//     cancelled).
//   - `lastEvent` is a free-form discriminator captured from the most
//     recent log entry's command — useful for postmortem replay.
//   - `checkpoint` is an opaque per-run blob (current step ids, last
//     emitted event index, etc.) that operators populate per command.
//
// Commands ('PipelineRunCommand'): a small union covering 'started',
// 'updated', 'checkpointed', 'terminated'. Built-in 'config-change' and
// 'noop' commands are intercepted by RaftStateMachineBase before
// applyCommand() runs.
//
// Idempotency contract: `apply(entry)` MUST be idempotent over `entry.index`.
// We track the highest applied index via the base class and reject older
// indexes by short-circuiting before mutating state. The base's `apply()`
// updates `lastAppliedIndex`/`lastAppliedTerm` for us — we just need to
// avoid double-mutating when the SAME index replays.
//
// Snapshot/restore: JSON-serializable. `exportUserState()` returns a
// versioned envelope so a future on-disk snapshot can detect format drift.

import { RaftStateMachineBase } from 'distributed-core';
import type {
  RaftClusterConfig,
  RaftLogEntry,
  RaftSnapshot,
  RunStatus,
} from 'distributed-core';

// ---------------------------------------------------------------------------
// Public command + state types
// ---------------------------------------------------------------------------

/**
 * The set of pipeline-run lifecycle commands proposed through Raft.
 *
 * Discriminated by `type` so the base class's `applyCommand()` switch is
 * exhaustive at compile time.
 */
export type PipelineRunCommand =
  | {
      type: 'run-started';
      runId: string;
      pipelineId: string;
      pipelineVersion: number;
      ownerNodeId: string;
      startedAt: string;
    }
  | {
      type: 'run-updated';
      runId: string;
      status: RunStatus;
      eventName: string;
      at: string;
    }
  | {
      type: 'run-checkpointed';
      runId: string;
      checkpoint: Record<string, unknown>;
      at: string;
    }
  | {
      type: 'run-terminated';
      runId: string;
      /** Must be a terminal status: completed | failed | cancelled. */
      status: Extract<RunStatus, 'completed' | 'failed' | 'cancelled'>;
      at: string;
      error?: { message: string; nodeId?: string };
    };

/**
 * Result returned from `apply()` for each entry. `null` for built-in
 * (`config-change` / `noop`) entries; otherwise echoes back the runId
 * and the new status so callers can correlate proposals with their
 * effect on the SM.
 */
export interface PipelineRunApplyResult {
  runId: string;
  status: RunStatus;
  applied: 'created' | 'updated' | 'checkpointed' | 'terminated' | 'idempotent-skip';
}

/** Per-run state held in the SM. */
export interface PipelineRunSlot {
  runId: string;
  pipelineId: string;
  pipelineVersion: number;
  ownerNodeId: string;
  status: RunStatus;
  lastEvent: string;
  /** Wall-clock ISO of the most recent transition. */
  lastEventAt: string;
  /** Most recent applied log index that touched this run. */
  lastAppliedIndex: number;
  checkpoint: Record<string, unknown>;
}

/**
 * Snapshot envelope — what `exportUserState()` returns. Versioned so a
 * future snapshot format change can be detected on `importUserState()`.
 */
export interface PipelineRaftSnapshotState {
  version: 1;
  runs: Record<string, PipelineRunSlot>;
}

const TERMINAL_STATUSES: ReadonlyArray<RunStatus> = ['completed', 'failed', 'cancelled'];

function isTerminal(status: RunStatus): boolean {
  return TERMINAL_STATUSES.includes(status);
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

/**
 * Default initial cluster config — single-node, just this node. Callers can
 * override at construction time when the SM is part of a real Raft group.
 */
export function defaultPipelineRaftClusterConfig(nodeId: string): RaftClusterConfig {
  return { members: [nodeId] };
}

/**
 * Pipeline-run Raft state machine.
 *
 * Subclasses {@link RaftStateMachineBase} so the standard Raft plumbing
 * (built-in command handling, lastAppliedIndex/Term bookkeeping, snapshot
 * envelope wrapping) is inherited. We only own the `applyCommand`,
 * `exportUserState`, and `importUserState` overrides.
 */
export class PipelineRaftStateMachine extends RaftStateMachineBase<
  PipelineRunCommand,
  PipelineRunApplyResult
> {
  /** runId → slot. */
  private readonly runs = new Map<string, PipelineRunSlot>();

  constructor(initialConfig: RaftClusterConfig) {
    super(initialConfig);
  }

  // ---------------------------------------------------------------------
  // Read API — used by tests and any in-process consumer (e.g. a future
  // postmortem aggregator). NOT part of the IRaftStateMachine contract.
  // ---------------------------------------------------------------------

  getRun(runId: string): PipelineRunSlot | undefined {
    return this.runs.get(runId);
  }

  listRuns(): PipelineRunSlot[] {
    return Array.from(this.runs.values());
  }

  size(): number {
    return this.runs.size;
  }

  // ---------------------------------------------------------------------
  // RaftStateMachineBase overrides
  // ---------------------------------------------------------------------

  /**
   * Apply a single user command. Built-in commands ('config-change' /
   * 'noop') never reach here — the base class intercepts them first.
   *
   * Idempotency: if `entry.index <= slot.lastAppliedIndex`, we treat the
   * entry as a replay and skip the mutation. The base class always
   * updates its own lastAppliedIndex/Term — that's safe because we only
   * track per-run dedup here.
   */
  protected applyCommand(
    command: PipelineRunCommand,
    entry: RaftLogEntry<PipelineRunCommand>,
  ): PipelineRunApplyResult {
    switch (command.type) {
      case 'run-started':
        return this.applyRunStarted(command, entry);
      case 'run-updated':
        return this.applyRunUpdated(command, entry);
      case 'run-checkpointed':
        return this.applyRunCheckpointed(command, entry);
      case 'run-terminated':
        return this.applyRunTerminated(command, entry);
      default: {
        // Exhaustiveness check — TS will flag a new command variant at
        // compile time if we forget to handle it.
        const _exhaustive: never = command;
        throw new Error(
          `[PipelineRaftStateMachine] unknown command: ${JSON.stringify(_exhaustive)}`,
        );
      }
    }
  }

  protected exportUserState(): PipelineRaftSnapshotState {
    return {
      version: 1,
      runs: Object.fromEntries(this.runs.entries()),
    };
  }

  /**
   * Convenience wrapper around `RaftStateMachineBase.exportRaftSnapshot()`
   * that narrows the envelope's `state` field from the base's default
   * `EntitySnapshot` to our `PipelineRaftSnapshotState`. Saves callers a
   * cast every time they want a typed snapshot.
   */
  exportPipelineRaftSnapshot(): RaftSnapshot<PipelineRaftSnapshotState> {
    return this.exportRaftSnapshot() as unknown as RaftSnapshot<PipelineRaftSnapshotState>;
  }

  /**
   * Counterpart to {@link exportPipelineRaftSnapshot}. Accepts the
   * narrowed envelope and forwards to the base's `importSnapshot()`,
   * which handles user-state extraction + the lastIncludedIndex /
   * lastIncludedTerm / clusterConfig restore.
   */
  importPipelineRaftSnapshot(snap: RaftSnapshot<PipelineRaftSnapshotState>): void {
    this.importSnapshot(snap as unknown as RaftSnapshot);
  }

  protected importUserState(state: unknown): void {
    if (!isPipelineRaftSnapshotState(state)) {
      throw new Error(
        '[PipelineRaftStateMachine] importUserState received a payload that is not a PipelineRaftSnapshotState (wrong version field or shape)',
      );
    }
    this.runs.clear();
    for (const [runId, slot] of Object.entries(state.runs)) {
      this.runs.set(runId, { ...slot, checkpoint: { ...slot.checkpoint } });
    }
  }

  // ---------------------------------------------------------------------
  // Per-command handlers
  // ---------------------------------------------------------------------

  private applyRunStarted(
    cmd: Extract<PipelineRunCommand, { type: 'run-started' }>,
    entry: RaftLogEntry<PipelineRunCommand>,
  ): PipelineRunApplyResult {
    const existing = this.runs.get(cmd.runId);
    if (existing && entry.index <= existing.lastAppliedIndex) {
      // Replay — don't double-create. Echo back the current status so the
      // proposer can still correlate the result.
      return { runId: cmd.runId, status: existing.status, applied: 'idempotent-skip' };
    }
    if (existing && isTerminal(existing.status)) {
      // run-started after terminal: refuse to overwrite. Mirror the
      // pipeline module's invariant — terminal is sticky.
      return { runId: cmd.runId, status: existing.status, applied: 'idempotent-skip' };
    }
    const slot: PipelineRunSlot = {
      runId: cmd.runId,
      pipelineId: cmd.pipelineId,
      pipelineVersion: cmd.pipelineVersion,
      ownerNodeId: cmd.ownerNodeId,
      status: 'running',
      lastEvent: 'run-started',
      lastEventAt: cmd.startedAt,
      lastAppliedIndex: entry.index,
      checkpoint: {},
    };
    this.runs.set(cmd.runId, slot);
    this.emit('pipeline:run:started', { runId: cmd.runId, slot });
    return { runId: cmd.runId, status: slot.status, applied: 'created' };
  }

  private applyRunUpdated(
    cmd: Extract<PipelineRunCommand, { type: 'run-updated' }>,
    entry: RaftLogEntry<PipelineRunCommand>,
  ): PipelineRunApplyResult {
    const slot = this.runs.get(cmd.runId);
    if (!slot) {
      // Update-before-start arrives — common in race conditions where the
      // proposer didn't see its own 'run-started' commit before sending
      // an update. Synthesize a placeholder slot rather than dropping the
      // command on the floor. Operators surface this as a warning via
      // the emitted event below.
      const synthesized: PipelineRunSlot = {
        runId: cmd.runId,
        pipelineId: '<unknown>',
        pipelineVersion: 0,
        ownerNodeId: '<unknown>',
        status: cmd.status,
        lastEvent: cmd.eventName,
        lastEventAt: cmd.at,
        lastAppliedIndex: entry.index,
        checkpoint: {},
      };
      this.runs.set(cmd.runId, synthesized);
      this.emit('pipeline:run:synthesized', { runId: cmd.runId, slot: synthesized });
      return { runId: cmd.runId, status: cmd.status, applied: 'updated' };
    }
    if (entry.index <= slot.lastAppliedIndex) {
      return { runId: cmd.runId, status: slot.status, applied: 'idempotent-skip' };
    }
    if (isTerminal(slot.status) && !isTerminal(cmd.status)) {
      // Terminal is sticky — can't transition out of completed/failed/
      // cancelled.
      return { runId: cmd.runId, status: slot.status, applied: 'idempotent-skip' };
    }
    slot.status = cmd.status;
    slot.lastEvent = cmd.eventName;
    slot.lastEventAt = cmd.at;
    slot.lastAppliedIndex = entry.index;
    this.emit('pipeline:run:updated', { runId: cmd.runId, slot });
    return { runId: cmd.runId, status: slot.status, applied: 'updated' };
  }

  private applyRunCheckpointed(
    cmd: Extract<PipelineRunCommand, { type: 'run-checkpointed' }>,
    entry: RaftLogEntry<PipelineRunCommand>,
  ): PipelineRunApplyResult {
    const slot = this.runs.get(cmd.runId);
    if (!slot) {
      // No slot for the runId — this is unusual but legal in the same
      // way as run-updated above: synthesize a minimal record so the
      // checkpoint isn't dropped silently.
      const synthesized: PipelineRunSlot = {
        runId: cmd.runId,
        pipelineId: '<unknown>',
        pipelineVersion: 0,
        ownerNodeId: '<unknown>',
        status: 'running',
        lastEvent: 'run-checkpointed',
        lastEventAt: cmd.at,
        lastAppliedIndex: entry.index,
        checkpoint: { ...cmd.checkpoint },
      };
      this.runs.set(cmd.runId, synthesized);
      return { runId: cmd.runId, status: synthesized.status, applied: 'checkpointed' };
    }
    if (entry.index <= slot.lastAppliedIndex) {
      return { runId: cmd.runId, status: slot.status, applied: 'idempotent-skip' };
    }
    slot.checkpoint = { ...cmd.checkpoint };
    slot.lastEvent = 'run-checkpointed';
    slot.lastEventAt = cmd.at;
    slot.lastAppliedIndex = entry.index;
    this.emit('pipeline:run:checkpointed', { runId: cmd.runId, slot });
    return { runId: cmd.runId, status: slot.status, applied: 'checkpointed' };
  }

  private applyRunTerminated(
    cmd: Extract<PipelineRunCommand, { type: 'run-terminated' }>,
    entry: RaftLogEntry<PipelineRunCommand>,
  ): PipelineRunApplyResult {
    const slot = this.runs.get(cmd.runId);
    if (!slot) {
      // Termination without a known slot — record the terminal state so
      // future replay sees it. The owner / pipelineId remain unknown.
      const synthesized: PipelineRunSlot = {
        runId: cmd.runId,
        pipelineId: '<unknown>',
        pipelineVersion: 0,
        ownerNodeId: '<unknown>',
        status: cmd.status,
        lastEvent: 'run-terminated',
        lastEventAt: cmd.at,
        lastAppliedIndex: entry.index,
        checkpoint: cmd.error ? { error: cmd.error } : {},
      };
      this.runs.set(cmd.runId, synthesized);
      this.emit('pipeline:run:terminated', { runId: cmd.runId, slot: synthesized });
      return { runId: cmd.runId, status: cmd.status, applied: 'terminated' };
    }
    if (entry.index <= slot.lastAppliedIndex) {
      return { runId: cmd.runId, status: slot.status, applied: 'idempotent-skip' };
    }
    if (isTerminal(slot.status)) {
      // Already terminal — sticky. Don't overwrite the recorded terminal
      // status with a new one, even if it's also terminal.
      return { runId: cmd.runId, status: slot.status, applied: 'idempotent-skip' };
    }
    slot.status = cmd.status;
    slot.lastEvent = 'run-terminated';
    slot.lastEventAt = cmd.at;
    slot.lastAppliedIndex = entry.index;
    if (cmd.error) {
      slot.checkpoint = { ...slot.checkpoint, error: cmd.error };
    }
    this.emit('pipeline:run:terminated', { runId: cmd.runId, slot });
    return { runId: cmd.runId, status: slot.status, applied: 'terminated' };
  }
}

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

/**
 * Serialize a {@link RaftSnapshot} envelope to a JSON string. Useful for
 * disk persistence outside the `RaftSnapshotManager` (e.g., a single-node
 * smoke test that just wants to round-trip state).
 */
export function snapshotToJSON(snap: RaftSnapshot<PipelineRaftSnapshotState>): string {
  return JSON.stringify(snap);
}

/**
 * Inverse of {@link snapshotToJSON}. Validates the envelope shape lightly
 * — the caller is expected to use this only on data they themselves
 * produced via `exportRaftSnapshot()`.
 */
export function snapshotFromJSON(payload: string): RaftSnapshot<PipelineRaftSnapshotState> {
  const parsed = JSON.parse(payload) as unknown;
  if (
    typeof parsed !== 'object'
    || parsed === null
    || !('lastIncludedIndex' in parsed)
    || !('state' in parsed)
  ) {
    throw new Error('[PipelineRaftStateMachine] snapshotFromJSON: payload is not a RaftSnapshot envelope');
  }
  return parsed as RaftSnapshot<PipelineRaftSnapshotState>;
}

function isPipelineRaftSnapshotState(value: unknown): value is PipelineRaftSnapshotState {
  if (typeof value !== 'object' || value === null) return false;
  const v = value as { version?: unknown; runs?: unknown };
  if (v.version !== 1) return false;
  if (typeof v.runs !== 'object' || v.runs === null) return false;
  return true;
}
