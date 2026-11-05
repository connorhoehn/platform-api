// platform-api/src/pipeline/run-control-forwarder.ts
//
// A run control — cancel, breakpoint resolve, resume-from-step — reaches the
// replica that holds the run whichever replica took it (NFR #120; the
// approval forwarder's sibling, same shape, its own topic). Before this,
// `POST /:runId/cancel` and `POST …/breakpoint/:stepId/resolve` on the
// replica that did not hold the run were silent no-ops (`PipelineModule`
// ignores an unknown runId) answered as 404 "run not found" / 204, and
// `POST …/resume?fromNodeId` threw `EntityNotFoundError` for a run another
// replica ran.
//
// The path a control takes:
//
//   1. The receiving replica RECORDS it on the pipeline's rollup row
//      (`controls`, keyed (runId, controlId), the same CAS put as the gates
//      and decisions), then PUBLISHES it on the cluster-wide dc pubsub topic
//      `pipeline.controls` (Redis-backed in a multi-replica deployment:
//      channel `dcore:topic:pipeline.controls`, every replica subscribed).
//   2. Every replica hears it; the one whose executor holds the run applies
//      it once per controlId, publishes an `applied` acknowledgement on the
//      same topic, and releases the control from the row. The route that
//      forwarded it waits a bounded time for that acknowledgement so its 202
//      says whether the control was applied or is still pending.
//   3. If no live executor holds the run — the owner died — the control stays
//      on the row through `interrupted` / `stuck`. The replica whose resume
//      path (`resumeInterruptedRuns` → `module.resumeRun`) re-enters the run
//      reads the row on its `pipeline:run:resumed` / `pipeline:run:started` /
//      `pipeline:step:breakpoint` and applies what it finds: a recorded
//      cancel ends the resumed run at once; a recorded breakpoint resolve is
//      applied when the run is paused again. A resume-from-step has no such
//      path (only the executor that ran the failed run can rewind it), so an
//      unacknowledged one is released and the route says so.
//
// Idempotency: (runId, controlId). The row refuses a duplicate controlId; the
// applier remembers the ids it applied.
//
// A resume-from-step re-runs steps, so it is also claimed once cluster-wide
// (`rerun:${runId}:${controlId}` in the shared claims table) by whichever
// replica executes it: the holder claims before it runs and publishes an
// `accepted` message at once (the re-run itself can take minutes or park at
// an approval), and the route that forwarded it takes the same claim before
// it falls back to rewinding the run locally. Losing the claim means another
// replica runs it. Two different controlIds for one run are not covered here.

import type { EventBus } from 'distributed-core';
import type { PipelineEventMap } from 'distributed-core/applications/pipeline';
import { withContext } from '../lib/logger';
import type { ClaimStore } from '../deck/deckReviseRequests';
import { pipelineRunRollupRepo, type PipelineRunRollupRepository } from '../repositories/PipelineRunRollupRepository';
import { controlsFor, isTerminalRollupStatus, normalizeRunStatus, type RollupRunControl, type RollupRunStatus } from './run-rollup';
import type { RunRollupSubscriber } from './run-rollup-subscriber';

const log = withContext({ service: 'run-control-forwarder' });

/** The dc pubsub topic every replica listens on. */
export const RUN_CONTROLS_TOPIC = 'pipeline.controls';

const MAX_SEEN_CONTROLS = 5000;
/** How long a forwarding route waits for the holder's acknowledgement by default. */
export const DEFAULT_AWAIT_APPLIED_MS = 1500;
/**
 * How long a re-run claim lives. Never released: it outlives the re-run, so
 * it must exceed the longest plausible one, approval and breakpoint parking
 * included. Seven days, the same as the review-apply and comment-agent claims.
 */
export const RERUN_CLAIM_TTL_SECONDS = 7 * 24 * 60 * 60;

export function rerunClaimKey(runId: string, controlId: string): string { return `rerun:${runId}:${controlId}`; }

export type RunControlKind = RollupRunControl['kind'];

/** What crosses the wire — the row's control plus where it came from. */
export type RunControlMessage = RollupRunControl & {
  v: 1;
  type: 'control';
  pipelineId: string;
  /** The replica that took it (not the pipeline node a resume starts from). */
  fromReplica: string;
};

/** The holder's acknowledgement, so the forwarding route can say "applied". */
export interface RunControlAppliedMessage {
  v: 1;
  type: 'applied';
  controlId: string;
  runId: string;
  kind: RunControlKind;
  nodeId: string;
  ok: boolean;
  error?: string;
  /** The holder had already applied this controlId (a retry), or another replica claimed it. */
  duplicate?: boolean;
  /** false: this replica did not run it (another replica holds the re-run claim). */
  applied?: boolean;
  at: string;
}

/** A resume-from-step the holder took and is now running (sent before the re-run finishes). */
export interface RunControlAcceptedMessage {
  v: 1;
  type: 'accepted';
  controlId: string;
  runId: string;
  nodeId: string;
}

/** The cluster pubsub, as far as this module needs it (`Cluster.pubsub`). */
export interface ClusterPubSubLike {
  publish(topic: string, payload: unknown): Promise<void>;
  subscribe(topic: string, handler: (topic: string, payload: unknown, metadata?: unknown) => void): string;
  unsubscribe(subscriptionId: string): boolean | void;
}

/** The executor side, as far as this module needs it (`PipelineModule`). */
export interface LocalRunControlsLike {
  getRun(runId: string): { status: string } | null;
  cancelRun(runId: string): void;
  resolveBreakpoint(runId: string, stepId: string, action: 'continue' | 'step_over' | 'resume_all'): void;
  resumeFromStep?(runId: string, fromNodeId: string, options?: { breakpoints?: string[]; allowCompleted?: boolean }): Promise<unknown> | unknown;
}

export interface RunControlForwarderOptions {
  nodeId: string;
  pubsub: ClusterPubSubLike;
  runs: LocalRunControlsLike;
  /** The local bus, for the resume pick-up. */
  eventBus: EventBus<PipelineEventMap>;
  /** Records controls on the row; null when rollups are disabled (then only the wire carries a control). */
  rollup: RunRollupSubscriber | null;
  repo?: Pick<PipelineRunRollupRepository, 'get'>;
  /** Resolves a run's pipeline when the subscriber cannot. */
  lookupPipelineId?: (runId: string) => Promise<string | null>;
  /** The shared one-time claims for re-runs; null/absent: no claim (single replica, tests). */
  claims?: ClaimStore | null;
}

export interface ForwardOutcome {
  message: RunControlMessage;
  /** The holder's acknowledgement, or null when none arrived in time. */
  applied: RunControlAppliedMessage | null;
  /** Milliseconds from publish to acknowledgement (when one arrived). */
  appliedInMs: number | null;
  /** A resume-from-step the holder accepted and is still running, or null. */
  accepted: RunControlAcceptedMessage | null;
  /** Whether the row holds the control (false: the wire alone carried it). */
  recorded: boolean;
}

interface BusEventLike<T> { id?: string; payload: T }

const PICKUP_EVENTS = ['pipeline:run:started', 'pipeline:run:resumed', 'pipeline:step:breakpoint'] as const;

export class RunControlForwarder {
  private readonly nodeId: string;
  private readonly pubsub: ClusterPubSubLike;
  private readonly runs: LocalRunControlsLike;
  private readonly eventBus: EventBus<PipelineEventMap>;
  private readonly rollup: RunRollupSubscriber | null;
  private readonly repo: Pick<PipelineRunRollupRepository, 'get'>;
  private readonly lookup: (runId: string) => Promise<string | null>;
  private readonly seen = new Set<string>();
  private readonly claims: ClaimStore | null;
  private readonly waiters = new Map<string, (ack: RunControlAppliedMessage | RunControlAcceptedMessage) => void>();
  private pubsubSubscriptionId: string | null = null;
  private busSubscriptionIds: string[] = [];
  private inFlight: Promise<void>[] = [];
  private active = false;
  /** Counters a test or a health line can read. */
  readonly stats = { forwarded: 0, received: 0, applied: 0, acked: 0, accepted: 0, claimLost: 0, duplicates: 0, notHere: 0, pickedUp: 0, released: 0, errors: 0 };

  constructor(options: RunControlForwarderOptions) {
    this.nodeId = options.nodeId;
    this.pubsub = options.pubsub;
    this.runs = options.runs;
    this.eventBus = options.eventBus;
    this.rollup = options.rollup;
    this.repo = options.repo ?? pipelineRunRollupRepo;
    this.lookup = options.lookupPipelineId ?? (async (runId) => this.rollup?.resolvePipelineId(runId) ?? null);
    this.claims = options.claims ?? null;
  }

  /**
   * Take the one-time claim to execute this re-run. True when this replica
   * may run it (also when no claim store is configured); `holder` names the
   * replica that holds it otherwise. Throws when the claims table cannot be
   * reached.
   */
  async claimRerun(runId: string, controlId: string): Promise<{ won: boolean; holder: string | null }> {
    if (!this.claims) return { won: true, holder: this.nodeId };
    const key = rerunClaimKey(runId, controlId);
    // 'pending' until the holder's re-run returns (then its nodeId): the
    // store's release() only drops a pending claim, and only the replica that
    // created one ever releases it.
    if (await this.claims.create(key, 'pending', RERUN_CLAIM_TTL_SECONDS)) return { won: true, holder: this.nodeId };
    return { won: false, holder: await this.claims.get(key).catch(() => null) };
  }

  /** Idempotent. */
  start(): void {
    if (this.active) return;
    this.active = true;
    this.pubsubSubscriptionId = this.pubsub.subscribe(RUN_CONTROLS_TOPIC, (_topic, payload) => {
      this.track(this.onMessage(payload));
    });
    // The bus delivers to every subscriber without awaiting them: never hold the executor.
    for (const name of PICKUP_EVENTS) {
      this.busSubscriptionIds.push(this.eventBus.subscribe(name, async (event) => {
        const payload = (event as BusEventLike<{ runId?: unknown; stepId?: unknown }>).payload;
        if (!payload || typeof payload.runId !== 'string') return;
        this.track(this.pickUp(payload.runId, name === 'pipeline:step:breakpoint' && typeof payload.stepId === 'string' ? payload.stepId : undefined));
      }));
    }
  }

  stop(): void {
    if (!this.active) return;
    this.active = false;
    if (current === this) current = null;
    if (this.pubsubSubscriptionId !== null) {
      try { this.pubsub.unsubscribe(this.pubsubSubscriptionId); } catch { /* already gone */ }
      this.pubsubSubscriptionId = null;
    }
    for (const id of this.busSubscriptionIds.splice(0)) {
      try { this.eventBus.unsubscribe(id); } catch { /* already gone */ }
    }
    for (const [controlId] of this.waiters) this.waiters.delete(controlId);
  }

  /** Wait for every control in flight (tests). */
  async idle(): Promise<void> {
    while (this.inFlight.length > 0) await Promise.allSettled(this.inFlight.splice(0));
  }

  /** The run's status as this process's executor sees it, or null when it never saw the run (or evicted it). */
  localStatus(runId: string): RollupRunStatus | null {
    try {
      const run = this.runs.getRun(runId);
      return run ? normalizeRunStatus(run.status) ?? 'running' : null;
    } catch (err) {
      log.warn({ runId, err: String(err) }, 'getRun failed; treating the run as not held here');
      return null;
    }
  }

  /** Whether this process's executor holds the run live right now (cancel, breakpoint). */
  holdsRun(runId: string): boolean {
    const status = this.localStatus(runId);
    return status !== null && !isTerminalRollupStatus(status);
  }

  /** Whether this process's executor ran the run (live or finished): only it can resume from a step. */
  knowsRun(runId: string): boolean {
    return this.localStatus(runId) !== null;
  }

  /** Whether a control of this kind can be applied here now. */
  canApply(control: Pick<RollupRunControl, 'kind' | 'runId'>): boolean {
    return control.kind === 'resume-from-step' ? this.knowsRun(control.runId) : this.holdsRun(control.runId);
  }

  /**
   * A control taken here for a run held elsewhere: record it on the row,
   * publish it, then wait a bounded time for the holder's acknowledgement.
   * Throws when neither the row nor the wire took it — the route answers
   * 503, never a silent no-op.
   */
  async forward(control: RollupRunControl & { pipelineId: string }, options: { awaitAppliedMs?: number } = {}): Promise<ForwardOutcome> {
    const { pipelineId, ...rest } = control;
    const message: RunControlMessage = { v: 1, type: 'control', ...rest, pipelineId, fromReplica: this.nodeId };
    let recorded = false;
    if (this.rollup) {
      try {
        await this.rollup.applyResolved({ pipelineId, runId: control.runId, status: 'running', at: control.at, control: rest });
        recorded = true;
      } catch (err) {
        log.warn({ runId: control.runId, controlId: control.controlId, kind: control.kind, err: String(err) }, 'control not recorded on the rollup row; the wire alone carries it');
      }
    }
    const awaitMs = options.awaitAppliedMs ?? DEFAULT_AWAIT_APPLIED_MS;
    const ack = this.awaitApplied(control.controlId, awaitMs);
    const publishedAt = Date.now();
    try {
      await this.pubsub.publish(RUN_CONTROLS_TOPIC, message);
    } catch (err) {
      this.waiters.delete(control.controlId);
      if (!recorded) throw err;
      log.warn({ runId: control.runId, controlId: control.controlId, kind: control.kind, err: String(err) }, 'control recorded on the row but not published; the holder applies it when the run is next resumed');
    }
    this.stats.forwarded += 1;
    const reply = await ack;
    const applied = reply?.type === 'applied' ? reply : null;
    const accepted = reply?.type === 'accepted' ? reply : null;
    const appliedInMs = applied ? Date.now() - publishedAt : null;
    log.info({ runId: control.runId, controlId: control.controlId, kind: control.kind, recorded, applied: applied?.ok ?? null, appliedBy: applied?.nodeId ?? null, acceptedBy: accepted?.nodeId ?? null, appliedInMs }, 'run control forwarded');
    return { message, applied, appliedInMs, accepted, recorded };
  }

  /** Release a control this replica recorded but nobody will apply (resume-from-step with no holder). */
  async release(pipelineId: string, runId: string, controlId: string): Promise<void> {
    if (!this.rollup) return;
    try {
      await this.rollup.applyResolved({ pipelineId, runId, status: 'running', at: new Date().toISOString(), controlReleased: { controlId } });
      this.stats.released += 1;
    } catch (err) {
      log.warn({ runId, controlId, err: String(err) }, 'control not released from the rollup row');
    }
  }

  /** Resolves on the holder's first reply for this control: `applied`, or (resume-from-step) `accepted`. */
  private awaitApplied(controlId: string, ms: number): Promise<RunControlAppliedMessage | RunControlAcceptedMessage | null> {
    if (ms <= 0) return Promise.resolve(null);
    return new Promise((resolve) => {
      const timer = setTimeout(() => { this.waiters.delete(controlId); resolve(null); }, ms);
      timer.unref?.();
      this.waiters.set(controlId, (ack) => { clearTimeout(timer); this.waiters.delete(controlId); resolve(ack); });
    });
  }

  private async onMessage(payload: unknown): Promise<void> {
    const accepted = asAccepted(payload);
    if (accepted) {
      this.stats.accepted += 1;
      this.waiters.get(accepted.controlId)?.(accepted);
      return;
    }
    const ack = asApplied(payload);
    if (ack) {
      this.stats.acked += 1;
      this.waiters.get(ack.controlId)?.(ack);
      return;
    }
    const message = asControl(payload);
    if (!message) return;
    this.stats.received += 1;
    if (!this.canApply(message)) { this.stats.notHere += 1; return; }
    await this.apply(message, message.pipelineId);
  }

  /** Apply one control to the local executor, once per controlId; acknowledge and release it. */
  private async apply(control: RollupRunControl, pipelineId: string | null): Promise<boolean> {
    let error: string | undefined;
    let duplicate = this.seen.has(control.controlId);
    let claimLost = false;
    if (duplicate) {
      // A retry with the same Idempotency-Key is the same control: acknowledge
      // it as applied (the route answers the same 202) and clear its re-record.
      this.stats.duplicates += 1;
    } else this.remember(control.controlId);
    try {
      if (duplicate) { /* applied already */ } else
      if (control.kind === 'cancel') {
        this.runs.cancelRun(control.runId);
      } else if (control.kind === 'breakpoint') {
        this.runs.resolveBreakpoint(control.runId, control.stepId, control.action);
      } else {
        const resumeFromStep = this.runs.resumeFromStep;
        if (typeof resumeFromStep !== 'function') throw new Error('resumeFromStep unavailable on this replica');
        // Claim before running: the forwarding route may have timed out and
        // claimed it to run locally. A claims table that cannot be reached
        // does not stop the holder (the route, unable to claim, does not run).
        const claim = await this.claimRerun(control.runId, control.controlId).catch((err: unknown) => {
          log.warn({ runId: control.runId, controlId: control.controlId, err: String(err) }, 're-run claim unavailable; the holder runs it unclaimed');
          return { won: true, holder: null };
        });
        if (!claim.won) {
          claimLost = true;
          duplicate = true;
          this.stats.claimLost += 1;
          log.info({ runId: control.runId, controlId: control.controlId, holder: claim.holder }, 'another replica claimed this re-run; not running it here');
        } else {
          const run = Promise.resolve().then(() => resumeFromStep.call(this.runs, control.runId, control.fromNodeId, {
            ...(control.breakpoints !== undefined ? { breakpoints: control.breakpoints } : {}),
            ...(control.allowCompleted ? { allowCompleted: true } : {}),
          }));
          try {
            // Acknowledge on accept: the re-run returns only at its terminal
            // snapshot. A refusal (state validation) rejects before the next
            // macrotask, so it still reaches the route as `applied: ok=false`.
            const settledEarly = await Promise.race([run.then(() => true, () => true), new Promise<false>((r) => setImmediate(() => r(false)))]);
            if (!settledEarly) {
              const accepted: RunControlAcceptedMessage = { v: 1, type: 'accepted', controlId: control.controlId, runId: control.runId, nodeId: this.nodeId };
              try { await this.pubsub.publish(RUN_CONTROLS_TOPIC, accepted); } catch (err) {
                log.warn({ runId: control.runId, controlId: control.controlId, err: String(err) }, 'run control acceptance not published');
              }
            }
            await run;
          } catch (err) {
            // A refused re-run did not run: free the claim so a retry can.
            await this.claims?.release(rerunClaimKey(control.runId, control.controlId)).catch(() => undefined);
            throw err;
          }
          await this.claims?.commit(rerunClaimKey(control.runId, control.controlId), this.nodeId, RERUN_CLAIM_TTL_SECONDS).catch(() => undefined);
        }
      }
    } catch (err) {
      this.stats.errors += 1;
      this.seen.delete(control.controlId);
      error = err instanceof Error ? err.message : String(err);
      log.error({ runId: control.runId, controlId: control.controlId, kind: control.kind, err: error }, 'forwarded run control failed to apply');
    }
    if (!error && !duplicate) {
      this.stats.applied += 1;
      log.info({ runId: control.runId, controlId: control.controlId, kind: control.kind }, 'forwarded run control applied');
    }
    const ack: RunControlAppliedMessage = { v: 1, type: 'applied', controlId: control.controlId, runId: control.runId, kind: control.kind, nodeId: this.nodeId, ok: !error, ...(error ? { error } : {}), ...(duplicate ? { duplicate: true } : {}), ...(claimLost ? { applied: false } : {}), at: new Date().toISOString() };
    try { await this.pubsub.publish(RUN_CONTROLS_TOPIC, ack); } catch (err) {
      log.warn({ runId: control.runId, controlId: control.controlId, err: String(err) }, 'run control acknowledgement not published');
    }
    // An applied control leaves the row; a refused one stays for the next pick-up
    // only when it can still make sense (the executor may not be paused yet).
    if (!error) {
      const pid = pipelineId ?? (await this.lookup(control.runId).catch(() => null));
      if (pid) await this.release(pid, control.runId, control.controlId);
    }
    return !error && !duplicate;
  }

  /**
   * The executor here just (re)started the run or paused it at a breakpoint:
   * apply any control the row already holds for it (the owner died before
   * applying it, or the control arrived before this executor had the run).
   */
  private async pickUp(runId: string, pausedStepId?: string): Promise<void> {
    let pipelineId: string | null = null;
    try { pipelineId = await this.lookup(runId); } catch (err) { log.warn({ runId, err: String(err) }, 'pipeline lookup failed; no recorded control picked up'); }
    if (!pipelineId) return;
    let waiting: RollupRunControl[] = [];
    try { waiting = controlsFor(await this.repo.get(pipelineId), runId); } catch (err) { log.warn({ runId, err: String(err) }, 'rollup read failed; no recorded control picked up'); return; }
    for (const control of waiting) {
      if (control.kind === 'resume-from-step') continue;
      if (control.kind === 'breakpoint') {
        const paused = pausedStepId !== undefined ? pausedStepId === control.stepId : this.localStatus(runId) === 'paused_at_breakpoint';
        if (!paused) continue;
      }
      if (!this.holdsRun(runId)) return;
      if (await this.apply(control, pipelineId)) this.stats.pickedUp += 1;
    }
  }

  private track(p: Promise<void>): void {
    const tracked = p.catch((err) => { this.stats.errors += 1; log.error({ err: String(err) }, 'run control forwarder task failed'); });
    this.inFlight.push(tracked);
    void tracked.finally(() => { this.inFlight = this.inFlight.filter((x) => x !== tracked); });
  }

  private remember(controlId: string): void {
    if (this.seen.size >= MAX_SEEN_CONTROLS) {
      const oldest = this.seen.values().next().value as string | undefined;
      if (oldest !== undefined) this.seen.delete(oldest);
    }
    this.seen.add(controlId);
  }
}

function isRecord(payload: unknown): payload is Record<string, unknown> {
  return !!payload && typeof payload === 'object';
}

function asApplied(payload: unknown): RunControlAppliedMessage | null {
  if (!isRecord(payload) || payload.v !== 1 || payload.type !== 'applied') return null;
  for (const key of ['controlId', 'runId', 'kind', 'nodeId', 'at']) if (typeof payload[key] !== 'string' || !payload[key]) return null;
  if (typeof payload.ok !== 'boolean') return null;
  return payload as unknown as RunControlAppliedMessage;
}

function asAccepted(payload: unknown): RunControlAcceptedMessage | null {
  if (!isRecord(payload) || payload.v !== 1 || payload.type !== 'accepted') return null;
  for (const key of ['controlId', 'runId', 'nodeId']) if (typeof payload[key] !== 'string' || !payload[key]) return null;
  return payload as unknown as RunControlAcceptedMessage;
}

function asControl(payload: unknown): RunControlMessage | null {
  if (!isRecord(payload) || payload.v !== 1 || payload.type !== 'control') return null;
  for (const key of ['controlId', 'runId', 'userId', 'pipelineId', 'at', 'fromReplica']) {
    if (typeof payload[key] !== 'string' || !payload[key]) return null;
  }
  switch (payload.kind) {
    case 'cancel':
      if (payload.reason !== undefined && typeof payload.reason !== 'string') return null;
      break;
    case 'breakpoint':
      if (typeof payload.stepId !== 'string' || !payload.stepId) return null;
      if (payload.action !== 'continue' && payload.action !== 'step_over' && payload.action !== 'resume_all') return null;
      break;
    case 'resume-from-step':
      if (typeof payload.fromNodeId !== 'string' || !payload.fromNodeId) return null;
      if (payload.breakpoints !== undefined && !(Array.isArray(payload.breakpoints) && payload.breakpoints.every((b) => typeof b === 'string'))) return null;
      if (payload.allowCompleted !== undefined && typeof payload.allowCompleted !== 'boolean') return null;
      break;
    default:
      return null;
  }
  return payload as unknown as RunControlMessage;
}

let current: RunControlForwarder | null = null;

/** The process-wide forwarder `startRunControlForwarder` installed, or null (not started). */
export function getRunControlForwarder(): RunControlForwarder | null { return current; }

/** Tests: install or clear the process-wide instance. */
export function setRunControlForwarder(forwarder: RunControlForwarder | null): void { current = forwarder; }

/** The one line bootstrap.ts calls. */
export function startRunControlForwarder(options: RunControlForwarderOptions): RunControlForwarder {
  const forwarder = new RunControlForwarder(options);
  forwarder.start();
  current = forwarder;
  log.info({ topic: RUN_CONTROLS_TOPIC }, 'run control forwarder ENABLED');
  return forwarder;
}
