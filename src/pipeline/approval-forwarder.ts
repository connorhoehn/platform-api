// platform-api/src/pipeline/approval-forwarder.ts
//
// An approval decision reaches the run's owner whichever replica took it
// (NFR #115). Before this, `POST /api/pipelines/:runId/approvals` on the
// replica that did not own the run answered 204 and did nothing: the bridge
// resolves against its own executor's queue, which has no such run, and
// `PipelineModule.resolveApproval` is a silent no-op for an unknown runId.
//
// The path a decision takes:
//
//   1. The receiving replica RECORDS it on the pipeline's rollup row
//      (`decisions`, keyed (runId, stepId, decisionId), a CAS put), so the
//      shared record exists before anything is sent, and PUBLISHES it on the
//      cluster-wide dc pubsub topic `pipeline.approvals` (Redis-backed in a
//      multi-replica deployment: channel `dcore:topic:pipeline.approvals`,
//      every replica subscribed — unlike the bus's own per-node channel).
//   2. Every replica hears it; the one whose executor holds the gate applies
//      it (`module.resolveApproval`), once per decisionId. The executor then
//      emits the normal `pipeline:approval:recorded`, whose rollup merge
//      releases the decision from the row.
//   3. If no live executor holds the gate — the owner died before applying it,
//      or the decision raced the executor's own `requested` — the decision
//      stays on the row. Whichever replica next raises
//      `pipeline:approval:requested` for that (runId, stepId) — the leader's
//      resume path re-asks every gate a resumed run was awaiting — reads the
//      row and applies what it finds.
//
// Idempotency: (runId, stepId, decisionId). The row refuses a duplicate
// decisionId; the applier remembers the ids it applied; and the executor
// itself treats a second (userId, decision) for a gate as a duplicate.

import type { EventBus } from 'distributed-core';
import type { PipelineEventMap } from 'distributed-core/applications/pipeline';
import { withContext } from '../lib/logger';
import { pipelineRunRollupRepo, type PipelineRunRollupRepository } from '../repositories/PipelineRunRollupRepository';
import { decisionsFor, type RollupApprovalDecision } from './run-rollup';
import type { RunRollupSubscriber } from './run-rollup-subscriber';

const log = withContext({ service: 'approval-forwarder' });

/** The dc pubsub topic every replica listens on. */
export const APPROVAL_DECISIONS_TOPIC = 'pipeline.approvals';

const MAX_SEEN_DECISIONS = 5000;

/** What crosses the wire — the row's decision plus where it came from. */
export interface ApprovalDecisionMessage extends RollupApprovalDecision {
  v: 1;
  pipelineId: string;
  fromNodeId: string;
}

/** The cluster pubsub, as far as this module needs it (`Cluster.pubsub`). */
export interface ClusterPubSubLike {
  publish(topic: string, payload: unknown): Promise<void>;
  subscribe(topic: string, handler: (topic: string, payload: unknown, metadata?: unknown) => void): string;
  unsubscribe(subscriptionId: string): boolean | void;
}

/** The executor side, as far as this module needs it (`PipelineModule`). */
export interface LocalRunsLike {
  getPendingApprovals(): ReadonlyArray<{ runId: string; stepId: string }>;
  resolveApproval(runId: string, stepId: string, userId: string, decision: 'approve' | 'reject', comment?: string): Promise<void> | void;
}

export interface ApprovalForwarderOptions {
  nodeId: string;
  pubsub: ClusterPubSubLike;
  runs: LocalRunsLike;
  /** The local bus, for `pipeline:approval:requested` (the resume pick-up). */
  eventBus: EventBus<PipelineEventMap>;
  /** Records decisions on the row; null when rollups are disabled (then only the wire carries a decision). */
  rollup: RunRollupSubscriber | null;
  repo?: Pick<PipelineRunRollupRepository, 'get'>;
  /** Resolves a run's pipeline when the subscriber cannot. */
  lookupPipelineId?: (runId: string) => Promise<string | null>;
}

interface BusEventLike<T> { id?: string; payload: T }

export class ApprovalForwarder {
  private readonly nodeId: string;
  private readonly pubsub: ClusterPubSubLike;
  private readonly runs: LocalRunsLike;
  private readonly eventBus: EventBus<PipelineEventMap>;
  private readonly rollup: RunRollupSubscriber | null;
  private readonly repo: Pick<PipelineRunRollupRepository, 'get'>;
  private readonly lookup: (runId: string) => Promise<string | null>;
  private readonly seen = new Set<string>();
  private pubsubSubscriptionId: string | null = null;
  private busSubscriptionId: string | null = null;
  private inFlight: Promise<void>[] = [];
  private active = false;
  /** Counters a test or a health line can read. */
  readonly stats = { forwarded: 0, received: 0, applied: 0, duplicates: 0, notHere: 0, pickedUp: 0, errors: 0 };

  constructor(options: ApprovalForwarderOptions) {
    this.nodeId = options.nodeId;
    this.pubsub = options.pubsub;
    this.runs = options.runs;
    this.eventBus = options.eventBus;
    this.rollup = options.rollup;
    this.repo = options.repo ?? pipelineRunRollupRepo;
    this.lookup = options.lookupPipelineId ?? (async (runId) => this.rollup?.resolvePipelineId(runId) ?? null);
  }

  /** Idempotent. */
  start(): void {
    if (this.active) return;
    this.active = true;
    this.pubsubSubscriptionId = this.pubsub.subscribe(APPROVAL_DECISIONS_TOPIC, (_topic, payload) => {
      this.track(this.onDecision(payload));
    });
    // The bus delivers to every subscriber without awaiting them, like the
    // rollup subscriber: never hold the executor.
    this.busSubscriptionId = this.eventBus.subscribe('pipeline:approval:requested', async (event) => {
      const { runId, stepId } = (event as BusEventLike<PipelineEventMap['pipeline:approval:requested']>).payload;
      if (typeof runId !== 'string' || typeof stepId !== 'string') return;
      this.track(this.pickUp(runId, stepId));
    });
  }

  stop(): void {
    if (!this.active) return;
    this.active = false;
    if (current === this) current = null;
    if (this.pubsubSubscriptionId !== null) {
      try { this.pubsub.unsubscribe(this.pubsubSubscriptionId); } catch { /* already gone */ }
      this.pubsubSubscriptionId = null;
    }
    if (this.busSubscriptionId !== null) {
      try { this.eventBus.unsubscribe(this.busSubscriptionId); } catch { /* already gone */ }
      this.busSubscriptionId = null;
    }
  }

  /** Wait for every decision in flight (tests). */
  async idle(): Promise<void> {
    while (this.inFlight.length > 0) await Promise.allSettled(this.inFlight.splice(0));
  }

  /** Whether this process's executor holds the gate right now. */
  holdsGate(runId: string, stepId: string): boolean {
    try {
      return this.runs.getPendingApprovals().some((p) => p.runId === runId && p.stepId === stepId);
    } catch (err) {
      log.warn({ runId, stepId, err: String(err) }, 'getPendingApprovals failed; treating the gate as not held here');
      return false;
    }
  }

  /**
   * A decision taken here for a run whose gate is held elsewhere: record it
   * on the row, then publish it. Throws when neither the row nor the wire
   * took it — the route answers 503, never a silent 204.
   */
  async forward(decision: RollupApprovalDecision & { pipelineId: string }): Promise<ApprovalDecisionMessage> {
    const message: ApprovalDecisionMessage = { v: 1, ...decision, fromNodeId: this.nodeId };
    let recorded = false;
    if (this.rollup) {
      try {
        const { pipelineId, ...rest } = decision;
        await this.rollup.applyResolved({ pipelineId, runId: decision.runId, status: 'awaiting_approval', at: decision.at, decision: rest });
        recorded = true;
      } catch (err) {
        log.warn({ runId: decision.runId, stepId: decision.stepId, decisionId: decision.decisionId, err: String(err) }, 'decision not recorded on the rollup row; the wire alone carries it');
      }
    }
    try {
      await this.pubsub.publish(APPROVAL_DECISIONS_TOPIC, message);
    } catch (err) {
      if (!recorded) throw err;
      log.warn({ runId: decision.runId, stepId: decision.stepId, decisionId: decision.decisionId, err: String(err) }, 'decision recorded on the row but not published; the owner applies it when the gate is next requested');
    }
    this.stats.forwarded += 1;
    log.info({ runId: decision.runId, stepId: decision.stepId, decisionId: decision.decisionId, decision: decision.decision, recorded }, 'approval decision forwarded to the run owner');
    return message;
  }

  private async onDecision(payload: unknown): Promise<void> {
    const message = asMessage(payload);
    if (!message) return;
    this.stats.received += 1;
    if (!this.holdsGate(message.runId, message.stepId)) { this.stats.notHere += 1; return; }
    await this.apply(message);
  }

  /** Apply one decision to the local executor, once per decisionId. */
  private async apply(decision: RollupApprovalDecision): Promise<boolean> {
    if (this.seen.has(decision.decisionId)) { this.stats.duplicates += 1; return false; }
    this.remember(decision.decisionId);
    try {
      await this.runs.resolveApproval(decision.runId, decision.stepId, decision.userId, decision.decision, decision.comment);
    } catch (err) {
      this.stats.errors += 1;
      this.seen.delete(decision.decisionId);
      log.error({ runId: decision.runId, stepId: decision.stepId, decisionId: decision.decisionId, err: String(err) }, 'forwarded approval decision failed to apply');
      return false;
    }
    this.stats.applied += 1;
    log.info({ runId: decision.runId, stepId: decision.stepId, decisionId: decision.decisionId, decision: decision.decision }, 'forwarded approval decision applied');
    return true;
  }

  /**
   * The executor here just asked for a gate: apply any decision the row
   * already holds for it (the owner died before applying it, or the decision
   * arrived before the gate existed).
   */
  private async pickUp(runId: string, stepId: string): Promise<void> {
    let pipelineId: string | null = null;
    try { pipelineId = await this.lookup(runId); } catch (err) { log.warn({ runId, err: String(err) }, 'pipeline lookup failed; no recorded decision picked up'); }
    if (!pipelineId) return;
    let waiting: RollupApprovalDecision[] = [];
    try { waiting = decisionsFor(await this.repo.get(pipelineId), runId, stepId); } catch (err) { log.warn({ runId, stepId, err: String(err) }, 'rollup read failed; no recorded decision picked up'); return; }
    for (const decision of waiting) {
      if (!this.holdsGate(runId, stepId)) return;
      if (await this.apply(decision)) this.stats.pickedUp += 1;
    }
  }

  private track(p: Promise<void>): void {
    const tracked = p.catch((err) => { this.stats.errors += 1; log.error({ err: String(err) }, 'approval forwarder task failed'); });
    this.inFlight.push(tracked);
    void tracked.finally(() => { this.inFlight = this.inFlight.filter((x) => x !== tracked); });
  }

  private remember(decisionId: string): void {
    if (this.seen.size >= MAX_SEEN_DECISIONS) {
      const oldest = this.seen.values().next().value as string | undefined;
      if (oldest !== undefined) this.seen.delete(oldest);
    }
    this.seen.add(decisionId);
  }
}

function asMessage(payload: unknown): ApprovalDecisionMessage | null {
  if (!payload || typeof payload !== 'object') return null;
  const m = payload as Record<string, unknown>;
  if (m.v !== 1) return null;
  for (const key of ['decisionId', 'runId', 'stepId', 'userId', 'pipelineId', 'at', 'fromNodeId']) {
    if (typeof m[key] !== 'string' || !m[key]) return null;
  }
  if (m.decision !== 'approve' && m.decision !== 'reject') return null;
  if (m.comment !== undefined && typeof m.comment !== 'string') return null;
  return m as unknown as ApprovalDecisionMessage;
}

let current: ApprovalForwarder | null = null;

/** The process-wide forwarder `startApprovalForwarder` installed, or null (not started). */
export function getApprovalForwarder(): ApprovalForwarder | null { return current; }

/** Tests: install or clear the process-wide instance. */
export function setApprovalForwarder(forwarder: ApprovalForwarder | null): void { current = forwarder; }

/** The one line bootstrap.ts calls. */
export function startApprovalForwarder(options: ApprovalForwarderOptions): ApprovalForwarder {
  const forwarder = new ApprovalForwarder(options);
  forwarder.start();
  current = forwarder;
  log.info({ topic: APPROVAL_DECISIONS_TOPIC }, 'approval forwarder ENABLED');
  return forwarder;
}
