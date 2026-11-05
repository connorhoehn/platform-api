// platform-api/src/routes/runControlForwarding.ts
//
// The route half of NFR #120: a cancel, breakpoint resolve or resume-from-step
// taken on a replica that does not hold the run. The route checks the run
// table (404 / 409 with a reason), records + publishes the control through
// the forwarder, waits a bounded time for the holder's acknowledgement, and
// answers 202 saying whether it was applied — or 503 when neither the row
// nor the wire took it. Never a silent no-op, never a 404 for a run that
// exists on another replica.

import { randomUUID } from 'crypto';
import { getDynamoClient } from '../lib/ddb-client';
import { PipelineRunRepository, type PipelineRunItem } from '../repositories/PipelineRunRepository';
import { getRunControlForwarder, type RunControlForwarder } from '../pipeline/run-control-forwarder';
import { getRunRollupSubscriber, lookupPipelineIdByRunId } from '../pipeline/run-rollup-subscriber';
import { isTerminalRollupStatus, normalizeRunStatus, type RollupRunControl, type RollupRunStatus } from '../pipeline/run-rollup';

let runRepoSingleton: PipelineRunRepository | null = null;
function getRunRepo(): PipelineRunRepository {
  if (!runRepoSingleton) runRepoSingleton = new PipelineRunRepository(getDynamoClient());
  return runRepoSingleton;
}
/** Tests: swap the run table. */
export function setRunControlRunRepo(repo: PipelineRunRepository | null): void { runRepoSingleton = repo; }

type LogLike = { info(obj: object, msg: string): void; warn(obj: object, msg: string): void; error(obj: object, msg: string): void };

export interface ForwardRunControlParams {
  req: { get(name: string): string | undefined };
  res: { status(code: number): { json(body: unknown): unknown } };
  reqLog: LogLike;
  forwarder: RunControlForwarder;
  runId: string;
  userId: string;
  /** Known from the URL (breakpoint, resume); resolved from the row otherwise. */
  pipelineId?: string;
  /** The control, minus the ids the helper mints. */
  control: (ids: { controlId: string; at: string }) => RollupRunControl;
  /** A 409 reason when the run's status makes the control meaningless (besides terminal). */
  refuse?: (status: RollupRunStatus, item: PipelineRunItem) => { error: string; message: string } | null;
  /** Called with the outcome before the 202 goes out (audit, metrics). */
  onForwarded?: (outcome: { controlId: string; pipelineId: string; item: PipelineRunItem; applied: boolean; appliedBy: string | null; appliedInMs: number | null }) => void;
  /** How long to wait for the holder's acknowledgement. */
  awaitAppliedMs?: number;
  /** The control is for a run that already ended (resume-from-step). */
  allowTerminal?: boolean;
  /**
   * resume-from-step only: when no replica acknowledges it (the replica that
   * ran the run is gone, or this process restarted), release it and return
   * false so the caller rewinds the run here from its stored checkpoint,
   * instead of answering 409 run-not-resumable.
   */
  fallBackLocally?: boolean;
}

/**
 * Handles the whole response for a control the local executor cannot apply.
 * Returns true when it answered (the caller returns), false when the caller
 * should take its local path (no forwarder installed, or the run is held here).
 */
export async function forwardRunControl(params: ForwardRunControlParams): Promise<boolean> {
  const { req, res, reqLog, forwarder, runId, userId } = params;
  const probe = params.control({ controlId: 'probe', at: '' });
  if (forwarder.canApply({ kind: probe.kind, runId })) return false;
  const kind = probe.kind;

  const pipelineId =
    params.pipelineId ?? (await getRunRollupSubscriber()?.resolvePipelineId(runId)) ?? (await lookupPipelineIdByRunId(runId));
  const item = pipelineId
    ? await getRunRepo().get(pipelineId, runId).catch((err: unknown) => {
        reqLog.warn({ runId, kind, err: err instanceof Error ? err.message : String(err) }, 'run row read failed for a forwarded run control');
        return null;
      })
    : null;
  if (!pipelineId || !item) {
    res.status(404).json({ error: 'run-not-found', kind, message: `run ${runId} is not on this replica and has no row in the run table` });
    return true;
  }
  const status = normalizeRunStatus(item.status) ?? 'running';
  if (!params.allowTerminal && isTerminalRollupStatus(status)) {
    res.status(409).json({ error: 'run-already-ended', kind, status, message: `run ${runId} is ${status}; nothing to ${kind}` });
    return true;
  }
  const refusal = params.refuse?.(status, item) ?? null;
  if (refusal) {
    res.status(409).json({ ...refusal, kind, status });
    return true;
  }

  const headerKey = req.get('Idempotency-Key');
  const controlId = headerKey && headerKey.length <= 200 ? headerKey : randomUUID();
  const at = new Date().toISOString();
  const control = params.control({ controlId, at });
  let outcome;
  try {
    outcome = await forwarder.forward({ ...control, pipelineId }, { awaitAppliedMs: params.awaitAppliedMs });
  } catch (err) {
    reqLog.error({ runId, kind, controlId, err: err instanceof Error ? err.message : String(err) }, 'run control could not be forwarded');
    res.status(503).json({ error: 'run-control-forwarding-unavailable', kind, controlId, message: 'the control reached neither the rollup row nor the cluster pubsub; retry with the same Idempotency-Key' });
    return true;
  }
  const applied = outcome.applied?.ok === true;
  const appliedBy = outcome.applied?.nodeId ?? null;
  if (outcome.applied && !outcome.applied.ok) {
    // The holder had the run and refused: its reason is the answer.
    reqLog.warn({ runId, kind, controlId, appliedBy, err: outcome.applied.error }, 'run control refused by the replica that holds the run');
    res.status(409).json({ error: 'run-control-refused', kind, controlId, status, ownerNodeId: appliedBy, message: outcome.applied.error ?? 'the replica that holds the run refused the control' });
    return true;
  }
  if (!applied && kind === 'resume-from-step') {
    // Nobody has the failed run's executor (its replica is gone): only a new
    // run can take it from here. The row does not keep a control no one can apply.
    await forwarder.release(pipelineId, runId, controlId);
    if (params.fallBackLocally) {
      reqLog.info({ runId, kind, controlId, ownerNodeId: item.ownerNodeId ?? null }, 'no replica holds the run; rewinding it here from its stored checkpoint');
      return false;
    }
    res.status(409).json({ error: 'run-not-resumable', kind, controlId, status, ownerNodeId: item.ownerNodeId ?? null, message: `no replica holds run ${runId} any more; trigger a new run` });
    return true;
  }
  params.onForwarded?.({ controlId, pipelineId, item, applied, appliedBy, appliedInMs: outcome.appliedInMs });
  reqLog.info({ runId, kind, controlId, userId, applied, appliedBy, appliedInMs: outcome.appliedInMs, ownerNodeId: item.ownerNodeId ?? null, action: `${kind}.forwarded` }, applied ? 'run control forwarded and applied by the replica that holds the run' : 'run control recorded; the replica that resumes the run applies it');
  res.status(202).json({
    controlId, kind, runId, pipelineId, status,
    ownerNodeId: item.ownerNodeId ?? null, forwarded: true, recorded: outcome.recorded,
    applied, appliedBy, appliedInMs: outcome.appliedInMs,
  });
  return true;
}

/** The forwarder when one is installed — the seam the routes and their tests share. */
export function runControlForwarder(): RunControlForwarder | null { return getRunControlForwarder(); }
