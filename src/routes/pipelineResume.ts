// platform-api/src/routes/pipelineResume.ts
//
// hub#2223 — Resume a failed pipeline run from a specific step. Unlike
// `pipelineRetry.ts` (which spawns a brand-new run that consumes the original
// run's stored step outputs as upstream context), `resume` re-enters the
// SAME run via `PipelineModule.resumeFromStep` so accumulated context (LLM
// tokens, fetched data, transform outputs) is preserved and `replayAttempts`
// increments on the original run.
//
// Endpoints:
//   POST /api/pipelines/:pipelineId/runs/:runId/resume?fromNodeId=<nodeId>
//
// Validations:
//   - 404 when the run is unknown
//   - 400 when the run is not in a resumable terminal state
//     (default: only `failed`. `completed` allowed when
//     `allowCompleted=true` is passed in the body for diagnostic re-execution.)
//   - 400 when `fromNodeId` is unknown to the original DAG
//   - 503 when the bridge is not wired (no executor on this node)
//
// On success: emits a `pipeline.run.resumed-from-step` audit row and returns
// `{ runId, replayAttempts }`. The executor's emitted
// `pipeline:run:resumed-from-step` event drives the live frontend update.

import { Router } from 'express';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import { asyncHandler, NotFoundError, ValidationError } from '../middleware/error-handler';
import { idempotency } from '../middleware/idempotency';
import { withContext } from '../lib/logger';
import { recordPipelineTrigger, recordPipelineError } from '../observability/metrics';
import { getPipelineBridge } from './pipelineTriggers';
import { normalizeRunStatus } from '@connorhoehn/pipeline-runtime';
import { PipelineRunRepository } from '../repositories/PipelineRunRepository';
import { getSystemPipeline } from '../pipeline/systemPipelines';
import { DefinitionsRepository } from '../pipeline/definitions-repository';
import { auditRepo, type AuditEvent } from '../pipeline/audit-repository';
import { getDynamoClient } from '../lib/ddb-client';

const log = withContext({ route: 'pipelineResume' });
const tracer = trace.getTracer('platform-api');

/** Fire-and-forget audit write — mirrors the pipelineRetry pattern. */
function fireAndForgetAudit(event: AuditEvent): void {
  auditRepo.record(event).catch((err: unknown) => {
    const message = err instanceof Error ? err.message : String(err);
    log.warn(
      { action: event.action, pipelineId: event.pipelineId, runId: event.runId, err: message },
      'audit write failed',
    );
  });
}

// Lazy-init repositories — same posture as pipelineRetry.
let runRepo: PipelineRunRepository | null = null;
function getRunRepo(): PipelineRunRepository {
  if (!runRepo) runRepo = new PipelineRunRepository(getDynamoClient());
  return runRepo;
}

let defRepo: DefinitionsRepository | null = null;
function getDefRepo(): DefinitionsRepository {
  if (!defRepo) defRepo = new DefinitionsRepository(getDynamoClient());
  return defRepo;
}

export const pipelineResumeRouter = Router({ mergeParams: true });

interface ResumeFromStepRequestBody {
  /** When true, allow resuming a `completed` run for diagnostic re-execution. */
  allowCompleted?: boolean;
  /** Optional breakpoint list for the resumed dispatch loop. */
  breakpoints?: string[];
}

interface ResumeFromStepResponse {
  runId: string;
  fromNodeId: string;
  /** Original step that failed; surfaces in the audit row + UI banner. */
  originalFailedStepId?: string;
  /** Monotonic count of `resumeFromStep` invocations (post-increment). */
  replayAttempts: number;
  at: string;
}

// POST /resume?fromNodeId=<nodeId>
pipelineResumeRouter.post(
  '/',
  idempotency({ scope: 'pipeline-resume-from-step' }),
  asyncHandler<
    { pipelineId: string; runId: string },
    ResumeFromStepResponse | { error: string; action: string },
    ResumeFromStepRequestBody
  >(async (req, res) => {
    const { pipelineId, runId } = req.params;
    const userId = req.user!.sub;
    const body = (req.body ?? {}) as ResumeFromStepRequestBody;
    const query = (req.query ?? {}) as Record<string, unknown>;
    const fromNodeIdRaw = query['fromNodeId'];
    const fromNodeId =
      typeof fromNodeIdRaw === 'string' && fromNodeIdRaw.length > 0
        ? fromNodeIdRaw
        : null;
    const reqLog = log.child({ userId, pipelineId, runId, fromNodeId });

    if (!fromNodeId) {
      throw new ValidationError('Missing required query parameter: fromNodeId');
    }

    // 1. Validate the run exists.
    const originalRun = await getRunRepo().get(pipelineId, runId);
    if (!originalRun) {
      throw new NotFoundError(`Run ${runId} not found for pipeline ${pipelineId}`);
    }

    // 2. Validate the run is in a resumable terminal state. Only `failed`
    //    by default; `completed` allowed for diagnostic re-execution when
    //    `allowCompleted=true` is set.
    //
    //    Compared post-normalization so the legacy `succeeded`/`done`
    //    spellings on pre-fix rows resolve to `completed` and stay resumable.
    const allowCompleted = body.allowCompleted === true;
    const allowedStatuses = allowCompleted ? ['failed', 'completed'] : ['failed'];
    if (!allowedStatuses.includes(normalizeRunStatus(originalRun.status))) {
      throw new ValidationError(
        `Cannot resume run ${runId} with status '${originalRun.status}'. ` +
          `Expected one of: ${allowedStatuses.join(', ')}.` +
          (allowCompleted ? '' : " Pass `allowCompleted: true` to resume a completed run."),
      );
    }

    // 3. Validate that fromNodeId exists in the original DAG.
    // A person's pipeline, or a platform-owned one: the system registry is
    // where the document and media pipelines live, and a run of one of those
    // is as retryable as any other. Same resolution as GET /pipelines/:id.
    const definition = (await getDefRepo().get(userId, pipelineId)) ?? getSystemPipeline(pipelineId);
    if (!definition) {
      throw new NotFoundError(`Pipeline ${pipelineId} not found`);
    }
    const nodes = Array.isArray(definition.nodes)
      ? (definition.nodes as Array<{ id: string }>)
      : [];
    if (!nodes.find((n) => n.id === fromNodeId)) {
      throw new ValidationError(
        `Node ${fromNodeId} not found in pipeline ${pipelineId}`,
      );
    }

    // 4. Capture the originally-failed stepId for audit + UI banner.
    //    Best-effort — surfaces `error.nodeId` from the run's error blob, or
    //    walks the steps summary for a row whose status is `failed`.
    const originalFailedStepId = pickOriginalFailedStepId(originalRun);

    const bridge = getPipelineBridge();
    if (!bridge?.resumeFromStep) {
      reqLog.warn({ action: 'resume.stub' }, 'pipeline bridge not wired; cannot resume');
      res.status(503).json({
        error: 'Pipeline bridge not wired',
        action: 'resumeFromStep',
      });
      return;
    }

    const span = tracer.startSpan('pipeline.resume_from_step', {
      attributes: {
        'pipeline.id': pipelineId,
        'pipeline.run_id': runId,
        'pipeline.from_node_id': fromNodeId,
        'user.id': userId,
        ...(originalFailedStepId
          ? { 'pipeline.original_failed_step_id': originalFailedStepId }
          : {}),
      },
    });

    try {
      const resumed = await bridge.resumeFromStep(runId, fromNodeId, {
        ...(body.breakpoints !== undefined ? { breakpoints: body.breakpoints } : {}),
        ...(allowCompleted ? { allowCompleted: true } : {}),
      });
      // The executor returns the terminal PipelineRun snapshot including
      // the post-increment `replayAttempts`. Surface it; default to 0 when
      // the runtime didn't expose the field (e.g. older mock).
      const replayAttempts = readReplayAttempts(resumed);

      recordPipelineTrigger();
      reqLog.info(
        { runId, fromNodeId, replayAttempts, originalFailedStepId },
        'Resume from step triggered',
      );

      span.setAttribute('pipeline.replay_attempts', replayAttempts);
      span.setStatus({ code: SpanStatusCode.OK });
      span.end();

      fireAndForgetAudit({
        action: 'pipeline.run.resumed-from-step',
        actorUserId: userId,
        pipelineId,
        runId,
        outcome: 'success',
        details: {
          fromNodeId,
          replayAttempts,
          ...(originalFailedStepId ? { originalFailedStepId } : {}),
          ...(allowCompleted ? { allowCompleted: true } : {}),
        },
      });

      res.status(202).json({
        runId,
        fromNodeId,
        ...(originalFailedStepId ? { originalFailedStepId } : {}),
        replayAttempts,
        at: new Date().toISOString(),
      });
    } catch (err) {
      recordPipelineError('resume_from_step');
      span.recordException(err as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      span.end();
      fireAndForgetAudit({
        action: 'pipeline.run.resumed-from-step',
        actorUserId: userId,
        pipelineId,
        runId,
        outcome: 'error',
        decision: 'error',
        details: {
          fromNodeId,
          ...(originalFailedStepId ? { originalFailedStepId } : {}),
          error: err instanceof Error ? err.message : String(err),
        },
      });
      throw err;
    }
  }),
);

/** Best-effort extract of the original failure point. */
function pickOriginalFailedStepId(run: unknown): string | undefined {
  if (!run || typeof run !== 'object') return undefined;
  const r = run as Record<string, unknown>;
  const error = r['error'];
  if (error && typeof error === 'object') {
    const nodeId = (error as Record<string, unknown>)['nodeId'];
    if (typeof nodeId === 'string' && nodeId.length > 0 && nodeId !== 'unknown') {
      return nodeId;
    }
  }
  const summary = r['stepsSummary'];
  if (summary && typeof summary === 'object') {
    for (const [stepId, s] of Object.entries(summary as Record<string, unknown>)) {
      if (s && typeof s === 'object' && (s as Record<string, unknown>)['status'] === 'failed') {
        return stepId;
      }
    }
  }
  return undefined;
}

/** Pull `replayAttempts` off the runtime's PipelineRun snapshot. */
function readReplayAttempts(run: unknown): number {
  if (!run || typeof run !== 'object') return 0;
  const v = (run as Record<string, unknown>)['replayAttempts'];
  return typeof v === 'number' && Number.isFinite(v) ? v : 0;
}
