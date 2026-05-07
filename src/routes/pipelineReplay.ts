// social-api/src/routes/pipelineReplay.ts
//
// Pipeline run replay — re-execute a completed/failed run from historical state.
//
// Endpoints:
//   POST /api/pipelines/:pipelineId/runs/:runId/replay  → trigger re-execution
//
// Replay semantics:
//   - Loads completed run from DynamoDB (PipelineRunRepository)
//   - Triggers new run with same definition and trigger payload
//   - Returns new runId (does NOT reuse old runId)
//   - Original run remains in history unchanged
//
// Use cases:
//   - Re-run a failed pipeline after fixing data/config
//   - Reproduce a bug from production run
//   - Test pipeline behavior with historical inputs

import { Router } from 'express';
import { randomUUID } from 'crypto';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import { asyncHandler, NotFoundError, ValidationError } from '../middleware/error-handler';
import { idempotency } from '../middleware/idempotency';
import { withContext } from '../lib/logger';
import { recordPipelineTrigger, recordPipelineError } from '../observability/metrics';
import { getPipelineBridge, type PipelineBridge } from './pipelineTriggers';
import { PipelineRunRepository } from '../repositories/PipelineRunRepository';
import { DefinitionsRepository } from '../pipeline/definitions-repository';
import { auditRepo, type AuditEvent } from '../pipeline/audit-repository';
import { getDynamoClient } from '../lib/ddb-client';

const log = withContext({ route: 'pipelineReplay' });
const tracer = trace.getTracer('social-api');

/** Fire-and-forget audit write — mirrors the pipelineTriggers pattern. */
function fireAndForgetAudit(event: AuditEvent): void {
  auditRepo.record(event).catch((err: unknown) => {
    const message = err instanceof Error ? err.message : String(err);
    log.warn(
      { action: event.action, pipelineId: event.pipelineId, runId: event.runId, err: message },
      'audit write failed',
    );
  });
}

// Lazy-init repositories (matches pattern from other routes)
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

export const pipelineReplayRouter = Router({ mergeParams: true });

interface ReplayRunRequest {
  /** Optional: override trigger payload from original run. */
  triggerPayload?: Record<string, unknown>;
}

interface ReplayRunResponse {
  newRunId: string;
  originalRunId: string;
  pipelineId: string;
  at: string;
}

// POST /:runId/replay
pipelineReplayRouter.post(
  '/',
  idempotency({ scope: 'pipeline-replay' }),
  asyncHandler<
    { pipelineId: string; runId: string },
    ReplayRunResponse | { error: string; action: string },
    ReplayRunRequest
  >(async (req, res) => {
    const { pipelineId, runId } = req.params;
    const userId = req.user!.sub;
    const body = (req.body ?? {}) as ReplayRunRequest;
    const reqLog = log.child({ userId, pipelineId, runId });

    // Validate that original run exists in DynamoDB
    const originalRun = await getRunRepo().get(pipelineId, runId);
    if (!originalRun) {
      throw new NotFoundError(`Run ${runId} not found for pipeline ${pipelineId}`);
    }

    // Validate run is in terminal state (replay only makes sense for completed runs)
    const terminalStatuses = ['succeeded', 'failed', 'canceled'];
    if (!terminalStatuses.includes(originalRun.status)) {
      throw new ValidationError(
        `Cannot replay run ${runId} with status '${originalRun.status}'. Only completed runs (succeeded/failed/canceled) can be replayed.`,
      );
    }

    const bridge = getPipelineBridge();
    if (!bridge?.trigger) {
      reqLog.warn({ action: 'replay.stub' }, 'pipeline bridge not wired; cannot replay');
      res.status(503).json({
        error: 'Pipeline bridge not wired',
        action: 'replay',
      });
      return;
    }

    // Load pipeline definition from storage
    const definition = await getDefRepo().get(userId, pipelineId);
    if (!definition) {
      throw new NotFoundError(`Pipeline definition ${pipelineId} not found`);
    }

    // Trigger new run with original definition and payload (or override)
    const triggerPayload = body.triggerPayload ?? originalRun.stepsSummary ?? {};
    let newRunId: string = '';

    try {
      await tracer.startActiveSpan(
        'pipeline.replay',
        {
          attributes: {
            'pipeline.id': pipelineId,
            'pipeline.original_run_id': runId,
            'pipeline.user_id': userId,
          },
        },
        async (span) => {
          try {
            const result = await bridge.trigger!({
              pipelineId,
              definition,
              triggerPayload,
              triggeredBy: { userId },
            });
            newRunId = result.runId;

            span.setAttribute('pipeline.new_run_id', newRunId);
            span.setStatus({ code: SpanStatusCode.OK });
          } catch (err) {
            span.recordException(err as Error);
            span.setStatus({
              code: SpanStatusCode.ERROR,
              message: err instanceof Error ? err.message : String(err),
            });
            throw err;
          } finally {
            span.end();
          }
        },
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      recordPipelineError();
      reqLog.error({ runId, err: message }, 'replay failed');
      fireAndForgetAudit({
        action: 'pipeline.replay',
        actorUserId: userId,
        pipelineId,
        runId,
        outcome: 'error',
        decision: 'error',
        details: { originalRunId: runId, error: message },
      });
      res.status(500).json({ error: message, action: 'replay' });
      return;
    }

    const at = new Date().toISOString();
    recordPipelineTrigger();
    reqLog.info({ runId, newRunId }, 'pipeline replayed');

    fireAndForgetAudit({
      action: 'pipeline.replay',
      actorUserId: userId,
      pipelineId,
      runId: newRunId,
      outcome: 'success',
      details: {
        originalRunId: runId,
        usedTriggerPayloadOverride: body.triggerPayload !== undefined,
      },
    });

    res.status(202).json({
      newRunId,
      originalRunId: runId,
      pipelineId,
      at,
    });
  }),
);
