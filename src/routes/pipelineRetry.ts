// platform-api/src/routes/pipelineRetry.ts
//
// Pipeline run retry from specific node — resume execution from a failed step
// without re-running succeeded upstream nodes.
//
// Endpoints:
//   POST /api/pipelines/:pipelineId/runs/:runId/retry/:nodeId
//
// Retry semantics:
//   - Loads completed run from DynamoDB (PipelineRunRepository)
//   - Validates that nodeId exists and failed in the original run
//   - Triggers new run starting from nodeId with upstream outputs as context
//   - Skips all nodes before nodeId (uses their stored outputs)
//   - Returns new runId with metadata indicating it's a partial retry
//
// Use cases:
//   - Retry a 10-node pipeline from node 8 after fixing the issue
//   - Save LLM costs by not re-running succeeded upstream nodes
//   - Debugging: isolate failures to specific nodes

import { Router } from 'express';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import { asyncHandler, NotFoundError, ValidationError } from '../middleware/error-handler';
import { runRefusal } from './pipelineResume';
import { idempotency } from '../middleware/idempotency';
import { withContext } from '../lib/logger';
import { recordPipelineTrigger, recordPipelineError } from '../observability/metrics';
import { getPipelineBridge } from './pipelineTriggers';
import { isTerminalRunStatus } from '@connorhoehn/pipeline-runtime';
import { PipelineRunRepository } from '../repositories/PipelineRunRepository';
import { getSystemPipeline } from '../pipeline/systemPipelines';
import { readAttempts } from '../repositories/PipelineRunRepository';
import { DefinitionsRepository } from '../pipeline/definitions-repository';
import { auditRepo, type AuditEvent } from '../pipeline/audit-repository';
import { getDynamoClient } from '../lib/ddb-client';
import { requireRunAccess } from '../pipeline/runAccess';
import { pinRequester } from '../pipeline/pinRequester';

const log = withContext({ route: 'pipelineRetry' });
const tracer = trace.getTracer('platform-api');

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

// Lazy-init repositories
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

export const pipelineRetryRouter = Router({ mergeParams: true });

// Separate router for actions that sit directly on /:pipelineId/runs/:runId
// (not under /retry/). Mounted at /pipelines/:pipelineId/runs/:runId.
export const pipelineRunActionsRouter = Router({ mergeParams: true });

interface RetryFromNodeRequest {
  /** Optional: override inputs for the target node. */
  inputs?: Record<string, unknown>;
  /** When true, use the persisted input snapshot from the original attempt instead of recomputing. */
  snapshotInput?: boolean;
}

interface RetryFromNodeResponse {
  newRunId: string;
  originalRunId: string;
  pipelineId: string;
  retryFromNodeId: string;
  skippedNodes: string[];
  at: string;
}

// POST /:runId/retry/:nodeId
pipelineRetryRouter.post(
  '/:nodeId',
  // Loop 29c (NFR #173): the original run's owner only — before any system-pipeline fallback.
  requireRunAccess('owner'),
  idempotency({ scope: 'pipeline-retry-from-node' }),
  asyncHandler<
    { pipelineId: string; runId: string; nodeId: string },
    RetryFromNodeResponse | { error: string; action: string },
    RetryFromNodeRequest
  >(async (req, res) => {
    const { pipelineId, runId, nodeId } = req.params;
    const userId = req.user!.sub;
    const body = (req.body ?? {}) as RetryFromNodeRequest;
    const reqLog = log.child({ userId, pipelineId, runId, nodeId });

    // Validate that original run exists in DynamoDB
    const originalRun = await getRunRepo().get(pipelineId, runId);
    if (!originalRun) {
      throw new NotFoundError(`Run ${runId} not found for pipeline ${pipelineId}`);
    }

    // Validate run is in terminal state. Shared predicate, so canonical
    // spellings and the legacy aliases on pre-fix rows are both accepted.
    if (!isTerminalRunStatus(originalRun.status)) {
      throw new ValidationError(
        `Cannot retry from run ${runId} with status '${originalRun.status}'. Only terminal runs can be retried.`,
      );
    }

    // A refused run (constraint-not-met, forbidden, revision-conflict,
    // invalid-request) is a decision about the inputs it already had; a
    // retry over the same trigger payload decides the same way. See
    // pipelineResume's `runRefusal`.
    const refusal = runRefusal(originalRun);
    if (refusal) {
      throw new ValidationError(
        `Run ${runId} was refused (${refusal.code}) and cannot be retried: ${refusal.message} ` +
          'Change the request and start a new run.',
      );
    }

    // Load pipeline definition to validate nodeId exists
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
    const targetNode = nodes.find((n) => n.id === nodeId);
    if (!targetNode) {
      throw new ValidationError(
        `Node ${nodeId} not found in pipeline ${pipelineId}`,
      );
    }

    // Validate that nodeId was executed in the original run
    // stepsSummary is JSONB: { [nodeId]: { status, startedAt, completedAt, durationMs, ... } }
    const stepsSummary = originalRun.stepsSummary as Record<string, unknown> | undefined;
    if (!stepsSummary || !stepsSummary[nodeId]) {
      throw new ValidationError(
        `Node ${nodeId} was not executed in run ${runId}. Cannot retry from a node that didn't run.`,
      );
    }

    const bridge = getPipelineBridge();
    if (!bridge?.trigger) {
      reqLog.warn({ action: 'retry.stub' }, 'pipeline bridge not wired; cannot retry');
      res.status(503).json({
        error: 'Pipeline bridge not wired',
        action: 'retry',
      });
      return;
    }

    // Build trigger payload with:
    // 1. Original trigger payload
    // 2. Special metadata indicating this is a partial retry
    // 3. Context from all succeeded upstream nodes
    // Build the trigger payload: optionally use the persisted input snapshot
    // (hub#1283) so replay produces the same downstream context for non-deterministic
    // upstream nodes (LLM with temperature > 0, time-based transforms).
    const nodeStepSummary = stepsSummary?.[nodeId] as { attempts?: unknown[] } | undefined;
    const nodeAttempts = readAttempts(
      nodeStepSummary as Parameters<typeof readAttempts>[0],
      runId,
      nodeId,
    );
    const snapshotInput =
      body.snapshotInput && nodeAttempts.length > 0
        ? nodeAttempts[nodeAttempts.length - 1].input
        : undefined;

    // The new run's context starts from what the ORIGINAL run started from —
    // the trigger's output is the run's input (a /doc command's documentId
    // and instruction, say). Without it a retried step that reads the run
    // context found nothing there and failed with "documentId missing", a
    // worse failure than the one being retried.
    const triggerNode = nodes.find((n) => (n as { type?: string }).type === 'trigger');
    const triggerOutput = triggerNode ? (stepsSummary[triggerNode.id] as { output?: unknown } | undefined)?.output : undefined;
    const originalInput = triggerOutput && typeof triggerOutput === 'object' && !Array.isArray(triggerOutput)
      ? (triggerOutput as Record<string, unknown>)
      : {};
    // Loop 29c (NFR #173): the retry acts as the verified caller, whatever the
    // original trigger output or `body.inputs` claim (pinRequester, #47).
    const retryPayload = {
      ...pinRequester({
        ...originalInput,
        ...(snapshotInput !== undefined ? { __snapshotInput: snapshotInput } : {}),
        ...(body.inputs ?? {}),
      }, userId).payload,
      __retry: {
        originalRunId: runId,
        retryFromNodeId: nodeId,
        upstreamOutputs: stepsSummary,
        usedSnapshotInput: snapshotInput !== undefined,
      },
    };

    const span = tracer.startSpan('pipeline.retry_from_node', {
      attributes: {
        'pipeline.id': pipelineId,
        'pipeline.original_run_id': runId,
        'pipeline.retry_from_node': nodeId,
        'user.id': userId,
      },
    });

    try {
      // `definition` is REQUIRED by PipelineModule.createResource (it throws
      // "createResource: metadata.applicationData.definition (PipelineDefinition)
      // is required" otherwise). We already loaded it above to validate nodeId,
      // so forward it — same shape the sibling trigger route sends.
      const { runId: newRunId } = await bridge.trigger({
        pipelineId,
        definition,
        triggerPayload: retryPayload,
        triggeredBy: { userId },
      });
      span.setAttribute('pipeline.new_run_id', newRunId);

      recordPipelineTrigger();
      reqLog.info(
        { newRunId, originalRunId: runId, retryFromNodeId: nodeId },
        'Retry from node triggered',
      );

      span.setStatus({ code: SpanStatusCode.OK });
      span.end();

      // Calculate which nodes will be skipped (all upstream of target)
      const skippedNodes = Object.keys(stepsSummary).filter(
        (id) => id !== nodeId && stepsSummary[id],
      );

      fireAndForgetAudit({
        action: 'pipeline.retry',
        actorUserId: userId,
        pipelineId,
        runId: newRunId,
        outcome: 'success',
        details: {
          originalRunId: runId,
          retryFromNodeId: nodeId,
          skippedNodeCount: skippedNodes.length,
          usedSnapshotInput: snapshotInput !== undefined,
        },
      });

      res.status(202).json({
        newRunId,
        originalRunId: runId,
        pipelineId,
        retryFromNodeId: nodeId,
        skippedNodes,
        at: new Date().toISOString(),
      });
    } catch (err) {
      recordPipelineError('retry_from_node');
      span.recordException(err as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      span.end();
      fireAndForgetAudit({
        action: 'pipeline.retry',
        actorUserId: userId,
        pipelineId,
        runId,
        outcome: 'error',
        decision: 'error',
        details: {
          originalRunId: runId,
          retryFromNodeId: nodeId,
          error: err instanceof Error ? err.message : String(err),
        },
      });
      throw err;
    }
  }),
);

// POST /force-fail (mounted at /pipelines/:pipelineId/runs/:runId)
//
// Hub#1285: operator action for stuck runs. Cancels the run via the pipeline
// bridge (fire-and-forget when the executor is unresponsive) and clears the
// `stuckAt` flag in DDB so the UI transitions to "interrupted" → retryable.
pipelineRunActionsRouter.post(
  '/force-fail',
  requireRunAccess('owner'),
  asyncHandler<
    { pipelineId: string; runId: string },
    { runId: string; status: string; hint: string; at: string } | { error: string }
  >(async (req, res) => {
    const { pipelineId, runId } = req.params;
    const userId = req.user!.sub;

    const run = await getRunRepo().get(pipelineId, runId);
    if (!run) throw new NotFoundError(`Run ${runId} not found for pipeline ${pipelineId}`);

    if (run.status !== 'running') {
      throw new ValidationError(
        `Cannot force-fail run ${runId}: status is '${run.status}', expected 'running'`,
      );
    }
    if (!run.stuckAt) {
      throw new ValidationError(
        `Run ${runId} has not been flagged as stuck. Only stuck runs can be force-failed.`,
      );
    }

    // Update DDB immediately so the UI badge clears even if the bridge is unresponsive.
    await getRunRepo().put({ ...run, stuckAt: undefined, status: 'interrupted', completedAt: new Date().toISOString() });

    // Best-effort cancel via bridge; don't block the response on it.
    const bridge = getPipelineBridge();
    if (bridge?.cancelRun) {
      void (bridge as { cancelRun: (runId: string, userId: string) => Promise<unknown> })
        .cancelRun(runId, userId)
        .catch(() => { /* bridge may be unresponsive when run is stuck */ });
    }

    log.warn({ runId, pipelineId, userId }, 'force-fail: run transitioned to interrupted');

    fireAndForgetAudit({
      action: 'pipeline.force_fail',
      actorUserId: userId,
      pipelineId,
      runId,
      outcome: 'success',
      decision: 'cancelled',
      before: { status: run.status, stuckAt: run.stuckAt },
      after: { status: 'interrupted' },
    });

    res.status(202).json({
      runId,
      status: 'interrupted',
      hint: 'Use "Retry from failed step" to resume execution from the stuck node.',
      at: new Date().toISOString(),
    });
  }),
);
