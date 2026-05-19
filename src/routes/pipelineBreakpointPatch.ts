// platform-api/src/routes/pipelineBreakpointPatch.ts
//
// hub#2226 — edit-and-continue HTTP surface for paused-run context patching.
// Sits next to pipelineBreakpointRouter (resolve) and shares the same URL
// prefix `/api/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId`.
//
// Endpoints:
//   POST   /patch              — apply a ContextPatch while paused
//   DELETE /patch/:patchId     — revert a previously-applied patch
//
// Validations (POST):
//   - 400 when body.patch is absent / malformed
//   - 404 when the run is unknown
//   - 409 when the run is not paused or stepId !== pausedAtNodeId
//   - 422 when the executor's validator rejects (validation.ok === false)
//   - 503 when the bridge is not wired
//
// Validations (DELETE):
//   - 404 when the run is unknown
//   - 409 when the run is not paused at the same stepId
//   - 503 when the bridge is not wired
//
// On success POST returns the executor's PatchResult; DELETE returns 204.
// Both routes write a fire-and-forget audit row keyed on the new
// `pipeline.step.context.patched` / `pipeline.step.context.patch-reverted`
// AuditAction values (added in `audit-repository.ts`).
//
// Errors thrown by the executor (`PatchValidationError`,
// `InvalidRunStateError`) are mapped onto 422/409 by name — we deliberately
// do not import the constructors so this file stays decoupled from the
// distributed-core type pin (the bridge layer already does the cast).

import { Router } from 'express';
import { withSpan } from 'distributed-core';
import { asyncHandler, NotFoundError, ValidationError } from '../middleware/error-handler';
import { withContext } from '../lib/logger';
import { getPipelineBridge, type PipelineRunSnapshot } from '../pipeline/bridge-types';
import { auditRepo, type AuditEvent } from '../pipeline/audit-repository';

const log = withContext({ route: 'pipelineBreakpointPatch' });

function fireAndForgetAudit(event: AuditEvent): void {
  auditRepo.record(event).catch((err: unknown) => {
    const message = err instanceof Error ? err.message : String(err);
    log.warn(
      { action: event.action, pipelineId: event.pipelineId, runId: event.runId, err: message },
      'audit write failed',
    );
  });
}

/**
 * Map a thrown executor error to an HTTP status. Name-based so we don't
 * have to import the distributed-core error classes here.
 */
function statusForError(err: unknown): number {
  if (err && typeof err === 'object') {
    const name = (err as { name?: string }).name;
    if (name === 'PatchValidationError') return 422;
    if (name === 'InvalidRunStateError') return 409;
  }
  return 500;
}

/** Body shape for the POST route. */
interface ApplyPatchRequestBody {
  patch?: unknown;
  appliedBy?: string;
}

/**
 * Validate a ContextPatch payload at the gateway boundary. We DON'T re-walk
 * jsonPatch ops here — the executor's PatchValidationError covers structural
 * issues. We only check the discriminator + the `merge` shape so a bad
 * caller gets a 400 rather than a 500 from the bridge throwing.
 */
function isContextPatch(v: unknown): boolean {
  if (!v || typeof v !== 'object') return false;
  const m = (v as { mode?: unknown }).mode;
  if (m === 'merge') {
    const values = (v as { values?: unknown }).values;
    return !!values && typeof values === 'object' && !Array.isArray(values);
  }
  if (m === 'jsonPatch') {
    const ops = (v as { ops?: unknown }).ops;
    return Array.isArray(ops);
  }
  return false;
}

export const pipelineBreakpointPatchRouter = Router({ mergeParams: true });

// ---------------------------------------------------------------------------
// POST  /api/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/patch
// ---------------------------------------------------------------------------

pipelineBreakpointPatchRouter.post(
  '/',
  asyncHandler<
    { pipelineId: string; runId: string; stepId: string },
    unknown,
    ApplyPatchRequestBody
  >(async (req, res) => {
    const { pipelineId, runId, stepId } = req.params;
    const userId = req.user!.sub;
    const body = (req.body ?? {}) as ApplyPatchRequestBody;
    const reqLog = log.child({ userId, pipelineId, runId, stepId });

    if (!isContextPatch(body.patch)) {
      throw new ValidationError(
        'patch is required and must be { mode: "merge", values: {...} } or { mode: "jsonPatch", ops: [...] }',
      );
    }
    if (body.appliedBy !== undefined && typeof body.appliedBy !== 'string') {
      throw new ValidationError('appliedBy must be a string when present');
    }

    const bridge = getPipelineBridge();
    if (!bridge?.patchPausedContext) {
      reqLog.warn({ action: 'patch.unwired' }, 'pipeline bridge not wired; cannot patch');
      res.status(503).json({
        error: 'Pipeline bridge not wired',
        action: 'patchPausedContext',
      });
      return;
    }

    // Pre-flight: confirm the run exists, is paused, and matches stepId. We
    // could rely on the executor to throw `InvalidRunStateError` here, but
    // checking up-front lets us surface 404 vs 409 without parsing the
    // error name twice and keeps the audit row's `before` snapshot accurate.
    let snap: PipelineRunSnapshot | null = null;
    let snapResolved = false;
    try {
      snap = await Promise.resolve(bridge.getRun(runId));
      snapResolved = true;
    } catch (err) {
      reqLog.warn(
        { err: err instanceof Error ? err.message : String(err) },
        'getRun lookup failed; continuing — executor will enforce',
      );
    }

    if (snapResolved && snap === null) {
      throw new NotFoundError(`Run ${runId} not found`);
    }
    if (snap) {
      const status = typeof snap.status === 'string' ? snap.status : '';
      const pausedAtNodeId = (snap as Record<string, unknown>)['pausedAtNodeId'];
      if (status !== 'paused_at_breakpoint') {
        res.status(409).json({
          error: `Run ${runId} is not paused (status='${status}')`,
          action: 'patchPausedContext',
        });
        return;
      }
      if (typeof pausedAtNodeId === 'string' && pausedAtNodeId !== stepId) {
        res.status(409).json({
          error: `Run ${runId} is paused at '${pausedAtNodeId}', not '${stepId}'`,
          action: 'patchPausedContext',
        });
        return;
      }
    }

    const appliedBy = body.appliedBy ?? userId;

    let result;
    try {
      result = await withSpan(
        'pipeline.patchPausedContext',
        {
          'pipeline.id': pipelineId,
          'pipeline.run_id': runId,
          'pipeline.step_id': stepId,
          'user.id': userId,
        },
        async () =>
          Promise.resolve(
            bridge.patchPausedContext!(runId, stepId, body.patch as Parameters<NonNullable<typeof bridge.patchPausedContext>>[2], appliedBy),
          ),
      );
    } catch (err) {
      const status = statusForError(err);
      const message = err instanceof Error ? err.message : String(err);
      reqLog.warn(
        { err: message, status, action: 'patchPausedContext.error' },
        'patchPausedContext threw',
      );
      fireAndForgetAudit({
        action: 'pipeline.step.context.patched',
        actorUserId: userId,
        pipelineId,
        runId,
        outcome: 'error',
        decision: 'error',
        details: { stepId, error: message, appliedBy },
      });
      res.status(status).json({ error: message, action: 'patchPausedContext' });
      return;
    }

    // Validator rejection → 422 with reason. The patch was NOT applied; the
    // executor returns the pre-patch context unchanged in before/after.
    if (result.validation && result.validation.ok === false) {
      const reason = result.validation.reason;
      reqLog.info(
        { stepId, patchId: result.patchId, reason, action: 'patch.validation-rejected' },
        'patch rejected by validator',
      );
      fireAndForgetAudit({
        action: 'pipeline.step.context.patched',
        actorUserId: userId,
        pipelineId,
        runId,
        outcome: 'rejected',
        decision: 'rejected',
        details: { stepId, patchId: result.patchId, reason, appliedBy },
      });
      res.status(422).json({
        error: reason,
        action: 'patchPausedContext',
        result,
      });
      return;
    }

    reqLog.info(
      { stepId, patchId: result.patchId, action: 'patch.applied' },
      'paused-context patch applied',
    );
    fireAndForgetAudit({
      action: 'pipeline.step.context.patched',
      actorUserId: userId,
      pipelineId,
      runId,
      outcome: 'success',
      details: { stepId, patchId: result.patchId, appliedBy },
      before: result.before,
      after: result.after,
    });

    res.status(200).json(result);
  }),
);

// ---------------------------------------------------------------------------
// DELETE /api/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/patch/:patchId
// ---------------------------------------------------------------------------

pipelineBreakpointPatchRouter.delete(
  '/:patchId',
  asyncHandler<{
    pipelineId: string;
    runId: string;
    stepId: string;
    patchId: string;
  }>(async (req, res) => {
    const { pipelineId, runId, stepId, patchId } = req.params;
    const userId = req.user!.sub;
    const reqLog = log.child({ userId, pipelineId, runId, stepId, patchId });

    const bridge = getPipelineBridge();
    if (!bridge?.revertContextPatch) {
      reqLog.warn({ action: 'patch.revert.unwired' }, 'pipeline bridge not wired; cannot revert');
      res.status(503).json({
        error: 'Pipeline bridge not wired',
        action: 'revertContextPatch',
      });
      return;
    }

    try {
      await withSpan(
        'pipeline.revertContextPatch',
        {
          'pipeline.id': pipelineId,
          'pipeline.run_id': runId,
          'pipeline.step_id': stepId,
          'pipeline.patch_id': patchId,
          'user.id': userId,
        },
        async () => Promise.resolve(bridge.revertContextPatch!(runId, stepId, patchId)),
      );
    } catch (err) {
      const status = statusForError(err);
      const message = err instanceof Error ? err.message : String(err);
      reqLog.warn(
        { err: message, status, action: 'patch.revert.error' },
        'revertContextPatch threw',
      );
      fireAndForgetAudit({
        action: 'pipeline.step.context.patch-reverted',
        actorUserId: userId,
        pipelineId,
        runId,
        outcome: 'error',
        decision: 'error',
        details: { stepId, patchId, error: message },
      });
      res.status(status).json({ error: message, action: 'revertContextPatch' });
      return;
    }

    reqLog.info(
      { stepId, patchId, action: 'patch.reverted' },
      'paused-context patch reverted',
    );
    fireAndForgetAudit({
      action: 'pipeline.step.context.patch-reverted',
      actorUserId: userId,
      pipelineId,
      runId,
      outcome: 'success',
      details: { stepId, patchId },
    });

    res.status(204).end();
  }),
);
