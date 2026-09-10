// POST /api/pipelines/:pipelineId/runs/:runId/review — a person's verdict on
// what a run SUGGESTED in a document.
//
// An agent's edit lands as tracked changes under one suggestion key (see
// documentAgentOps `mode: 'suggest'`); the run is complete the moment the
// proposal is in the document. Accepting or rejecting it is the person's act,
// and this is where that act is recorded ON THE RUN — so a run's page can say
// not only "Haiku proposed a closing paragraph" but "Grace rejected it".
//
// Two ways the decision reaches the document:
//   * The reviewer had the page open and resolved the marks in the editor. We
//     only record (`applyServerSide` absent/false).
//   * Nobody has the page open. `applyServerSide: true` starts the
//     `document-agent-review` pipeline — one `document:apply` step in direct
//     mode with `acceptSuggestion` / `rejectSuggestion` ops — so the resolve
//     still goes through the one write path and shows up as a run of its own.
import { Router } from 'express';
import { randomUUID } from 'crypto';
import { asyncHandler, ConflictError, NotFoundError, ValidationError } from '../middleware/error-handler';
import { withContext } from '../lib/logger';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { getRunPersistenceService } from '../pipeline/runPersistence';
import { PipelineRunRepository, type RunReview } from '../repositories/PipelineRunRepository';
import { getDynamoClient } from '../lib/ddb-client';
import { buildDocumentReviewApplyDefinition, DOCUMENT_AGENT_REVIEW_PIPELINE_ID } from '../documents/documentAgentPipeline';

const log = withContext({ route: 'pipelineRunReview' });

/** Bus event a recorded verdict publishes (relayed to the gateway like every `pipeline:*` event). */
export const RUN_REVIEWED_EVENT = 'pipeline:run:reviewed';

export interface RunReviewRequest {
  decision: 'accept' | 'reject';
  suggestionKey: string;
  documentId: string;
  comment?: string;
  /** Display name for the record; the token carries only `sub` and email. */
  byName?: string;
  /** Resolve the marks in the document from here (nobody has it open). */
  applyServerSide?: boolean;
}

export interface RunReviewResponse {
  runId: string;
  pipelineId: string;
  review: RunReview;
  /** The `document-agent-review` run that is resolving the marks, when `applyServerSide` was asked for. */
  applied?: { runId: string; pipelineId: string };
}

let runRepoSingleton: PipelineRunRepository | null = null;
function getRunRepo(): PipelineRunRepository {
  if (!runRepoSingleton) runRepoSingleton = new PipelineRunRepository(getDynamoClient());
  return runRepoSingleton;
}

/** Test seam. */
export function setRunReviewRepository(repo: PipelineRunRepository | null): void { runRepoSingleton = repo; }

function parseBody(raw: unknown): RunReviewRequest {
  const b = (raw ?? {}) as Record<string, unknown>;
  if (b.decision !== 'accept' && b.decision !== 'reject') throw new ValidationError("decision must be 'accept' or 'reject'");
  const suggestionKey = typeof b.suggestionKey === 'string' ? b.suggestionKey.trim() : '';
  if (!suggestionKey || !suggestionKey.includes('@')) throw new ValidationError('suggestionKey is required (the `user@date` key the run reported)');
  const documentId = typeof b.documentId === 'string' ? b.documentId.trim() : '';
  if (!documentId) throw new ValidationError('documentId is required');
  const comment = typeof b.comment === 'string' && b.comment.trim() ? b.comment.trim().slice(0, 2000) : undefined;
  const byName = typeof b.byName === 'string' && b.byName.trim() ? b.byName.trim().slice(0, 80) : undefined;
  return { decision: b.decision, suggestionKey, documentId, comment, byName, applyServerSide: b.applyServerSide === true };
}

export const pipelineRunReviewRouter = Router({ mergeParams: true });

pipelineRunReviewRouter.post(
  '/:runId/review',
  asyncHandler<{ pipelineId: string; runId: string }, RunReviewResponse>(async (req, res) => {
    const { pipelineId, runId } = req.params;
    const userId = req.user!.sub;
    const body = parseBody(req.body);

    const run = await getRunRepo().get(pipelineId, runId);
    if (!run) throw new NotFoundError(`run ${runId} not found for pipeline ${pipelineId}`);
    // One verdict per run: the suggestion is gone from the document once resolved either way.
    if (run.review) throw new ConflictError(`run ${runId} was already ${run.review.decision}ed by ${run.review.byName ?? run.review.by} at ${run.review.at}`);

    let applied: RunReviewResponse['applied'];
    if (body.applyServerSide) {
      const bridge = getPipelineBridge();
      if (!bridge?.trigger) { res.status(503).json({ error: 'pipeline engine is not available' } as never); return; }
      const out = await Promise.resolve(bridge.trigger({
        pipelineId: DOCUMENT_AGENT_REVIEW_PIPELINE_ID,
        definition: buildDocumentReviewApplyDefinition() as never,
        triggerPayload: {
          documentId: body.documentId,
          reviewOps: [{ op: body.decision === 'accept' ? 'acceptSuggestion' : 'rejectSuggestion', key: body.suggestionKey }],
          reviewOf: runId, reviewOfPipelineId: pipelineId,
          requestedBy: userId, requestedByName: body.byName ?? userId,
          instruction: `${body.decision === 'accept' ? 'Accept' : 'Reject'} suggestion ${body.suggestionKey}`,
          requestId: randomUUID(),
        },
        triggeredBy: { userId },
      } as never));
      const applyRunId = (out as { runId: string }).runId;
      getRunPersistenceService()?.registerTrigger({ runId: applyRunId, pipelineId: DOCUMENT_AGENT_REVIEW_PIPELINE_ID, userId, triggerType: 'manual', at: new Date().toISOString() });
      applied = { runId: applyRunId, pipelineId: DOCUMENT_AGENT_REVIEW_PIPELINE_ID };
    }

    const review: RunReview = {
      decision: body.decision,
      suggestionKey: body.suggestionKey,
      documentId: body.documentId,
      by: userId,
      ...(body.byName ? { byName: body.byName } : {}),
      at: new Date().toISOString(),
      ...(body.comment ? { comment: body.comment } : {}),
      ...(applied ? { appliedByRunId: applied.runId } : {}),
    };
    const stored = await getRunRepo().setReview(pipelineId, runId, review);
    if (!stored) throw new NotFoundError(`run ${runId} not found for pipeline ${pipelineId}`);
    const persistence = getRunPersistenceService();
    persistence?.setReview(runId, review);

    // The bus is only reachable through the persistence service; without it
    // (persistence disabled) the row is the record and nobody is told live.
    const bus = persistence?.getEventBus();
    if (bus) {
      void bus.publish(RUN_REVIEWED_EVENT as never, {
        runId, pipelineId, documentId: body.documentId, decision: body.decision, suggestionKey: body.suggestionKey,
        by: userId, byName: body.byName, at: review.at, ...(applied ? { appliedByRunId: applied.runId } : {}),
      } as never).catch((err: unknown) => log.warn({ runId, err: err instanceof Error ? err.message : String(err) }, 'run reviewed event publish failed'));
    }

    log.info({ pipelineId, runId, userId, decision: body.decision, suggestionKey: body.suggestionKey, applyServerSide: body.applyServerSide, appliedByRunId: applied?.runId }, 'pipeline run reviewed');
    res.status(200).json({ runId, pipelineId, review, ...(applied ? { applied } : {}) });
  }),
);
