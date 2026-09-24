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
//     only record (`applyServerSide` absent/false). This is the path the
//     document's MARGIN takes: accepting or rejecting a suggestion in the
//     gutter is an editor transaction that replicates through Yjs, and it calls
//     here afterwards so the run that proposed the change learns what became of
//     it. Before it did, the card in the conversation went on offering Accept
//     and Reject for marks that were no longer in the document, and pressing
//     Accept there recorded an acceptance of a change that had been rejected.
//   * Nobody has the page open. `applyServerSide: true` starts the
//     `document-agent-review` pipeline — one `document:apply` step in direct
//     mode with `acceptSuggestion` / `rejectSuggestion` ops — so the resolve
//     still goes through the one write path and shows up as a run of its own.
//
// `comment` is also how a PARTIAL resolve is recorded. A run writes its marks
// under one suggestion key, but that key can span several section editors and
// each is resolved on its own, so a person can accept the run's change in one
// section and reject it in another. `decision` has no value for that — and a
// third value would be worse than none, because the clients' `reviewOf()` drops
// a review it does not recognise and the card would go back to offering both
// buttons. So the first verdict is the decision and the comment says it was not
// the whole story.
//
// Who may give a verdict (NFR #171): accepting a suggestion changes the
// document, so the caller must be allowed to EDIT it — #41's `document:edit`
// through the document gate. No bearer 401; cannot read it 404; can read but
// not edit 403 `{ code: 'forbidden' }`. A run answering a comment is bound to
// that comment's document (its link), so a verdict naming another document —
// one the caller does own — cannot resolve someone else's thread: 404.
import { Router } from 'express';
import { randomUUID } from 'crypto';
import { asyncHandler, ConflictError, NotFoundError, ValidationError } from '../middleware/error-handler';
import { withContext } from '../lib/logger';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { getRunPersistenceService } from '../pipeline/runPersistence';
import { PipelineRunRepository, type RunReview } from '../repositories/PipelineRunRepository';
import { getDynamoClient } from '../lib/ddb-client';
import { buildDocumentReviewApplyDefinition, DOCUMENT_AGENT_REVIEW_PIPELINE_ID } from '../documents/documentAgentPipeline';
import { documentReadGate } from '../documents/documentReadGate';
import { DocumentEditForbiddenError } from '../deck/documentEditAccess';
import { commentForRun, settleCommentSuggestionReview, type CommentRunLink } from '../documents/commentAgentRequests';
import { suggestionKeyRunId } from '../documents/documentAgentOps';

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

    try {
      await documentReadGate().assertCanEdit(req, body.documentId);
    } catch (err) {
      if (err instanceof DocumentEditForbiddenError) {
        res.status(403).json({ error: err.message, code: err.code, documentId: body.documentId } as never);
        return;
      }
      throw err;
    }

    // NFR #213: a per-run key names its run; a verdict for run A must not
    // carry run B's key (it would resolve B's marks server-side and record A).
    const keyRun = suggestionKeyRunId(body.suggestionKey);
    if (keyRun && keyRun !== runId) throw new ValidationError(`suggestionKey ${body.suggestionKey} belongs to another run`);

    const run = await getRunRepo().get(pipelineId, runId);
    if (!run) throw new NotFoundError(`run ${runId} not found for pipeline ${pipelineId}`);
    const link = await commentForRun(runId);
    if (link && link.documentId !== body.documentId) {
      log.warn({ pipelineId, runId, userId, documentId: body.documentId, linkedDocumentId: link.documentId }, 'run review refused: the run belongs to another document');
      throw new NotFoundError(`run ${runId} not found for pipeline ${pipelineId}`);
    }
    // One verdict per run, and the FIRST one stands. The suggestion is gone
    // from the document once resolved either way, so a second verdict is
    // describing something that is no longer there.
    //
    // Two surfaces reach this: the run's card in the conversation, and the
    // document's own margin (which records here without `applyServerSide` —
    // the editor already resolved the marks). They race. Whichever lands
    // second must not overwrite a true record with a false one, and must not
    // be thrown back at a reader who did nothing wrong:
    //
    //   * The SAME verdict arriving twice is the ordinary race — one person
    //     resolving in the margin while their own card reports it, or two
    //     people agreeing. It is not a conflict: answer with the record that
    //     stands. Crucially this returns BEFORE `applyServerSide`, so the
    //     loser of the race never starts a second `document-agent-review` run
    //     to re-resolve marks that are already gone.
    //   * The OPPOSITE verdict is a conflict, and the record is the one that
    //     landed first. The caller is told what the record actually holds so it
    //     can show that rather than an error.
    if (run.review) {
      const held = run.review;
      const who = held.byName ?? held.by;
      if (held.decision === body.decision) {
        // A retry after the first call recorded the verdict but died before the
        // comment was settled (a restart mid-request) finishes that settle here.
        if (link) await settleLinkedComment(link, runId, held);
        res.status(200).json({ runId, pipelineId, review: held });
        return;
      }
      throw new ConflictError(
        `These suggestions were already ${held.decision}ed by ${who}, so they are no longer in the document.`,
      );
    }

    let applied: RunReviewResponse['applied'];
    // A legacy key (`agent:haiku@<ten-minute bucket>`, before #213) is shared by
    // every run in its bucket: resolving it server-side would resolve the other
    // runs' marks too. Refuse; the margin (which resolves only what the reader
    // clicked and records without applyServerSide) still works for them.
    if (body.applyServerSide && !keyRun) {
      throw new ConflictError('This suggestion predates per-run suggestions and may be shared with other runs; accept or reject it in the document.');
    }
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
    if (!stored) {
      // Lost a race to the opposite verdict (another replica, another reader).
      const now = await getRunRepo().get(pipelineId, runId);
      if (!now) throw new NotFoundError(`run ${runId} not found for pipeline ${pipelineId}`);
      const held = now.review;
      throw new ConflictError(
        `These suggestions were already ${held?.decision ?? 'review'}ed by ${held?.byName ?? held?.by ?? 'someone'}, so they are no longer in the document.`,
      );
    }
    const persistence = getRunPersistenceService();
    persistence?.setReview(runId, review);

    // NFR #201: the linked comment's reply (and resolve, when asked) is written
    // HERE, by the replica whose compare-and-set recorded the verdict, before
    // the caller is answered — no open tab and no bus delivery needed. The
    // `pipeline:run:reviewed` event below still reaches every replica's
    // CommentAgentResolver; the comment row's own CAS makes that a no-op.
    if (link) await settleLinkedComment(link, runId, review);

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

/** Settles the comment a run answers from the recorded verdict; idempotent (the comment row's CAS). */
async function settleLinkedComment(link: CommentRunLink, runId: string, review: Pick<RunReview, 'decision' | 'by' | 'byName'>): Promise<void> {
  try {
    const result = await settleCommentSuggestionReview(link, runId, {
      decision: review.decision, by: review.by, ...(review.byName ? { byName: review.byName } : {}),
    });
    log.info({ runId, commentId: link.commentId, result }, 'linked comment settled from the verdict');
  } catch (err) {
    // The verdict stands; a retry of this route or the bus subscriber settles it.
    log.warn({ runId, commentId: link.commentId, err: err instanceof Error ? err.message : String(err) }, 'linked comment settle failed');
  }
}
