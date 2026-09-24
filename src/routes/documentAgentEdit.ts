// POST /api/documents/:documentId/agent-edit — the chat's "/doc …" command.
// Starts the document-agent-edit pipeline for this document and answers with
// the run id; everything after that is the pipeline's (status events on
// `pipeline:run:<runId>`, the edit through the collaboration channel).
import { Router, Request, Response } from 'express';
import { randomUUID } from 'crypto';
import { requireAuth } from '../middleware/auth';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { getRunPersistenceService } from '../pipeline/runPersistence';
import { buildDocumentAgentEditDefinition, buildDocumentTransformDefinition, DOCUMENT_AGENT_EDIT_PIPELINE_ID, DOCUMENT_TRANSFORM_PIPELINE_ID } from '../documents/documentAgentPipeline';
import { buildConversationSummarizeDefinition, CONVERSATION_SUMMARIZE_PIPELINE_ID, DEFAULT_HISTORY_LIMIT } from '../documents/conversationSummarizePipeline';
import { buildDiagramGenerateDefinition, DIAGRAM_GENERATE_PIPELINE_ID } from '../documents/diagramGeneratePipeline';
import log from '../lib/logger';
import { documentCommentRepo } from '../repositories';
import { documentReadGate } from '../documents/documentReadGate';
import { DocumentEditForbiddenError } from '../deck/documentEditAccess';
import { publishSignal } from '../lib/event-catalog';
import { commentScopedInstruction, linkRunToComment } from '../documents/commentAgentRequests';
import { getCommentAgentResolver } from '../documents/commentAgentResolver';
import { NotFoundError } from '../middleware/error-handler';
import { bodyId, requireChannelMember, requireDocumentEdit, requireDocumentRead } from '../middleware/resource-access';

export const documentAgentEditRouter = Router();

documentAgentEditRouter.post('/:documentId/agent-edit', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const documentId = String(req.params.documentId ?? '');
  const body = (req.body ?? {}) as { instruction?: unknown; chatChannel?: unknown; requestedByName?: unknown; commentId?: unknown; resolveOnApply?: unknown };
  const typed = typeof body.instruction === 'string' ? body.instruction.trim() : '';
  if (!documentId) throw new ValidationError('documentId is required');
  if (!typed) throw new ValidationError('instruction is required');
  if (typed.length > 2000) throw new ValidationError('instruction must be 2000 characters or fewer');
  const userId = req.user!.sub;
  const requestedByName = typeof body.requestedByName === 'string' && body.requestedByName ? body.requestedByName.slice(0, 80) : userId;
  const chatChannel = typeof body.chatChannel === 'string' ? body.chatChannel : undefined;

  // WS-J lane B: a comment handed to the agent. The run answers it — the
  // instruction carries the comment and its anchored text, and the comment is
  // linked to the run so its completion replies to (and may resolve) it.
  if (body.commentId !== undefined && (typeof body.commentId !== 'string' || !body.commentId.trim())) {
    throw new ValidationError('commentId must be a non-empty string');
  }
  if (body.resolveOnApply !== undefined && typeof body.resolveOnApply !== 'boolean') {
    throw new ValidationError('resolveOnApply must be a boolean');
  }
  const commentId = typeof body.commentId === 'string' ? body.commentId.trim() : undefined;
  // NFR #171: the run writes into the document (suggestions, or a direct edit
  // after approval), so asking for one takes #41's `document:edit` — not only
  // read. No bearer 401; cannot read it 404; reader 403 `{ code: 'forbidden' }`.
  try {
    await documentReadGate().assertCanEdit(req, documentId);
  } catch (err) {
    if (err instanceof DocumentEditForbiddenError) {
      res.status(403).json({ error: err.message, code: err.code, documentId });
      return;
    }
    throw err;
  }
  let instruction = typed;
  if (commentId) {
    const comment = await documentCommentRepo.getComment(documentId, commentId);
    if (!comment || comment.documentId !== documentId) throw new NotFoundError('Comment not found in this document');
    if (comment.parentCommentId) throw new ValidationError('Ask the agent about the thread, not a reply');
    instruction = commentScopedInstruction(typed, comment);
  }

  const bridge = getPipelineBridge();
  if (!bridge?.trigger) {
    res.status(503).json({ error: 'pipeline engine is not available' });
    return;
  }
  // The requester approves a big edit — set on the definition here, since approvers are not templated.
  const definition = buildDocumentAgentEditDefinition(undefined, { approverUserId: userId });
  const out = await Promise.resolve(bridge.trigger({
    pipelineId: DOCUMENT_AGENT_EDIT_PIPELINE_ID,
    definition: definition as never,
    triggerPayload: { documentId, instruction, requestedBy: userId, requestedByName, ...(chatChannel ? { chatChannel } : {}), ...(commentId ? { commentId } : {}), requestId: randomUUID() },
    triggeredBy: { userId },
  } as never));
  const runId = (out as { runId: string }).runId;
  getRunPersistenceService()?.registerTrigger({ runId, pipelineId: DOCUMENT_AGENT_EDIT_PIPELINE_ID, userId, triggerType: 'manual', at: new Date().toISOString() });
  let resolveOnApply: boolean | undefined;
  if (commentId) {
    resolveOnApply = body.resolveOnApply !== false;
    const link = { documentId, commentId, pipelineId: DOCUMENT_AGENT_EDIT_PIPELINE_ID };
    // Both before the answer: the run takes seconds (a model call), and the
    // terminal event must find the link and a pending request.
    getCommentAgentResolver()?.remember(runId, link);
    await linkRunToComment(runId, link);
    const comment = await documentCommentRepo.setAgentRequest(documentId, commentId, {
      runId, pipelineId: DOCUMENT_AGENT_EDIT_PIPELINE_ID, state: 'pending', resolveOnApply,
      requestedBy: userId, requestedByName, at: new Date().toISOString(),
    });
    publishSignal(`doc-comments:${documentId}`, 'doc:comment_updated', { documentId, comment })
      .catch((err: unknown) => log.error({ event: 'doc:comment_updated', err: String(err) }, 'signal publish failed'));
  }
  log.info({ runId, documentId, userId, ...(commentId ? { commentId } : {}) }, 'document agent edit triggered');
  res.status(202).json({ runId, pipelineId: DOCUMENT_AGENT_EDIT_PIPELINE_ID, documentId, ...(commentId ? { commentId, resolveOnApply } : {}), agent: { userId: 'agent:haiku', displayName: 'Haiku' } });
}));

// POST /api/documents/:documentId/transform-run — "Transform to <type>" as a
// pipeline run. `:documentId` is the NEW, empty, typed page the client just
// created; `sourceDocumentId` is the page being transformed. The run reads the
// source, plans the target, and writes it as a collaborator — the person
// watching the new page sees Haiku build it.
// Loop 29c: the run writes `:documentId` and reads `sourceDocumentId`, so it
// takes document:edit on the target and document:read on the source (a
// stranger's source is 404, as if it did not exist).
documentAgentEditRouter.post('/:documentId/transform-run', requireAuth, requireDocumentEdit(), requireDocumentRead(bodyId('sourceDocumentId'), { optional: true }), asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const documentId = String(req.params.documentId ?? '');
  const body = (req.body ?? {}) as { sourceDocumentId?: unknown; target?: { id?: unknown; name?: unknown; icon?: unknown; fields?: unknown }; requestedByName?: unknown };
  const sourceDocumentId = typeof body.sourceDocumentId === 'string' ? body.sourceDocumentId : '';
  const target = body.target ?? {};
  const targetName = typeof target.name === 'string' ? target.name.trim() : '';
  if (!documentId) throw new ValidationError('documentId is required');
  if (!sourceDocumentId) throw new ValidationError('sourceDocumentId is required');
  if (!targetName) throw new ValidationError('target.name is required');
  const fields = (Array.isArray(target.fields) ? target.fields : [])
    .map((f) => (f && typeof f === 'object' ? f as { name?: unknown; sectionType?: unknown } : null))
    .filter((f): f is { name?: unknown; sectionType?: unknown } => !!f && typeof f.name === 'string' && !!f.name)
    .slice(0, 40)
    .map((f) => ({ name: String(f.name), ...(typeof f.sectionType === 'string' ? { sectionType: f.sectionType } : {}) }));
  const userId = req.user!.sub;
  const bridge = getPipelineBridge();
  if (!bridge?.trigger) { res.status(503).json({ error: 'pipeline engine is not available' }); return; }
  const out = await Promise.resolve(bridge.trigger({
    pipelineId: DOCUMENT_TRANSFORM_PIPELINE_ID,
    definition: buildDocumentTransformDefinition(undefined, { approverUserId: userId }) as never,
    triggerPayload: {
      documentId, sourceDocumentId, requestedBy: userId,
      requestedByName: typeof body.requestedByName === 'string' && body.requestedByName ? body.requestedByName.slice(0, 80) : userId,
      targetName, targetId: typeof target.id === 'string' ? target.id : '', targetIcon: typeof target.icon === 'string' ? target.icon : '',
      targetFieldList: fields.length ? fields.map((f) => `- ${f.name}${f.sectionType ? ` (${f.sectionType})` : ''}`).join('\n') : '- (the type declares no fields; keep the source headings)',
      instruction: `Transform into ${targetName}`,
      requestId: randomUUID(),
    },
    triggeredBy: { userId },
  } as never));
  const runId = (out as { runId: string }).runId;
  getRunPersistenceService()?.registerTrigger({ runId, pipelineId: DOCUMENT_TRANSFORM_PIPELINE_ID, userId, triggerType: 'manual', at: new Date().toISOString() });
  log.info({ runId, documentId, sourceDocumentId, targetName, userId }, 'document transform run triggered');
  res.status(202).json({ runId, pipelineId: DOCUMENT_TRANSFORM_PIPELINE_ID, documentId, sourceDocumentId, agent: { userId: 'agent:haiku', displayName: 'Haiku' } });
}));

// POST /api/documents/:documentId/summarize-run — the chat's "/summarize"
// command. `:documentId` is the NEW, empty page the client just created; the
// run reads the conversation through the chat service, plans a page, writes it
// as a collaborator and leaves a card in the thread that opens the page.
// Loop 29c: writes `:documentId` (document:edit) from `chatChannel`'s history,
// which only a member of that conversation may read.
documentAgentEditRouter.post('/:documentId/summarize-run', requireAuth, requireDocumentEdit(), requireChannelMember(bodyId('chatChannel'), { optional: true }), asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const documentId = String(req.params.documentId ?? '');
  const body = (req.body ?? {}) as { chatChannel?: unknown; requestedByName?: unknown; limit?: unknown };
  const chatChannel = typeof body.chatChannel === 'string' ? body.chatChannel.trim() : '';
  if (!documentId) throw new ValidationError('documentId is required');
  if (!chatChannel) throw new ValidationError('chatChannel is required');
  const limit = typeof body.limit === 'number' && Number.isFinite(body.limit) && body.limit > 0 ? Math.min(Math.floor(body.limit), 1000) : DEFAULT_HISTORY_LIMIT;
  const userId = req.user!.sub;
  const requestedByName = typeof body.requestedByName === 'string' && body.requestedByName ? body.requestedByName.slice(0, 80) : userId;
  const bridge = getPipelineBridge();
  if (!bridge?.trigger) { res.status(503).json({ error: 'pipeline engine is not available' }); return; }
  const out = await Promise.resolve(bridge.trigger({
    pipelineId: CONVERSATION_SUMMARIZE_PIPELINE_ID,
    definition: buildConversationSummarizeDefinition() as never,
    triggerPayload: { documentId, chatChannel, limit, requestedBy: userId, requestedByName, instruction: `Summarise ${chatChannel}`, requestId: randomUUID() },
    triggeredBy: { userId },
  } as never));
  const runId = (out as { runId: string }).runId;
  getRunPersistenceService()?.registerTrigger({ runId, pipelineId: CONVERSATION_SUMMARIZE_PIPELINE_ID, userId, triggerType: 'manual', at: new Date().toISOString() });
  log.info({ runId, documentId, chatChannel, userId }, 'conversation summarize run triggered');
  res.status(202).json({ runId, pipelineId: CONVERSATION_SUMMARIZE_PIPELINE_ID, documentId, chatChannel, agent: { userId: 'agent:haiku', displayName: 'Haiku' } });
}));

// POST /api/documents/:documentId/diagram-run — the chat's "/diagram <what>"
// command. `:documentId` is the NEW diagram-shaped document the client just
// created; the run sketches it with a light model, draws the elements through
// the CRDT as a collaborator, and leaves a card in the thread that opens it.
// Loop 29c: the run draws into `:documentId` — document:edit.
documentAgentEditRouter.post('/:documentId/diagram-run', requireAuth, requireDocumentEdit(), asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const documentId = String(req.params.documentId ?? '');
  const body = (req.body ?? {}) as { title?: unknown; prompt?: unknown; chatChannel?: unknown; requestedByName?: unknown };
  const prompt = typeof body.prompt === 'string' ? body.prompt.trim() : '';
  const title = (typeof body.title === 'string' && body.title.trim() ? body.title.trim() : prompt).slice(0, 120);
  if (!documentId) throw new ValidationError('documentId is required');
  if (!prompt) throw new ValidationError('prompt is required');
  if (prompt.length > 2000) throw new ValidationError('prompt must be 2000 characters or fewer');
  const userId = req.user!.sub;
  const requestedByName = typeof body.requestedByName === 'string' && body.requestedByName ? body.requestedByName.slice(0, 80) : userId;
  const chatChannel = typeof body.chatChannel === 'string' && body.chatChannel.trim() ? body.chatChannel.trim() : undefined;
  const bridge = getPipelineBridge();
  if (!bridge?.trigger) { res.status(503).json({ error: 'pipeline engine is not available' }); return; }
  const out = await Promise.resolve(bridge.trigger({
    pipelineId: DIAGRAM_GENERATE_PIPELINE_ID,
    definition: buildDiagramGenerateDefinition() as never,
    triggerPayload: { documentId, title, prompt, requestedBy: userId, requestedByName, ...(chatChannel ? { chatChannel } : {}), instruction: title, requestId: randomUUID() },
    triggeredBy: { userId },
  } as never));
  const runId = (out as { runId: string }).runId;
  getRunPersistenceService()?.registerTrigger({ runId, pipelineId: DIAGRAM_GENERATE_PIPELINE_ID, userId, triggerType: 'manual', at: new Date().toISOString() });
  log.info({ runId, documentId, title, userId }, 'diagram generate run triggered');
  res.status(202).json({ runId, pipelineId: DIAGRAM_GENERATE_PIPELINE_ID, documentId, title, ...(chatChannel ? { chatChannel } : {}), agent: { userId: 'agent:haiku', displayName: 'Haiku' } });
}));
