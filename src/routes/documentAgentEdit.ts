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

export const documentAgentEditRouter = Router();

documentAgentEditRouter.post('/:documentId/agent-edit', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const documentId = String(req.params.documentId ?? '');
  const body = (req.body ?? {}) as { instruction?: unknown; chatChannel?: unknown; requestedByName?: unknown };
  const instruction = typeof body.instruction === 'string' ? body.instruction.trim() : '';
  if (!documentId) throw new ValidationError('documentId is required');
  if (!instruction) throw new ValidationError('instruction is required');
  if (instruction.length > 2000) throw new ValidationError('instruction must be 2000 characters or fewer');
  const userId = req.user!.sub;
  const requestedByName = typeof body.requestedByName === 'string' && body.requestedByName ? body.requestedByName.slice(0, 80) : userId;
  const chatChannel = typeof body.chatChannel === 'string' ? body.chatChannel : undefined;

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
    triggerPayload: { documentId, instruction, requestedBy: userId, requestedByName, ...(chatChannel ? { chatChannel } : {}), requestId: randomUUID() },
    triggeredBy: { userId },
  } as never));
  const runId = (out as { runId: string }).runId;
  getRunPersistenceService()?.registerTrigger({ runId, pipelineId: DOCUMENT_AGENT_EDIT_PIPELINE_ID, userId, triggerType: 'manual', at: new Date().toISOString() });
  log.info({ runId, documentId, userId }, 'document agent edit triggered');
  res.status(202).json({ runId, pipelineId: DOCUMENT_AGENT_EDIT_PIPELINE_ID, documentId, agent: { userId: 'agent:haiku', displayName: 'Haiku' } });
}));

// POST /api/documents/:documentId/transform-run — "Transform to <type>" as a
// pipeline run. `:documentId` is the NEW, empty, typed page the client just
// created; `sourceDocumentId` is the page being transformed. The run reads the
// source, plans the target, and writes it as a collaborator — the person
// watching the new page sees Haiku build it.
documentAgentEditRouter.post('/:documentId/transform-run', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
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
documentAgentEditRouter.post('/:documentId/summarize-run', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
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
documentAgentEditRouter.post('/:documentId/diagram-run', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
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
