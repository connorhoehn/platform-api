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
  const definition = buildDocumentAgentEditDefinition();
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
    definition: buildDocumentTransformDefinition() as never,
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
