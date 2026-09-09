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
import { buildDocumentAgentEditDefinition, DOCUMENT_AGENT_EDIT_PIPELINE_ID } from '../documents/documentAgentPipeline';
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
