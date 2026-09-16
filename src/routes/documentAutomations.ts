// Per-document automations ("doc bots"). See documents/documentAutomations.ts
// for what one is. Mounted under /api/documents.
import { Router, Request, Response } from 'express';
import { randomUUID } from 'crypto';
import { requireAuth } from '../middleware/auth';
import { asyncHandler } from '../middleware/error-handler';
import { definitionsRepo } from '../pipeline/definitions-repository';
import { pipelineDefinitionsCache } from '../pipeline/definitions-cache';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { getRunPersistenceService } from '../pipeline/runPersistence';
import { automationOf, buildAutomationDefinition, validateAutomationInput, withNextRun } from '../documents/documentAutomations';
import log from '../lib/logger';

export const documentAutomationsRouter = Router();
documentAutomationsRouter.use(requireAuth);

async function listFor(userId: string, documentId: string) {
  const defs = await definitionsRepo.list(userId);
  return defs
    .map((d) => automationOf(d as Record<string, unknown>))
    .filter((a): a is NonNullable<typeof a> => a !== null && a.documentId === documentId)
    .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
    .map((a) => withNextRun(a));
}

documentAutomationsRouter.get('/:documentId/automations', asyncHandler(async (req: Request, res: Response) => {
  const userId = req.user!.sub;
  res.json({ automations: await listFor(userId, req.params.documentId) });
}));

documentAutomationsRouter.post('/:documentId/automations', asyncHandler(async (req: Request, res: Response) => {
  const userId = req.user!.sub;
  const documentId = req.params.documentId;
  const checked = validateAutomationInput(req.body);
  if ('error' in checked) { res.status(400).json({ error: checked.error }); return; }
  const body = (req.body ?? {}) as { requestedByName?: unknown };
  const userName = typeof body.requestedByName === 'string' && body.requestedByName ? body.requestedByName.slice(0, 80) : userId;
  const def = buildAutomationDefinition({ documentId, instruction: checked.instruction, cron: checked.cron, userId, userName });
  await definitionsRepo.put(userId, def as { id: string; [k: string]: unknown });
  // The evaluator reads the definitions cache; refresh so the first fire is
  // not up to a minute late.
  await pipelineDefinitionsCache.refresh().catch((err: unknown) => log.warn({ err: String(err) }, 'automation: cache refresh failed'));
  const automation = withNextRun(automationOf(def)!);
  log.info({ userId, documentId, automationId: automation.id, cron: automation.cron }, 'document automation created');
  res.status(201).json({ automation });
}));

documentAutomationsRouter.delete('/:documentId/automations/:automationId', asyncHandler(async (req: Request, res: Response) => {
  const userId = req.user!.sub;
  const { documentId, automationId } = req.params;
  const def = await definitionsRepo.get(userId, automationId);
  const a = automationOf(def as Record<string, unknown> | null);
  if (!a || a.documentId !== documentId) { res.status(404).json({ error: 'automation not found' }); return; }
  await definitionsRepo.delete(userId, automationId);
  await pipelineDefinitionsCache.refresh().catch(() => undefined);
  log.info({ userId, documentId, automationId }, 'document automation removed');
  res.status(204).end();
}));

/** Fire it now — the same run the schedule would start, so a person can see what the bot does before waiting for it. */
documentAutomationsRouter.post('/:documentId/automations/:automationId/run', asyncHandler(async (req: Request, res: Response) => {
  const userId = req.user!.sub;
  const { documentId, automationId } = req.params;
  const def = await definitionsRepo.get(userId, automationId);
  const a = automationOf(def as Record<string, unknown> | null);
  if (!def || !a || a.documentId !== documentId) { res.status(404).json({ error: 'automation not found' }); return; }
  const bridge = getPipelineBridge();
  if (!bridge?.trigger) { res.status(503).json({ error: 'pipeline engine is not available' }); return; }
  const payload = ((def as { triggerBinding?: { payload?: Record<string, unknown> } }).triggerBinding?.payload) ?? {};
  const out = await Promise.resolve(bridge.trigger({
    pipelineId: automationId,
    definition: def as never,
    triggerPayload: { ...payload, triggerType: 'manual', requestId: randomUUID() },
    triggeredBy: { userId },
  } as never));
  const runId = (out as { runId: string }).runId;
  getRunPersistenceService()?.registerTrigger({ runId, pipelineId: automationId, userId, triggerType: 'manual', at: new Date().toISOString() });
  res.status(202).json({ runId, pipelineId: automationId, documentId });
}));
