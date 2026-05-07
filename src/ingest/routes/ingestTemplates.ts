// social-api/src/ingest/routes/ingestTemplates.ts
//
// REST endpoints for ingest pipeline templates — pre-built knowledge workflow
// definitions that can be browsed and instantiated into real pipelines.
//
// Endpoints (mounted at /api/ingest/templates in routes/index.ts):
//   GET    /                  — list all templates (metadata only, no full definition)
//   GET    /:id               — get full template including definition
//   POST   /:id/instantiate   — create a pipeline from template (copies definition into pipelines table)

import { Router, type Request, type Response } from 'express';
import { randomUUID } from 'crypto';
import { asyncHandler, NotFoundError } from '../../middleware/error-handler';
import { withContext } from '../../lib/logger';
import { ingestPipelineTemplates } from '../templates';
import { definitionsRepo } from '../../pipeline/definitions-repository';

const log = withContext({ route: 'ingestTemplates' });

export const ingestTemplatesRouter = Router();

// List all templates (metadata without full definition)
ingestTemplatesRouter.get(
  '/',
  asyncHandler(async (_req: Request, res: Response): Promise<void> => {
    const summaries = ingestPipelineTemplates.map((t) => ({
      id: t.id,
      name: t.name,
      description: t.description,
      category: t.category,
    }));
    res.json({ templates: summaries });
  }),
);

// Get full template including definition
ingestTemplatesRouter.get(
  '/:id',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const template = ingestPipelineTemplates.find((t) => t.id === req.params.id);
    if (!template) {
      throw new NotFoundError(`template ${req.params.id} not found`);
    }
    res.json(template);
  }),
);

// Instantiate — create a pipeline from template
ingestTemplatesRouter.post(
  '/:id/instantiate',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const template = ingestPipelineTemplates.find((t) => t.id === req.params.id);
    if (!template) {
      throw new NotFoundError(`template ${req.params.id} not found`);
    }

    const userId = req.user?.sub ?? 'anonymous';
    const body = (req.body ?? {}) as Record<string, unknown>;
    const pipelineName = typeof body.name === 'string' && body.name
      ? body.name
      : template.definition.name;

    const now = new Date().toISOString();
    const pipelineId = randomUUID();

    // Deep-clone the template definition and assign fresh IDs so each
    // instantiation is independent.
    const definition = {
      ...JSON.parse(JSON.stringify(template.definition)),
      id: pipelineId,
      name: pipelineName,
      status: 'draft',
      publishedVersion: undefined,
      version: 1,
      createdAt: now,
      updatedAt: now,
      createdBy: userId,
    };

    await definitionsRepo.put(userId, definition);

    log.info(
      { templateId: template.id, pipelineId, userId, action: 'template.instantiate' },
      'pipeline instantiated from template',
    );

    res.status(201).json({
      pipelineId,
      name: pipelineName,
      templateId: template.id,
      definition,
    });
  }),
);
