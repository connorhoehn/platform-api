// social-api/src/ingest/routes/ingestProposals.ts
//
// Operator promotion queue for auto-discovered DocType proposals.
// AGENTIC_ETL_VISION §B Tier 3 — when the LLM mapper sees N similar files
// of an unknown shape, it proposes a new DocType. Operator promotes /
// rejects via this surface.
//
// Endpoints (mounted at /api/ingest/mapping/proposals):
//   GET    /                  → { proposals: DocTypeProposalEntry[] }
//   POST   /:id/promote       → { proposal: DocTypeProposalEntry }
//   POST   /:id/reject        → { proposal: DocTypeProposalEntry }
//
// PATCH and the cross-cutting "creates the actual DocType in
// document-types store on promote" wiring are deferred to a follow-up;
// this route just records the operator's decision against the proposal
// row. The companion follow-up creates the canonical DocType.

import { Router, type Request, type Response } from 'express';
import { asyncHandler, NotFoundError } from '../../middleware/error-handler';
import { withContext } from '../../lib/logger';
import { docTypeProposalRepo } from '../repositories';

const log = withContext({ route: 'ingestProposals' });

export const ingestProposalsRouter = Router();

ingestProposalsRouter.get(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const sourceId = typeof req.query.sourceId === 'string' ? req.query.sourceId : null;
    const proposals = sourceId
      ? await docTypeProposalRepo.listPendingBySource(sourceId)
      : await docTypeProposalRepo.listAllPending();
    res.json({ proposals });
  }),
);

ingestProposalsRouter.post(
  '/:id/promote',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const id = req.params.id;
    const existing = await docTypeProposalRepo.getById(id);
    if (!existing) {
      throw new NotFoundError(`proposal ${id} not found`);
    }
    const reviewer =
      typeof (req.body as { reviewedBy?: unknown })?.reviewedBy === 'string'
        ? (req.body as { reviewedBy: string }).reviewedBy
        : 'operator';
    await docTypeProposalRepo.resolve(id, 'promoted', reviewer);
    const updated = await docTypeProposalRepo.getById(id);
    log.info({ id, reviewer, action: 'proposal.promote' }, 'proposal promoted');
    res.json({ proposal: updated });
  }),
);

ingestProposalsRouter.post(
  '/:id/reject',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const id = req.params.id;
    const existing = await docTypeProposalRepo.getById(id);
    if (!existing) {
      throw new NotFoundError(`proposal ${id} not found`);
    }
    const reviewer =
      typeof (req.body as { reviewedBy?: unknown })?.reviewedBy === 'string'
        ? (req.body as { reviewedBy: string }).reviewedBy
        : 'operator';
    await docTypeProposalRepo.resolve(id, 'rejected', reviewer);
    const updated = await docTypeProposalRepo.getById(id);
    log.info({ id, reviewer, action: 'proposal.reject' }, 'proposal rejected');
    res.json({ proposal: updated });
  }),
);
