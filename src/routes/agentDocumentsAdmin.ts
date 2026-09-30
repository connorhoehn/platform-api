// POST /api/admin/agent-documents/repair — the backfill for /agent runs whose
// document never reached its conversation (loop/runDocumentRepair.ts).
//
// Walks every chat-agent-loop row, and for each finished run with a document
// step: attaches the page to the run's conversation (channel + the members as
// editors), creates the page a devbox-era step never made, and turns the run
// card into the document card. Idempotent — a second pass reports `already`.
//
//   curl -XPOST -H 'Authorization: Bearer <ADMIN_TOKEN>' \
//     'http://localhost:13001/api/admin/agent-documents/repair?apply=1[&runId=<loop id>]'
//
// Without `apply=1` it is a dry run. Behind the admin secret, like the other
// operator routes (middleware/requireAdmin.ts).

import { Router, type Request, type Response } from 'express';
import { requireAdmin } from '../middleware/requireAdmin';
import { pipelineRunRepo } from '../repositories';
import { AGENT_LOOP_PIPELINE_ID, repairAllRunDocuments, repairRunDocuments } from '../loop/runDocumentRepair';

export const agentDocumentsAdminRouter = Router();

agentDocumentsAdminRouter.post('/agent-documents/repair', requireAdmin, (req: Request, res: Response) => {
  const apply = req.query.apply === '1' || req.query.apply === 'true';
  const runId = typeof req.query.runId === 'string' ? req.query.runId : '';
  const deps = { getRun: (p: string, r: string) => pipelineRunRepo.get(p, r) };
  const work = runId
    ? repairRunDocuments(AGENT_LOOP_PIPELINE_ID, runId, deps, { apply }).then((one) => ({ apply, scanned: 1, repaired: one.skipped ? [] : [one], skipped: one.skipped ? [one] : [], failed: [] }))
    : repairAllRunDocuments(deps, { apply }).then((all) => ({ apply, ...all }));
  work.then((body) => res.status(200).json(body)).catch((err: unknown) => {
    res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
  });
});
