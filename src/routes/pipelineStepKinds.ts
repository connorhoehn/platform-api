// platform-api/src/routes/pipelineStepKinds.ts
//
// GET /api/pipelines/step-kinds — every step kind this server can execute.
//
// A pipeline node carries its kind as a plain string (`transcription:transcribe`,
// `media:classify`). Those strings are registered ad-hoc at bootstrap by
// whichever module owns the handler, and until now nothing could enumerate
// them. An author had to already know the string. That is survivable for a
// person working in the editor, where the palette lists what it supports — and
// no use at all to an agent composing a definition and POSTing it, which is
// increasingly who writes these. A kind with a typo in it saved happily,
// published happily, and failed at run time with the pipeline already live.
//
// So: one route that answers "what can I actually put in `node.type`?".
//
// Read-only and cheap — the registry is an in-memory Map populated once at
// bootstrap — so there is no caching here and nothing to invalidate.

import { Router, type Request, type Response } from 'express';
import { listStepKinds } from '../pipeline/customStepHandlers';

export const pipelineStepKindsRouter: Router = Router();

pipelineStepKindsRouter.get('/', (_req: Request, res: Response): void => {
  const kinds = listStepKinds();
  res.json({
    kinds,
    // Split out so a caller does not have to filter to answer the two
    // questions it actually has: "is this kind real?" and "what else is there?".
    builtin: kinds.filter((k) => k.source === 'builtin').map((k) => k.kind),
    registered: kinds.filter((k) => k.source === 'registered').map((k) => k.kind),
  });
});
