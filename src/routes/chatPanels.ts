// /api/chat/panels — what a chat card's `panel` result opens.
//
//   GET /api/chat/panels/:kind/:id   → the run's final `panelData`, 404 otherwise
//
// A run that answers with `result: { kind: 'panel', panel: { kind, id } }`
// (src/pipeline/runResult.ts) also leaves `panelData` in its final context;
// this route hands that back. It knows nothing about any particular kind —
// the id names the run and the run says which kind it is.
//
// Id scheme: `<pipelineId>:<runId>` (split at the first colon). The live
// bridge finds a run by runId alone, but only while the process that ran it
// is still up; the durable row is keyed by (pipelineId, runId), so a panel
// that should still open tomorrow has to carry both. An id with no colon is
// tried against the live bridge only.
import { Router, type Request, type Response } from 'express';
import { asyncHandler, NotFoundError } from '../middleware/error-handler';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { pipelineRunRepo } from '../repositories';
import { runResultFromContext } from '../pipeline/runResult';

export interface ChatPanelsDeps {
  /** The run as the executor holds it, with its `context`; null when this process never ran it. */
  getLiveRun: (runId: string) => Promise<{ context?: unknown } | null> | { context?: unknown } | null;
  /** The persisted run's last checkpoint context; null when there is no row. */
  getStoredRunContext: (pipelineId: string, runId: string) => Promise<unknown | null>;
}

/** `<pipelineId>:<runId>` → both; a bare id → runId only. */
export function parsePanelId(id: string): { pipelineId?: string; runId: string } {
  const at = id.indexOf(':');
  if (at <= 0 || at === id.length - 1) return { runId: id };
  return { pipelineId: id.slice(0, at), runId: id.slice(at + 1) };
}

const defaultDeps: ChatPanelsDeps = {
  getLiveRun: (runId) => {
    const bridge = getPipelineBridge();
    if (!bridge?.getRun) return null;
    return bridge.getRun(runId) as { context?: unknown } | null;
  },
  getStoredRunContext: async (pipelineId, runId) => {
    const item = await pipelineRunRepo.get(pipelineId, runId);
    return item?.checkpoint?.context ?? null;
  },
};

export function createChatPanelsRouter(deps: ChatPanelsDeps = defaultDeps): Router {
  const router = Router();
  router.get('/:kind/:id', asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const kind = String(req.params.kind ?? '');
    const { pipelineId, runId } = parsePanelId(String(req.params.id ?? ''));
    if (!kind || !runId) throw new NotFoundError('panel not found');

    let context: unknown = null;
    try {
      const live = await Promise.resolve(deps.getLiveRun(runId));
      context = live?.context ?? null;
    } catch { context = null; }
    if (!context && pipelineId) {
      try { context = await deps.getStoredRunContext(pipelineId, runId); } catch { context = null; }
    }
    if (!context || typeof context !== 'object') throw new NotFoundError('panel not found');

    // The run has to have answered with THIS panel, and left something to show in it.
    const result = runResultFromContext(context);
    if (!result || result.kind !== 'panel' || result.panel.kind !== kind) throw new NotFoundError('panel not found');
    const panelData = (context as Record<string, unknown>).panelData;
    if (!panelData || typeof panelData !== 'object') throw new NotFoundError('panel not found');

    res.json(panelData);
  }));
  return router;
}

export const chatPanelsRouter = createChatPanelsRouter();
