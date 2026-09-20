import { Router, type NextFunction, type Request, type Response } from 'express';
import {
  closeComputeCapabilitySession,
  loadComputeCapabilitySession,
  revalidateComputeCapabilitySession,
  useComputeCapabilitySession,
} from '../work-graph/runtime';

const ID = /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/;

function exact(value: unknown, keys: readonly string[]): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value)
    && Reflect.ownKeys(value).length === keys.length
    && Reflect.ownKeys(value).every((key) => typeof key === 'string' && keys.includes(key));
}

export const workGraphComputeSessionsRouter = Router();

const handler = (
  fn: (req: Request, res: Response) => Promise<void>,
) => async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    if (!req.user?.sub || !req.user.orgId) {
      res.status(401).json({ error: 'Authorization required' });
      return;
    }
    if (![req.params.personId, req.params.requestKey, req.params.sessionId].every((value) => typeof value === 'string' && ID.test(value))) {
      res.status(400).json({ error: 'WORK_GRAPH_INVALID_REQUEST' });
      return;
    }
    await fn(req, res);
  } catch (error) {
    next(error);
  }
};

async function session(req: Request) {
  return loadComputeCapabilitySession(
    req.user!.orgId!,
    req.user!.sub!,
    req.params.personId!,
    req.params.requestKey!,
    req.params.sessionId!,
  );
}

workGraphComputeSessionsRouter.get('/:personId/:requestKey/:sessionId', handler(async (req, res) => {
  const current = await session(req);
  if (!current || !await revalidateComputeCapabilitySession(current)) {
    res.status(404).json({ error: 'WORK_GRAPH_UNAVAILABLE', message: 'This work item is unavailable.' });
    return;
  }
  res.status(200).json({
    sessionId: current.sessionId,
    capability: current.capability,
    expiresAt: current.expiresAt,
    active: true,
  });
}));

workGraphComputeSessionsRouter.post('/:personId/:requestKey/:sessionId/use', handler(async (req, res) => {
  if (!exact(req.body, ['capability'])
    || (req.body.capability !== 'observe-terminal' && req.body.capability !== 'control-terminal')) {
    res.status(400).json({ error: 'WORK_GRAPH_INVALID_REQUEST' });
    return;
  }
  const current = await session(req);
  if (!current || current.capability !== req.body.capability) {
    res.status(404).json({ error: 'WORK_GRAPH_UNAVAILABLE', message: 'This work item is unavailable.' });
    return;
  }
  const used = await useComputeCapabilitySession(current);
  if (!used.allowed) {
    res.status(404).json({ error: 'WORK_GRAPH_UNAVAILABLE', message: 'This work item is unavailable.' });
    return;
  }
  res.status(200).json({ capability: current.capability, result: used.result ?? {} });
}));

workGraphComputeSessionsRouter.delete('/:personId/:requestKey/:sessionId', handler(async (req, res) => {
  const current = await session(req);
  if (current) await closeComputeCapabilitySession(current);
  res.status(204).end();
}));
