import { Router, type NextFunction, type Request, type Response } from 'express';
import type {
  SharingAudience,
  SharingSelection,
} from '../repositories/WorkSharingRepository';
import type {
  WorkCollaborationCapability,
  WorkCollaborationTarget,
} from '../repositories/WorkCollaborationRepository';
import {
  WorkGraphAuthorizationUnavailableError,
  WorkGraphCapabilityDeniedError,
  WorkGraphConflictError,
  WorkGraphInputError,
  WorkGraphOrchestrator,
  WorkGraphUnavailableError,
  type AuthenticatedWorkGraphViewer,
  type CreateCollaborationInput,
  type CreateGrantInput,
  type UpdateGrantInput,
  type WorkReference,
} from '../work-graph/orchestrator';

type JsonRecord = Record<string, unknown>;
const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/;
const CAPABILITIES = new Set<WorkCollaborationCapability>([
  'message', 'view-document', 'edit-document', 'join-conversation', 'join-meeting',
  'observe-terminal', 'control-terminal', 'view-transcript', 'view-run', 'dispatch-run',
]);

function isRecord(value: unknown): value is JsonRecord {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function exact(value: unknown, required: readonly string[], optional: readonly string[] = []): value is JsonRecord {
  if (!isRecord(value)) return false;
  const keys = Reflect.ownKeys(value);
  const allowed = new Set([...required, ...optional]);
  return required.every((key) => Object.prototype.hasOwnProperty.call(value, key))
    && keys.every((key) => typeof key === 'string' && allowed.has(key));
}

function isId(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= 128 && ID_PATTERN.test(value);
}

function isTimestamp(value: unknown): value is string {
  if (typeof value !== 'string' || value.length > 64) return false;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) && new Date(parsed).toISOString() === value;
}

function isDay(value: unknown): value is string {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return Number.isFinite(parsed.valueOf()) && parsed.toISOString().slice(0, 10) === value;
}

function isTimezone(value: unknown): value is string {
  if (typeof value !== 'string' || value.length === 0 || value.length > 64) return false;
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: value }).format();
    return true;
  } catch {
    return false;
  }
}

function uniqueIds(value: unknown, maximum: number, nonEmpty = false): value is string[] {
  return Array.isArray(value)
    && (!nonEmpty || value.length > 0)
    && value.length <= maximum
    && value.every(isId)
    && new Set(value).size === value.length;
}

function audience(value: unknown): SharingAudience | null {
  if (!isRecord(value)) return null;
  if (value.kind === 'organization' && exact(value, ['kind'])) return { kind: 'organization' };
  if (value.kind === 'people'
    && exact(value, ['kind', 'personIds'])
    && uniqueIds(value.personIds, 100, true)) return { kind: 'people', personIds: [...value.personIds] };
  if (value.kind === 'conversation'
    && exact(value, ['kind', 'conversationIds'])
    && uniqueIds(value.conversationIds, 100, true)) {
    return { kind: 'conversation', conversationIds: [...value.conversationIds] };
  }
  return null;
}

function selection(value: unknown): SharingSelection | null {
  if (!exact(value, ['nodeIds', 'edgeIds', 'disclosure'])
    || !uniqueIds(value.nodeIds, 250)
    || !uniqueIds(value.edgeIds, 500)
    || value.nodeIds.length + value.edgeIds.length === 0
    || (value.disclosure !== 'existence' && value.disclosure !== 'summary' && value.disclosure !== 'details')) return null;
  return { nodeIds: [...value.nodeIds], edgeIds: [...value.edgeIds], disclosure: value.disclosure };
}

function target(value: unknown): WorkCollaborationTarget | null {
  if (!exact(value, ['kind', 'id'])
    || (value.kind !== 'node' && value.kind !== 'edge')
    || !isId(value.id)) return null;
  return { kind: value.kind, id: value.id };
}

function reference(value: unknown): WorkReference | null {
  if (!exact(value, ['version', 'personId', 'day', 'timezone', 'target'], ['sessionId', 'eventId', 'observedAt'])
    || value.version !== 1
    || !isId(value.personId)
    || !isDay(value.day)
    || !isTimezone(value.timezone)
    || (value.sessionId !== undefined && !isId(value.sessionId))
    || (value.eventId !== undefined && !isId(value.eventId))
    || (value.observedAt !== undefined && !isTimestamp(value.observedAt))) return null;
  const parsedTarget = target(value.target);
  if (!parsedTarget) return null;
  const parsed: WorkReference = {
    version: 1,
    personId: value.personId,
    day: value.day,
    timezone: value.timezone,
    target: parsedTarget,
  };
  if (typeof value.sessionId === 'string') parsed.sessionId = value.sessionId;
  if (typeof value.eventId === 'string') parsed.eventId = value.eventId;
  if (typeof value.observedAt === 'string') parsed.observedAt = value.observedAt;
  return parsed;
}

function parseGraphQuery(req: Request): { personId: string; day: string; timezone: string; cursor?: string } {
  const keys = Object.keys(req.query);
  if (keys.some((key) => key !== 'personId' && key !== 'day' && key !== 'timezone' && key !== 'cursor')) {
    throw new WorkGraphInputError('Unknown graph query parameter.');
  }
  const personId = String(req.params.personId ?? req.query['personId'] ?? '');
  const day = req.query['day'];
  const timezone = req.query['timezone'];
  const cursor = req.query['cursor'];
  if (!isId(personId) || !isDay(day) || !isTimezone(timezone)
    || (cursor !== undefined && (typeof cursor !== 'string' || cursor.length > 8_192))) {
    throw new WorkGraphInputError('A valid person, day, and timezone are required.');
  }
  return { personId, day, timezone, ...(typeof cursor === 'string' ? { cursor } : {}) };
}

function parseCreateGrant(value: unknown): CreateGrantInput {
  if (!exact(value, ['audience', 'selection', 'expiresAt'], ['history'])) {
    throw new WorkGraphInputError('Sharing grant body is invalid.');
  }
  const parsedAudience = audience(value.audience);
  const parsedSelection = selection(value.selection);
  if (!parsedAudience || !parsedSelection || !isTimestamp(value.expiresAt)) {
    throw new WorkGraphInputError('Sharing grant body is invalid.');
  }
  let history: CreateGrantInput['history'];
  if (value.history !== undefined) {
    if (!exact(value.history, ['dayFrom', 'dayThrough', 'expiresAt'])
      || !isDay(value.history.dayFrom)
      || !isDay(value.history.dayThrough)
      || !isTimestamp(value.history.expiresAt)) throw new WorkGraphInputError('History grant body is invalid.');
    history = {
      dayFrom: value.history.dayFrom,
      dayThrough: value.history.dayThrough,
      expiresAt: value.history.expiresAt,
    };
  }
  return { audience: parsedAudience, selection: parsedSelection, expiresAt: value.expiresAt, ...(history ? { history } : {}) };
}

function parseUpdateGrant(value: unknown): UpdateGrantInput {
  if (!exact(value, ['expectedRevision'], ['action', 'audience', 'selection', 'expiresAt'])
    || !Number.isSafeInteger(value.expectedRevision)
    || (value.expectedRevision as number) < 1
    || (value.action !== undefined && value.action !== 'pause' && value.action !== 'resume' && value.action !== 'stop')
    || (value.expiresAt !== undefined && !isTimestamp(value.expiresAt))) {
    throw new WorkGraphInputError('Sharing grant update is invalid.');
  }
  const parsedAudience = value.audience === undefined ? undefined : audience(value.audience);
  const parsedSelection = value.selection === undefined ? undefined : selection(value.selection);
  if (parsedAudience === null || parsedSelection === null) throw new WorkGraphInputError('Sharing grant update is invalid.');
  return {
    expectedRevision: value.expectedRevision as number,
    ...(typeof value.action === 'string' ? { action: value.action as UpdateGrantInput['action'] } : {}),
    ...(parsedAudience ? { audience: parsedAudience } : {}),
    ...(parsedSelection ? { selection: parsedSelection } : {}),
    ...(typeof value.expiresAt === 'string' ? { expiresAt: value.expiresAt } : {}),
  };
}

function parseCreateCollaboration(value: unknown): CreateCollaborationInput {
  if (!exact(value, ['target', 'capability', 'expiresAt'])
    || !CAPABILITIES.has(value.capability as WorkCollaborationCapability)
    || !isTimestamp(value.expiresAt)) throw new WorkGraphInputError('Collaboration request body is invalid.');
  const parsedTarget = target(value.target);
  if (!parsedTarget) throw new WorkGraphInputError('Collaboration request body is invalid.');
  return { target: parsedTarget, capability: value.capability as WorkCollaborationCapability, expiresAt: value.expiresAt };
}

function parseAction(value: unknown): { target: WorkCollaborationTarget; capability: WorkCollaborationCapability } {
  if (!exact(value, ['target', 'capability'])
    || !CAPABILITIES.has(value.capability as WorkCollaborationCapability)) {
    throw new WorkGraphInputError('Work action body is invalid.');
  }
  const parsedTarget = target(value.target);
  if (!parsedTarget) throw new WorkGraphInputError('Work action body is invalid.');
  return { target: parsedTarget, capability: value.capability as WorkCollaborationCapability };
}

function parseUpdateCollaboration(value: unknown): { expectedRevision: number; action: 'accept' | 'decline' | 'revoke' } {
  if (!exact(value, ['expectedRevision', 'action'])
    || !Number.isSafeInteger(value.expectedRevision)
    || (value.expectedRevision as number) < 1
    || (value.action !== 'accept' && value.action !== 'decline' && value.action !== 'revoke')) {
    throw new WorkGraphInputError('Collaboration request update is invalid.');
  }
  return { expectedRevision: value.expectedRevision as number, action: value.action };
}

function sendError(error: unknown, res: Response): boolean {
  if (error instanceof WorkGraphUnavailableError) {
    res.status(404).json({ error: 'WORK_GRAPH_UNAVAILABLE', message: 'This work item is unavailable.' });
    return true;
  }
  if (error instanceof WorkGraphAuthorizationUnavailableError) {
    res.status(503).json({
      error: 'WORK_GRAPH_AUTHORIZATION_UNAVAILABLE',
      message: 'Work graph authorization is temporarily unavailable.',
    });
    return true;
  }
  if (error instanceof WorkGraphCapabilityDeniedError) {
    res.status(403).json({ error: 'WORK_GRAPH_CAPABILITY_DENIED', message: error.message });
    return true;
  }
  if (error instanceof WorkGraphConflictError
    || (error as { code?: string }).code === 'WORK_SHARING_REVISION_CONFLICT'
    || (error as { code?: string }).code === 'WORK_COLLABORATION_REVISION_CONFLICT') {
    res.status(409).json({ error: 'WORK_GRAPH_REVISION_CONFLICT', message: 'The expected revision is stale.' });
    return true;
  }
  if (error instanceof WorkGraphInputError
    || (error as { code?: string }).code === 'WORK_SHARING_INVALID_GRANT'
    || (error as { code?: string }).code === 'WORK_COLLABORATION_INVALID_REQUEST') {
    res.status(400).json({ error: 'WORK_GRAPH_INVALID_REQUEST', message: (error as Error).message });
    return true;
  }
  return false;
}

export function createWorkGraphRouter(orchestrator: WorkGraphOrchestrator): Router {
  const router = Router();

  const handler = (
    fn: (req: Request, res: Response, viewer: AuthenticatedWorkGraphViewer) => Promise<void>,
  ) => async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      if (!req.user?.sub || !req.user.orgId) {
        res.status(401).json({ error: 'Authorization required' });
        return;
      }
      const viewer = await orchestrator.resolveViewer(req.user.sub, req.user.orgId);
      if (!viewer || viewer.organizationId !== req.user.orgId) {
        res.status(401).json({ error: 'Authorization required' });
        return;
      }
      await fn(req, res, viewer);
    } catch (error) {
      if (!sendError(error, res)) next(error);
    }
  };

  router.get('/people', handler(async (_req, res, viewer) => {
    res.status(200).json(await orchestrator.listPeople(viewer));
  }));

  router.get('/people/:personId/graph', handler(async (req, res, viewer) => {
    const query = parseGraphQuery(req);
    const result = await orchestrator.getGraph(viewer, query.personId, query.day, query.timezone, query.cursor);
    res.status(200).json(result.snapshot);
  }));

  router.get('/nodes/:id', handler(async (req, res, viewer) => {
    if (!isId(req.params.id)) throw new WorkGraphInputError('Node id is invalid.');
    const query = parseGraphQuery(req);
    res.status(200).json(await orchestrator.getNode(viewer, query.personId, query.day, query.timezone, req.params.id));
  }));

  router.get('/edges/:id', handler(async (req, res, viewer) => {
    if (!isId(req.params.id)) throw new WorkGraphInputError('Edge id is invalid.');
    const query = parseGraphQuery(req);
    res.status(200).json(await orchestrator.getEdge(viewer, query.personId, query.day, query.timezone, req.params.id));
  }));

  router.post('/sharing-grants', handler(async (req, res, viewer) => {
    res.status(201).json(await orchestrator.createGrant(viewer, parseCreateGrant(req.body)));
  }));

  router.patch('/sharing-grants/:id', handler(async (req, res, viewer) => {
    if (!isId(req.params.id)) throw new WorkGraphInputError('Sharing grant id is invalid.');
    res.status(200).json(await orchestrator.updateGrant(viewer, req.params.id, parseUpdateGrant(req.body)));
  }));

  router.post('/references/resolve', handler(async (req, res, viewer) => {
    if (!exact(req.body, ['references'])
      || !Array.isArray(req.body.references)
      || req.body.references.length > 100) throw new WorkGraphInputError('Reference resolution body is invalid.');
    const references = req.body.references.map(reference);
    if (references.some((item) => item === null)) throw new WorkGraphInputError('Reference resolution body is invalid.');
    res.status(200).json(await orchestrator.resolveReferences(viewer, references as WorkReference[]));
  }));

  router.post('/collaboration-requests', handler(async (req, res, viewer) => {
    res.status(201).json(await orchestrator.createCollaborationRequest(viewer, parseCreateCollaboration(req.body)));
  }));

  router.post('/actions', handler(async (req, res, viewer) => {
    res.status(200).json(await orchestrator.exchangeAction(viewer, parseAction(req.body)));
  }));

  router.patch('/collaboration-requests/:id', handler(async (req, res, viewer) => {
    if (!isId(req.params.id)) throw new WorkGraphInputError('Collaboration request id is invalid.');
    res.status(200).json(await orchestrator.updateCollaborationRequest(viewer, req.params.id, parseUpdateCollaboration(req.body)));
  }));

  return router;
}
