import { runRowCacheCounters } from '../work-graph/sources/runRowCache';
import { countDynamoCalls } from '../lib/aws-clients';
import { createHash } from 'crypto';
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
  type WorkGraphWindowInput,
  type WorkReference,
} from '../work-graph/orchestrator';
import { publishViewBase, type ViewBaseStore } from '../work-graph/viewBase';
import { withFullReads } from '../work-graph/buildMemo';
import { getRedisCacheClient } from '../lib/redis-client';
import {
  perUserRateLimit, WORK_GRAPH_PREVIEW_PER_MINUTE, WORK_GRAPH_READ_PER_MINUTE,
} from '../middleware/rateLimit';

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

const V2_QUERY_KEYS = ['schemaVersion', 'windowStart', 'windowEnd', 'mode', 'viewBase'] as const;

/**
 * Reads the additive schemaVersion 2 parameters. A request that omits them is
 * parsed and answered exactly as before, so the v1 route is unchanged.
 */
function parseActivityQuery(req: Request): WorkGraphWindowInput | null {
  const version = req.query['schemaVersion'];
  if (version === undefined) {
    if (V2_QUERY_KEYS.some((key) => req.query[key] !== undefined)) {
      throw new WorkGraphInputError('Window parameters require schemaVersion=2.');
    }
    return null;
  }
  if (version !== '2') throw new WorkGraphInputError('Unsupported work graph schema version.');
  const read = (key: 'windowStart' | 'windowEnd'): string | undefined => {
    const value = req.query[key];
    if (value === undefined) return undefined;
    if (!isTimestamp(value)) throw new WorkGraphInputError('Window bounds must be exact ISO timestamps.');
    return value;
  };
  const mode = req.query['mode'];
  if (mode !== undefined && mode !== 'live' && mode !== 'as-of') {
    throw new WorkGraphInputError('Window mode must be live or as-of.');
  }
  const viewBase = req.query['viewBase'];
  if (viewBase !== undefined && viewBase !== '1') throw new WorkGraphInputError('viewBase must be 1.');
  const windowStart = read('windowStart');
  const windowEnd = read('windowEnd');
  return {
    ...(windowStart ? { windowStart } : {}),
    ...(windowEnd ? { windowEnd } : {}),
    ...(mode ? { mode } : {}),
  };
}

/** At most this many handles in one preview batch: a deck grid's first screen. */
export const PREVIEW_BATCH_LIMIT = 60;
/**
 * Per-viewer browser caching only. `immutable` because a handle names bytes
 * that never change; a day, not a year, so a revoked viewer's own cache ages
 * out too (the server refuses them at once regardless).
 */
export const PREVIEW_CACHE_CONTROL = 'private, max-age=86400, immutable';

/** `authorize;dur=12, activity;dur=40, …` — phase costs a browser's network panel shows. */
function serverTiming(timing: Record<string, number>): string {
  return Object.entries(timing).map(([phase, ms]) => `${phase};dur=${ms}`).join(', ');
}

/** The strong validator for a preview: SHA-256 of its bytes, base64url. */
export function previewContentHash(body: Buffer): string {
  return createHash('sha256').update(body).digest('base64url');
}

function parseGraphQuery(req: Request): { personId: string; day: string; timezone: string; cursor?: string } {
  const allowed = new Set(['personId', 'day', 'timezone', 'cursor', ...V2_QUERY_KEYS]);
  const keys = Object.keys(req.query);
  if (keys.some((key) => !allowed.has(key))) {
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

/** The shared cache Redis every platform and gateway replica reads (NFR #85, #88). */
async function redisViewBaseStore(): Promise<ViewBaseStore | null> {
  const client = await getRedisCacheClient().catch(() => null);
  return client ? { set: (key, value, ttlSeconds) => client.set(key, value, { EX: ttlSeconds }) } : null;
}

export function createWorkGraphRouter(
  orchestrator: WorkGraphOrchestrator,
  options: {
    viewBaseStore?: () => Promise<ViewBaseStore | null>;
    /** Per-user limits (NFR #9); tests that fire many requests pass false. */
    rateLimit?: boolean;
  } = {},
): Router {
  const router = Router();
  const viewBaseStore = options.viewBaseStore ?? redisViewBaseStore;

  if (options.rateLimit !== false) {
    // Previews are one request per image, so they get their own, larger
    // bucket; everything else here is a read of the graph. The sibling
    // compute-sessions router is mounted under this prefix and is not charged.
    const readLimit = perUserRateLimit('work-graph:read', WORK_GRAPH_READ_PER_MINUTE);
    const previewLimit = perUserRateLimit('work-graph:preview', WORK_GRAPH_PREVIEW_PER_MINUTE);
    router.use((req, res, next) => {
      if (req.path.startsWith('/compute-sessions')) { next(); return; }
      const limit = req.path.startsWith('/previews/') ? previewLimit : readLimit;
      void limit(req, res, next);
    });
  }

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
    const activity = parseActivityQuery(req);
    if (activity) {
      const started = Date.now();
      const phases: Record<string, number> = {};
      const rowCacheBefore = { ...runRowCacheCounters };
      // `x-graph-full-read: 1` builds from whole rows (NFR #157's drift check).
      const build = () => orchestrator.getGraphV2(
        viewer, query.personId, query.day, query.timezone, activity, query.cursor, phases,
      );
      const { result: snapshot, tally } = await countDynamoCalls(
        () => (req.get('x-graph-full-read') === '1' ? withFullReads(build) : build()),
      );
      // NFR #16: what this build cost — wall time and DynamoDB calls by kind.
      const reads = [...tally.entries()].filter(([key]) => !key.startsWith('ms:')).reduce((sum, [, n]) => sum + n, 0);
      const scans = [...tally.entries()].filter(([key]) => key.startsWith('Scan:')).reduce((sum, [, n]) => sum + n, 0);
      // NFR #157: and where the time went, phase by phase (person, load, acl, activity).
      res.setHeader('server-timing', [
        `build;dur=${Date.now() - started}`,
        ...(Object.keys(phases).length ? [serverTiming(phases)] : []),
        `ddb;desc="calls=${reads} scans=${scans}"`,
        // Settled run rows served from the shared cache vs read (Loop 32);
        // process-wide counters, so exact only when builds do not overlap.
        `runrow;desc="hits=${runRowCacheCounters.hits - rowCacheBefore.hits} misses=${runRowCacheCounters.misses - rowCacheBefore.misses}"`,
      ].join(', '));
      if (req.get('x-ddb-tally') === '1') res.setHeader('x-ddb-tally', [...tally.entries()].map(([k, n]) => `${k}=${n}`).join(' '));
      // NFR #85: only a reader that asked (it can seed its stream from this
      // view) gets the extra field; the strict older validators never see it.
      const viewHash = req.query['viewBase'] === '1'
        ? await publishViewBase(await viewBaseStore(), {
          organizationId: viewer.organizationId,
          viewerId: viewer.viewerId,
          personId: query.personId,
          day: query.day,
          timezone: query.timezone,
          watermark: snapshot.watermark,
        }, snapshot)
        : null;
      res.status(200).json(viewHash ? { ...snapshot, viewHash } : snapshot);
      return;
    }
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

  router.get('/sharing-grants', handler(async (_req, res, viewer) => {
    res.status(200).json(await orchestrator.listOwnedGrants(viewer));
  }));

  router.patch('/sharing-grants/:id', handler(async (req, res, viewer) => {
    if (!isId(req.params.id)) throw new WorkGraphInputError('Sharing grant id is invalid.');
    res.status(200).json(await orchestrator.updateGrant(viewer, req.params.id, parseUpdateGrant(req.body)));
  }));

  /**
   * A question answered by quoting an authorized transcript. Access is
   * re-checked against the source on every call, and an unanswerable question
   * returns `answer: null` rather than anything the transcript does not say.
   */
  router.post('/transcripts/:id/ask', handler(async (req, res, viewer) => {
    if (!isId(req.params.id)) throw new WorkGraphInputError('Transcript node id is invalid.');
    if (!exact(req.body, ['personId', 'day', 'timezone', 'question'])
      || !isId(req.body.personId)
      || !isDay(req.body.day)
      || !isTimezone(req.body.timezone)
      || typeof req.body.question !== 'string') {
      throw new WorkGraphInputError('Transcript question body is invalid.');
    }
    res.status(200).json(await orchestrator.askTranscript(
      viewer, req.body.personId, req.body.day, req.body.timezone, req.params.id, req.body.question,
    ));
  }));

  /**
   * The bytes behind one revision preview. Authorization is re-decided on
   * every request (node-scoped: the deck the handle names, not the day), and
   * the response is inert: no sniffing, no framing.
   *
   * A handle names an immutable thing — one slide of one stored revision — so
   * the bytes carry their content hash as a strong ETag and may be kept by
   * THIS viewer's browser (`private`, never a shared cache, which could serve
   * them to someone the source no longer allows). A revalidation is still
   * authorized first and answered 304 without the body.
   */
  router.get('/previews/:handle', handler(async (req, res, viewer) => {
    if (!isId(req.params.handle)) throw new WorkGraphInputError('Preview handle is invalid.');
    const query = parseGraphQuery(req);
    const timing: Record<string, number> = {};
    const preview = await orchestrator.getRevisionPreview(
      viewer, query.personId, query.day, query.timezone, req.params.handle, timing,
    );
    res.setHeader('server-timing', serverTiming(timing));
    const etag = `"${previewContentHash(preview.body)}"`;
    res.setHeader('etag', etag)
      .setHeader('cache-control', PREVIEW_CACHE_CONTROL)
      .setHeader('vary', 'Authorization')
      .setHeader('x-content-type-options', 'nosniff');
    const ifNoneMatch = req.headers?.['if-none-match'];
    if (typeof ifNoneMatch === 'string' && ifNoneMatch.split(',').map((tag) => tag.trim()).includes(etag)) {
      res.status(304).end();
      return;
    }
    res.status(200)
      .setHeader('content-type', preview.contentType)
      .setHeader('content-length', String(preview.body.byteLength))
      .setHeader('content-disposition', 'inline')
      .setHeader('content-security-policy', "default-src 'none'; style-src 'unsafe-inline'; sandbox")
      .send(preview.body);
  }));

  /**
   * Several previews under one authorization pass — what a deck's pane asks
   * for when it opens (its card, then every slide of the revision on screen).
   * Answers are per handle, in request order; the bodies are base64 so one
   * JSON response carries all of them, each with its content hash.
   */
  router.post('/previews/batch', handler(async (req, res, viewer) => {
    if (!exact(req.body, ['personId', 'day', 'timezone', 'handles'])
      || !isId(req.body.personId)
      || !isDay(req.body.day)
      || !isTimezone(req.body.timezone)
      || !Array.isArray(req.body.handles)
      || req.body.handles.length === 0
      || req.body.handles.length > PREVIEW_BATCH_LIMIT
      || !req.body.handles.every((handle: unknown) => isId(handle))) {
      throw new WorkGraphInputError('Preview batch body is invalid.');
    }
    const handles = [...new Set(req.body.handles as string[])];
    const timing: Record<string, number> = {};
    const answers = await orchestrator.getRevisionPreviews(
      viewer, req.body.personId, req.body.day, req.body.timezone, handles, timing,
    );
    res.status(200)
      .setHeader('server-timing', serverTiming(timing))
      .setHeader('cache-control', 'private, no-store')
      .setHeader('x-content-type-options', 'nosniff')
      .json({
        previews: answers.map((answer) => (answer.status === 'ok'
          ? {
            handle: answer.handle,
            status: 'ok',
            contentType: answer.preview.contentType,
            contentHash: previewContentHash(answer.preview.body),
            body: answer.preview.body.toString('base64'),
          }
          : { handle: answer.handle, status: answer.status })),
      });
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

  router.get('/collaboration-requests', handler(async (_req, res, viewer) => {
    res.status(200).json(await orchestrator.listOwnedCollaborationRequests(viewer));
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
