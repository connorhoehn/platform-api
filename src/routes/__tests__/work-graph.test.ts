import express, { type NextFunction, type Request, type Response } from 'express';
import { createWorkGraphCursorCodec } from '../../work-graph/cursor';
import {
  WorkGraphOrchestrator,
  type WorkGraphOrchestratorDependencies,
  type WorkSourceActionResult,
} from '../../work-graph/orchestrator';
import type { WorkSourceAclAdapter } from '../../work-graph/policy';
import type { WorkProjectionState } from '../../repositories/WorkGraphRepository';
import type { StoredWorkGrant, WorkSharingGrant } from '../../repositories/WorkSharingRepository';
import type { WorkCollaborationRequest } from '../../repositories/WorkCollaborationRepository';
import { createWorkGraphRouter } from '../work-graph';

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  send(body?: unknown): MockRes;
  setHeader(name: string, value: unknown): MockRes;
  getHeader(name: string): unknown;
  end(): MockRes;
}

function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((done) => { resolve = done; });
  const headers = new Map<string, unknown>();
  return {
    statusCode: 200,
    body: undefined,
    ended: false,
    finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolve(); return this; },
    send(body) { this.body = body; this.ended = true; resolve(); return this; },
    setHeader(name, value) { headers.set(name.toLowerCase(), value); return this; },
    getHeader(name) { return headers.get(name.toLowerCase()); },
    end() { this.ended = true; resolve(); return this; },
  };
}

async function run(
  app: express.Express,
  method: string,
  url: string,
  user: { sub: string; orgId: string },
  body?: unknown,
): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method,
    url,
    originalUrl: url,
    headers: { 'content-type': 'application/json' },
    body,
    params: {},
    ip: '127.0.0.1',
    user,
  } as unknown as Request;
  await new Promise<void>((resolve, reject) => {
    const done = (error?: unknown): void => {
      if (error) reject(error);
      else {
        if (!res.ended) res.status(404).json({ error: 'route not found' });
        resolve();
      }
    };
    (app as unknown as (request: Request, response: Response, next: NextFunction) => void)(
      req,
      res as unknown as Response,
      done,
    );
    res.finished.then(resolve, reject);
  });
  return res;
}

const NOW = '2026-09-19T16:00:00.000Z';
const DAY = '2026-09-19';
const projection: WorkProjectionState = {
  schemaVersion: 1,
  organizationId: 'org-1',
  actorId: 'alice',
  revision: 7,
  sessions: {},
  nodes: {
    'node-secret': {
      id: 'node-secret',
      organizationId: 'org-1',
      actorId: 'alice',
      kind: 'document',
      sourceRef: { source: 'document', resourceId: 'private-document-44' },
      policyRef: 'private-policy-99',
      title: 'Secret acquisition plan',
      description: 'Never disclose this text on a denied path.',
      status: 'running',
      updatedAt: NOW,
      sourceEventId: 'event-1',
      revision: 1,
    },
  },
  edges: {},
  sourceCheckpoints: {},
  appliedEventIds: ['event-1'],
};

function liveGrant(overrides: Partial<WorkSharingGrant> = {}): WorkSharingGrant {
  return {
    schemaVersion: 1,
    id: 'grant-1',
    organizationId: 'org-1',
    ownerId: 'alice',
    audience: { kind: 'people', personIds: ['bob'] },
    selection: { nodeIds: ['node-secret'], edgeIds: [], disclosure: 'details' },
    state: 'active',
    createdAt: NOW,
    updatedAt: NOW,
    expiresAt: '2026-09-19T18:00:00.000Z',
    revision: 1,
    ...overrides,
  };
}

function setup(): {
  app: express.Express;
  source: WorkSourceAclAdapter;
  setOutage(value: boolean): void;
  setCapabilities(value: WorkCollaborationRequest['capability'][]): void;
  setActionResult(value: WorkSourceActionResult): void;
  grantRows: Map<string, StoredWorkGrant>;
  collaborationRows: Map<string, WorkCollaborationRequest>;
  putGrant: jest.Mock;
  putCollaboration: jest.Mock;
  exchangeAction: jest.Mock;
  accepted: jest.Mock;
  revoked: jest.Mock;
} {
  let outage = false;
  let capabilities: WorkCollaborationRequest['capability'][] = ['view-document'];
  let actionResult: WorkSourceActionResult = {
    decision: 'allow',
    descriptor: { kind: 'route', capability: 'view-document', path: '/work-actions/ticket-1' },
  };
  let id = 0;
  const grantRows = new Map<string, StoredWorkGrant>([['grant-1', liveGrant()]]);
  const collaborationRows = new Map<string, WorkCollaborationRequest>();
  const putGrant = jest.fn(async (grant: StoredWorkGrant, expected: number | null) => {
    const existing = grantRows.get(grant.id);
    if ((expected === null && existing) || (expected !== null && existing?.revision !== expected)) {
      const error = new Error('conflict') as Error & { code: string };
      error.code = 'WORK_SHARING_REVISION_CONFLICT';
      throw error;
    }
    grantRows.set(grant.id, grant);
    return grant;
  });
  const putCollaboration = jest.fn(async (request: WorkCollaborationRequest, expected: number | null) => {
    const existing = collaborationRows.get(request.id);
    if ((expected === null && existing) || (expected !== null && existing?.revision !== expected)) {
      const error = new Error('conflict') as Error & { code: string };
      error.code = 'WORK_COLLABORATION_REVISION_CONFLICT';
      throw error;
    }
    collaborationRows.set(request.id, request);
    return request;
  });
  const source: WorkSourceAclAdapter = {
    source: 'document',
    authorizeDisclosure: jest.fn(async (scope, targets) => {
      if (outage) throw new Error('document policy unavailable');
      return new Map(targets.map((item) => [item.id, {
        targetId: item.id,
        decision: 'allow' as const,
        maximumDisclosure: 'details' as const,
        capabilities,
        policyRevision: scope.policyRevision,
      }]));
    }),
    authorizeCapability: jest.fn(async (_scope, _target, capability) => (
      outage ? 'unavailable' : capabilities.includes(capability) ? 'allow' : 'deny'
    )),
  };
  const exchangeAction = jest.fn(async () => actionResult);
  const accepted = jest.fn(async () => undefined);
  const revoked = jest.fn(async () => undefined);
  const deps: WorkGraphOrchestratorDependencies = {
    graph: {
      loadProjection: jest.fn(async (organizationId, actorId, day) => (
        organizationId === 'org-1' && actorId === 'alice' && day === DAY ? projection : null
      )),
    },
    sharing: {
      getGrant: jest.fn(async (organizationId, grantId) => {
        const grant = grantRows.get(grantId);
        return grant?.organizationId === organizationId ? grant : null;
      }),
      listEffectiveGrants: jest.fn(async (organizationId, ownerId, viewer) => {
        if (organizationId !== 'org-1' || ownerId !== 'alice' || viewer.viewerId !== 'bob') return [];
        return [...grantRows.values()].filter((grant) => grant.ownerId === ownerId && grant.state === 'active');
      }),
      putGrant,
      putGrantPair: jest.fn(async (current, history) => {
        grantRows.set(current.id, current);
        grantRows.set(history.id, history);
        return { current, history };
      }),
    },
    collaboration: {
      getRequest: jest.fn(async (organizationId, requestId) => {
        const request = collaborationRows.get(requestId);
        return request?.organizationId === organizationId ? request : null;
      }),
      putRequest: putCollaboration,
      findAcceptedRequest: jest.fn(async (organizationId, ownerId, requesterId, target, capability) => (
        [...collaborationRows.values()].find((request) => request.organizationId === organizationId
          && request.ownerId === ownerId
          && request.requesterId === requesterId
          && request.target.kind === target.kind
          && request.target.id === target.id
          && request.capability === capability
          && request.state === 'accepted') ?? null
      )),
    },
    directory: {
      resolveViewer: jest.fn(async (subject, organizationClaim) => (
        organizationClaim === 'org-1' && (subject === 'alice' || subject === 'bob' || subject === 'mallory')
          ? { viewerId: subject, organizationId: 'org-1', conversationIds: [] }
          : organizationClaim === 'org-2' && subject === 'eve'
            ? { viewerId: 'eve', organizationId: 'org-2', conversationIds: [] }
            : null
      )),
      resolveViewerById: jest.fn(async (organizationId, viewerId) => (
        organizationId === 'org-1' && ['alice', 'bob', 'mallory'].includes(viewerId)
          ? { viewerId, organizationId, conversationIds: [] }
          : null
      )),
      personBelongsToOrganization: jest.fn(async (organizationId, personId) => (
        organizationId === 'org-1' && ['alice', 'bob', 'mallory'].includes(personId)
      )),
      listPeople: jest.fn(async (organizationId) => organizationId === 'org-1'
        ? [{ personId: 'alice', updatedAt: NOW, fresh: true }]
        : []),
      audienceBelongsToOrganization: jest.fn(async (organizationId, ownerId, value) => (
        organizationId === 'org-1'
        && ownerId === 'alice'
        && (value.kind !== 'people' || value.personIds.every((personId) => ['alice', 'bob', 'mallory'].includes(personId)))
      )),
    },
    locator: {
      locate: jest.fn(async (organizationId, target) => (
        organizationId === 'org-1' && target.id === 'node-secret'
          ? { personId: 'alice', day: DAY, timezone: 'America/New_York' }
          : null
      )),
    },
    collaborationLifecycle: { accepted, revoked },
    sourceAcls: new Map([['document', source]]),
    sourceActions: new Map([['document', { source: 'document', exchangeAction }]]),
    cursorCodec: createWorkGraphCursorCodec('test-secret-that-is-longer-than-thirty-two-bytes'),
    policyRevision: jest.fn(async () => 'policy-7'),
    now: () => NOW,
    id: () => `generated-${++id}`,
  };
  const app = express();
  app.use('/api/work-graph', createWorkGraphRouter(new WorkGraphOrchestrator(deps)));
  app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
    res.status(500).json({ error: (error as Error).message });
  });
  return {
    app,
    source,
    setOutage(value) { outage = value; },
    setCapabilities(value) { capabilities = value; },
    setActionResult(value) { actionResult = value; },
    grantRows,
    collaborationRows,
    putGrant,
    putCollaboration,
    exchangeAction,
    accepted,
    revoked,
  };
}

describe('work graph authenticated routes', () => {
  it('denies outsiders and cross-organization viewers with one generic response and no hidden metadata', async () => {
    const { app } = setup();
    const outsider = await run(
      app,
      'GET',
      `/api/work-graph/people/alice/graph?day=${DAY}&timezone=America%2FNew_York`,
      { sub: 'mallory', orgId: 'org-1' },
    );
    const crossOrg = await run(
      app,
      'GET',
      `/api/work-graph/people/alice/graph?day=${DAY}&timezone=America%2FNew_York`,
      { sub: 'eve', orgId: 'org-2' },
    );
    expect(outsider.statusCode).toBe(404);
    expect(crossOrg.statusCode).toBe(404);
    expect(outsider.body).toEqual({ error: 'WORK_GRAPH_UNAVAILABLE', message: 'This work item is unavailable.' });
    expect(crossOrg.body).toEqual(outsider.body);
    const wire = JSON.stringify([outsider.body, crossOrg.body]);
    expect(wire).not.toContain('Secret acquisition plan');
    expect(wire).not.toContain('private-document-44');
    expect(wire).not.toContain('private-policy-99');
  });

  it('returns a partial snapshot for source outage, 503 for selected detail, and generic reference unavailability', async () => {
    const { app, setOutage } = setup();
    setOutage(true);
    const graph = await run(
      app,
      'GET',
      `/api/work-graph/people/alice/graph?day=${DAY}&timezone=America%2FNew_York`,
      { sub: 'bob', orgId: 'org-1' },
    );
    expect(graph.statusCode).toBe(200);
    expect(graph.body).toMatchObject({
      nodes: [],
      edges: [],
      partial: true,
      sources: [{ source: 'document', health: 'authorization-unavailable' }],
    });

    const detail = await run(
      app,
      'GET',
      `/api/work-graph/nodes/node-secret?personId=alice&day=${DAY}&timezone=America%2FNew_York`,
      { sub: 'bob', orgId: 'org-1' },
    );
    expect(detail.statusCode).toBe(503);
    expect(detail.body).toEqual({
      error: 'WORK_GRAPH_AUTHORIZATION_UNAVAILABLE',
      message: 'Work graph authorization is temporarily unavailable.',
    });

    const references = await run(
      app,
      'POST',
      '/api/work-graph/references/resolve',
      { sub: 'bob', orgId: 'org-1' },
      { references: [{ version: 1, personId: 'alice', day: DAY, timezone: 'America/New_York', target: { kind: 'node', id: 'node-secret' } }] },
    );
    expect(references.statusCode).toBe(200);
    expect(references.body).toEqual([{
      reference: { version: 1, personId: 'alice', day: DAY, timezone: 'America/New_York', target: { kind: 'node', id: 'node-secret' } },
      state: 'unavailable',
    }]);
    expect(JSON.stringify(references.body)).not.toContain('authorization');
  });

  it('uses a viewer-local placeholder for existence-only disclosure', async () => {
    const { app, grantRows } = setup();
    grantRows.set('grant-1', liveGrant({
      selection: { nodeIds: ['node-secret'], edgeIds: [], disclosure: 'existence' },
    }));
    const result = await run(
      app,
      'GET',
      `/api/work-graph/people/alice/graph?day=${DAY}&timezone=America%2FNew_York`,
      { sub: 'bob', orgId: 'org-1' },
    );
    expect(result.statusCode).toBe(200);
    expect((result.body as { nodes: unknown[] }).nodes).toEqual([{
      id: expect.stringMatching(/^wg_placeholder_[0-9a-f]{64}$/),
      kind: 'task',
      title: 'Shared work item',
      status: 'idle',
      disclosure: 'existence',
      updatedAt: '1970-01-01T00:00:00.000Z',
      capabilities: [],
      locked: true,
    }]);
    expect(JSON.stringify(result.body)).not.toMatch(/node-secret|Secret acquisition plan|private-document-44|private-policy-99/);
  });

  it('returns 409 before writing when a grant expected revision is stale', async () => {
    const { app, grantRows, putGrant } = setup();
    grantRows.set('grant-1', liveGrant({ ownerId: 'alice', revision: 2 }));
    const result = await run(
      app,
      'PATCH',
      '/api/work-graph/sharing-grants/grant-1',
      { sub: 'alice', orgId: 'org-1' },
      { expectedRevision: 1, action: 'pause' },
    );
    expect(result.statusCode).toBe(409);
    expect(result.body).toEqual({ error: 'WORK_GRAPH_REVISION_CONFLICT', message: 'The expected revision is stale.' });
    expect(putGrant).not.toHaveBeenCalled();
  });

  it('cannot escalate a visible view capability into an edit request', async () => {
    const { app, setCapabilities, putCollaboration } = setup();
    setCapabilities(['view-document']);
    const result = await run(
      app,
      'POST',
      '/api/work-graph/collaboration-requests',
      { sub: 'bob', orgId: 'org-1' },
      { target: { kind: 'node', id: 'node-secret' }, capability: 'edit-document', expiresAt: '2026-09-19T17:00:00.000Z' },
    );
    expect(result.statusCode).toBe(403);
    expect(result.body).toEqual({
      error: 'WORK_GRAPH_CAPABILITY_DENIED',
      message: 'The requested collaboration capability is not available.',
    });
    expect(putCollaboration).not.toHaveBeenCalled();
  });

  it('rejects browser-supplied identity fields instead of accepting claimed actor or organization', async () => {
    const { app, putGrant } = setup();
    const result = await run(
      app,
      'POST',
      '/api/work-graph/sharing-grants',
      { sub: 'alice', orgId: 'org-1' },
      {
        ownerId: 'mallory',
        organizationId: 'org-2',
        audience: { kind: 'people', personIds: ['bob'] },
        selection: { nodeIds: ['node-secret'], edgeIds: [], disclosure: 'details' },
        expiresAt: '2026-09-19T17:00:00.000Z',
      },
    );
    expect(result.statusCode).toBe(400);
    expect(putGrant).not.toHaveBeenCalled();
  });

  it('exchanges an opaque target for an allowlisted same-origin route only after fresh authorization', async () => {
    const { app, exchangeAction } = setup();
    const result = await run(
      app,
      'POST',
      '/api/work-graph/actions',
      { sub: 'bob', orgId: 'org-1' },
      { target: { kind: 'node', id: 'node-secret' }, capability: 'view-document' },
    );
    expect(result.statusCode).toBe(200);
    expect(result.body).toEqual({ kind: 'route', capability: 'view-document', path: '/work-actions/ticket-1' });
    expect(exchangeAction).toHaveBeenCalledTimes(1);
    const wire = JSON.stringify(result.body);
    expect(wire).not.toContain('private-document-44');
    expect(wire).not.toContain('node-secret');
    expect(wire).not.toMatch(/token|credential/i);
  });

  it.each([
    { kind: 'route', capability: 'view-document', path: 'https://evil.example/steal' },
    { kind: 'route', capability: 'view-document', path: '/safe', token: 'secret' },
    { kind: 'source-session', capability: 'view-document', sessionId: 'session-1', connectPath: '/connect/1', expiresAt: '2026-09-19T17:00:00.000Z' },
    { kind: 'route', capability: 'view-document', path: '/safe?token=secret' },
  ])('fails closed when a source action adapter returns an unsafe descriptor %#', async (descriptor) => {
    const { app, setActionResult } = setup();
    setActionResult({ decision: 'allow', descriptor });
    const result = await run(
      app,
      'POST',
      '/api/work-graph/actions',
      { sub: 'bob', orgId: 'org-1' },
      { target: { kind: 'node', id: 'node-secret' }, capability: 'view-document' },
    );
    expect(result.statusCode).toBe(503);
    expect(result.body).toEqual({
      error: 'WORK_GRAPH_AUTHORIZATION_UNAVAILABLE',
      message: 'Work graph authorization is temporarily unavailable.',
    });
    expect(JSON.stringify(result.body)).not.toMatch(/evil|secret|token|session-1/);
  });

  it('never exchanges control-terminal without an unexpired accepted exact-capability request', async () => {
    const { app, setCapabilities, exchangeAction } = setup();
    setCapabilities(['control-terminal']);
    const result = await run(
      app,
      'POST',
      '/api/work-graph/actions',
      { sub: 'bob', orgId: 'org-1' },
      { target: { kind: 'node', id: 'node-secret' }, capability: 'control-terminal' },
    );
    expect(result.statusCode).toBe(404);
    expect(result.body).toEqual({ error: 'WORK_GRAPH_UNAVAILABLE', message: 'This work item is unavailable.' });
    expect(exchangeAction).not.toHaveBeenCalled();
  });

  it('accepts only the immutable requested capability and revocation immediately disables its control exchange', async () => {
    const {
      app,
      setCapabilities,
      setActionResult,
      accepted,
      revoked,
    } = setup();
    setCapabilities(['control-terminal']);
    setActionResult({
      decision: 'allow',
      descriptor: { kind: 'route', capability: 'control-terminal', path: '/work-actions/control-ticket-1' },
    });
    const created = await run(
      app,
      'POST',
      '/api/work-graph/collaboration-requests',
      { sub: 'bob', orgId: 'org-1' },
      { target: { kind: 'node', id: 'node-secret' }, capability: 'control-terminal', expiresAt: '2026-09-19T17:00:00.000Z' },
    );
    expect(created.statusCode).toBe(201);
    const requestId = (created.body as WorkCollaborationRequest).id;
    expect(created.body).toMatchObject({ capability: 'control-terminal', state: 'requested', revision: 1 });

    const approved = await run(
      app,
      'PATCH',
      `/api/work-graph/collaboration-requests/${requestId}`,
      { sub: 'alice', orgId: 'org-1' },
      { expectedRevision: 1, action: 'accept' },
    );
    expect(approved.statusCode).toBe(200);
    expect(approved.body).toMatchObject({ capability: 'control-terminal', state: 'accepted', revision: 2 });
    expect(accepted).toHaveBeenCalledWith(expect.objectContaining({ id: requestId, capability: 'control-terminal' }));

    const activeAction = await run(
      app,
      'POST',
      '/api/work-graph/actions',
      { sub: 'bob', orgId: 'org-1' },
      { target: { kind: 'node', id: 'node-secret' }, capability: 'control-terminal' },
    );
    expect(activeAction.statusCode).toBe(200);
    expect(activeAction.body).toEqual({
      kind: 'route',
      capability: 'control-terminal',
      path: '/work-actions/control-ticket-1',
    });

    const revokedResponse = await run(
      app,
      'PATCH',
      `/api/work-graph/collaboration-requests/${requestId}`,
      { sub: 'bob', orgId: 'org-1' },
      { expectedRevision: 2, action: 'revoke' },
    );
    expect(revokedResponse.statusCode).toBe(200);
    expect(revokedResponse.body).toMatchObject({ state: 'revoked', revision: 3 });
    expect(revoked).toHaveBeenCalledWith(expect.objectContaining({ id: requestId, state: 'revoked' }));

    const afterRevocation = await run(
      app,
      'POST',
      '/api/work-graph/actions',
      { sub: 'bob', orgId: 'org-1' },
      { target: { kind: 'node', id: 'node-secret' }, capability: 'control-terminal' },
    );
    expect(afterRevocation.statusCode).toBe(404);
    expect(afterRevocation.body).toEqual({ error: 'WORK_GRAPH_UNAVAILABLE', message: 'This work item is unavailable.' });
  });
});
