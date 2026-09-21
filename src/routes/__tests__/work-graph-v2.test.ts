import express, { type NextFunction, type Request, type Response } from 'express';
import { validateWorkGraphSnapshotV2, type WorkGraphSnapshotV2 } from '@connorhoehn/realtime-modules/work-graph';
import { createWorkGraphCursorCodec } from '../../work-graph/cursor';
import {
  WorkGraphOrchestrator,
  type WorkGraphOrchestratorDependencies,
} from '../../work-graph/orchestrator';
import type { WorkActivityReader } from '../../work-graph/activity';
import type { WorkSourceAclAdapter, WorkDisclosure } from '../../work-graph/policy';
import type { StoredWorkEdge, StoredWorkNode, WorkProjectionState } from '../../repositories/WorkGraphRepository';
import type { StoredWorkGrant } from '../../repositories/WorkSharingRepository';
import { createWorkGraphRouter } from '../work-graph';

const DAY = '2026-09-21';

const NOW = '2026-09-21T13:45:00.000Z'; // 09:45 local
const at = (time: string): string => `2026-09-21T${time}:00.000Z`;

function node(
  id: string,
  kind: StoredWorkNode['kind'],
  title: string,
  resourceId: string,
  source: StoredWorkNode['sourceRef']['source'],
  updatedAt: string,
  extra: Partial<StoredWorkNode> = {},
): StoredWorkNode {
  return {
    id,
    organizationId: 'org-1',
    actorId: 'connor',
    kind,
    sourceRef: { source, resourceId },
    policyRef: `${source}:${kind}`,
    title,
    status: 'completed',
    startedAt: updatedAt,
    updatedAt,
    sourceEventId: `event-${id}`,
    revision: 1,
    ...extra,
  };
}

function edge(id: string, fromId: string, toId: string, relation: StoredWorkEdge['relation'], status: StoredWorkEdge['status'] = 'completed'): StoredWorkEdge {
  return {
    id,
    organizationId: 'org-1',
    actorId: 'connor',
    fromId,
    toId,
    relation,
    policyRef: `relation:${relation}`,
    status,
    startedAt: at('13:30'),
    updatedAt: at('13:42'),
    sourceEventId: `event-${id}`,
    provenance: 'source-event',
    revision: 1,
  };
}

// The prototype scenario as real projected entities: a meeting effort whose
// transcript and a project document feed a run that produced a deck, and a
// separate cloud-terminal effort anchored on its own work item.
const nodes: StoredWorkNode[] = [
  node('meeting', 'meeting', 'Planning sync', 'call-1', 'meeting', at('13:00')),
  node('transcript', 'transcript', 'Transcript', 'transcript-1', 'meeting', at('13:05')),
  node('release-plan', 'document', 'Release plan', 'doc-release', 'document', at('13:08')),
  node('run', 'run', 'Generate presentation', 'run-9', 'pipeline', at('13:42'), { status: 'running' }),
  node('deck', 'document', 'Sprint review', 'doc-deck', 'document', at('13:31')),
  node('deck-v1', 'change', 'Document change', 'doc-deck:v1', 'document', at('13:27')),
  node('deck-v2', 'change', 'Document change', 'doc-deck:v2', 'document', at('13:31')),
  node('work-144', 'project', 'WORK-144 · Coverage', 'work-144', 'cloud-compute', at('13:10'), { description: 'Gateway · Cloud project' }),
  node('terminal', 'terminal', 'Cloud terminal', 'box-7', 'cloud-compute', at('13:43'), { status: 'running', description: 'Tests running' }),
];
const edges: StoredWorkEdge[] = [
  edge('e-transcript', 'transcript', 'meeting', 'derived-from'),
  edge('e-input-transcript', 'run', 'transcript', 'derived-from'),
  edge('e-input-plan', 'run', 'release-plan', 'derived-from'),
  edge('e-produced', 'run', 'deck', 'produced', 'running'),
  edge('e-v1', 'deck-v1', 'deck', 'edited'),
  edge('e-v2', 'deck-v2', 'deck', 'edited'),
  edge('e-terminal', 'terminal', 'work-144', 'works-on', 'running'),
];

const projection: WorkProjectionState = {
  schemaVersion: 1,
  organizationId: 'org-1',
  actorId: 'connor',
  revision: 12,
  sessions: {},
  nodes: Object.fromEntries(nodes.map((item) => [item.id, item])),
  edges: Object.fromEntries(edges.map((item) => [item.id, item])),
  sourceCheckpoints: {},
  appliedEventIds: [],
};

const grant: StoredWorkGrant = {
  schemaVersion: 1,
  id: 'grant-1',
  organizationId: 'org-1',
  ownerId: 'connor',
  audience: { kind: 'people', personIds: ['frank'] },
  selection: {
    nodeIds: nodes.map((item) => item.id),
    edgeIds: edges.map((item) => item.id),
    disclosure: 'details',
  },
  state: 'active',
  createdAt: at('13:00'),
  updatedAt: at('13:00'),
  expiresAt: '2026-09-21T18:00:00.000Z',
  revision: 1,
};

function reader(overrides: Partial<WorkActivityReader> = {}): WorkActivityReader {
  return {
    pipelineRun: async (runId) => (runId === 'run-9'
      ? {
        runId, status: 'running' as const, attempt: 3,
        startedAt: at('13:33'), updatedAt: at('13:42'),
        tools: ['Slides', 'Charts', 'Files'],
      }
      : null),
    artifact: async (documentId) => (documentId === 'doc-deck'
      ? {
        mediaKind: 'presentation' as const,
        pageCount: 12,
        anchors: [{ kind: 'slide' as const, id: 'slide-4', label: 'Release readiness', index: 4 }],
        feedbackCount: 2,
      }
      : null),
    transcript: async (transcriptId) => (transcriptId === 'transcript-1'
      ? {
        segments: [
          { id: 'segment-1', at: at('12:42'), speaker: 'Connor', text: 'Release risks before noon.' },
          { id: 'segment-2', at: at('12:47'), speaker: 'Frank', text: 'I will take the rollback owner.' },
        ],
        speakerCount: 2,
        actionItemCount: 1,
      }
      : null),
    meeting: async () => ({
      startedAt: at('12:30'), endedAt: at('13:00'), recorded: true, transcriptAvailable: true,
    }),
    attention: async () => null,
    ...overrides,
  };
}

function setup(options: { disclosure?: WorkDisclosure; activityReader?: WorkActivityReader } = {}): express.Express {
  const source: WorkSourceAclAdapter = {
    source: 'document',
    authorizeDisclosure: async (scope, targets) => new Map(targets.map((target) => [target.id, {
      targetId: target.id,
      decision: 'allow' as const,
      maximumDisclosure: options.disclosure ?? ('details' as const),
      capabilities: ['view-document' as const, 'view-transcript' as const],
      policyRevision: scope.policyRevision,
    }])),
    authorizeCapability: async () => 'allow' as const,
  };
  const acls = new Map(
    (['document', 'meeting', 'pipeline', 'cloud-compute'] as const)
      .map((kind) => [kind, { ...source, source: kind } as WorkSourceAclAdapter]),
  );
  const deps: WorkGraphOrchestratorDependencies = {
    graph: { loadProjection: async (organizationId, actorId) => (organizationId === 'org-1' && actorId === 'connor' ? projection : null) },
    sharing: {
      getGrant: async () => grant,
      listEffectiveGrants: async (_organizationId, ownerId, viewer) => (ownerId === 'connor' && viewer.viewerId === 'frank' ? [grant] : []),
      putGrant: async (value) => value,
    },
    collaboration: {
      getRequest: async () => null,
      putRequest: async (value) => value,
      findAcceptedRequest: async () => null,
    },
    directory: {
      resolveViewer: async (subject, organizationClaim) => (organizationClaim === 'org-1'
        ? { viewerId: subject, organizationId: 'org-1', conversationIds: [] }
        : null),
      resolveViewerById: async (organizationId, viewerId) => ({ viewerId, organizationId, conversationIds: [] }),
      personBelongsToOrganization: async (_organizationId, personId) => personId === 'connor',
      listPeople: async () => [],
      audienceBelongsToOrganization: async () => true,
    },
    locator: { locate: async () => null },
    collaborationLifecycle: { accepted: async () => undefined, revoked: async () => undefined },
    sourceAcls: acls,
    sourceActions: new Map(),
    cursorCodec: createWorkGraphCursorCodec('test-secret-that-is-longer-than-thirty-two-bytes'),
    ...('activityReader' in options ? { activityReader: options.activityReader } : { activityReader: reader() }),
    policyRevision: async () => 'policy-7',
    now: () => NOW,
    id: () => 'generation-1',
  };
  const app = express();
  app.use('/api/work-graph', createWorkGraphRouter(new WorkGraphOrchestrator(deps)));
  app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
    res.status(500).json({ error: (error as Error).message });
  });
  return app;
}

interface Captured { statusCode: number; body: unknown }

async function get(app: express.Express, url: string, sub = 'frank'): Promise<Captured> {
  const captured: Captured = { statusCode: 200, body: undefined };
  let settle!: () => void;
  const finished = new Promise<void>((resolve) => { settle = resolve; });
  const res = {
    statusCode: 200,
    status(code: number) { captured.statusCode = code; return this; },
    json(body: unknown) { captured.body = body; settle(); return this; },
    setHeader() { return this; },
    end() { settle(); return this; },
  };
  const req = {
    method: 'GET', url, originalUrl: url,
    headers: { 'content-type': 'application/json' },
    params: {}, ip: '127.0.0.1', user: { sub, orgId: 'org-1' },
  } as unknown as Request;
  await new Promise<void>((resolve, reject) => {
    (app as unknown as (request: Request, response: Response, next: NextFunction) => void)(
      req, res as unknown as Response, (error?: unknown) => (error ? reject(error) : resolve()),
    );
    finished.then(resolve, reject);
  });
  return captured;
}

const base = `/api/work-graph/people/connor/graph?day=${DAY}&timezone=America%2FNew_York`;
const v2 = `${base}&schemaVersion=2&windowStart=${encodeURIComponent(at('13:30'))}&windowEnd=${encodeURIComponent(at('13:45'))}&mode=live`;

describe('work graph schemaVersion 2 route', () => {
  it('leaves the v1 response exactly as it was', async () => {
    const response = await get(setup(), base);
    expect(response.statusCode).toBe(200);
    const body = response.body as Record<string, unknown>;
    expect(body.schemaVersion).toBe(1);
    expect(Object.keys(body).sort()).toEqual([
      'cursor', 'edges', 'nodes', 'partial', 'revision', 'schemaVersion', 'scope', 'sources', 'watermark',
    ]);
  });

  it('returns two source-anchored efforts with the authorized activity layer', async () => {
    const response = await get(setup(), v2);
    expect(response.statusCode).toBe(200);
    const body = response.body as WorkGraphSnapshotV2;
    expect(validateWorkGraphSnapshotV2(body).ok).toBe(true);
    expect(body.efforts).toHaveLength(2);

    const sprint = body.efforts.find((effort) => effort.anchorNodeId === 'meeting');
    expect(sprint?.title).toBe('Sprint review');
    expect(sprint?.subtitle).toBe('v3 generating · v2 available');
    expect(sprint?.nodeIds.sort()).toEqual(['deck', 'deck-v1', 'deck-v2', 'meeting', 'release-plan', 'run', 'transcript']);
    // Work observed before the selected window is folded context, not dropped.
    expect(sprint?.contextNodeIds).toContain('transcript');

    const coverage = body.efforts.find((effort) => effort.anchorNodeId === 'work-144');
    expect(coverage?.title).toBe('WORK-144 · Coverage');
    expect(coverage?.nodeIds.sort()).toEqual(['terminal', 'work-144']);

    const deck = body.details.find((detail) => detail.nodeId === 'deck');
    expect(deck?.artifact?.mediaKind).toBe('presentation');
    expect(deck?.artifact?.revisions.map((revision) => revision.label)).toEqual(['v1', 'v2']);
    expect(deck?.artifact?.pending).toMatchObject({ revisionId: 'v3', label: 'v3', status: 'generating', attemptId: 'attempt-3' });
    expect(deck?.artifact?.pageCount).toBe(12);
    expect(deck?.feedback).toEqual({ count: 2, through: NOW });
    expect(deck?.sources?.map((item) => item.nodeId).sort()).toEqual(['release-plan', 'transcript']);

    const run = body.details.find((detail) => detail.nodeId === 'run');
    expect(run?.tools).toEqual(['Slides', 'Charts', 'Files']);
    expect(run?.summary).toBe('Creating the next revision of Sprint review');
    expect(run?.inputs?.map((item) => item.nodeId).sort()).toEqual(['release-plan', 'transcript']);

    const transcript = body.details.find((detail) => detail.nodeId === 'transcript');
    expect(transcript?.transcript?.segments).toHaveLength(2);
    expect(transcript?.transcript?.askEnabled).toBe(true);
    expect(transcript?.lines).toEqual(['2 speakers · 1 action item']);

    const terminal = body.details.find((detail) => detail.nodeId === 'terminal');
    expect(terminal?.workItem).toMatchObject({ nodeId: 'work-144', label: 'WORK-144 · Coverage' });
    expect(terminal?.freshness?.expiresAt).toBeDefined();
    expect(Date.parse(terminal?.freshness?.expiresAt ?? '') > Date.parse(NOW)).toBe(true);

    // Only fresh running process edges become operations.
    expect(body.operations.map((operation) => operation.edgeId).sort()).toEqual(['e-produced', 'e-terminal']);
    expect(body.eventBuckets.length).toBeGreaterThan(0);
    expect(body.temporal.mode).toBe('live');
  });

  it('reports a failed attempt with its sanitized message and an error run', async () => {
    const failing = reader({
      pipelineRun: async () => ({
        runId: 'run-9', status: 'error' as const, attempt: 3,
        startedAt: at('13:33'), updatedAt: at('13:42'),
        tools: ['Slides', 'Charts', 'Files'],
        error: 'Slides tool timed out',
      }),
    });
    const body = (await get(setup({ activityReader: failing }), v2)).body as WorkGraphSnapshotV2;
    const deck = body.details.find((detail) => detail.nodeId === 'deck');
    expect(deck?.artifact?.pending).toMatchObject({ status: 'failed', message: 'Slides tool timed out' });
    expect(deck?.artifact?.revisions.map((revision) => revision.label)).toEqual(['v1', 'v2']);
    expect(body.efforts.find((effort) => effort.anchorNodeId === 'meeting')?.subtitle)
      .toBe('v3 failed · v2 available');
    expect(body.details.find((detail) => detail.nodeId === 'run')?.summary).toBe('Slides tool timed out');
  });

  it('discloses no detail at all when the sources allow only summary', async () => {
    const body = (await get(setup({ disclosure: 'summary' }), v2)).body as WorkGraphSnapshotV2;
    expect(validateWorkGraphSnapshotV2(body).ok).toBe(true);
    expect(body.details).toEqual([]);
    expect(body.operations).toEqual([]);
    // Membership still comes from the same source-declared relationships.
    expect(body.efforts).toHaveLength(2);
  });

  it('answers an as-of request as recent rather than claiming reconstruction', async () => {
    const body = (await get(setup(), `${v2.replace('mode=live', 'mode=as-of')}`)).body as WorkGraphSnapshotV2;
    expect(body.query.mode).toBe('as-of');
    expect(body.temporal.mode).toBe('recent');
    expect(body.temporal.coverage.complete).toBe(false);
  });

  it('rejects a malformed or unsupported version request without answering it', async () => {
    const app = setup();
    expect((await get(app, `${base}&schemaVersion=3`)).statusCode).toBe(400);
    expect((await get(app, `${base}&windowStart=${encodeURIComponent(at('13:30'))}`)).statusCode).toBe(400);
    expect((await get(app, `${base}&schemaVersion=2&windowStart=not-a-time`)).statusCode).toBe(400);
    expect((await get(app, `${base}&schemaVersion=2&mode=yesterday`)).statusCode).toBe(400);
  });

  it('gives an unshared viewer nothing, in v2 exactly as in v1', async () => {
    const app = setup();
    expect((await get(app, v2, 'mallory')).statusCode).toBe(404);
    expect((await get(app, base, 'mallory')).statusCode).toBe(404);
  });

  it('refuses schemaVersion 2 on a deployment without an activity reader', async () => {
    const response = await get(setup({ activityReader: undefined }), v2);
    expect(response.statusCode).toBe(400);
  });
});
