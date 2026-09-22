/**
 * End to end over the pure halves of the pipeline surface: an `/agent` command
 * typed in a chat becomes source events, the reducer projects them, and the v2
 * activity layer groups them into one effort anchored on that conversation.
 *
 * Nothing here mocks the reducer or the effort derivation — the point is that
 * the events this service actually publishes produce that graph, rather than
 * that a hand-written graph would look right.
 */
import { projectWorkEvent, type AuthenticatedWorkEvent, type WorkProjectionState } from '@connorhoehn/realtime-modules/work-graph';
import { mapPipeline, pipelineStepResourceId, type AuthenticatedPipelineContext } from '../sources/mapPipeline';
import { agentStepLines, buildWorkActivity, formatTokens, type PipelineRunFacts, type WorkActivityReader } from '../activity';
import type { AuthorizedWorkGraph, ViewerWorkEdge, ViewerWorkNode } from '../policy';
import { workGraphPlanFromDefinition } from '../../pipeline/workGraphPlan';

const ORGANIZATION = 'dev-org';
const ACTOR = 'dev-frank';
const SUBJECT = 'dev-frank';
const RUN = '0a762076-bcce-47e8-b972-b015de5e6199';
const CHANNEL = 'chat:c:general';
const DAY = '2026-09-21';

const context: AuthenticatedPipelineContext = {
  actor: { actorId: ACTOR, organizationId: ORGANIZATION, sourceSubject: SUBJECT },
  producer: { serviceId: 'platform-api', organizationId: ORGANIZATION, credentialId: 'platform-api-runtime' },
  receivedAt: `${DAY}T18:00:00.000Z`,
};

function emptyState(): WorkProjectionState {
  return {
    organizationId: ORGANIZATION,
    actorId: ACTOR,
    day: DAY,
    revision: 0,
    nodes: {},
    edges: {},
    sourceCheckpoints: {},
    appliedEventIds: [],
  } as unknown as WorkProjectionState;
}

/** The conversation node the gateway publishes when the command is posted. */
function conversationEvent(at: string): AuthenticatedWorkEvent {
  return {
    schemaVersion: 1,
    eventId: `conversation:${at}`,
    idempotencyKey: `conversation:${at}`,
    source: 'conversation',
    sourceSequence: at,
    occurredAt: at,
    receivedAt: at,
    actor: { actorId: ACTOR, organizationId: ORGANIZATION, sourceSubject: SUBJECT },
    producer: { serviceId: 'websocket-gateway', organizationId: ORGANIZATION, credentialId: 'gateway' },
    resource: { source: 'conversation', resourceId: CHANNEL, resourceVersion: at },
    payload: {
      kind: 'conversation',
      lifecycle: 'contributed',
      conversationId: CHANNEL,
      conversationKind: 'channel',
      safeLabel: 'general',
    },
  } as AuthenticatedWorkEvent;
}

function runEvent(status: string, at: string, conversation = true): AuthenticatedWorkEvent {
  const mapped = mapPipeline({
    kind: 'pipeline-run',
    run: { runId: RUN, pipelineId: 'chat-agent-loop', status, startedAt: `${DAY}T18:00:00.000Z` } as never,
    attributedSubject: SUBJECT,
    occurredAt: at,
    safeLabel: 'Agent loop · create a meeting with the X team',
    ...(conversation ? { inputRefs: [{ source: 'conversation' as const, resourceId: CHANNEL }] } : {}),
  }, context);
  if (!mapped.supported) throw new Error(mapped.reason);
  return mapped.event as unknown as AuthenticatedWorkEvent;
}

function stepEvent(
  stepId: string,
  title: string,
  status: string,
  at: string,
  dependsOn: string[] = [],
  extra: { workspace?: { resourceId: string; safeLabel?: string }; produces?: string } = {},
): AuthenticatedWorkEvent {
  const mapped = mapPipeline({
    kind: 'pipeline-step',
    run: { runId: RUN, pipelineId: 'chat-agent-loop', status: 'running', startedAt: `${DAY}T18:00:00.000Z` } as never,
    step: { stepId, status },
    attributedSubject: SUBJECT,
    occurredAt: at,
    safeLabel: title,
    ...(extra.workspace ? { workspace: extra.workspace } : {}),
    ...(extra.produces ? { producesRefs: [{ source: 'document' as const, resourceId: extra.produces }] } : {}),
    inputRefs: [
      { source: 'pipeline', resourceId: RUN },
      ...dependsOn.map((dependency) => ({ source: 'pipeline' as const, resourceId: pipelineStepResourceId(RUN, dependency) })),
    ],
  }, context);
  if (!mapped.supported) throw new Error(mapped.reason);
  return mapped.event as unknown as AuthenticatedWorkEvent;
}

/** A document the gateway already projected, so a `produced` claim can land. */
function documentEvent(at: string): AuthenticatedWorkEvent {
  return {
    schemaVersion: 1,
    eventId: `document:${at}`,
    idempotencyKey: `document:${at}`,
    source: 'document',
    sourceSequence: at,
    occurredAt: at,
    receivedAt: at,
    actor: { actorId: ACTOR, organizationId: ORGANIZATION, sourceSubject: SUBJECT },
    producer: { serviceId: 'websocket-gateway', organizationId: ORGANIZATION, credentialId: 'gateway' },
    resource: { source: 'document', resourceId: 'doc-1', resourceVersion: 'v1' },
    payload: {
      kind: 'document', lifecycle: 'revision-saved', documentId: 'doc-1', revisionId: 'v1', safeLabel: 'Team deck',
    },
  } as AuthenticatedWorkEvent;
}

function project(events: AuthenticatedWorkEvent[]): WorkProjectionState {
  return events.reduce((state, event) => projectWorkEvent(state, event), emptyState());
}

/** Everything the projection produced, disclosed in full to its own owner. */
function authorize(state: WorkProjectionState): {
  nodes: ViewerWorkNode[];
  edges: ViewerWorkEdge[];
  authorization: AuthorizedWorkGraph;
} {
  const nodes = Object.values(state.nodes).filter((node) => !node.deletedAt).map((node) => ({
    id: node.id,
    kind: node.kind,
    title: node.title,
    ...(node.description ? { description: node.description } : {}),
    status: node.status,
    disclosure: 'details' as const,
    startedAt: node.startedAt,
    updatedAt: node.updatedAt,
    capabilities: [],
  }));
  const edges = Object.values(state.edges).filter((edge) => !edge.deletedAt).map((edge) => ({
    id: edge.id,
    fromId: edge.fromId,
    toId: edge.toId,
    relation: edge.relation,
    status: edge.status,
    disclosure: 'details' as const,
    startedAt: edge.startedAt,
    updatedAt: edge.updatedAt,
    capabilities: [],
  }));
  const targets = new Map(Object.values(state.nodes).map((node) => [node.id, { kind: 'node' as const, node, sourceRef: node.sourceRef }]));
  return {
    nodes: nodes as ViewerWorkNode[],
    edges: edges as ViewerWorkEdge[],
    authorization: {
      nodes: nodes as ViewerWorkNode[],
      edges: edges as ViewerWorkEdge[],
      sources: [],
      partial: false,
      unavailableTargetIds: new Set<string>(),
      targets: targets as unknown as AuthorizedWorkGraph['targets'],
      publicTargetIds: new Map(),
    },
  };
}

function reader(facts: Record<string, PipelineRunFacts>): WorkActivityReader {
  return {
    pipelineRun: async (resource) => facts[resource] ?? null,
    artifact: async () => null,
    revisionPreview: async () => null,
    transcript: async () => null,
    meeting: async () => null,
    attention: async () => null,
  };
}

const NOW = `${DAY}T18:05:00.000Z`;
const query = {
  schemaVersion: 2 as const,
  personId: ACTOR,
  day: DAY,
  timezone: 'America/New_York',
  windowStart: `${DAY}T17:00:00.000Z`,
  windowEnd: NOW,
  mode: 'live' as const,
};

describe('an /agent loop in the requester\'s work graph', () => {
  const events = [
    conversationEvent(`${DAY}T17:59:00.000Z`),
    runEvent('running', `${DAY}T18:00:00.000Z`),
    stepEvent('agent:s1', 'Draft the agenda', 'running', `${DAY}T18:00:10.000Z`),
    stepEvent('agent:s2', 'Build the deck', 'pending', `${DAY}T18:00:10.000Z`, ['agent:s1']),
  ];

  it('projects the run, a node per step, and the conversation it came from', () => {
    const state = project(events);
    const byKind = Object.values(state.nodes).reduce<Record<string, string[]>>((acc, node) => {
      acc[node.kind] = [...(acc[node.kind] ?? []), node.title];
      return acc;
    }, {});
    expect(byKind.conversation).toEqual(['general']);
    expect(byKind.run?.sort()).toEqual([
      'Agent loop · create a meeting with the X team', 'Build the deck', 'Draft the agenda',
    ]);
    const steps = Object.values(state.nodes).filter((node) => node.sourceRef.resourceId.includes('#'));
    expect(steps.map((node) => node.sourceRef.resourceId).sort())
      .toEqual([`${RUN}#agent:s1`, `${RUN}#agent:s2`]);
    // A planned step is waiting; the one the executor reached is running.
    expect(steps.find((node) => node.title === 'Build the deck')?.status).toBe('waiting');
    expect(steps.find((node) => node.title === 'Draft the agenda')?.status).toBe('running');
  });

  it('relates each step to its run and to the step it depends on', () => {
    const state = project(events);
    const id = (resourceId: string) => Object.values(state.nodes)
      .find((node) => node.sourceRef.resourceId === resourceId)?.id;
    const relations = Object.values(state.edges).map((edge) => [edge.fromId, edge.toId, edge.relation]);
    expect(relations).toContainEqual([id(`${RUN}#agent:s1`), id(RUN), 'derived-from']);
    expect(relations).toContainEqual([id(`${RUN}#agent:s2`), id(`${RUN}#agent:s1`), 'derived-from']);
    expect(relations).toContainEqual([id(RUN), id(CHANNEL), 'derived-from']);
  });

  it('groups all of it into ONE effort anchored on the conversation', async () => {
    const state = project(events);
    const { nodes, edges, authorization } = authorize(state);
    const activity = await buildWorkActivity({
      query, nodes, edges, authorization, reader: reader({}), now: NOW,
    });
    expect(activity.efforts).toHaveLength(1);
    const [effort] = activity.efforts;
    const anchor = nodes.find((node) => node.id === effort.anchorNodeId);
    expect(anchor?.kind).toBe('conversation');
    expect(effort.nodeIds).toHaveLength(4);
    // "general" names no work; the run it started does.
    expect(effort.title).toBe('Agent loop · create a meeting with the X team');
  });

  it('describes a running step with its own model, budget and turns', async () => {
    const state = project(events);
    const { nodes, edges, authorization } = authorize(state);
    const activity = await buildWorkActivity({
      query,
      nodes,
      edges,
      authorization,
      reader: reader({
        [`${RUN}#agent:s1`]: {
          runId: RUN,
          status: 'running',
          attempt: 1,
          startedAt: `${DAY}T18:00:10.000Z`,
          updatedAt: `${DAY}T18:04:00.000Z`,
          tools: [],
          step: {
            stepId: 'agent:s1',
            model: 'sonnet',
            iterations: 4,
            contextTokens: 146_000,
            contextBudgetTokens: 200_000,
            runtime: 'devbox',
            sessionId: '28a319a9',
          },
        },
      }),
      now: NOW,
    });
    const stepNode = nodes.find((node) => node.title === 'Draft the agenda');
    const detail = activity.details.find((entry) => entry.nodeId === stepNode?.id);
    expect(detail?.lines?.[0]).toBe('Sonnet · 146k / 200k · 4 iterations');
    expect(detail?.lines?.[1]).toBe('Cloud workspace · 28a319a9');
    // The step is still inside its lease, so it reads as live rather than stale.
    expect(detail?.freshness).toBeDefined();
  });

  it('shows ONE node when the planner fell back to a single step', () => {
    const state = project([
      conversationEvent(`${DAY}T17:59:00.000Z`),
      runEvent('running', `${DAY}T18:00:00.000Z`),
      stepEvent('agent:s1', 'create a meeting with the X team', 'running', `${DAY}T18:00:10.000Z`),
    ]);
    const steps = Object.values(state.nodes).filter((node) => node.sourceRef.resourceId.includes('#'));
    expect(steps.map((node) => node.title)).toEqual(['create a meeting with the X team']);
  });

  it('drops the conversation relationship when that conversation was never projected', () => {
    const state = project([runEvent('running', `${DAY}T18:00:00.000Z`)]);
    expect(Object.values(state.nodes).map((node) => node.kind)).toEqual(['run']);
    expect(Object.values(state.edges)).toHaveLength(0);
  });
});

describe('workGraphPlanFromDefinition', () => {
  const definition = {
    id: 'chat-agent-loop',
    nodes: [
      { id: 'trigger', type: 'trigger', data: { type: 'trigger', label: '/agent command' } },
      { id: 'agent:s1', type: 'agent', data: { type: 'agent', label: 'Draft the agenda' } },
      { id: 'agent:s2', type: 'agent', data: { type: 'agent', label: 'Build the deck' } },
    ],
    edges: [
      { id: 'e1', source: 'trigger', target: 'agent:s1' },
      { id: 'e2', source: 'agent:s1', target: 'agent:s2' },
    ],
  };

  it('reads the steps and their dependencies from the definition being run', () => {
    expect(workGraphPlanFromDefinition(definition, CHANNEL)).toEqual({
      conversationId: CHANNEL,
      steps: [
        { stepId: 'agent:s1', title: 'Draft the agenda', dependsOn: [] },
        { stepId: 'agent:s2', title: 'Build the deck', dependsOn: ['agent:s1'] },
      ],
    });
  });

  it('never treats the trigger as a step', () => {
    const plan = workGraphPlanFromDefinition(definition, CHANNEL);
    expect(plan?.steps.map((step) => step.stepId)).not.toContain('trigger');
  });

  it('still names the conversation for a definition with no labelled steps', () => {
    expect(workGraphPlanFromDefinition({ nodes: [], edges: [] }, CHANNEL))
      .toEqual({ conversationId: CHANNEL, steps: [] });
    expect(workGraphPlanFromDefinition({ nodes: [], edges: [] })).toBeUndefined();
  });
});

describe('agentStepLines', () => {
  it('formats tokens the way the composer does', () => {
    expect(formatTokens(200_000)).toBe('200k');
    expect(formatTokens(146_320)).toBe('146k');
    expect(formatTokens(900)).toBe('900');
  });

  it('drops every clause the step did not report', () => {
    expect(agentStepLines({ stepId: 'agent:s1' })).toEqual([]);
    expect(agentStepLines({ stepId: 'agent:s1', model: 'sonnet' })).toEqual(['Sonnet']);
    expect(agentStepLines({ stepId: 'agent:s1', iterations: 1 })).toEqual(['1 iteration']);
  });

  it('says why a loop stopped short, and nothing when it simply finished', () => {
    expect(agentStepLines({ stepId: 'agent:s1', model: 'opus', stopReason: 'budget' }))
      .toEqual(['Opus · stopped: budget']);
    expect(agentStepLines({ stepId: 'agent:s1', model: 'opus', stopReason: 'complete' }))
      .toEqual(['Opus']);
  });

  it('names the workspace only for a devbox step that reported its session', () => {
    expect(agentStepLines({ stepId: 'agent:s1', runtime: 'local', sessionId: 'x' })).toEqual([]);
    expect(agentStepLines({ stepId: 'agent:s1', runtime: 'devbox', sessionId: 'x' }))
      .toEqual(['Cloud workspace · x']);
  });
});

describe('a running step is a leased operation, not a status word', () => {
  const WORKSPACE = { resourceId: 'session-28a319a9', safeLabel: 'Cloud workspace · box-7' };
  const events = [
    conversationEvent(`${DAY}T17:59:00.000Z`),
    runEvent('running', `${DAY}T18:00:00.000Z`),
    stepEvent('agent:s1', 'Draft the agenda', 'running', `${DAY}T18:04:30.000Z`, [], { workspace: WORKSPACE }),
  ];

  const facts: Record<string, PipelineRunFacts> = {
    [`${RUN}#agent:s1`]: {
      runId: RUN, status: 'running', attempt: 1,
      startedAt: `${DAY}T18:00:10.000Z`, updatedAt: `${DAY}T18:04:30.000Z`, tools: [],
      step: { stepId: 'agent:s1', model: 'sonnet', iterations: 4, contextTokens: 146_000, contextBudgetTokens: 200_000, runtime: 'devbox', sessionId: '28a319a9' },
    },
  };

  async function activityFor(source: AuthenticatedWorkEvent[], factsFor = facts) {
    const state = project(source);
    const { nodes, edges, authorization } = authorize(state);
    const activity = await buildWorkActivity({ query, nodes, edges, authorization, reader: reader(factsFor), now: NOW });
    return { state, nodes, edges, activity };
  }

  it('projects the devbox session as a terminal the step runs in', async () => {
    const { nodes, edges } = await activityFor(events);
    const terminal = nodes.find((node) => node.kind === 'terminal');
    expect(terminal?.title).toBe('Cloud workspace · box-7');
    const runsIn = edges.find((edge) => edge.relation === 'runs-in');
    expect(runsIn && nodes.find((node) => node.id === runsIn.fromId)?.title).toBe('Draft the agenda');
    expect(runsIn && nodes.find((node) => node.id === runsIn.toId)?.id).toBe(terminal?.id);
  });

  it('reports the running step as an operation with a lease that expires', async () => {
    const { nodes, activity } = await activityFor(events);
    const step = nodes.find((node) => node.title === 'Draft the agenda');
    expect(activity.operations).toHaveLength(1);
    const [operation] = activity.operations;
    expect(operation.processNodeId).toBe(step?.id);
    expect(operation.attemptId).toBe('attempt-1');
    expect(Date.parse(operation.expiresAt)).toBeGreaterThan(Date.parse(NOW));
    expect(Date.parse(operation.expiresAt)).toBeGreaterThan(Date.parse(operation.observedAt));
  });

  it('withdraws the operation when the step completes', async () => {
    const { activity } = await activityFor([
      ...events,
      stepEvent('agent:s1', 'Draft the agenda', 'completed', `${DAY}T18:04:45.000Z`, [], { workspace: WORKSPACE }),
    ], { [`${RUN}#agent:s1`]: { ...facts[`${RUN}#agent:s1`], status: 'completed' } });
    expect(activity.operations).toHaveLength(0);
  });

  it('withdraws the operation once the lease has run out, without a new event', async () => {
    const state = project(events);
    const { nodes, edges, authorization } = authorize(state);
    const stale = await buildWorkActivity({
      query, nodes, edges, authorization, reader: reader(facts), now: `${DAY}T19:00:00.000Z`,
    });
    expect(stale.operations).toHaveLength(0);
  });

  it('gives the step a `produced` edge to the document it is still writing', async () => {
    const withDocument = [
      documentEvent(`${DAY}T18:01:00.000Z`),
      ...events,
      stepEvent('agent:s1', 'Draft the agenda', 'running', `${DAY}T18:04:40.000Z`, [], { workspace: WORKSPACE, produces: 'doc-1' }),
    ];
    const { nodes, edges, activity } = await activityFor(withDocument);
    const produced = edges.find((edge) => edge.relation === 'produced');
    expect(produced && nodes.find((node) => node.id === produced.fromId)?.title).toBe('Draft the agenda');
    expect(produced && nodes.find((node) => node.id === produced.toId)?.kind).toBe('document');
    // Two process edges now, and both belong to the same running step.
    expect(new Set(activity.operations.map((operation) => operation.processNodeId)).size).toBe(1);
  });

  it('conjures nothing for a document this person has never been shown', async () => {
    const { edges } = await activityFor([
      ...events,
      stepEvent('agent:s1', 'Draft the agenda', 'running', `${DAY}T18:04:40.000Z`, [], { workspace: WORKSPACE, produces: 'doc-unseen' }),
    ]);
    expect(edges.filter((edge) => edge.relation === 'produced')).toHaveLength(0);
  });
});

describe('a run started in a channel the person has never posted in', () => {
  // The conversation node is the gateway's to publish, and it publishes it
  // when a message is PERSISTED. Until then there is no conversation node in
  // this person's projection, however real the channel is.
  const events = [
    runEvent('running', `${DAY}T18:00:00.000Z`, false),
    stepEvent('agent:s1', 'Draft the agenda', 'running', `${DAY}T18:04:30.000Z`),
  ];

  it('anchors the effort on the run itself rather than dropping it', async () => {
    const state = project(events);
    const { nodes, edges, authorization } = authorize(state);
    const activity = await buildWorkActivity({ query, nodes, edges, authorization, reader: reader({}), now: NOW });
    expect(nodes.some((node) => node.kind === 'conversation')).toBe(false);
    expect(activity.efforts).toHaveLength(1);
    const [effort] = activity.efforts;
    expect(nodes.find((node) => node.id === effort.anchorNodeId)?.kind).toBe('run');
    expect(effort.title).toBe('Agent loop · create a meeting with the X team');
    // The step travels with its run, not into an effort of its own.
    expect(effort.nodeIds).toHaveLength(2);
  });

  it('prefers the conversation once the person has posted in it', async () => {
    const state = project([conversationEvent(`${DAY}T17:59:00.000Z`), ...[
      runEvent('running', `${DAY}T18:00:00.000Z`),
      stepEvent('agent:s1', 'Draft the agenda', 'running', `${DAY}T18:04:30.000Z`),
    ]]);
    const { nodes, edges, authorization } = authorize(state);
    const activity = await buildWorkActivity({ query, nodes, edges, authorization, reader: reader({}), now: NOW });
    expect(activity.efforts).toHaveLength(1);
    expect(nodes.find((node) => node.id === activity.efforts[0].anchorNodeId)?.kind).toBe('conversation');
  });
});
