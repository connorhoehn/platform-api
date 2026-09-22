// The chat-command registry: registration, listing, `requires`, the run path
// through a stubbed bridge, and the result convention lifted onto the run
// snapshot — with a fake `/search` command standing in for what an embedder
// would register.

const mockRegisterTrigger = jest.fn();
const mockPostCard = jest.fn().mockResolvedValue(true);

jest.mock('../../pipeline/runPersistence', () => ({
  getRunPersistenceService: () => ({ registerTrigger: mockRegisterTrigger }),
}));
// The source-account join is a DDB write; the registry only has to CALL it.
const mockEnsurePrincipal = jest.fn().mockResolvedValue(true);
const mockEnsureSourcePrincipal = jest.fn().mockResolvedValue(true);
jest.mock('../../repositories', () => ({
  workSourceAccountRepo: {
    ensureVerifiedPrincipal: (...args: unknown[]) => mockEnsurePrincipal(...args),
    ensureVerifiedSourcePrincipal: (...args: unknown[]) => mockEnsureSourcePrincipal(...args),
  },
}));
jest.mock('../../documents/chatRunCard', () => ({
  postRunCardToChannel: (...args: unknown[]) => mockPostCard(...args),
  str: (v: unknown) => (typeof v === 'string' && v ? v : undefined),
}));
jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => l };
  return { __esModule: true, default: l, withContext: () => l };
});

import { setPipelineBridge } from '../../pipeline/bridge-types';
import type { PipelineBridge } from '../../pipeline/bridge-types';
import { getSystemPipeline } from '../../pipeline/systemPipelines';
import { createBridge, runItemToSnapshot } from '../../pipeline/createBridge';
import type { PipelineModule } from 'distributed-core/applications/pipeline';
import type { PipelineRunItem } from '../../repositories/PipelineRunRepository';
import {
  registerChatCommandPipeline, getChatCommandPipeline, listChatCommandPipelines, runChatCommand,
  clearChatCommandPipelines, normalizeTrigger, parseRunResult, runResultFromContext,
  ChatCommandRequiresError, UnknownChatCommandError,
  type ChatCommandPipeline, type ChatCommandRunInput,
} from '../chatCommandRegistry';

// A `/search` command an embedder might register: one custom step that
// answers with a panel to open. `search:graph` has a colon, so the system-
// pipeline lint wants a retry policy on it.
const SEARCH_RESULT = { kind: 'panel', panel: { kind: 'graph', id: 'k8s-experts', title: 'People who know k8s' } } as const;
const searchCommand: ChatCommandPipeline = {
  trigger: '/search',
  pipelineId: 'embedder-search',
  description: 'Find the people who know about something.',
  requires: { argumentText: true },
  buildDefinition: () => ({
    id: 'embedder-search', name: 'Search', version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: { x: 0, y: 0 }, data: { type: 'trigger', triggerType: 'manual', label: '/search' } },
      { id: 'search', type: 'search:graph', position: { x: 200, y: 0 }, data: { type: 'search:graph', label: 'Search the graph', retryPolicy: { maxAttempts: 2, backoffMs: 500 } } },
    ],
    edges: [{ id: 'trigger->search', source: 'trigger', sourceHandle: 'out', target: 'search', targetHandle: 'in' }],
  }),
  triggerPayload: (input) => ({ query: input.argumentText, instruction: `Search: ${input.argumentText}` }),
  card: (input) => ({ query: input.argumentText, opens: 'panel' }),
};

const input = (over: Partial<ChatCommandRunInput> = {}): ChatCommandRunInput => ({
  trigger: '/search', channel: 'room:platform', text: '/search k8s experts', argumentText: 'k8s experts',
  requestedBy: 'u-grace', requestedByName: 'Grace', ...over,
});

let trigger: jest.Mock;
beforeEach(() => {
  clearChatCommandPipelines();
  mockRegisterTrigger.mockReset();
  mockPostCard.mockReset().mockResolvedValue(true);
  trigger = jest.fn().mockResolvedValue({ runId: 'run-1' });
  setPipelineBridge({ trigger } as unknown as PipelineBridge);
});
afterEach(() => setPipelineBridge(null));

describe('registration', () => {
  test('normalises the trigger and lists what a client needs to offer the command', () => {
    registerChatCommandPipeline({ ...searchCommand, trigger: 'Search' });
    expect(getChatCommandPipeline('/search')?.pipelineId).toBe('embedder-search');
    expect(getChatCommandPipeline('search')?.pipelineId).toBe('embedder-search');
    expect(listChatCommandPipelines()).toEqual([
      { trigger: '/search', pipelineId: 'embedder-search', description: 'Find the people who know about something.', requires: { argumentText: true } },
    ]);
    expect(normalizeTrigger('/Doc ')).toBe('/doc');
  });

  test('is idempotent by trigger — the later registration wins', () => {
    registerChatCommandPipeline(searchCommand);
    registerChatCommandPipeline({ ...searchCommand, description: 'Newer' });
    expect(listChatCommandPipelines()).toHaveLength(1);
    expect(getChatCommandPipeline('/search')?.description).toBe('Newer');
  });

  test('refuses a command without a trigger or a pipelineId', () => {
    expect(() => registerChatCommandPipeline({ ...searchCommand, trigger: '' })).toThrow(/trigger/);
    expect(() => registerChatCommandPipeline({ ...searchCommand, pipelineId: '' })).toThrow(/pipelineId/);
  });
});

describe('requires', () => {
  test('an unknown trigger is a 404 naming the known ones', async () => {
    registerChatCommandPipeline(searchCommand);
    const err = await runChatCommand(input({ trigger: '/nope' })).catch((e) => e);
    expect(err).toBeInstanceOf(UnknownChatCommandError);
    expect(err.status).toBe(404);
    expect(err.known).toEqual(['/search']);
    expect(trigger).not.toHaveBeenCalled();
  });

  test('every unmet requirement is named and the bridge is never reached', async () => {
    registerChatCommandPipeline({ ...searchCommand, requires: { document: true, argument: true, argumentText: true, extra: ['sourceDocumentId'] } });
    const err = await runChatCommand(input({ argumentText: '  ' })).catch((e) => e);
    expect(err).toBeInstanceOf(ChatCommandRequiresError);
    expect(err.status).toBe(400);
    expect(err.message).toBe('/search needs documentId, argument, argumentText, extra.sourceDocumentId');
    expect(err.requires).toEqual({ document: true, argument: true, argumentText: true, extra: ['sourceDocumentId'] });
    expect(trigger).not.toHaveBeenCalled();
  });

  test('a met requirement passes', async () => {
    registerChatCommandPipeline({ ...searchCommand, requires: { document: true, argument: true, extra: ['sourceDocumentId'] } });
    await expect(runChatCommand(input({ documentId: 'd-1', argument: { id: 't-1', label: 'RFC' }, extra: { sourceDocumentId: 'd-0' } }))).resolves.toMatchObject({ runId: 'run-1' });
  });

  test('a missing bridge is a 503, not a crash', async () => {
    setPipelineBridge(null);
    registerChatCommandPipeline(searchCommand);
    const err = await runChatCommand(input()).catch((e) => e);
    expect(err.status).toBe(503);
  });
});

describe('runChatCommand — the fake /search through a stubbed bridge', () => {
  test('triggers like the bespoke routes, registers the trigger, posts and returns the card', async () => {
    registerChatCommandPipeline(searchCommand);
    const out = await runChatCommand(input());

    expect(out).toEqual({
      runId: 'run-1', pipelineId: 'embedder-search',
      card: { kind: 'agent-run', command: '/search', instruction: 'Search: k8s experts', runId: 'run-1', pipelineId: 'embedder-search', query: 'k8s experts', opens: 'panel' },
    });
    expect(trigger).toHaveBeenCalledTimes(1);
    const args = trigger.mock.calls[0][0];
    expect(args.pipelineId).toBe('embedder-search');
    expect(args.definition.nodes.map((n: { id: string }) => n.id)).toEqual(['trigger', 'search']);
    expect(args.triggeredBy).toEqual({ userId: 'u-grace' });
    // The payload is the command's, filled in with what every run needs.
    expect(args.triggerPayload).toMatchObject({ query: 'k8s experts', requestedBy: 'u-grace', requestedByName: 'Grace', chatChannel: 'room:platform' });
    expect(typeof args.triggerPayload.requestId).toBe('string');
    expect(mockRegisterTrigger).toHaveBeenCalledWith(expect.objectContaining({ runId: 'run-1', pipelineId: 'embedder-search', userId: 'u-grace', triggerType: 'manual' }));
    // Default: the card goes into the command's channel, carrying the command's own metadata.
    expect(mockPostCard).toHaveBeenCalledWith({ channel: 'room:platform', runId: 'run-1', pipelineId: 'embedder-search', command: '/search', instruction: 'Search: k8s experts', extra: { query: 'k8s experts', opens: 'panel' } });
    // The first run registered the pipeline as a system pipeline, so it lists with its steps named.
    expect(getSystemPipeline('embedder-search')).toMatchObject({ id: 'embedder-search', system: true });
  });

  test('postCard: false leaves the card to the client (it still gets the metadata)', async () => {
    registerChatCommandPipeline({ ...searchCommand, postCard: false });
    const out = await runChatCommand(input({ documentId: 'd-9' }));
    expect(mockPostCard).not.toHaveBeenCalled();
    expect(out.card).toMatchObject({ kind: 'agent-run', documentId: 'd-9' });
  });

  test('clientPostsCard: the flag rides in every trigger payload, and the dispatcher does not post a second card', async () => {
    registerChatCommandPipeline(searchCommand);
    const out = await runChatCommand({ ...input(), clientPostsCard: true });
    expect(trigger.mock.calls[0][0].triggerPayload).toMatchObject({ clientPostsCard: true });
    expect(mockPostCard).not.toHaveBeenCalled();
    expect(out.card).toMatchObject({ kind: 'agent-run', runId: 'run-1' });
    // A run with no client says so, and the default post happens.
    await runChatCommand(input());
    expect(trigger.mock.calls[1][0].triggerPayload).toMatchObject({ clientPostsCard: false });
    expect(mockPostCard).toHaveBeenCalledTimes(1);
  });

  test('prepare runs before the builders and can enrich the input', async () => {
    registerChatCommandPipeline({
      ...searchCommand,
      prepare: async (i) => ({ ...i, extra: { ...i.extra, resolved: 'yes' } }),
      triggerPayload: (i) => ({ resolved: i.extra?.resolved }),
    });
    await runChatCommand(input());
    expect(trigger.mock.calls[0][0].triggerPayload.resolved).toBe('yes');
  });

  test('when the run finishes with the result convention, the snapshot carries it top-level', () => {
    // What `GET /api/pipelines/:pipelineId/runs/:runId` answers once the search step returned
    // `{ status: 'completed', output: { result: SEARCH_RESULT } }` and the executor merged it in.
    const mod = { getRun: jest.fn().mockReturnValue({ id: 'run-1', pipelineId: 'embedder-search', status: 'completed', context: { query: 'k8s experts', result: SEARCH_RESULT } }) };
    const snap = createBridge(mod as unknown as PipelineModule).getRun('run-1') as Record<string, unknown> | null;
    expect(snap?.result).toEqual(SEARCH_RESULT);
  });
});

describe('the result convention', () => {
  test('accepts the three shapes and nothing half-formed', () => {
    expect(parseRunResult(SEARCH_RESULT)).toEqual(SEARCH_RESULT);
    expect(parseRunResult({ kind: 'document', documentId: 'd-1' })).toEqual({ kind: 'document', documentId: 'd-1' });
    expect(parseRunResult({ kind: 'link', href: 'https://x', label: 'X' })).toEqual({ kind: 'link', href: 'https://x', label: 'X' });
    expect(parseRunResult({ kind: 'panel', panel: { kind: 'graph' } })).toBeUndefined();
    expect(parseRunResult({ kind: 'document' })).toBeUndefined();
    expect(parseRunResult({ kind: 'link', href: 'https://x' })).toBeUndefined();
    expect(parseRunResult({ kind: 'other' })).toBeUndefined();
    expect(parseRunResult('panel')).toBeUndefined();
    expect(runResultFromContext({ steps: {} })).toBeUndefined();
    expect(runResultFromContext(undefined)).toBeUndefined();
  });

  test('a persisted run lifts it from its last checkpoint context', () => {
    const item = {
      pipelineId: 'embedder-search', runId: 'run-1', status: 'completed', triggeredBy: 'u-grace', triggerType: 'manual', startedAt: '2026-09-09T00:00:00Z',
      checkpoint: { sequenceNumber: 3, at: '2026-09-09T00:00:01Z', context: { result: SEARCH_RESULT }, currentStepIds: [] },
    } as unknown as PipelineRunItem;
    expect((runItemToSnapshot(item) as Record<string, unknown>).result).toEqual(SEARCH_RESULT);
    expect((runItemToSnapshot({ ...item, checkpoint: undefined }) as Record<string, unknown>).result).toBeUndefined();
  });
});

// The /agent loop answers its POST before the run exists, plans in the
// background, then RE-ENTERS `runChatCommand` with the plan in `extra`. That
// second pass is the only one that builds the real definition, so everything
// the work graph is told about the run is decided there.
describe('the work-graph plan on the async-planning re-entry', () => {
  const planned: ChatCommandPipeline = {
    trigger: '/agent',
    pipelineId: 'chat-agent-loop',
    description: 'plan an instruction into agent steps',
    buildDefinition: () => ({
      id: 'chat-agent-loop',
      name: 'Agent loop',
      nodes: [
        { id: 'trigger', type: 'trigger', position: { x: 0, y: 0 }, data: { type: 'trigger', label: '/agent command' } },
        { id: 'agent:s1', type: 'agent', position: { x: 1, y: 0 }, data: { type: 'agent', label: 'Draft the agenda', retryPolicy: { maxAttempts: 2, backoffMs: 5000 } } },
        { id: 'agent:s2', type: 'agent', position: { x: 2, y: 0 }, data: { type: 'agent', label: 'Build the deck', retryPolicy: { maxAttempts: 2, backoffMs: 5000 } } },
        { id: 'deck:s2', type: 'deck', position: { x: 3, y: 0 }, data: { type: 'deck', label: 'Render the deck', retryPolicy: { maxAttempts: 2, backoffMs: 5000 } } },
      ],
      edges: [
        { id: 'e1', source: 'trigger', target: 'agent:s1' },
        { id: 'e2', source: 'agent:s1', target: 'agent:s2' },
        { id: 'e3', source: 'agent:s2', target: 'deck:s2' },
      ],
    }),
    triggerPayload: () => ({ instruction: 'create a meeting with the X team' }),
  };

  const input = (): ChatCommandRunInput => ({
    trigger: '/agent', channel: 'chat:c:general', text: '/agent create a meeting with the X team',
    argumentText: 'create a meeting with the X team',
    requestedBy: 'dev-frank', requestedByName: 'Frank', organizationId: 'dev-org',
    extra: { agentLoop: { plan: { source: 'planner', steps: [] } } },
  });

  beforeEach(() => {
    clearChatCommandPipelines();
    mockRegisterTrigger.mockClear();
    registerChatCommandPipeline(planned);
  });

  /**
   * The engine NORMALISES the definition it is handed — this stands in for
   * that. Reading the plan after the trigger returned a pipeline name with no
   * steps at all, and every run then projected as one opaque node.
   */
  const normalisingBridge = (): PipelineBridge => ({
    trigger: (args: { definition?: Record<string, unknown> }) => {
      for (const node of (args.definition?.nodes ?? []) as Array<Record<string, unknown>>) {
        node.data = { type: (node.data as Record<string, unknown>)?.type };
      }
      return { runId: 'executor-run-1' };
    },
  } as unknown as PipelineBridge);

  it('registers every planned step even though the bridge rewrites the definition', async () => {
    setPipelineBridge(normalisingBridge());
    await runChatCommand(input());
    expect(mockRegisterTrigger).toHaveBeenCalledTimes(1);
    const args = mockRegisterTrigger.mock.calls[0][0] as {
      runId: string; organizationId?: string; workGraph?: { title?: string; conversationId?: string; steps: Array<{ stepId: string; title: string; dependsOn: string[] }> };
    };
    expect(args.runId).toBe('executor-run-1');
    expect(args.organizationId).toBe('dev-org');
    expect(args.workGraph?.title).toBe('Agent loop');
    expect(args.workGraph?.conversationId).toBe('chat:c:general');
    expect(args.workGraph?.steps).toEqual([
      { stepId: 'agent:s1', title: 'Draft the agenda', dependsOn: [] },
      { stepId: 'agent:s2', title: 'Build the deck', dependsOn: ['agent:s1'] },
      { stepId: 'deck:s2', title: 'Render the deck', dependsOn: ['agent:s2'] },
    ]);
  });

  it('joins the pipeline source account before the run can publish anything', async () => {
    setPipelineBridge(normalisingBridge());
    await runChatCommand(input());
    expect(mockEnsurePrincipal).toHaveBeenCalledWith({
      source: 'pipeline', organizationId: 'dev-org', subject: 'dev-frank',
    });
  });

  it('joins the document source under the producer every document join lives under', async () => {
    // A run that renders a deck publishes on the document source, and that
    // join's producer is not part of its key — so it has to match the one the
    // gateway already wrote or the deck is refused as unmapped.
    setPipelineBridge(normalisingBridge());
    await runChatCommand(input());
    expect(mockEnsureSourcePrincipal).toHaveBeenCalledWith({
      source: 'document', producerServiceId: 'websocket-gateway',
      organizationId: 'dev-org', subject: 'dev-frank',
    });
  });

  it('carries the verified organization, without which nothing is projected', async () => {
    setPipelineBridge(normalisingBridge());
    await runChatCommand({ ...input(), organizationId: undefined });
    expect((mockRegisterTrigger.mock.calls[0][0] as { organizationId?: string }).organizationId).toBeUndefined();
  });
});
