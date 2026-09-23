// The /search plugin: term parsing, ranking + tags over a stubbed profile
// source, the definition against the system-pipeline lint, the publish step's
// card, and the registry listing the command once the plugins register.

const mockPostCard = jest.fn().mockResolvedValue(true);
const mockListProfiles = jest.fn();

jest.mock('../../../repositories', () => ({
  profileRepo: { listProfiles: (...a: unknown[]) => mockListProfiles(...a) },
}));
jest.mock('../../../documents/chatRunCard', () => ({
  postRunCard: (...args: unknown[]) => mockPostCard(...args),
  runCardOutcomeOf: jest.requireActual('../../../documents/chatRunCard').runCardOutcomeOf,
  runCardOutputOf: jest.requireActual('../../../documents/chatRunCard').runCardOutputOf,
  runCardMetadata: (i: { runId: string; command: string; extra?: Record<string, unknown> }) => ({ kind: 'agent-run', runId: i.runId, command: i.command, ...(i.extra ?? {}) }),
  str: (v: unknown) => (typeof v === 'string' && v ? v : undefined),
}));
jest.mock('../../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => l };
  return { __esModule: true, default: l, withContext: () => l };
});

import type { PipelineNode } from 'distributed-core/applications/pipeline';
import type { ProfileItem } from '../../../repositories';
import { lintSystemPipeline } from '../../../pipeline/systemPipelineLint';
import { clearChatCommandPipelines, listChatCommandPipelines, getChatCommandPipeline } from '../../chatCommandRegistry';
import { installBuiltinChatCommandPlugins } from '..';
import { clearCustomStepHandlers, getCustomStepHandler, getCustomStepHandlers } from '../../../pipeline/customStepHandlers';
import {
  parseSearchTerms, rankPeople, panelTitle, peoplePanelId, buildPeopleSearchDefinition,
  createPeopleSearchHandler, createPeoplePublishHandler, peopleSearchCommand, peopleSearchPlugin,
  PEOPLE_SEARCH_KIND, PEOPLE_PUBLISH_KIND, PEOPLE_SEARCH_PIPELINE_ID,
} from '../peopleSearch';

const profile = (userId: string, displayName: string, bio: string, extra: Partial<ProfileItem> = {}): ProfileItem => ({
  userId, displayName, bio, avatarUrl: '', visibility: 'public', createdAt: '2026-01-01T00:00:00Z', updatedAt: '2026-01-01T00:00:00Z', ...extra,
});
const node = { id: 'n', type: 'x', position: { x: 0, y: 0 }, data: { type: 'x' } } as unknown as PipelineNode;
const helpers = { runId: 'run-9', ownerNodeId: 'node-1', emit: jest.fn() } as unknown as Parameters<ReturnType<typeof createPeopleSearchHandler>['execute']>[2];

beforeEach(() => {
  mockPostCard.mockReset().mockResolvedValue(true);
  mockListProfiles.mockReset();
});

describe('parseSearchTerms', () => {
  test('drops filler, lowercases, folds duplicates, keeps order', () => {
    expect(parseSearchTerms('find me all kubernetes experts')).toEqual(['kubernetes']);
    expect(parseSearchTerms('Who are the Terraform and Kubernetes experts in Berlin')).toEqual(['terraform', 'kubernetes', 'berlin']);
    expect(parseSearchTerms('k8s k8s C++ .net')).toEqual(['k8s', 'c++', 'net']);
  });
  test('nothing usable → no terms', () => {
    expect(parseSearchTerms('find me all the experts')).toEqual([]);
    expect(parseSearchTerms(undefined)).toEqual([]);
    expect(parseSearchTerms('   ')).toEqual([]);
  });
});

describe('rankPeople', () => {
  const people = [
    profile('u-hank', 'Hank Hill', 'SRE, kubernetes and terraform'),
    profile('u-bob', 'Bob Martinez', 'Frontend; some kubernetes on the side', { avatarUrl: 'https://x/bob.png' }),
    profile('u-kube', 'Kubernetes Kate', 'Designer'),
    profile('u-nobody', 'Alice Chen', 'Data engineer'),
  ];
  test('score = matched terms across name/bio, tags = terms found in the bio, best first', () => {
    const ranked = rankPeople(people, ['kubernetes', 'terraform']);
    expect(ranked).toEqual([
      { id: 'u-hank', name: 'Hank Hill', tags: ['kubernetes', 'terraform'], score: 2 },
      { id: 'u-bob', name: 'Bob Martinez', avatarUrl: 'https://x/bob.png', tags: ['kubernetes'], score: 1 },
      // A name match scores but does not tag: tags are what the bio says they know.
      { id: 'u-kube', name: 'Kubernetes Kate', tags: [], score: 1 },
    ]);
  });
  test('title-like fields count toward the score and ride along', () => {
    const withTitle = profile('u-t', 'Tia', 'nothing here', { title: 'Kubernetes lead' } as Partial<ProfileItem>);
    expect(rankPeople([withTitle], ['kubernetes'])).toEqual([{ id: 'u-t', name: 'Tia', title: 'Kubernetes lead', tags: [], score: 1 }]);
  });
  test('caps at the limit and ignores malformed rows', () => {
    const many = Array.from({ length: 40 }, (_, i) => profile(`u-${i}`, `P${i}`, 'kubernetes'));
    expect(rankPeople([...many, { bio: 'kubernetes' } as unknown as ProfileItem], ['kubernetes'])).toHaveLength(25);
    expect(rankPeople(many, [])).toEqual([]);
  });
});

describe('the definition', () => {
  test('passes the system-pipeline lint and names both custom kinds', () => {
    const def = buildPeopleSearchDefinition();
    expect(lintSystemPipeline(def)).toEqual([]);
    const kinds = (def.nodes as Array<{ data: { type: string } }>).map((n) => n.data.type);
    expect(kinds).toEqual(['trigger', PEOPLE_SEARCH_KIND, PEOPLE_PUBLISH_KIND]);
    clearCustomStepHandlers();
    installBuiltinChatCommandPlugins();
    expect([...getCustomStepHandlers().keys()]).toEqual([PEOPLE_SEARCH_KIND, PEOPLE_PUBLISH_KIND]);
  });
});

describe('people:search step', () => {
  test('answers with people, panelData and the panel result whose id carries pipelineId:runId', async () => {
    mockListProfiles.mockResolvedValue([profile('u-hank', 'Hank Hill', 'SRE, kubernetes and terraform'), profile('u-a', 'Alice', 'data')]);
    const out = await createPeopleSearchHandler().execute(node, { query: 'find me all kubernetes experts' }, helpers);
    expect(out.status).toBe('completed');
    expect(mockListProfiles).toHaveBeenCalledWith(500);
    expect(out.output).toEqual({
      query: 'find me all kubernetes experts',
      terms: ['kubernetes'],
      people: [{ id: 'u-hank', name: 'Hank Hill', tags: ['kubernetes'], score: 1 }],
      panelData: { title: 'People: kubernetes', people: [{ id: 'u-hank', name: 'Hank Hill', tags: ['kubernetes'], score: 1 }], query: 'find me all kubernetes experts' },
      result: { kind: 'panel', panel: { kind: 'graph', id: 'people-search:run-9', title: 'People: kubernetes' } },
    });
    expect(peoplePanelId('run-9')).toBe(`${PEOPLE_SEARCH_PIPELINE_ID}:run-9`);
  });
  test('a query of only filler does not read profiles and still answers a (empty) panel', async () => {
    const out = await createPeopleSearchHandler().execute(node, { query: 'find me the experts' }, helpers);
    expect(mockListProfiles).not.toHaveBeenCalled();
    expect(out.output).toMatchObject({ terms: [], people: [], panelData: { title: 'People', people: [] }, result: { kind: 'panel', panel: { kind: 'graph', id: 'people-search:run-9', title: 'People' } } });
    expect(panelTitle([])).toBe('People');
  });
  test('a failing source fails the step (the executor retries it)', async () => {
    const source = { listProfiles: jest.fn().mockRejectedValue(new Error('ddb down')) };
    const out = await createPeopleSearchHandler({ source }).execute(node, { query: 'kubernetes' }, helpers);
    expect(out).toEqual({ status: 'failed', error: 'ddb down' });
  });
});

describe('people:publish step', () => {
  test('posts the card with the result in its metadata and reports posted', async () => {
    const result = { kind: 'panel', panel: { kind: 'graph', id: 'people-search:run-9', title: 'People: kubernetes' } };
    const ctx = { chatChannel: 'room:pipeline-live', query: 'find me all kubernetes experts', terms: ['kubernetes'], people: [{ id: 'u-hank', name: 'Hank', tags: ['kubernetes'], score: 1 }, { id: 'u-bob', name: 'Bob', tags: ['kubernetes'], score: 1 }], result };
    const out = await createPeoplePublishHandler().execute(node, ctx, helpers);
    // The run context rides along so the post can step down when the client posted the card.
    expect(mockPostCard).toHaveBeenCalledWith({
      channel: 'room:pipeline-live', runId: 'run-9', pipelineId: 'people-search', command: '/search',
      instruction: '2 people match "kubernetes"',
      extra: { result, query: 'find me all kubernetes experts', terms: ['kubernetes'], matched: 2 },
      context: ctx,
    });
    expect(out).toEqual({
      status: 'completed',
      output: { posted: true, card: expect.objectContaining({ kind: 'agent-run', runId: 'run-9', command: '/search', result, matched: 2 }), matched: 2, instruction: '2 people match "kubernetes"', result },
    });
  });
  test('clientPostsCard in the context reaches the post, and the card metadata is in the output even when nothing was posted', async () => {
    mockPostCard.mockResolvedValue(false);
    const ctx = { chatChannel: 'room:r', clientPostsCard: true, terms: ['k8s'], people: [] };
    const out = await createPeoplePublishHandler().execute(node, ctx, helpers);
    expect(mockPostCard.mock.calls[0][0].context).toBe(ctx);
    expect(out.output).toMatchObject({ posted: false, card: { kind: 'agent-run', runId: 'run-9', command: '/search' } });
  });
  test('one match reads as a person, and a missing result is rebuilt from the run id', async () => {
    const out = await createPeoplePublishHandler().execute(node, { chatChannel: 'room:r', terms: ['k8s'], people: [{ id: 'u', name: 'U', tags: [], score: 1 }] }, helpers);
    expect(mockPostCard.mock.calls[0][0].instruction).toBe('1 person matches "k8s"');
    expect(out.output).toMatchObject({ result: { kind: 'panel', panel: { kind: 'graph', id: 'people-search:run-9', title: 'People: k8s' } } });
  });
});

describe('registration', () => {
  test('the built-in plugin registers /search with what it needs and both step kinds; the trigger payload carries the query and channel', () => {
    clearChatCommandPipelines();
    clearCustomStepHandlers();
    expect(peopleSearchPlugin.name).toBe('people-search');
    installBuiltinChatCommandPlugins();
    expect(getCustomStepHandler(PEOPLE_SEARCH_KIND)?.kind).toBe(PEOPLE_SEARCH_KIND);
    expect(getCustomStepHandler(PEOPLE_PUBLISH_KIND)?.kind).toBe(PEOPLE_PUBLISH_KIND);
    expect(listChatCommandPipelines()).toEqual([
      { trigger: '/search', pipelineId: 'people-search', description: 'Find people by what they know', label: 'Find people', hint: 'by what they know', requires: { argumentText: true } },
    ]);
    const def = getChatCommandPipeline('search')!;
    expect(def.postCard).toBe(false);
    const input = { trigger: '/search', channel: 'room:pipeline-live', text: '/search find me all kubernetes experts', argumentText: 'find me all kubernetes experts', requestedBy: 'dev-grace', requestedByName: 'Grace' };
    expect(def.triggerPayload(input)).toEqual({ query: 'find me all kubernetes experts', chatChannel: 'room:pipeline-live', requestedBy: 'dev-grace', requestedByName: 'Grace' });
    expect(def.card!(input, { runId: 'run-1' })).toEqual({ query: 'find me all kubernetes experts', opens: 'panel', panelKind: 'graph' });
    expect(peopleSearchCommand.buildDefinition(input)).toEqual(buildPeopleSearchDefinition());
  });
});
