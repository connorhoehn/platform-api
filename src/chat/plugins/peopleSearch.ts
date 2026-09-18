// `/search find me all kubernetes experts` — a chat command backed by a
// pipeline, written the way an EMBEDDER would write one: nothing here touches
// the registry's code, the run-result convention, or the run-detail route. It
// is a `ChatCommandPlugin` (`peopleSearchPlugin`, name `people-search`)
// installed through the same loader an external plugin goes through
// (src/chat/chatCommandPlugin.ts): its `setup` registers the command and the
// two custom steps the definition names, and the run answers with a `panel`
// result the card can open. No panel data provider — the panels route serves
// `panelData` from the run itself.
//
//   people-search:  trigger → people:search → people:publish
//
// `people:search` turns the text after the command into terms (filler words
// dropped), matches every profile's name and bio against them, tags each
// person with the terms found in their bio, and keeps the 25 best. It also
// writes `panelData` — what the side panel renders — and `result`, the
// run-result convention (src/pipeline/runResult.ts) that gets lifted onto the
// run snapshot. `people:publish` posts the card into the conversation with
// that same `result` in its metadata, so the card can open the panel before
// (or without) anyone polling the run.
//
// Panel id scheme — `<pipelineId>:<runId>`. The panel route has to find the
// run again later; the bridge's `getRun(runId)` is runId-only but forgets
// every run once the process restarts, and the durable row is keyed by
// (pipelineId, runId). The id carries both so the panel outlives the process.
import type { DescribedStepHandler } from '../../pipeline/customStepHandlers';
import type { StepHandler, StepOutcome, PipelineNode } from 'distributed-core/applications/pipeline';
import type { ProfileItem } from '../../repositories';
import { profileRepo } from '../../repositories';
import { postRunCardToChannel, runCardMetadata, str } from '../../documents/chatRunCard';
import { registerChatCommandPipeline, type ChatCommandPipeline } from '../chatCommandRegistry';
import type { ChatCommandPlugin } from '../chatCommandPlugin';

export const PEOPLE_SEARCH_PIPELINE_ID = 'people-search';
export const PEOPLE_SEARCH_KIND = 'people:search';
export const PEOPLE_PUBLISH_KIND = 'people:publish';
export const PEOPLE_PANEL_KIND = 'graph';
export const MAX_PEOPLE = 25;
/** How many profiles one search reads; the repository's own scan cap. */
export const PROFILE_SCAN_LIMIT = 500;

const RETRY = { maxAttempts: 2, backoffMs: 500 } as const;

/** Words that carry no meaning for a people search. */
export const FILLER_WORDS: ReadonlySet<string> = new Set([
  'find', 'me', 'all', 'show', 'who', 'are', 'experts', 'expert', 'in', 'on', 'with', 'the',
  'a', 'an', 'of', 'for', 'and', 'or', 'is', 'knows', 'know', 'about', 'people', 'person', 'anyone', 'someone',
]);

/** The search terms in a query: lowercase words, filler dropped, duplicates folded, order kept. */
export function parseSearchTerms(query: unknown): string[] {
  if (typeof query !== 'string') return [];
  const out: string[] = [];
  for (const raw of query.toLowerCase().split(/[^a-z0-9+#.-]+/)) {
    const word = raw.replace(/^[.-]+|[.-]+$/g, '');
    if (word.length < 2 || FILLER_WORDS.has(word) || out.includes(word)) continue;
    out.push(word);
  }
  return out;
}

export interface Person {
  id: string;
  name: string;
  title?: string;
  avatarUrl?: string;
  /** The query terms found in this person's bio. */
  tags: string[];
  /** How many query terms matched anywhere (name, bio, title). */
  score: number;
}

export interface PeoplePanelData { title: string; people: Person[]; query: string }

/** Where the search reads people from; the profile repository by default, a stub in tests. */
export interface PeopleSource { listProfiles(limit: number): Promise<ProfileItem[]> }

/** A title-like field, when a profile carries one (`title`, `headline`, `role`); the core profile has none. */
function titleOf(p: ProfileItem): string | undefined {
  const extra = p as unknown as Record<string, unknown>;
  return str(extra.title) ?? str(extra.headline) ?? str(extra.role);
}

/** Score every profile against the terms; those matching nothing are dropped, the rest ordered best first. */
export function rankPeople(profiles: readonly ProfileItem[], terms: readonly string[], limit = MAX_PEOPLE): Person[] {
  if (terms.length === 0) return [];
  const ranked: Person[] = [];
  for (const p of profiles) {
    if (!p || typeof p.userId !== 'string') continue;
    const name = typeof p.displayName === 'string' ? p.displayName : '';
    const bio = typeof p.bio === 'string' ? p.bio.toLowerCase() : '';
    const title = titleOf(p);
    const nameL = name.toLowerCase();
    const titleL = (title ?? '').toLowerCase();
    const tags: string[] = [];
    let score = 0;
    for (const term of terms) {
      const inBio = bio.includes(term);
      if (inBio) tags.push(term);
      if (inBio || nameL.includes(term) || titleL.includes(term)) score += 1;
    }
    if (score === 0) continue;
    ranked.push({
      id: p.userId, name: name || p.userId,
      ...(title ? { title } : {}),
      ...(typeof p.avatarUrl === 'string' && p.avatarUrl ? { avatarUrl: p.avatarUrl } : {}),
      tags, score,
    });
  }
  ranked.sort((a, b) => b.score - a.score || a.name.localeCompare(b.name));
  return ranked.slice(0, limit);
}

export function panelTitle(terms: readonly string[]): string {
  return terms.length ? `People: ${terms.join(', ')}` : 'People';
}

/** The panel id the route resolves — see the header. */
export function peoplePanelId(runId: string, pipelineId = PEOPLE_SEARCH_PIPELINE_ID): string {
  return `${pipelineId}:${runId}`;
}

export function buildPeopleSearchDefinition(): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  const edge = (source: string, target: string) => ({ id: `${source}->${target}`, source, sourceHandle: 'out', target, targetHandle: 'in' });
  return {
    id: PEOPLE_SEARCH_PIPELINE_ID,
    name: 'People search',
    description: 'A /search command from a chat: match people by what their profile says they know, answer with a people panel, leave a card in the thread.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'platform', label: '/search command' } },
      { id: 'search', type: PEOPLE_SEARCH_KIND, position: pos(200), data: { type: PEOPLE_SEARCH_KIND, label: 'Search people', retryPolicy: RETRY } },
      { id: 'publish', type: PEOPLE_PUBLISH_KIND, position: pos(400), data: { type: PEOPLE_PUBLISH_KIND, label: 'Post the card to the conversation', retryPolicy: RETRY } },
    ],
    edges: [edge('trigger', 'search'), edge('search', 'publish')],
  };
}

/** Step 1: terms → people → `panelData` + `result`. */
export function createPeopleSearchHandler(deps: { source?: PeopleSource } = {}): DescribedStepHandler {
  const source: PeopleSource = deps.source ?? profileRepo;
  return {
    kind: PEOPLE_SEARCH_KIND,
    description:
      'Ranks people in the directory against the search terms. An empty query is not an error — it returns no one.',
    reads: 'query, from the chat command.',
    emits:
      'The ranked people, the parsed terms, and a title for the panel.',
    async execute(_node: PipelineNode, ctx: Record<string, unknown>, helpers): Promise<StepOutcome> {
      const query = str(ctx.query) ?? '';
      const terms = parseSearchTerms(query);
      let people: Person[] = [];
      if (terms.length > 0) {
        try {
          people = rankPeople(await source.listProfiles(PROFILE_SCAN_LIMIT), terms);
        } catch (err) {
          return { status: 'failed', error: err instanceof Error ? err.message : String(err) };
        }
      }
      const title = panelTitle(terms);
      const panelData: PeoplePanelData = { title, people, query };
      const result = { kind: 'panel' as const, panel: { kind: PEOPLE_PANEL_KIND, id: peoplePanelId(helpers.runId), title } };
      return { status: 'completed', output: { query, terms, people, panelData, result } };
    },
  };
}

/** Step 2: the card in the thread — `/search 3 people match "kubernetes"` — carrying the panel to open. */
export function createPeoplePublishHandler(deps: { postCard?: typeof postRunCardToChannel } = {}): DescribedStepHandler {
  const post = deps.postCard ?? postRunCardToChannel;
  return {
    kind: PEOPLE_PUBLISH_KIND,
    description:
      'Posts the search result to the conversation as a panel card saying how many people matched.',
    reads: 'The people and terms from the search step.',
    emits:
      'The card metadata, returned whether or not the step posted it — the client posts it itself when clientPostsCard is set.',
    async execute(_node: PipelineNode, ctx: Record<string, unknown>, helpers): Promise<StepOutcome> {
      const terms = Array.isArray(ctx.terms) ? (ctx.terms as unknown[]).filter((t): t is string => typeof t === 'string') : [];
      const people = Array.isArray(ctx.people) ? (ctx.people as Person[]) : [];
      const n = people.length;
      const instruction = `${n} ${n === 1 ? 'person matches' : 'people match'} "${terms.join(' ')}"`;
      const result = ctx.result && typeof ctx.result === 'object'
        ? ctx.result as Record<string, unknown>
        : { kind: 'panel', panel: { kind: PEOPLE_PANEL_KIND, id: peoplePanelId(helpers.runId), title: panelTitle(terms) } };
      // `context: ctx` lets the post step down when the client posted the card (clientPostsCard); the metadata is returned either way.
      const cardInput = {
        channel: str(ctx.chatChannel), runId: helpers.runId, pipelineId: PEOPLE_SEARCH_PIPELINE_ID,
        command: '/search', instruction,
        extra: { result, query: str(ctx.query) ?? '', terms, matched: n },
        context: ctx,
      };
      const posted = await post(cardInput);
      return { status: 'completed', output: { posted, card: runCardMetadata(cardInput), matched: n, instruction, result } };
    },
  };
}

export function buildPeopleSearchHandlers(): Map<string, StepHandler> {
  const m = new Map<string, StepHandler>();
  for (const h of [createPeopleSearchHandler(), createPeoplePublishHandler()]) m.set(h.kind, h);
  return m;
}

/** The command as the registry sees it. Exported so tests can inspect it without registering. */
export const peopleSearchCommand: ChatCommandPipeline = {
  trigger: '/search',
  pipelineId: PEOPLE_SEARCH_PIPELINE_ID,
  description: 'Find people by what they know',
  label: 'Find people',
  hint: 'by what they know',
  requires: { argumentText: true },
  buildDefinition: () => buildPeopleSearchDefinition(),
  triggerPayload: (input) => ({
    query: input.argumentText.trim().slice(0, 500), chatChannel: input.channel,
    requestedBy: input.requestedBy, requestedByName: input.requestedByName,
  }),
  card: (input) => ({ query: input.argumentText.trim().slice(0, 500), opens: 'panel', panelKind: PEOPLE_PANEL_KIND }),
  // The publish step posts the card, so it arrives with the result attached.
  postCard: false,
};

/** Register just the command (no steps); the plugin below is the whole thing. */
export function registerPeopleSearchCommand(): void {
  registerChatCommandPipeline(peopleSearchCommand);
}

/** The plugin — the command plus its step kinds, installed through src/chat/chatCommandPlugin.ts like an external one. */
export const peopleSearchPlugin: ChatCommandPlugin = {
  name: 'people-search',
  setup(ctx) {
    ctx.registerCommand(peopleSearchCommand);
    ctx.registerStepHandlers([createPeopleSearchHandler(), createPeoplePublishHandler()]);
  },
};

export default peopleSearchPlugin;
