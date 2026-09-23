// /deck as the registry sees it: what it requires, what the run is handed, and
// what the card says the deck will stand on.

const buildDeckFixture = jest.fn();
jest.mock('../../deck/deckFixture', () => {
  const actual = jest.requireActual('../../deck/deckFixture');
  return { ...actual, buildDeckFixture: (...args: unknown[]) => buildDeckFixture(...args) };
});

// The card must never be posted by the dispatcher for a preview: the whole point
// of `--preview` is that nothing lands in the conversation or the store.
const postRunCardToChannel = jest.fn();
jest.mock('../../documents/chatRunCard', () => {
  const actual = jest.requireActual('../../documents/chatRunCard');
  return { ...actual, postRunCardToChannel: (...args: unknown[]) => postRunCardToChannel(...args) };
});

import { clearChatCommandPipelines, getChatCommandPipeline, runChatCommand, type ChatCommandRunInput } from '../chatCommandRegistry';
import { buildDeckPreviewCard, deckSourcesFromInput, parseDeckCommand, registerBuiltinChatCommands } from '../builtinChatCommands';
import { sampleDeck } from '../../deck/__tests__/fixtures';
import { DECK_GENERATE_PIPELINE_ID } from '../../documents/deckGeneratePipeline';
import { DECK_READ_SOURCES_KIND } from '../../deck/DeckSourcesStepHandler';
import { DECK_STEP_KIND } from '../../deck/DeckStepHandler';

const input = (over: Partial<ChatCommandRunInput> = {}): ChatCommandRunInput => ({
  trigger: '/deck',
  channel: 'chat:room:eng',
  requestedBy: 'user-frank',
  requestedByName: 'Frank',
  documentId: 'doc-new-presentation',
  argumentText: 'the Q4 platform review for the leadership sync',
  extra: {
    documents: [
      { id: 'doc-sprint-review', title: 'Sprint review' },
      { id: 'doc-roadmap', title: 'Q4 roadmap' },
    ],
  },
  ...over,
} as ChatCommandRunInput);

beforeAll(() => {
  clearChatCommandPipelines();
  registerBuiltinChatCommands();
});

describe('/deck', () => {
  it('is registered against the deck-generate pipeline and needs a subject', () => {
    const command = getChatCommandPipeline('/deck')!;
    expect(command.pipelineId).toBe(DECK_GENERATE_PIPELINE_ID);
    // `document` is NOT a static requirement — a preview never gets one. It
    // is enforced by `prepare`, conditioned on `--preview`; see the
    // "runChatCommand" describe block below.
    expect(command.requires).toMatchObject({ argumentText: true });
    expect(command.requires).not.toHaveProperty('document');
    expect(command.postCard).toBe(false);
  });

  it('hands the run the sources the composer referenced', () => {
    const command = getChatCommandPipeline('/deck')!;
    const payload = command.triggerPayload!(input()) as Record<string, unknown>;
    expect(payload).toMatchObject({
      documentId: 'doc-new-presentation',
      chatChannel: 'chat:room:eng',
      requestedBy: 'user-frank',
      prompt: 'the Q4 platform review for the leadership sync',
    });
    expect(payload.sources).toEqual([
      { documentId: 'doc-sprint-review', title: 'Sprint review' },
      { documentId: 'doc-roadmap', title: 'Q4 roadmap' },
    ]);
  });

  it('names the sources on the card', () => {
    const command = getChatCommandPipeline('/deck')!;
    const card = command.card!(input(), { runId: 'run-1' }) as Record<string, unknown>;
    expect(card.sourceNames).toEqual(['Sprint review', 'Q4 roadmap']);
  });

  it('builds a definition that reads, drafts and then renders', () => {
    const command = getChatCommandPipeline('/deck')!;
    const definition = command.buildDefinition!(input()) as { nodes: Array<{ id: string; type: string }> };
    expect(definition.nodes.map((node) => node.type)).toEqual(['trigger', 'chat:read-history', DECK_READ_SOURCES_KIND, 'agent', DECK_STEP_KIND]);
  });
});

describe('deckSourcesFromInput', () => {
  it('drops the new presentation itself, duplicates, and anything unnamed', () => {
    const sources = deckSourcesFromInput(input({
      extra: {
        documents: [
          { id: 'doc-new-presentation', title: 'The deck being written' },
          { id: 'doc-a', title: 'A' },
          { documentId: 'doc-a', title: 'A again' },
          { title: 'no id' },
          'nonsense',
        ],
      },
    }));
    expect(sources).toEqual([{ documentId: 'doc-a', title: 'A' }]);
  });

  it('answers with nothing when the composer referenced nothing', () => {
    expect(deckSourcesFromInput(input({ extra: {} }))).toEqual([]);
  });
});


describe('parseDeckCommand', () => {
  it('takes the flags out of the instruction and leaves the words', () => {
    expect(parseDeckCommand('the Q4 review --preview --from transcript')).toEqual({
      prompt: 'the Q4 review', preview: true, from: 'transcript',
    });
    expect(parseDeckCommand('--from=#release-plan the release story')).toEqual({
      prompt: 'the release story', preview: false, from: 'documents',
    });
    expect(parseDeckCommand('just a deck about onboarding')).toEqual({
      prompt: 'just a deck about onboarding', preview: false,
    });
  });

  it('keeps an unrecognised flag in the instruction rather than swallowing it', () => {
    expect(parseDeckCommand('a deck --pretty').prompt).toBe('a deck --pretty');
  });

  it('reads a recording id straight out of `--from recording:<id>` — the token the client picker inserts', () => {
    expect(parseDeckCommand('x --from recording:rec-2')).toEqual({
      prompt: 'x', preview: false, from: 'transcript', recordingId: 'rec-2',
    });
  });

  it('the real conversation this fixes: a picked recording token, then free text with a colon, then --preview — none of it gets dropped or truncates a colon-bearing word', () => {
    // Mirrors the reported bug: `/deck --preview --from transcript`, picked a
    // recording, and the message that reached the server was
    // `/deck --from recording:<id> Frank Davis · Sep 22, 08:30 AM --preview`.
    expect(parseDeckCommand('--from recording:rec-2 Frank Davis · Sep 22, 08:30 AM --preview')).toEqual({
      prompt: 'Frank Davis · Sep 22, 08:30 AM', preview: true, from: 'transcript', recordingId: 'rec-2',
    });
  });
});

describe('/deck --from transcript', () => {
  it('carries the recording and the source of the material into the run', () => {
    const command = getChatCommandPipeline('/deck')!;
    const payload = command.triggerPayload!(input({
      argumentText: 'the sprint review --from transcript',
      extra: { recordingId: 'rec-1' },
    })) as Record<string, unknown>;
    expect(payload).toMatchObject({ prompt: 'the sprint review', recordingId: 'rec-1', from: 'transcript' });
  });
});

describe('/deck --preview', () => {
  beforeEach(() => buildDeckFixture.mockReset());

  it('answers with slide 1 as the card thumbnail and persists nothing', async () => {
    buildDeckFixture.mockResolvedValue({
      spec: sampleDeck(), source: 'model', material: {},
      warnings: ['Release plan was provided but not cited.'], transcriptIds: [],
    });
    const card = await buildDeckPreviewCard(input({ argumentText: 'the Q4 review --preview' }));
    expect(card.warnings as string[]).toContain('Release plan was provided but not cited.');
    expect(card).toMatchObject({ kind: 'deck-preview', persisted: false, specSource: 'model', slideCount: 9 });
    const thumbnail = card.thumbnail as { slideId: string; svg: string; contentType: string };
    expect(thumbnail.slideId).toBe('cover');
    expect(thumbnail.contentType).toBe('image/svg+xml');
    expect(thumbnail.svg.startsWith('<svg')).toBe(true);
    // The material it was built from: the first referenced document.
    expect(buildDeckFixture).toHaveBeenCalledWith(
      { documentId: 'doc-sprint-review', personId: 'user-frank' },
      'the Q4 review',
      {},
    );
  });

  it('says on the card when the deck came from the offline fixture', async () => {
    buildDeckFixture.mockResolvedValue({ spec: sampleDeck(), source: 'fixture', reason: 'no model is configured on this deployment', warnings: [], transcriptIds: [], material: {} });
    const card = await buildDeckPreviewCard(input({ argumentText: 'x --preview' }));
    expect(card).toMatchObject({ specSource: 'fixture', specSourceReason: 'no model is configured on this deployment' });
  });

  it('reads the recording when the command asked for the transcript', async () => {
    buildDeckFixture.mockResolvedValue({ spec: sampleDeck(), source: 'model', material: {}, warnings: [], transcriptIds: [] });
    await buildDeckPreviewCard(input({ argumentText: 'x --preview --from transcript', extra: { recordingId: 'rec-1' } }));
    expect(buildDeckFixture).toHaveBeenCalledWith({ recordingId: 'rec-1', personId: 'user-frank' }, 'x', {});
  });

  it('refuses when nothing was named to build from', async () => {
    await expect(buildDeckPreviewCard(input({ argumentText: 'x --preview', extra: {} }))).rejects.toThrow(/needs a document/);
  });

  it('reads the recording straight out of a `--from recording:<id>` token, with no `extra.recordingId` at all', async () => {
    buildDeckFixture.mockResolvedValue({ spec: sampleDeck(), source: 'model', material: {}, warnings: [], transcriptIds: [] });
    await buildDeckPreviewCard(input({ argumentText: 'x --preview --from recording:rec-9', extra: {} }));
    expect(buildDeckFixture).toHaveBeenCalledWith({ recordingId: 'rec-9', personId: 'user-frank' }, 'x', {});
  });
});

// The bug this closes: `requires: { document: true }` was checked BEFORE
// `start` ever read `--preview`, so `/deck --preview` — which never gets a
// documentId, because the client creates nothing to preview — was rejected
// with a 400 and no card, through the real dispatch path
// (`runChatCommand`), even though `buildDeckPreviewCard` alone worked fine.
describe('/deck --preview through runChatCommand (the real dispatch path, not just buildDeckPreviewCard)', () => {
  beforeEach(() => buildDeckFixture.mockReset());

  it('runs with no documentId at all and answers with the preview card', async () => {
    buildDeckFixture.mockResolvedValue({
      spec: sampleDeck(), source: 'model', material: {}, warnings: [], transcriptIds: [],
    });
    const out = await runChatCommand({
      trigger: '/deck',
      channel: 'chat:room:eng',
      text: '/deck the Q4 review --preview',
      argumentText: 'the Q4 review --preview',
      requestedBy: 'user-frank',
      requestedByName: 'Frank',
      extra: { documents: [{ id: 'doc-sprint-review', title: 'Sprint review' }] },
    } as ChatCommandRunInput);
    expect(out.card).not.toHaveProperty('documentId');
    expect(out.card).toMatchObject({ persisted: false, specSource: 'model' });
  });

  it('the recording picker path: `--from recording:<id>` and a colon-bearing title, no documentId, no # documents — previews from the transcript and posts nothing', async () => {
    postRunCardToChannel.mockReset();
    buildDeckFixture.mockResolvedValue({
      spec: sampleDeck(), source: 'fixture', reason: 'claude is not authenticated in this environment',
      material: {}, warnings: [], transcriptIds: ['planning-transcript'],
    });
    const argumentText = '--from recording:active-now-2026-09-22-planning-recording Frank Davis · Sep 22, 08:30 AM --preview';
    const out = await runChatCommand({
      trigger: '/deck',
      channel: 'chat:dm:dev-connor:dev-frank',
      text: `/deck ${argumentText}`,
      argumentText,
      requestedBy: 'user-frank',
      requestedByName: 'Frank',
      extra: {},
    } as ChatCommandRunInput);
    expect(out.runId).toMatch(/^deck-preview:/);
    expect(buildDeckFixture).toHaveBeenCalledWith(
      { recordingId: 'active-now-2026-09-22-planning-recording', personId: 'user-frank' },
      'Frank Davis · Sep 22, 08:30 AM',
      {},
    );
    expect(out.card).toMatchObject({
      kind: 'deck-preview', persisted: false, from: 'transcript',
      recordingId: 'active-now-2026-09-22-planning-recording',
      specSource: 'fixture', specSourceReason: 'claude is not authenticated in this environment',
      transcriptIds: ['planning-transcript'],
    });
    expect((out.card as { slideCount: number }).slideCount).toBeGreaterThan(0);
    expect((out.card as { thumbnail: { svg: string } }).thumbnail.svg.startsWith('<svg')).toBe(true);
    expect(postRunCardToChannel).not.toHaveBeenCalled();
  });

  // Two previews by the same person at the same moment. The card used to be
  // handed from `start` to `card` through a map keyed by requester, so the
  // second preview's `start` could overwrite the first's entry before the
  // first's `card` read it: each answer carried the OTHER deck. Both orders of
  // completion are driven, since which one crossed depended on timing.
  it.each(['first', 'second'] as const)(
    'two concurrent previews by one person each get their own card (%s finishes first)',
    async (firstDone) => {
      const pending = new Map<string, () => void>();
      buildDeckFixture.mockImplementation((from: { recordingId: string }) => new Promise((resolve) => {
        pending.set(from.recordingId, () => resolve({
          spec: { ...sampleDeck(), title: `Deck from ${from.recordingId}` },
          source: 'model', material: {}, warnings: [], transcriptIds: [from.recordingId],
        }));
      }));
      const run = (recordingId: string) => runChatCommand({
        trigger: '/deck',
        channel: 'chat:room:eng',
        text: `/deck --from recording:${recordingId} --preview`,
        argumentText: `--from recording:${recordingId} --preview`,
        requestedBy: 'user-frank',
        requestedByName: 'Frank',
        extra: {},
      } as ChatCommandRunInput);
      const a = run('rec-first');
      const b = run('rec-second');
      await new Promise((resolve) => setImmediate(resolve));
      const order = firstDone === 'first' ? ['rec-first', 'rec-second'] : ['rec-second', 'rec-first'];
      for (const id of order) pending.get(id)!();
      const [outA, outB] = await Promise.all([a, b]);
      for (const [out, id] of [[outA, 'rec-first'], [outB, 'rec-second']] as const) {
        expect(out.card).toMatchObject({
          kind: 'deck-preview', recordingId: id, title: `Deck from ${id}`, transcriptIds: [id],
        });
        expect((out.card as { spec: { title: string } }).spec.title).toBe(`Deck from ${id}`);
      }
      expect(outA.runId).not.toBe(outB.runId);
    },
  );

  it('a real (non-preview) build still needs a documentId', async () => {
    await expect(runChatCommand({
      trigger: '/deck',
      channel: 'chat:room:eng',
      text: '/deck the Q4 review',
      argumentText: 'the Q4 review',
      requestedBy: 'user-frank',
      requestedByName: 'Frank',
    } as ChatCommandRunInput)).rejects.toThrow(/needs documentId/);
  });
});

// NFR #47: /deck writes a revision as the requester, so it answers 403 before
// the run starts when the named presentation is someone else's.
describe('/deck names an existing presentation', () => {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { setPresentationDocumentStore } = require('../../deck/presentationDocuments') as typeof import('../../deck/presentationDocuments');
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { DocumentEditForbiddenError } = require('../../deck/documentEditAccess') as typeof import('../../deck/documentEditAccess');
  const rows: Record<string, Record<string, unknown>> = {
    'audit-deck-owned': { documentId: 'audit-deck-owned', type: 'presentation', createdBy: 'user-connor', editors: ['user-erin'], viewers: ['user-vic'] },
    'audit-page': { documentId: 'audit-page', type: 'page', createdBy: 'user-connor' },
  };
  beforeAll(() => setPresentationDocumentStore({ get: async (id: string) => (rows[id] ?? null) as never, appendRevision: async () => { throw new Error('not in this test'); } } as never));
  afterAll(() => setPresentationDocumentStore(null));
  const prepare = (requestedBy: string, documentId: string) => getChatCommandPipeline('/deck')!.prepare!({
    trigger: '/deck', channel: 'chat:room:eng', text: '/deck the Q4 review', argumentText: 'the Q4 review',
    documentId, requestedBy, requestedByName: requestedBy,
  } as ChatCommandRunInput);

  it.each([['owner', 'user-connor'], ['editor', 'user-erin']])('the %s may run it', async (_role, user) => {
    await expect(prepare(user, 'audit-deck-owned')).resolves.toMatchObject({ documentId: 'audit-deck-owned' });
  });
  it.each([['viewer', 'user-vic'], ['stranger', 'user-frank']])('a %s gets 403 forbidden', async (_role, user) => {
    const err = await Promise.resolve(prepare(user, 'audit-deck-owned')).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(DocumentEditForbiddenError);
    expect(err).toMatchObject({ status: 403, code: 'forbidden', documentId: 'audit-deck-owned' });
  });
  it('a fresh id, or a document of another kind (the step files a new deck), passes', async () => {
    await expect(prepare('user-frank', 'audit-new-id')).resolves.toBeTruthy();
    await expect(prepare('user-frank', 'audit-page')).resolves.toBeTruthy();
  });
});
