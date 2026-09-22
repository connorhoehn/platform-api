// /deck as the registry sees it: what it requires, what the run is handed, and
// what the card says the deck will stand on.

const buildDeckFixture = jest.fn();
jest.mock('../../deck/deckFixture', () => {
  const actual = jest.requireActual('../../deck/deckFixture');
  return { ...actual, buildDeckFixture: (...args: unknown[]) => buildDeckFixture(...args) };
});

import { clearChatCommandPipelines, getChatCommandPipeline, type ChatCommandRunInput } from '../chatCommandRegistry';
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
  it('is registered against the deck-generate pipeline and needs a document and a subject', () => {
    const command = getChatCommandPipeline('/deck')!;
    expect(command.pipelineId).toBe(DECK_GENERATE_PIPELINE_ID);
    expect(command.requires).toMatchObject({ document: true, argumentText: true });
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
    const card = command.card!(input()) as Record<string, unknown>;
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
});
