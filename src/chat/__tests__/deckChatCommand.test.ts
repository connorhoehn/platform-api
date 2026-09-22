// /deck as the registry sees it: what it requires, what the run is handed, and
// what the card says the deck will stand on.

import { clearChatCommandPipelines, getChatCommandPipeline, type ChatCommandRunInput } from '../chatCommandRegistry';
import { deckSourcesFromInput, registerBuiltinChatCommands } from '../builtinChatCommands';
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
