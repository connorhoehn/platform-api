// The `deck-revise` definition on its own, so the system-pipeline registry can
// list it without loading the step handlers (and everything they import).
// Steps and rules: deckRevisePipeline.ts.

import { buildDocumentEditDefinition } from '../documents/documentEditPipeline';

export const DECK_REVISE_PIPELINE_ID = 'deck-revise';
export const DECK_REVISE_LOAD_KIND = 'deck:revise-load';
export const DECK_REVISE_PROPOSE_KIND = 'deck:revise';
export const DECK_REVISE_WRITE_KIND = 'deck:revise-write';

export function buildDeckReviseDefinition(): Record<string, unknown> {
  return buildDocumentEditDefinition({
    id: DECK_REVISE_PIPELINE_ID,
    name: 'Presentation edit',
    description: 'A composer edit of a presentation: read the deck, have the model make the edit from the deck\'s sources, and save it as a new revision conditioned on the revision it was made against.',
    triggerLabel: 'Composer edit',
    stages: {
      load: { kind: DECK_REVISE_LOAD_KIND, label: 'Read the deck' },
      propose: { kind: DECK_REVISE_PROPOSE_KIND, label: 'Write the edit' },
      write: { kind: DECK_REVISE_WRITE_KIND, label: 'Save the new revision' },
    },
  });
}
