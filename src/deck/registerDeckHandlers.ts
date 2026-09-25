// Boot-time wiring for the deck step handlers, in the shape bootstrap.ts
// already merges (see registerVisionHandlers.ts / registerMemoryHandlers.ts).

import type { StepHandler } from 'distributed-core/applications/pipeline';
import { createDeckStepHandler } from './DeckStepHandler';
import { createDeckReadSourcesHandler } from './DeckSourcesStepHandler';
import { createDeckDraftFromSourcesHandler } from './DeckDraftFromSourcesStep';
import { buildDeckReviseStepHandlers } from './deckRevisePipeline';

export function buildDeckStepHandlers(): Map<string, StepHandler> {
  const handlers = new Map<string, StepHandler>();
  for (const handler of [createDeckStepHandler(), createDeckReadSourcesHandler(), createDeckDraftFromSourcesHandler(), ...buildDeckReviseStepHandlers()]) {
    handlers.set(handler.kind, handler);
  }
  return handlers;
}
