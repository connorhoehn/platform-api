// `presentation-deck` — a presentation drafted from SEVERAL documents, started
// from a run draft ("/workflow presentation-deck" in a document composer, with
// the documents gathered in its prompt context as the draft's `sources`).
//
//   trigger → draft (deck:draft-from-sources) → render (deck)
//
// The definition on its own, so the system-pipeline registry can list it
// without loading the step handlers. Steps: DeckDraftFromSourcesStep.ts and
// DeckStepHandler.ts. The run's `documentId` is the presentation the client
// created for it; the render step files the deck as that document's revision.

export const PRESENTATION_DECK_PIPELINE_ID = 'presentation-deck';
/** Kept in step with DeckDraftFromSourcesStep.ts (not imported: that pulls in the handlers). */
const DRAFT_KIND = 'deck:draft-from-sources';
const RENDER_KIND = 'deck';

export function buildPresentationDeckDefinition(): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  const edge = (source: string, target: string) => ({ id: `${source}->${target}`, source, sourceHandle: 'out', target, targetHandle: 'in' });
  const retry = { maxAttempts: 3, backoffMs: 1500, backoffMultiplier: 2, retryUnknown: true } as const;
  return {
    id: PRESENTATION_DECK_PIPELINE_ID,
    name: 'Presentation from documents',
    description: 'A run draft with source documents: check the requester may read every one, read them all, draft one grounded deck citing each (deck model pool), render a real .pptx with per-slide previews into the presentation the draft is on.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'manual', label: 'Run draft' } },
      { id: 'draft', type: DRAFT_KIND, position: pos(200), data: { type: DRAFT_KIND, label: 'Read the sources and draft the slides', retryPolicy: retry } },
      { id: 'render', type: RENDER_KIND, position: pos(400), data: { type: RENDER_KIND, label: 'Render the deck (.pptx)', specFrom: 'steps.draft.spec', retryPolicy: retry } },
    ],
    edges: [edge('trigger', 'draft'), edge('draft', 'render')],
    workType: 'documents', kind: 'presentation', route: ['Run draft', 'Read', 'Draft', 'Render'], origin: { kind: 'system' },
  };
}
