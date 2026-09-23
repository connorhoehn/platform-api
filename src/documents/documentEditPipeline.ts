// The shape every composer edit of a document runs as: a pipeline, so the
// change is made by the run and not by the browser.
//
//   document-edit:  trigger → load → propose → write
//
//   load     reads the document as it is now and pins the BASE the edit is
//            made against (a revision id, a CRDT version — whatever the kind
//            keeps), validating the request against it.
//   propose  asks the model for the change and checks it. Writes nothing.
//   write    applies the change CONDITIONED ON THE BASE, and is idempotent by
//            the request's `requestId`: a retried write finds its own change
//            and returns it instead of making a second one.
//
// A kind supplies one step kind per stage; the definition, labels, retry
// policy and work-graph plan come from here. `deck-revise` is the first
// instance (src/deck/deckRevisePipeline.ts); the page editor and the
// Excalidraw canvas are meant to register their own three kinds the same way.
//
// Retries: `load` and `write` touch a store and retry transient errors only
// (`retryUnknown: false` — a conflict or a bad request is not transient and
// must not be re-tried into a duplicate); `propose` talks to a model and gets
// one retry for a transient failure.

export interface DocumentEditStages {
  load: { kind: string; label: string };
  propose: { kind: string; label: string };
  write: { kind: string; label: string };
}

export interface DocumentEditDefinitionInit {
  id: string;
  name: string;
  description: string;
  stages: DocumentEditStages;
  /** Label of the trigger node, e.g. "Composer edit". */
  triggerLabel?: string;
}

export const DOCUMENT_EDIT_STORE_RETRY = { maxAttempts: 3, backoffMs: 1000, backoffMultiplier: 2, retryUnknown: false } as const;
export const DOCUMENT_EDIT_MODEL_RETRY = { maxAttempts: 2, backoffMs: 2000, backoffMultiplier: 2, retryUnknown: false } as const;

/** The step ids every document-edit run reports, in order. */
export const DOCUMENT_EDIT_STEP_IDS = ['load', 'propose', 'write'] as const;

export function buildDocumentEditDefinition(init: DocumentEditDefinitionInit): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  const edge = (source: string, target: string) => ({ id: `${source}->${target}`, source, sourceHandle: 'out', target, targetHandle: 'in' });
  const node = (id: string, x: number, stage: { kind: string; label: string }, retryPolicy: object) => ({
    id, type: stage.kind, position: pos(x), data: { type: stage.kind, label: stage.label, retryPolicy },
  });
  return {
    id: init.id,
    name: init.name,
    description: init.description,
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'platform', label: init.triggerLabel ?? 'Composer edit' } },
      node('load', 200, init.stages.load, DOCUMENT_EDIT_STORE_RETRY),
      node('propose', 400, init.stages.propose, DOCUMENT_EDIT_MODEL_RETRY),
      node('write', 600, init.stages.write, DOCUMENT_EDIT_STORE_RETRY),
    ],
    edges: [edge('trigger', 'load'), edge('load', 'propose'), edge('propose', 'write')],
  };
}
