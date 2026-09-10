// What a run has to SHOW for itself when it is done.
//
// A pipeline started from a chat command ends with a card in the thread; the
// card wants one thing to open. Rather than every pipeline inventing its own
// output field and every client learning each one, a run whose final context
// carries `result` in one of three shapes is what the card offers:
//
//   { kind: 'panel',    panel: { kind: string; id: string; title?: string; params?: Record<string, unknown> } }
//   { kind: 'document', documentId: string }
//   { kind: 'link',     href: string; label: string }
//
// A step puts it there by returning `{ status: 'completed', output: { result } }`
// — step outputs merge into the run context — and the run-detail endpoint
// (`GET /api/pipelines/:pipelineId/runs/:runId`) lifts it to a top-level
// `result` so a client polling the run never has to know the context layout.
// Anything else under `context.result` is ignored, never lifted half-formed.

export interface ChatCommandPanelResult {
  kind: 'panel';
  panel: { kind: string; id: string; title?: string; params?: Record<string, unknown> };
}
export interface ChatCommandDocumentResult { kind: 'document'; documentId: string }
export interface ChatCommandLinkResult { kind: 'link'; href: string; label: string }

export type ChatCommandResult = ChatCommandPanelResult | ChatCommandDocumentResult | ChatCommandLinkResult;

const nonEmpty = (v: unknown): v is string => typeof v === 'string' && v.length > 0;

/** The well-formed `result` a value is, or undefined when it is not one of the three shapes. */
export function parseRunResult(value: unknown): ChatCommandResult | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const r = value as Record<string, unknown>;
  switch (r.kind) {
    case 'panel': {
      const p = r.panel as Record<string, unknown> | undefined;
      if (!p || typeof p !== 'object' || !nonEmpty(p.kind) || !nonEmpty(p.id)) return undefined;
      return {
        kind: 'panel',
        panel: {
          kind: p.kind, id: p.id,
          ...(nonEmpty(p.title) ? { title: p.title } : {}),
          ...(p.params && typeof p.params === 'object' ? { params: p.params as Record<string, unknown> } : {}),
        },
      };
    }
    case 'document':
      return nonEmpty(r.documentId) ? { kind: 'document', documentId: r.documentId } : undefined;
    case 'link':
      return nonEmpty(r.href) && nonEmpty(r.label) ? { kind: 'link', href: r.href, label: r.label } : undefined;
    default:
      return undefined;
  }
}

/** `context.result`, when the run context carries a well-formed one. */
export function runResultFromContext(context: unknown): ChatCommandResult | undefined {
  if (!context || typeof context !== 'object') return undefined;
  return parseRunResult((context as Record<string, unknown>).result);
}
