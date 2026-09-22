// Reads the documents a deck is supposed to be grounded in.
//
// The `#`-referenced documents arrive as ids and titles; their text lives in the
// gateway's CRDT, so this step joins each one as a collaborator and reads its
// outline — the same way the /doc and /transform pipelines read a page before
// editing it. A document that cannot be read says so in the prompt block rather
// than disappearing from it: the drafting model must not be able to cite text it
// was never shown, and the person reading the deck must be able to tell the
// difference between "nothing in that document" and "that document was down".

import type { PipelineNode, StepOutcome } from 'distributed-core/applications/pipeline';
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import { AgentCollaborator, HAIKU_AGENT } from '../documents/AgentCollaborator';
import { outlineOf } from '../documents/documentAgentOps';
import log from '../lib/logger';

export const DECK_READ_SOURCES_KIND = 'deck:read-sources';

/** A document the deck may cite. */
export interface DeckSourceRef {
  documentId: string;
  title: string;
}

export const MAX_SOURCE_DOCUMENTS = 6;
export const MAX_CHARS_PER_SOURCE = 6000;
export const MAX_TRANSCRIPT_CHARS = 8000;

function str(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

/** The `sources` the trigger carried, as ids and titles, capped. */
export function parseSourceRefs(raw: unknown): DeckSourceRef[] {
  if (!Array.isArray(raw)) return [];
  const out: DeckSourceRef[] = [];
  const seen = new Set<string>();
  for (const entry of raw) {
    if (!entry || typeof entry !== 'object') continue;
    const item = entry as { documentId?: unknown; id?: unknown; title?: unknown; name?: unknown };
    const documentId = str(item.documentId) ?? str(item.id);
    if (!documentId || seen.has(documentId)) continue;
    seen.add(documentId);
    out.push({ documentId, title: (str(item.title) ?? str(item.name) ?? documentId).slice(0, 160) });
    if (out.length >= MAX_SOURCE_DOCUMENTS) break;
  }
  return out;
}

/** Reads one document's text through the gateway, or reports why it could not. */
export async function readDocumentText(documentId: string): Promise<{ text?: string; error?: string }> {
  const collaborator = new AgentCollaborator({ documentId, identity: HAIKU_AGENT });
  try {
    await collaborator.join();
    const outline = outlineOf(collaborator.doc);
    return { text: outline.text.slice(0, MAX_CHARS_PER_SOURCE) };
  } catch (err) {
    return { error: err instanceof Error ? err.message : String(err) };
  } finally {
    try { await collaborator.leave(0); } catch { /* the socket is closing either way */ }
  }
}

/** The block the drafting prompt interpolates: one section per source, transcript last. */
export function sourcesBlock(
  sections: Array<{ documentId: string; title: string; text?: string; error?: string }>,
  transcript?: string,
): string {
  const parts = sections.map((section) => [
    `### ${section.title}`,
    `documentId: ${section.documentId}`,
    section.text?.trim()
      ? section.text.trim()
      : section.error
        ? `(this document could not be read: ${section.error})`
        : '(this document is empty)',
  ].join('\n'));
  if (transcript?.trim()) {
    parts.push(['### Meeting transcript', 'documentId: transcript', transcript.trim().slice(0, MAX_TRANSCRIPT_CHARS)].join('\n'));
  }
  return parts.join('\n\n') || '(no sources were provided)';
}

export interface DeckReadSourcesOpts {
  readDocument?: typeof readDocumentText;
}

export function createDeckReadSourcesHandler(opts: DeckReadSourcesOpts = {}): DescribedStepHandler {
  const read = opts.readDocument ?? readDocumentText;
  return {
    kind: DECK_READ_SOURCES_KIND,
    description:
      'Reads the documents the deck must be grounded in and flattens them, with the conversation transcript, into one prompt block.',
    reads: 'sources (documentId + title) from the trigger, and conversationText when a step before it read the chat.',
    emits: 'sourcesBlock, sourceList and how many documents were readable — a document that could not be read says so in the block.',
    async execute(_node: PipelineNode, context: Record<string, unknown>): Promise<StepOutcome> {
      const refs = parseSourceRefs(context.sources);
      const transcript = str(context.conversationText);
      const sections: Array<{ documentId: string; title: string; text?: string; error?: string }> = [];
      let unavailable = 0;
      for (const ref of refs) {
        const result = await read(ref.documentId);
        if (result.error) {
          unavailable += 1;
          log.warn({ documentId: ref.documentId, err: result.error }, 'deck: a source document could not be read');
        }
        sections.push({ ...ref, ...result });
      }
      const sourceList = refs.map((ref) => `- ${ref.title} (documentId: ${ref.documentId})`).join('\n')
        + (transcript ? `${refs.length ? '\n' : ''}- Meeting transcript (documentId: transcript)` : '');
      if (!refs.length && !transcript) {
        return { status: 'failed', error: 'deck: no sources — a deck is only allowed to say what its inputs say' };
      }
      return {
        status: 'completed',
        output: {
          sourcesBlock: sourcesBlock(sections, transcript),
          sourceList: sourceList || '(none)',
          sourcesRead: sections.length - unavailable,
          sourcesUnavailable: unavailable,
          sourceCount: sections.length + (transcript ? 1 : 0),
        },
      };
    },
  };
}
