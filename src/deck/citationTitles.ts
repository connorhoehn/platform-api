// A citation titled with a call's wire id ("dm:dev-connor:dev-frank") is a
// leftover: decks drafted before `recordingTitle()` (f100e61) stored the lobby
// name as the transcript's title, and every revision saved from them since
// copied it forward. Stored revisions are never rewritten; instead the places
// that hand a spec back to an editor — the spec read and the agent's revise —
// name such a citation the way a new draft would ("Call with Connor Hoehn &
// Frank Davis"). The next save then stores the human title, so the slide
// pictures, the .pptx and the revision's source list follow.

import log from '../lib/logger';
import { callRecordingRepo } from '../repositories';
import type { DeckSource, DeckSpec } from './DeckSpec';
import { WIRE_LOBBY, recordingTitle } from './deckFixture';
import { findRecordingIdByTranscript } from './deckRevise';

/** True for a title that is a wire id, not something a person would read. */
export function isWireTitle(title: string | undefined): boolean {
  return !!title && WIRE_LOBBY.test(title.trim());
}

export type TranscriptTitleLookup = (transcriptId: string) => Promise<string | null>;

// Legacy rows only, and the lookup is a table scan, so a small process cache.
const CACHE_LIMIT = 200;
const cache = new Map<string, string>();

/** The human title of the recording behind a transcript id, or null. */
export const lookupTranscriptTitle: TranscriptTitleLookup = async (transcriptId) => {
  const hit = cache.get(transcriptId);
  if (hit) return hit;
  const recordingId = await findRecordingIdByTranscript(transcriptId);
  if (!recordingId) return null;
  const row = await callRecordingRepo.findByRecordingId(recordingId);
  if (!row) return null;
  const title = recordingTitle(row as { lobbyName?: unknown; participants?: unknown });
  if (cache.size >= CACHE_LIMIT) cache.delete(cache.keys().next().value as string);
  cache.set(transcriptId, title);
  return title;
};

/** Human titles for every wire-titled citation in `sources`, keyed by documentId. */
async function resolveWireTitles(
  sources: readonly DeckSource[],
  lookup: TranscriptTitleLookup,
): Promise<Map<string, string>> {
  const ids = [...new Set(sources.filter((source) => isWireTitle(source.title)).map((source) => source.documentId))];
  const resolved = new Map<string, string>();
  await Promise.all(ids.map(async (id) => {
    try {
      const title = await lookup(id);
      if (title) resolved.set(id, title);
    } catch (err) {
      // A failed lookup leaves the stored title; it is ugly, not wrong.
      log.warn({ err: err instanceof Error ? err.message : String(err), documentId: id }, 'deck citation title lookup failed');
    }
  }));
  return resolved;
}

const relabel = <T extends { documentId: string; title: string }>(source: T, titles: Map<string, string>): T => {
  const title = isWireTitle(source.title) ? titles.get(source.documentId) : undefined;
  return title ? { ...source, title } : source;
};

/**
 * `spec` (and the revision's own source list, when given) with each
 * wire-titled citation renamed from its recording. Anything that cannot be
 * resolved is returned as stored. No lookup happens when nothing is wire-titled.
 */
export async function healCitationTitles<S extends { documentId: string; title: string }>(
  spec: DeckSpec,
  listed: readonly S[] = [],
  lookup: TranscriptTitleLookup = lookupTranscriptTitle,
): Promise<{ spec: DeckSpec; sources: S[] }> {
  const all = [...spec.slides.flatMap((slide) => slide.sources), ...listed];
  if (!all.some((source) => isWireTitle(source.title))) return { spec, sources: [...listed] };
  const titles = await resolveWireTitles(all, lookup);
  if (!titles.size) return { spec, sources: [...listed] };
  return {
    spec: { ...spec, slides: spec.slides.map((slide) => ({ ...slide, sources: slide.sources.map((source) => relabel(source, titles)) })) },
    sources: listed.map((source) => relabel(source, titles)),
  };
}
