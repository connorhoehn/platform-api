// `deck:draft-from-sources` — the drafting step of `presentation-deck`, the
// pipeline a document run draft starts ("/workflow presentation-deck" with
// several documents in the composer's context).
//
// It reads EVERY source the draft names and drafts one deck grounded in all of
// them, through the same drafter the deck preview uses (`buildDeckFixture`):
// the model breaker, a slot from the deck pool, a refusal of any citation that
// is not one of the sources, and the offline deck when the model cannot answer.
//
// The run acts as its requester. Each source is checked against `document:read`
// for that person HERE, not only where the draft was prepared: a draft may sit
// for a while, and access can be taken away in the meantime. One source that
// cannot be read — refused, missing, or its text unavailable — refuses the
// whole run and names it. A deck drafted from a subset would look sourced from
// everything the person picked while quietly leaving part of it out.

import type { PipelineNode, StepOutcome } from 'distributed-core/applications/pipeline';
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import { buildDeckFixture, citableSources, documentsOf, readFixtureMaterial, type BuildFixtureOpts, type FixtureMaterial } from './deckFixture';
import { unreadableSourcesFor, type RunSourceAccessDeps } from '../pipeline/runSourceAccess';
import { MAX_SOURCE_DOCUMENTS } from './DeckSourcesStepHandler';
import type { DeckSpec } from './DeckSpec';
import { MAX_REQUESTED_SLIDES, requestedSlideCount } from './draftRequest';
import log from '../lib/logger';

export const DECK_DRAFT_FROM_SOURCES_KIND = 'deck:draft-from-sources';
const MAX_TITLE = 100;

function str(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

/**
 * The document ids a run's `sources` name: run-draft entries (`{kind:'document', id}`)
 * and deck refs (`{documentId, title}`). Other kinds (transcripts, recordings)
 * are not documents and are left out. Deduped, capped.
 */
export function sourceDocumentIds(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  const out: string[] = [];
  for (const entry of raw) {
    if (!entry || typeof entry !== 'object') continue;
    const e = entry as { kind?: unknown; id?: unknown; documentId?: unknown };
    if (e.kind !== undefined && e.kind !== 'document') continue;
    const id = str(e.documentId) ?? str(e.id);
    if (!id || out.includes(id)) continue;
    out.push(id);
    if (out.length >= MAX_SOURCE_DOCUMENTS) break;
  }
  return out;
}

/** Any source the spec never cites is added to the cover's citations, so the Sources slide lists every one. */
export function citeEverySource(spec: DeckSpec, sources: Array<{ documentId: string; title: string }>): DeckSpec {
  const cited = new Set(spec.slides.flatMap((slide) => slide.sources.map((source) => source.documentId)));
  const missing = sources.filter((source) => !cited.has(source.documentId));
  if (!missing.length || !spec.slides.length) return spec;
  const [first, ...rest] = spec.slides;
  return { ...spec, slides: [{ ...first!, sources: [...first!.sources, ...missing] }, ...rest] };
}

export interface DeckDraftFromSourcesDeps {
  /** Rows and policy for the read check; the platform's by default. */
  access?: RunSourceAccessDeps;
  readMaterial?: typeof readFixtureMaterial;
  draft?: typeof buildDeckFixture;
  /** Passed to the drafter (tests inject a model client here). */
  fixtureOpts?: BuildFixtureOpts;
}

export function createDeckDraftFromSourcesHandler(deps: DeckDraftFromSourcesDeps = {}): DescribedStepHandler {
  const readMaterial = deps.readMaterial ?? readFixtureMaterial;
  const draft = deps.draft ?? buildDeckFixture;
  const refuse = (error: string): StepOutcome => ({ status: 'failed', error, retry: false } as StepOutcome);
  return {
    kind: DECK_DRAFT_FROM_SOURCES_KIND,
    description:
      'Checks that the requester may read every source document, reads them all, and drafts one grounded DeckSpec citing each (deck model pool, behind the model breaker).',
    reads: 'sources ({kind:"document", id} or {documentId,title}), requestedBy, organizationId, instruction.',
    emits: 'spec (the DeckSpec), source (model | fixture), reason (why the model\'s answer was not used), warnings, sources (ids + titles), sourceCount, slideCheck + measured ("Measured: slides 4 → 6 (asked 6)") when the instruction states a slide count.',
    async execute(_node: PipelineNode, context: Record<string, unknown>): Promise<StepOutcome> {
      const ids = sourceDocumentIds(context.sources);
      if (!ids.length) return refuse('deck: no source documents — a deck is only allowed to say what its inputs say');
      const requestedBy = str(context.requestedBy) ?? str((context.trigger as Record<string, unknown> | undefined)?.requestedBy);
      if (!requestedBy) return refuse('deck: the run names no requester, so its sources cannot be checked — nothing was drafted');
      const orgId = str(context.organizationId);
      const user = { sub: requestedBy, ...(orgId ? { orgId } : {}) };

      // 1. document:read on every source, as the requester (runSourceAccess.ts).
      let refused: string[];
      try {
        refused = await unreadableSourcesFor(user, ids, deps.access);
      } catch (err) {
        return refuse(`deck: the sources could not be checked (${err instanceof Error ? err.message : String(err)}) — nothing was drafted`);
      }
      if (refused.length) {
        log.warn({ requestedBy, refused }, 'deck: a source document is not readable by the requester');
        return refuse(`deck: source ${refused.length === 1 ? 'document' : 'documents'} ${refused.join(', ')} cannot be read by the requester — nothing was drafted`);
      }

      // 2. Read them all; one unreadable text refuses the run.
      let material: FixtureMaterial;
      try {
        material = await readMaterial({ documentIds: ids });
      } catch (err) {
        return refuse(`deck: the sources could not be read (${err instanceof Error ? err.message : String(err)}) — nothing was drafted`);
      }
      const unreadable = documentsOf(material).filter((document) => document.unreadable);
      if (unreadable.length) {
        return refuse(`deck: ${unreadable.map((document) => `“${document.title}” (${document.documentId}) could not be read: ${document.unreadable}`).join('; ')} — nothing was drafted`);
      }

      // 3. Draft: breaker + deck slot + invented-citation refusal inside.
      const sources = citableSources(material);
      const title = (str(context.instruction) ?? sources.map((source) => source.title).join(' · ')).slice(0, MAX_TITLE);
      // An explicit count ("a 6-slide review") is the number of content
      // slides; the Sources slide the render step adds is extra.
      const instruction = str(context.instruction);
      const slideCount = requestedSlideCount(instruction);
      if (slideCount !== undefined && slideCount > MAX_REQUESTED_SLIDES) {
        return refuse(`deck: ${slideCount} slides were asked for; a deck holds at most ${MAX_REQUESTED_SLIDES} besides its Sources slide — nothing was drafted`);
      }
      let result: Awaited<ReturnType<typeof buildDeckFixture>>;
      try {
        result = await draft({ documentIds: ids }, title, {
          ...deps.fixtureOpts, material,
          ...(slideCount !== undefined ? { slideCount } : {}),
          ...(instruction ? { instruction } : {}),
        });
      } catch (err) {
        return refuse(`deck: ${err instanceof Error ? err.message : String(err)}`);
      }
      // The drafter said the sources cannot fill the asked count: say so, never pad.
      if (result.refused) {
        return refuse(`deck: ${result.refused}${result.measured ? `. ${result.measured}` : ''} — nothing was saved`);
      }
      const spec = citeEverySource(result.spec, sources);
      return {
        status: 'completed',
        output: {
          spec,
          source: result.source,
          ...(result.reason ? { reason: result.reason } : {}),
          warnings: result.warnings,
          sources,
          sourceCount: sources.length,
          ...(result.slideCheck ? { slideCheck: result.slideCheck } : {}),
          ...(result.measured ? { measured: result.measured } : {}),
        },
      };
    },
  };
}
