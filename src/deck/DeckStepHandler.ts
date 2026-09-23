// The `deck` step: a validated DeckSpec in, a stored presentation out.
//
// It renders nothing it was not given and stores nothing it could not render:
// an invalid spec fails the step with the validator's own list of problems,
// which is what a re-ask needs, rather than half a deck nobody asked for.

import { randomUUID } from 'node:crypto';
import type { PipelineNode, StepHandlerHelpers, StepOutcome } from 'distributed-core/applications/pipeline';
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import { readPath } from '../pipeline/image/ScatterStepHandler';
import { resolveTemplate } from '../pipeline/image/ImageSaveStepHandler';
import { parseDeckSpecJson, validateDeckSpec, type DeckSpec } from './DeckSpec';
import { renderPptx } from './renderPptx';
import { renderAllSlideSvgs } from './renderSlideSvg';
import { SOURCES_SLIDE_ID, SOURCES_SLIDE_TITLE } from './theme';
import {
  getPresentationDocumentStore,
  PRESENTATION_DOCUMENT_TYPE,
  type PresentationDocumentStore,
} from './presentationDocuments';
import { canEditDocument, documentRoleOf, editRefusal } from './documentEditAccess';
import log from '../lib/logger';

export const DECK_STEP_KIND = 'deck';

/** The same escape hatch the agent and vision handlers use for their own events. */
type LooseEmit = (event: string, payload: unknown) => Promise<void>;

export interface DeckNodeData {
  type: typeof DECK_STEP_KIND;
  /**
   * Where the spec is: a context path (`steps.draft.text`, `$.spec`) or a
   * `{{ … }}` template resolved against the run context. Whatever it resolves
   * to may be the object itself or the JSON text a model answered with.
   */
  specFrom?: string;
  /** The spec inline, for a pipeline that already knows its deck. */
  spec?: DeckSpec | string;
  /** The presentation document to add a revision to. Defaults to the run's `documentId`. */
  documentId?: string;
  /** Overrides the document's title. Defaults to the deck's own title. */
  title?: string;
  label?: string;
}

const SPEC_FALLBACK_PATHS = ['$.steps.draft.text', '$.steps.plan.text', '$.spec', '$.deckSpec'];
/**
 * Where the step that read the material said which source was a transcript.
 * A transcript is known by the reader that fetched it, never by its id: the
 * seeded one is `active-now-…-planning-transcript`, and a document may be named
 * anything at all.
 */
const TRANSCRIPT_SOURCE_PATHS = ['$.transcriptSource.documentId', "$.steps['read-sources'].transcriptSource.documentId"];
const DOCUMENT_FALLBACK_PATHS = ['$.documentId', '$.trigger.documentId'];
const REQUESTER_FALLBACK_PATHS = ['$.requestedBy', '$.trigger.requestedBy'];

function str(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

/** Resolve a node-data string that may be a `{{ … }}` template or a context path. */
function resolve(value: string | undefined, context: Record<string, unknown>): unknown {
  if (!value) return undefined;
  if (value.includes('{{')) {
    const rendered = resolveTemplate(value, context).trim();
    return rendered || undefined;
  }
  return readPath(context, value);
}

function firstOf(paths: readonly string[], context: Record<string, unknown>): string | undefined {
  for (const path of paths) {
    const value = str(readPath(context, path));
    if (value) return value;
  }
  return undefined;
}

/** The spec the node points at, whether it arrived as an object or as model text. */
export function resolveSpecSource(data: DeckNodeData, context: Record<string, unknown>): unknown {
  const candidates: unknown[] = [
    data.spec,
    resolve(data.specFrom, context),
    ...SPEC_FALLBACK_PATHS.map((path) => readPath(context, path)),
  ];
  for (const candidate of candidates) {
    if (!candidate) continue;
    if (typeof candidate === 'string') {
      const parsed = parseDeckSpecJson(candidate);
      if (parsed) return parsed;
      continue;
    }
    if (typeof candidate === 'object') return candidate;
  }
  return undefined;
}

/**
 * The files a revision is made of: the .pptx and one SVG per slide, Sources
 * last. Shared by this step and the composer's `deck:revise-write` step.
 */
export async function renderDeckFiles(spec: DeckSpec): Promise<{ pptx: Buffer; slides: Array<{ id: string; title: string; svg: string }> }> {
  const pptx = await renderPptx(spec);
  const svgs = renderAllSlideSvgs(spec);
  // The renderers agree on slide order: the deck's own slides, then Sources.
  const slides = svgs.map((svg, index) => {
    const slide = spec.slides[index];
    return { id: slide?.id ?? SOURCES_SLIDE_ID, title: slide?.title ?? SOURCES_SLIDE_TITLE, svg };
  });
  return { pptx, slides };
}

export interface DeckStepHandlerOpts {
  store?: PresentationDocumentStore;
  newDocumentId?: () => string;
}

export function createDeckStepHandler(opts: DeckStepHandlerOpts = {}): DescribedStepHandler {
  const newDocumentId = opts.newDocumentId ?? (() => randomUUID());
  return {
    kind: DECK_STEP_KIND,
    description:
      'Renders a validated DeckSpec into a real .pptx and one SVG per slide, and stores them as a new revision of a presentation document.',
    reads:
      'The spec at the node\'s specFrom (or inline), the target documentId from the node or the run context, and who asked.',
    emits:
      'documentId, revisionId, slideCount and one preview handle per slide — including the Sources slide that closes every deck.',
    async execute(node: PipelineNode, context: Record<string, unknown>, helpers: StepHandlerHelpers): Promise<StepOutcome> {
      const data = (node.data ?? {}) as unknown as DeckNodeData;
      const store = opts.store ?? getPresentationDocumentStore();

      const source = resolveSpecSource(data, context);
      if (source === undefined) {
        return { status: 'failed', error: 'deck: no DeckSpec found — set the node\'s specFrom to the step that drafts it' };
      }
      const validation = validateDeckSpec(source);
      if (!validation.ok) {
        return { status: 'failed', error: `deck: ${validation.errors.join('; ')}` };
      }
      const spec = validation.spec;

      const candidate = str(resolve(data.documentId, context))
        ?? str(data.documentId)
        ?? firstOf(DOCUMENT_FALLBACK_PATHS, context);
      // A loop run's context carries whatever the step before it produced —
      // an agenda step leaves its own `documentId` there — and a deck rendered
      // onto that id would file its revision inside somebody else's page. The
      // deck writes into a presentation or into a new one, never into a
      // document of another kind.
      const notices: string[] = [];
      let documentId = candidate ?? newDocumentId();
      let displaced: { documentId: string; title: string; type: string } | undefined;
      let existing: Awaited<ReturnType<PresentationDocumentStore['get']>> = null;
      if (candidate) {
        // Fail closed: a row that could not be read cannot be checked (#47).
        try { existing = await store.get(candidate); } catch (err) {
          return { status: 'failed', error: `deck: could not read ${candidate} to check who may change it (${err instanceof Error ? err.message : String(err)})` };
        }
        const type = str(existing?.type);
        if (existing && type !== PRESENTATION_DOCUMENT_TYPE) {
          displaced = { documentId: candidate, title: str(existing.title) ?? candidate, type: type ?? 'unknown' };
          documentId = newDocumentId();
          notices.push(
            `${displaced.title} is a ${displaced.type}, not a presentation, so the deck was filed as a new presentation instead of a revision of it.`,
          );
        }
      }
      const requestedBy = firstOf(REQUESTER_FALLBACK_PATHS, context) ?? 'agent:deck';
      // The revision is written as the requester, so the #41 rule applies here
      // too, at write time: an existing presentation takes a revision only from
      // its owner or an editor (NFR #47). The chat command checks the same rule
      // before the run starts; this catches a run whose context names someone
      // else's deck however it got there, and a requester who lost access
      // while the run was queued.
      if (!displaced && candidate && existing) {
        const decision = await canEditDocument(existing, { sub: requestedBy }, documentId);
        if (!decision.allow) {
          log.warn({ runId: helpers.runId, documentId, requestedBy, role: documentRoleOf(existing, requestedBy) },
            'deck step refused: the requester may not edit this presentation');
          return { status: 'failed', error: `deck forbidden: ${editRefusal(existing, requestedBy)}` };
        }
      }
      const transcriptIds = [...new Set([
        ...TRANSCRIPT_SOURCE_PATHS.map((path) => str(readPath(context, path))).filter((value): value is string => !!value),
        ...(Array.isArray(context.transcriptIds) ? context.transcriptIds.map((value) => str(value)).filter((value): value is string => !!value) : []),
      ])];
      const title = str(data.title) ?? spec.title;

      try {
        const { pptx, slides } = await renderDeckFiles(spec);

        const { revision } = await store.appendRevision({
          documentId, title, createdBy: requestedBy, pptx, slides, spec,
          ...(transcriptIds.length ? { transcriptIds } : {}),
        });
        const previewHandles = revision.slides.map((slide) => slide.previewHandle);

        try {
          await (helpers.emit as unknown as LooseEmit)('pipeline:step:deck:produced', {
            runId: helpers.runId,
            stepId: helpers.stepId,
            documentId,
            revisionId: revision.id,
            slideCount: revision.slideCount,
            title,
            at: revision.createdAt,
          });
        } catch {
          // a bad subscriber never fails the step
        }

        return {
          status: 'completed',
          output: {
            documentId,
            revisionId: revision.id,
            slideCount: revision.slideCount,
            previewHandles,
            title,
            ...(notices.length ? { notices } : {}),
            ...(displaced
              ? {
                // Named, so the run says which document it did NOT write into.
                displacedDocument: displaced,
                citesDisplacedDocument: spec.slides.some((slide) => slide.sources.some((source) => source.documentId === displaced!.documentId)),
              }
              : {}),
            pptxSizeBytes: revision.asset.sizeBytes,
            slides: revision.slides.map((slide) => ({ id: slide.id, index: slide.index, title: slide.title, previewHandle: slide.previewHandle })),
            // What the run PRODUCED, declared on the step's own output: the run
            // is the provenance of this revision, and the work-graph event
            // carries the declaration as an artifact of the run (see
            // src/pipeline/declaredArtifacts.ts).
            artifacts: [{
              artifactId: documentId,
              kind: 'presentation',
              revisionId: revision.id,
              slideCount: revision.slideCount,
              title,
              sources: revision.sources ?? [],
            }],
            // The run-result convention: what 'Open' on the card opens.
            result: { kind: 'document', documentId },
          },
        };
      } catch (err) {
        return { status: 'failed', error: `deck: ${err instanceof Error ? err.message : String(err)}` };
      }
    },
  };
}
