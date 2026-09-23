// A composer edit of a presentation, as a pipeline run: `deck-revise`.
//
//   trigger → load ("Read the deck") → propose ("Write the edit") → write ("Save the new revision")
//
// The browser starts the run (`POST /api/deck/revise-run`) and watches it; the
// RUN reads the deck, asks the model and files the revision. It is a
// `document-edit` shaped pipeline (src/documents/documentEditPipeline.ts).
//
// - load     the presentation row → the input spec (the editor's unsaved `spec`,
//            or the head revision's) and the BASE revision the write is
//            conditioned on. A `baseRevisionId` that is no longer the head
//            fails here, before any model time is spent.
// - propose  `reviseDeck()` — the same grounding, validation and refusals as
//            `POST /api/deck/revise`. Writes nothing.
// - write    renders and appends the revision with `expectedHead = base`.
//            Idempotent by `requestId` (recorded as `producedBy` on the
//            revision). On a concurrent save (409), a slide- or field-scoped
//            edit REBASES when the new head still has the edited slide (or the
//            subtitle) exactly as the base had it; otherwise — and always for a
//            deck-scoped edit — the run fails with `revision-conflict` and
//            writes nothing. Nobody's save is overwritten.
//
// Progress: the steps publish `pipeline.deck.revise.*` on the document's
// channel (deckReviseEvents.ts) with `pipelineRunId`; the terminal `failed` for
// a run that dies in any step comes from `attachDeckReviseRunEvents`, which
// listens for the run's own terminal events on the executor's bus.

import type { PipelineNode, StepHandlerHelpers, StepOutcome } from 'distributed-core/applications/pipeline';
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import log from '../lib/logger';
import { validateReviseTarget, type DeckReviseTarget, type DeckSlide, type DeckSpec } from './DeckSpec';
import { DeckReviseError, reviseDeck, validateReviseInputSpec, type DeckReviseOpts, type DeckReviseSourceRef } from './deckRevise';
import {
  createDeckReviseEmitter, durableDeckRevisePublish,
  type DeckReviseDurablePublish, type DeckReviseEmitter, type DeckRevisePublish,
} from './deckReviseEvents';
import { renderDeckFiles } from './DeckStepHandler';
import {
  getPresentationDocumentStore, PresentationRevisionConflictError, PRESENTATION_DOCUMENT_TYPE,
  type PresentationDocumentRow, type PresentationDocumentStore, type StoredRevision,
} from './presentationDocuments';
import { canEditDocument, documentRoleOf, editRefusal } from './documentEditAccess';

export {
  DECK_REVISE_PIPELINE_ID, DECK_REVISE_LOAD_KIND, DECK_REVISE_PROPOSE_KIND, DECK_REVISE_WRITE_KIND, buildDeckReviseDefinition,
} from './deckReviseDefinition';
import { DECK_REVISE_PIPELINE_ID, DECK_REVISE_LOAD_KIND, DECK_REVISE_PROPOSE_KIND, DECK_REVISE_WRITE_KIND } from './deckReviseDefinition';
/** How many times a scoped edit may rebase onto a newer head before giving up. */
export const MAX_REBASES = 3;

// ---------------------------------------------------------------------------
// The request, as the run context carries it
// ---------------------------------------------------------------------------

export interface DeckReviseRunRequest {
  documentId: string;
  requestId: string;
  requestedBy: string;
  instruction: string;
  slideId?: string;
  target?: DeckReviseTarget;
  sources: DeckReviseSourceRef[];
  /** The editor's unsaved spec, when it sent one. */
  spec?: unknown;
  /** undefined: not given (use the head). null: "no revision yet". */
  baseRevisionId?: string | null;
}

function str(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

/** A trigger field, wherever the executor put it (top level, or under `trigger`). */
function field(context: Record<string, unknown>, key: string): unknown {
  if (key in context) return context[key];
  const trigger = context.trigger as Record<string, unknown> | undefined;
  return trigger && typeof trigger === 'object' ? trigger[key] : undefined;
}

export function readRunRequest(context: Record<string, unknown>): DeckReviseRunRequest {
  const base = field(context, 'baseRevisionId');
  const sources = field(context, 'sources');
  const target = field(context, 'target');
  return {
    documentId: str(field(context, 'documentId')) ?? '',
    requestId: str(field(context, 'requestId')) ?? '',
    requestedBy: str(field(context, 'requestedBy')) ?? 'unknown',
    instruction: str(field(context, 'instruction')) ?? '',
    ...(str(field(context, 'slideId')) ? { slideId: str(field(context, 'slideId')) } : {}),
    ...(target && typeof target === 'object' ? { target: target as DeckReviseTarget } : {}),
    sources: Array.isArray(sources) ? sources as DeckReviseSourceRef[] : [],
    ...(field(context, 'spec') !== undefined ? { spec: field(context, 'spec') } : {}),
    ...(base === null ? { baseRevisionId: null } : str(base) ? { baseRevisionId: str(base) } : {}),
  };
}

function stepOutput<T>(context: Record<string, unknown>, stepId: string): T | undefined {
  const steps = context.steps as Record<string, unknown> | undefined;
  const out = steps?.[stepId];
  return out && typeof out === 'object' ? out as T : undefined;
}

// ---------------------------------------------------------------------------
// Per-run progress events
// ---------------------------------------------------------------------------

interface RunEvents {
  emitter: DeckReviseEmitter;
  startedSent?: boolean;
  failure?: { status: number; reason: string; code?: string };
}

const runs = new Map<string, RunEvents>();

export interface DeckReviseRunEventDeps {
  publish?: DeckRevisePublish;
  durable?: DeckReviseDurablePublish | null;
}

let eventDeps: DeckReviseRunEventDeps = {};
/** Tests: where the run's events go. */
export function setDeckReviseRunEventDeps(next: DeckReviseRunEventDeps): void { eventDeps = next; }
/** Tests: forget every run. */
export function resetDeckReviseRuns(): void { runs.clear(); }

/**
 * The run's emitter, made on first use from the run context. Every step of a
 * run executes on the node that owns it, so this map is the run's; a run
 * resumed on another node after a crash makes a fresh one there.
 */
export function eventsForRun(runId: string, request: DeckReviseRunRequest): RunEvents {
  let entry = runs.get(runId);
  if (!entry) {
    entry = {
      emitter: createDeckReviseEmitter({
        documentId: request.documentId,
        requestId: request.requestId,
        userId: request.requestedBy,
        pipelineRunId: runId,
        ...(request.slideId ? { slideId: request.slideId } : {}),
        ...(request.target ? { target: request.target } : {}),
        ...(eventDeps.publish ? { publish: eventDeps.publish } : {}),
        ...(eventDeps.durable === null ? {} : { durable: eventDeps.durable ?? durableDeckRevisePublish }),
      }),
    };
    runs.set(runId, entry);
    subscribeTerminalEvents();
  }
  return entry;
}

/** Fail the step, and remember how, for the run's terminal `failed` event. */
function fail(runId: string, request: DeckReviseRunRequest, status: number, reason: string, code?: string): StepOutcome {
  eventsForRun(runId, request).failure = { status, reason, ...(code ? { code } : {}) };
  // The executor retries only errors that read as transient; a code word
  // first keeps a refusal from being mistaken for one.
  return { status: 'failed', error: `deck-revise ${code ?? 'failed'}: ${reason}` };
}

/** Minimal bus surface: the executor's EventBus. */
export interface RunEventBus {
  subscribe(type: string, handler: (event: unknown) => Promise<void>): string;
}

let terminalBus: RunEventBus | null = null;
let terminalSubscribed = false;

/**
 * Sends the terminal event for a run that ended without its write step saying
 * so — a failure in any step, a cancel — and forgets the run. Handed the
 * executor's bus once, at pipeline bootstrap; it subscribes when the first
 * composer edit runs, so a node that never runs one carries no extra
 * listeners on every run's terminal events.
 */
export function attachDeckReviseRunEvents(bus: RunEventBus): string[] {
  terminalBus = bus;
  terminalSubscribed = false;
  return [];
}

function subscribeTerminalEvents(): void {
  if (terminalSubscribed || !terminalBus) return;
  terminalSubscribed = true;
  const bus = terminalBus;
  const payloadOf = (event: unknown) => ((event as { payload?: Record<string, unknown> })?.payload ?? {}) as Record<string, unknown>;
  const settle = (outcome: 'completed' | 'failed' | 'cancelled') => async (event: unknown): Promise<void> => {
    const runId = str(payloadOf(event).runId);
    if (!runId) return;
    const entry = runs.get(runId);
    if (!entry) return;
    runs.delete(runId);
    if (outcome === 'completed') {
      entry.emitter.completed([], { revisionId: null });
      return;
    }
    const failure = outcome === 'cancelled'
      ? { status: 500, reason: 'The edit was stopped, so nothing was changed.', code: 'cancelled' }
      : entry.failure ?? { status: 500, reason: 'The edit failed unexpectedly, so nothing was changed.' };
    entry.emitter.failed(failure.status, failure.reason, failure.code);
  };
  bus.subscribe('pipeline:run:completed', settle('completed'));
  bus.subscribe('pipeline:run:failed', settle('failed'));
  bus.subscribe('pipeline:run:cancelled', settle('cancelled'));
}

// ---------------------------------------------------------------------------
// Rebase
// ---------------------------------------------------------------------------

const same = (a: unknown, b: unknown): boolean => JSON.stringify(a ?? null) === JSON.stringify(b ?? null);

/**
 * The edit re-applied to a newer head, or null when it cannot be applied
 * without overwriting what the other save changed. Only a slide- or
 * field-scoped edit rebases, and only when the head still has the part it
 * edited exactly as the base had it.
 */
export function rebaseScopedEdit(
  base: DeckSpec, edited: DeckSpec, head: DeckSpec, slideId: string | undefined, target: DeckReviseTarget | undefined,
): DeckSpec | null {
  if (target?.field === 'subtitle') {
    if (!same(head.subtitle, base.subtitle)) return null;
    const { subtitle: _drop, ...rest } = head;
    return edited.subtitle !== undefined ? { ...rest, subtitle: edited.subtitle } : rest;
  }
  if (!slideId) return null;
  const baseSlide = base.slides.find((slide) => slide.id === slideId);
  const headSlide = head.slides.find((slide) => slide.id === slideId);
  const editedSlide = edited.slides.find((slide) => slide.id === slideId);
  if (!baseSlide || !headSlide || !editedSlide || !same(baseSlide, headSlide)) return null;
  return { ...head, slides: head.slides.map((slide): DeckSlide => (slide.id === slideId ? editedSlide : slide)) };
}

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

export interface LoadOutput {
  spec: DeckSpec;
  /** The revision the write is conditioned on; null = the deck has none yet. */
  baseRevisionId: string | null;
  /** Where the spec came from. */
  specFrom: 'request' | 'head';
  title: string;
  /** Cited ids the base revision knew were transcripts. */
  transcriptIds: string[];
}

export interface ProposeOutput {
  spec: DeckSpec;
  summary: string;
  changedSlideIds: string[];
  warnings: string[];
  /** False when the answer is the deck as it was: nothing to write. */
  changed: boolean;
}

/**
 * A run writes a revision as the person who asked for it, so that person must
 * be the document's owner or an editor — at load (before the model is paid for)
 * and again at write. A missing row is the load step's 404, not this.
 */
async function refuseNonEditor(runId: string, request: DeckReviseRunRequest, row: PresentationDocumentRow | null): Promise<StepOutcome | null> {
  if (!row) return null;
  const decision = await canEditDocument(row, { sub: request.requestedBy === 'unknown' ? '' : request.requestedBy }, request.documentId);
  if (decision.allow) return null;
  log.warn({ runId, documentId: request.documentId, requestedBy: request.requestedBy, role: documentRoleOf(row, request.requestedBy) },
    'deck revise run refused: the requester may not edit this document');
  return fail(runId, request, 403, editRefusal(row, request.requestedBy), 'forbidden');
}

export interface DeckReviseStepOpts {
  store?: PresentationDocumentStore;
  revise?: typeof reviseDeck;
  reviseOpts?: DeckReviseOpts;
}

const headOf = (revisions: StoredRevision[] | undefined): StoredRevision | null =>
  revisions?.length ? revisions[revisions.length - 1]! : null;

export function createDeckReviseLoadHandler(opts: DeckReviseStepOpts = {}): DescribedStepHandler {
  return {
    kind: DECK_REVISE_LOAD_KIND,
    description: 'Reads the presentation a composer edit is for, and pins the revision the edit is made against.',
    reads: 'documentId, spec (the editor\'s unsaved deck) or the head revision, baseRevisionId, slideId, target.',
    emits: 'spec, baseRevisionId, specFrom, title, transcriptIds.',
    async execute(_node: PipelineNode, context: Record<string, unknown>, helpers: StepHandlerHelpers): Promise<StepOutcome> {
      const request = readRunRequest(context);
      const { runId } = helpers;
      if (!request.documentId || !request.requestId || !request.instruction) {
        return fail(runId, request, 400, 'The edit is missing its document, request id or instruction.', 'invalid-request');
      }
      const events = eventsForRun(runId, request);
      if (!events.startedSent) { events.startedSent = true; events.emitter.started(); }
      const store = opts.store ?? getPresentationDocumentStore();
      const row = await store.get(request.documentId);
      if (!row || (row.type && row.type !== PRESENTATION_DOCUMENT_TYPE)) {
        return fail(runId, request, 404, 'That presentation does not exist.', 'not-found');
      }
      const denied = await refuseNonEditor(runId, request, row);
      if (denied) return denied;
      const head = headOf(row.revisions);
      const headId = head?.id ?? null;
      if (request.baseRevisionId !== undefined && request.baseRevisionId !== headId) {
        return fail(runId, request, 409,
          `${head?.label || headId || 'Another revision'} was saved after the one this edit started from, so nothing was changed.`,
          'revision-conflict');
      }
      const raw = request.spec !== undefined ? request.spec : head?.spec;
      if (raw === undefined) {
        return fail(runId, request, 400, 'This presentation has no saved deck to edit yet.', 'no-spec');
      }
      const checked = validateReviseInputSpec(raw);
      if (!checked.ok) return fail(runId, request, 400, `The deck is not valid: ${checked.errors[0]}`, 'invalid-spec');
      if (request.slideId && !checked.spec.slides.some((slide) => slide.id === request.slideId)) {
        return fail(runId, request, 400, 'That slide is not in this deck.', 'invalid-slide');
      }
      if (request.target) {
        const target = validateReviseTarget(request.target, checked.spec, request.slideId);
        if (!target.ok) return fail(runId, request, 400, target.error, 'invalid-target');
      }
      const output: LoadOutput = {
        spec: checked.spec,
        baseRevisionId: headId,
        specFrom: request.spec !== undefined ? 'request' : 'head',
        title: str(row.title) ?? checked.spec.title,
        transcriptIds: (head?.sources ?? []).filter((source) => source.kind === 'transcript').map((source) => source.documentId),
      };
      return { status: 'completed', output: output as unknown as Record<string, unknown> };
    },
  };
}

export function createDeckReviseProposeHandler(opts: DeckReviseStepOpts = {}): DescribedStepHandler {
  const revise = opts.revise ?? reviseDeck;
  return {
    kind: DECK_REVISE_PROPOSE_KIND,
    description: 'Has the model make the edit, grounded in the deck\'s sources, and checks it against the deck\'s rules. Writes nothing.',
    reads: 'The loaded spec, instruction, slideId, target, sources.',
    emits: 'spec (the edited deck), summary, changedSlideIds, warnings, changed.',
    async execute(_node: PipelineNode, context: Record<string, unknown>, helpers: StepHandlerHelpers): Promise<StepOutcome> {
      const request = readRunRequest(context);
      const { runId } = helpers;
      const loaded = stepOutput<LoadOutput>(context, 'load');
      if (!loaded?.spec) return fail(runId, request, 500, 'The deck was not loaded, so nothing was changed.', 'no-spec');
      const events = eventsForRun(runId, request).emitter;
      try {
        const result = await revise(
          {
            spec: loaded.spec, instruction: request.instruction,
            ...(request.slideId ? { slideId: request.slideId } : {}),
            ...(request.target ? { target: request.target } : {}),
            sources: request.sources,
          },
          { ...opts.reviseOpts, onPhase: (phase) => events.phase(phase) },
        );
        const output: ProposeOutput = {
          spec: result.spec,
          summary: result.summary,
          changedSlideIds: result.changedSlideIds,
          warnings: result.warnings,
          changed: !same(result.spec, loaded.spec),
        };
        return { status: 'completed', output: output as unknown as Record<string, unknown> };
      } catch (err) {
        if (err instanceof DeckReviseError) {
          // A model that could not be reached is worth one more try; the
          // executor retries a failure whose message reads as transient.
          // Not when the model's credentials are the problem: another try is the same answer.
          const transient = err.status === 503 && !/authenticat|log ?in|credential|forbidden|permission/i.test(err.reason) ? ' (model unavailable)' : '';
          const outcome = fail(runId, request, err.status, err.message, `model-${err.status === 400 ? 'refused' : 'failed'}`);
          log.warn({ runId, status: err.status, reason: err.reason, documentId: request.documentId }, 'deck revise run: the model step failed');
          return { status: 'failed', error: `${(outcome as { error: string }).error} — ${err.reason}${transient}` };
        }
        throw err;
      }
    },
  };
}

export interface WriteOutput {
  documentId: string;
  written: boolean;
  revisionId: string | null;
  baseRevisionId: string | null;
  rebasedOnto?: string;
  replayed?: boolean;
  summary: string;
  changedSlideIds: string[];
  warnings: string[];
  artifacts?: unknown[];
  result: { kind: 'document'; documentId: string };
}

/**
 * Tell run persistence the revision this run wrote, on the same hook the
 * `/deck` render step uses (`pipeline:step:deck:produced`). That is what
 * projects the revision into the work graph as a document revision with the
 * run as its producer — without it the revision a composer edit saved never
 * reached Active Now, the run had no `produced` edge, and its pane could not
 * open the deck at the revision it wrote. Emitted on the run's owning replica
 * (the only one holding its run state); the projection is idempotent per
 * (document, revision), so a resumed run that re-announces is harmless.
 */
async function announceProduced(
  helpers: StepHandlerHelpers,
  documentId: string,
  revisionId: string,
  slideCount: number | undefined,
  title: string | undefined,
  at: string | undefined,
): Promise<void> {
  try {
    await (helpers.emit as unknown as (event: string, payload: unknown) => Promise<void>)('pipeline:step:deck:produced', {
      runId: helpers.runId,
      stepId: helpers.stepId,
      documentId,
      revisionId,
      ...(slideCount !== undefined ? { slideCount } : {}),
      ...(title ? { title } : {}),
      at: at ?? new Date().toISOString(),
    });
  } catch {
    // a bad subscriber never fails the write
  }
}

export function createDeckReviseWriteHandler(opts: DeckReviseStepOpts = {}): DescribedStepHandler {
  return {
    kind: DECK_REVISE_WRITE_KIND,
    description: 'Saves the edited deck as a new revision, conditioned on the revision the edit was made against; rebases a slide-scoped edit when it safely can.',
    reads: 'The loaded base revision and spec, the proposed spec, requestId.',
    emits: 'revisionId (null when nothing changed), rebasedOnto, summary, changedSlideIds, warnings, and the presentation as the run\'s artifact.',
    async execute(_node: PipelineNode, context: Record<string, unknown>, helpers: StepHandlerHelpers): Promise<StepOutcome> {
      const request = readRunRequest(context);
      const { runId } = helpers;
      const loaded = stepOutput<LoadOutput>(context, 'load');
      const proposed = stepOutput<ProposeOutput>(context, 'propose');
      if (!loaded?.spec || !proposed?.spec) return fail(runId, request, 500, 'The edit was not made, so nothing was saved.', 'no-edit');
      const events = eventsForRun(runId, request).emitter;
      const store = opts.store ?? getPresentationDocumentStore();
      const base: WriteOutput = {
        documentId: request.documentId,
        written: false,
        revisionId: null,
        baseRevisionId: loaded.baseRevisionId,
        summary: proposed.summary,
        changedSlideIds: proposed.changedSlideIds,
        warnings: proposed.warnings,
        result: { kind: 'document', documentId: request.documentId },
      };
      if (!proposed.changed) {
        events.completed([], { revisionId: null });
        return { status: 'completed', output: base as unknown as Record<string, unknown> };
      }

      // Idempotent: a write that already landed for this request (a retried
      // step, a run resumed after the pod died) is returned, not repeated.
      const row = await store.get(request.documentId);
      // Checked again at write time, with the person who asked: access can be
      // revoked while the run waits on the model.
      const denied = await refuseNonEditor(runId, request, row);
      if (denied) return denied;
      const already = row?.revisions?.find((revision) => revision.producedBy?.requestId === request.requestId);
      if (already) {
        await announceProduced(helpers, request.documentId, already.id, already.slideCount, loaded.title, already.createdAt);
        events.completed(proposed.changedSlideIds, { revisionId: already.id });
        return { status: 'completed', output: { ...base, written: true, revisionId: already.id, replayed: true } as unknown as Record<string, unknown> };
      }

      events.phase('saving');
      let spec = proposed.spec;
      let expectedHead: string | null = loaded.baseRevisionId;
      let rebasedOnto: string | undefined;
      for (let attempt = 0; ; attempt += 1) {
        try {
          const { pptx, slides } = await renderDeckFiles(spec);
          const { revision } = await store.appendRevision({
            documentId: request.documentId,
            title: loaded.title,
            createdBy: request.requestedBy,
            pptx, slides, spec,
            expectedHead,
            producedBy: { pipelineId: DECK_REVISE_PIPELINE_ID, runId, requestId: request.requestId },
            ...(loaded.transcriptIds.length ? { transcriptIds: loaded.transcriptIds } : {}),
          });
          await announceProduced(helpers, request.documentId, revision.id, revision.slideCount, loaded.title, revision.createdAt);
          events.completed(proposed.changedSlideIds, { revisionId: revision.id, ...(rebasedOnto ? { rebasedOnto } : {}) });
          log.info({ runId, documentId: request.documentId, revisionId: revision.id, rebasedOnto, requestId: request.requestId }, 'deck revise run saved a revision');
          const output: WriteOutput = {
            ...base,
            written: true,
            revisionId: revision.id,
            ...(rebasedOnto ? { rebasedOnto } : {}),
            artifacts: [{
              artifactId: request.documentId, kind: 'presentation', revisionId: revision.id,
              slideCount: revision.slideCount, title: loaded.title, sources: revision.sources ?? [],
            }],
          };
          return { status: 'completed', output: output as unknown as Record<string, unknown> };
        } catch (err) {
          if (!(err instanceof PresentationRevisionConflictError)) {
            return { status: 'failed', error: `deck-revise write: ${err instanceof Error ? err.message : String(err)}` };
          }
          const head = err.head;
          const next = attempt < MAX_REBASES && head?.spec
            ? rebaseScopedEdit(loaded.spec, proposed.spec, head.spec, request.slideId, request.target)
            : null;
          if (!next || !head) {
            return fail(runId, request, 409,
              `${head?.label || head?.id || 'Another revision'} was saved while this edit was being made, and it changed the same ${request.slideId || request.target ? 'part' : 'deck'}, so nothing was saved.`,
              'revision-conflict');
          }
          log.info({ runId, documentId: request.documentId, from: expectedHead, onto: head.id }, 'deck revise run rebased a scoped edit');
          spec = next;
          expectedHead = head.id;
          rebasedOnto = head.id;
        }
      }
    },
  };
}

export function buildDeckReviseStepHandlers(opts: DeckReviseStepOpts = {}): DescribedStepHandler[] {
  return [createDeckReviseLoadHandler(opts), createDeckReviseProposeHandler(opts), createDeckReviseWriteHandler(opts)];
}
