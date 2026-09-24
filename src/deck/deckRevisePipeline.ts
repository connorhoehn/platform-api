// A composer edit of a presentation, as a pipeline run: `deck-revise`.
//
//   trigger → load ("Read the deck") → propose ("Write the edit") → check ("Check the edit") → write ("Save the new revision")
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
// - check    measures the proposal against the countable parts of the
//            instruction (deckConstraints.ts: "one word shorter", "three
//            bullets", "under 40 characters", "keep the title") before and
//            after. A violation gets the model ONE more try, told the numbers;
//            still violated, the run fails `constraint-not-met` listing the
//            measurements and writes nothing. The summary the run records is
//            the model's claim with contradicted sentences dropped, plus the
//            facts ("Title: 3 → 2 words"). The proposal it reads is `propose`'s
//            recorded output, so a retried check never re-runs the model for a
//            proposal that is already on the run.
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
import { MODEL_AUTH_RE } from '../pipeline/error-classification';
import { validateReviseTarget, type DeckReviseTarget, type DeckSlide, type DeckSpec } from './DeckSpec';
import { DeckReviseError, reviseDeck, validateReviseInputSpec, type DeckReviseOpts, type DeckReviseSourceRef } from './deckRevise';
import {
  checkConstraints, describeChanges, factLines, parseConstraints, reconcileSummary, retryInstruction,
  type Constraint, type ConstraintCheck,
} from './deckConstraints';
import {
  createDeckReviseEmitter, durableDeckRevisePublish,
  type DeckReviseDurablePublish, type DeckReviseEmitter, type DeckRevisePublish,
} from './deckReviseEvents';
import { renderDeckSlides } from './DeckStepHandler';
import {
  getPresentationDocumentStore, PresentationRevisionConflictError, PRESENTATION_DOCUMENT_TYPE,
  type PresentationDocumentRow, type PresentationDocumentStore, type StoredRevision,
} from './presentationDocuments';
import { canEditDocument, documentRoleOf, editRefusal } from './documentEditAccess';

export {
  DECK_REVISE_PIPELINE_ID, DECK_REVISE_LOAD_KIND, DECK_REVISE_PROPOSE_KIND, DECK_REVISE_CHECK_KIND, DECK_REVISE_WRITE_KIND, buildDeckReviseDefinition,
} from './deckReviseDefinition';
import { DECK_REVISE_PIPELINE_ID, DECK_REVISE_LOAD_KIND, DECK_REVISE_PROPOSE_KIND, DECK_REVISE_CHECK_KIND, DECK_REVISE_WRITE_KIND } from './deckReviseDefinition';
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

/**
 * Every model slot taken: the step says so in the run pane — a step frame,
 * `pipeline:step:waiting` (wire `pipeline.step.waiting`), with its place in
 * line — and once more with state 'granted' when its call starts.
 */
function modelSlotFrames(helpers: StepHandlerHelpers): NonNullable<DeckReviseOpts['onModelSlot']> {
  const emit = helpers.emit as unknown as (event: string, payload: unknown) => Promise<void>;
  return (slot) => {
    void emit('pipeline:step:waiting', {
      runId: helpers.runId, stepId: helpers.stepId, reason: 'model-slot', state: slot.state,
      ...(slot.position !== undefined ? { position: slot.position } : {}),
      waitedMs: slot.waitedMs, at: new Date().toISOString(),
    }).catch(() => undefined);
  };
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

/** A value with its object keys sorted, so two equal specs compare equal whatever their key order. */
function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value as Record<string, unknown>).sort()
      .filter((key) => (value as Record<string, unknown>)[key] !== undefined)
      .map((key) => [key, canonical((value as Record<string, unknown>)[key])]));
  }
  return value;
}

/**
 * Deep equality by content. Key order is NOT content: the load step's spec has
 * the validator's key order, a head read back from DynamoDB has the order it
 * was stored in, so a key-order-sensitive compare called every unchanged slide
 * "changed" and no scoped edit ever rebased (live 2026-09-23, NFR loop 25: two
 * runs on different slides of one deck, the second refused as "changed the same
 * part"). Undefined-valued keys are absent, as they are once stored.
 */
export const same = (a: unknown, b: unknown): boolean => JSON.stringify(canonical(a ?? null)) === JSON.stringify(canonical(b ?? null));

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
          {
            ...opts.reviseOpts,
            onPhase: (phase) => events.phase(phase),
            onModelSlot: modelSlotFrames(helpers),
          },
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
          if (/^model unavailable:/.test(err.reason)) {
            // The model circuit breaker is open (NFR #225): the step says
            // "Model unavailable: <reason>" at once, and is not retried — the
            // breaker would refuse the retry the same way.
            const outcome = fail(runId, request, 503, err.message, 'model-unavailable');
            log.warn({ runId, reason: err.reason, documentId: request.documentId }, 'deck revise run: model unavailable (breaker open)');
            // "unavailable" reads as transient to the executor's classifier;
            // retry: false (distributed-core 0.46.5) returns it at once.
            return { ...outcome, retry: false };
          }
          if (MODEL_AUTH_RE.test(err.reason)) {
            // The model's login, not the edit: say so plainly, once, and never
            // retry (ModelAuth is terminal). The operator step is in the log.
            const outcome = fail(runId, request, 503, 'The agent could not sign in to the model — its credentials have expired, so nothing was changed. Ask the operator to refresh them, then try again.', 'model-auth');
            log.warn({ runId, reason: err.reason, documentId: request.documentId }, 'deck revise run: model credentials rejected (tilt trigger claude-creds-extract)');
            return outcome;
          }
          if (/^model busy:/.test(err.reason)) {
            // Waited its turn for a model slot and none came free: say so, and
            // do not retry — another try joins the back of the same line.
            const outcome = fail(runId, request, 503, err.message, 'model-busy');
            log.warn({ runId, reason: err.reason, documentId: request.documentId }, 'deck revise run: no model slot came free');
            return outcome;
          }
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

export interface CheckOutput {
  documentId: string;
  /** The proposal the write saves: the retried one when the first missed a constraint. */
  spec: DeckSpec;
  /** The model's claim reconciled with the measurements. */
  summary: string;
  changedSlideIds: string[];
  warnings: string[];
  changed: boolean;
  /** What the instruction asked in countable terms, as parsed. */
  constraints: Constraint[];
  /** "Title: 3 → 2 words" — before the edit → after it. */
  facts: string[];
  /** What moved, when nothing was counted too: "slide 1 subtitle", "slide 3 bullets". */
  changes: string[];
  /** True when the first proposal missed a constraint and the model was asked once more. */
  retried: boolean;
}

function constraintsFailure(check: ConstraintCheck): string {
  const lines = check.violations.map((v) => v.message);
  return `The edit did not do what was asked: ${lines.join(' ')} Measured: ${factLines(check.facts).join('; ')}. Nothing was saved.`;
}

export function createDeckReviseCheckHandler(opts: DeckReviseStepOpts = {}): DescribedStepHandler {
  const revise = opts.revise ?? reviseDeck;
  return {
    kind: DECK_REVISE_CHECK_KIND,
    description: 'Measures the proposed edit against the countable parts of the instruction (words, characters, bullets, slides, parts to keep or remove), asks the model once more with the numbers when it missed, and fails the run honestly when it still misses. Writes nothing.',
    reads: 'The loaded spec, the proposal, instruction, slideId, target.',
    emits: 'spec (the proposal to save), summary (claims reconciled with the measurements), facts, constraints, retried, changedSlideIds, warnings, changed.',
    async execute(_node: PipelineNode, context: Record<string, unknown>, helpers: StepHandlerHelpers): Promise<StepOutcome> {
      const request = readRunRequest(context);
      const { runId } = helpers;
      const loaded = stepOutput<LoadOutput>(context, 'load');
      const proposed = stepOutput<ProposeOutput>(context, 'propose');
      if (!loaded?.spec || !proposed?.spec) return fail(runId, request, 500, 'The edit was not made, so there was nothing to check.', 'no-edit');
      const events = eventsForRun(runId, request).emitter;
      events.phase('checking');
      const scope = { slideId: request.slideId, target: request.target };
      const constraints = parseConstraints(request.instruction, scope);
      let spec = proposed.spec;
      let summary = proposed.summary;
      let changedSlideIds = proposed.changedSlideIds;
      let warnings = proposed.warnings;
      // The model's summary, read like an instruction, says what it claims to
      // have done; those subjects are measured whether or not they were asked.
      const claimsOf = (claim: string) => parseConstraints(claim, scope);
      let check = checkConstraints(constraints, loaded.spec, spec, scope, claimsOf(summary));
      let retried = false;
      if (check.violations.length) {
        retried = true;
        log.info({ runId, documentId: request.documentId, violations: check.violations.map((v) => v.message) }, 'deck revise run: the proposal missed a constraint; asking the model once more');
        try {
          const again = await revise(
            {
              spec: loaded.spec, instruction: retryInstruction(request.instruction, check.violations),
              ...(request.slideId ? { slideId: request.slideId } : {}),
              ...(request.target ? { target: request.target } : {}),
              sources: request.sources,
            },
            { ...opts.reviseOpts, onPhase: (phase) => events.phase(phase), onModelSlot: modelSlotFrames(helpers) },
          );
          const secondCheck = checkConstraints(constraints, loaded.spec, again.spec, scope, claimsOf(again.summary));
          if (secondCheck.violations.length) {
            const outcome = fail(runId, request, 422, constraintsFailure(secondCheck), 'constraint-not-met');
            log.warn({ runId, documentId: request.documentId, violations: secondCheck.violations.map((v) => v.message) }, 'deck revise run: the edit still misses a constraint after a second try');
            return outcome;
          }
          spec = again.spec;
          summary = again.summary;
          changedSlideIds = again.changedSlideIds;
          warnings = again.warnings;
          check = secondCheck;
        } catch (err) {
          if (err instanceof DeckReviseError) {
            // The second try could not be made at all: the first one stands
            // judged, and the run fails on the measurements, not the outage.
            const outcome = fail(runId, request, 422, `${constraintsFailure(check)} (A second try was not possible: ${err.reason}.)`, 'constraint-not-met');
            return outcome;
          }
          throw err;
        }
      }
      const fallback = changedSlideIds.length ? `Changed ${changedSlideIds.length} slide${changedSlideIds.length === 1 ? '' : 's'}.` : 'No slides changed.';
      const changes = describeChanges(loaded.spec, spec);
      const output: CheckOutput = {
        documentId: request.documentId,
        spec,
        summary: reconcileSummary(summary, check.facts, fallback, changes),
        changedSlideIds,
        warnings,
        changed: !same(spec, loaded.spec),
        constraints,
        facts: factLines(check.facts),
        changes,
        retried,
      };
      return { status: 'completed', output: output as unknown as Record<string, unknown> };
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
  /** The check step's measurements: "Title: 3 → 2 words". */
  facts?: string[];
  /** What the edit changed: "slide 1 subtitle". */
  changes?: string[];
  /** True when the model was asked a second time to meet a constraint. */
  retried?: boolean;
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
      // The checked proposal (possibly the model's second try); the raw one
      // only on a definition without a check step.
      const checked = stepOutput<CheckOutput>(context, 'check');
      const proposed: CheckOutput | ProposeOutput | undefined = checked ?? stepOutput<ProposeOutput>(context, 'propose');
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
        ...(checked ? { facts: checked.facts, changes: checked.changes ?? [], retried: checked.retried } : {}),
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
          // No .pptx now: most edits are never downloaded, and the first
          // download builds it once (content key, pptxCache.ts, NFR #178).
          const slides = renderDeckSlides(spec);
          const { revision } = await store.appendRevision({
            documentId: request.documentId,
            title: loaded.title,
            createdBy: request.requestedBy,
            slides, spec,
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
  return [createDeckReviseLoadHandler(opts), createDeckReviseProposeHandler(opts), createDeckReviseCheckHandler(opts), createDeckReviseWriteHandler(opts)];
}
