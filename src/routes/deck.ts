// /api/deck — the editor's two fast paths, neither of which writes anything.
//
//   POST /api/deck/preview  { spec }        → the slides, as SVG, plus warnings
//   POST /api/deck/fixture  { from, title } → a DeckSpec derived from real local material
//   POST /api/deck/revise   { spec, instruction, slideId?, target?, sources?, documentId?, requestId? }
//                            → the spec with the agent's edit (+ live `deck:revise:*` progress)
//                            INTERNAL / preview only: composer edits use revise-run.
//   POST /api/deck/revise-run { documentId, instruction, slideId?, target?, sources?, requestId?,
//                               baseRevisionId?, spec? } → 202 { runId } — the `deck-revise`
//                            pipeline makes the edit AND saves it (src/deck/deckRevisePipeline.ts).
//
// Persisting a person's own edits is a separate, deliberate act:
// POST /api/documents/:id/revisions. An agent's edit is persisted by its run.

import { randomUUID } from 'crypto';
import { Router, type Request, type Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { asyncHandler, AppError, ValidationError } from '../middleware/error-handler';
import { previewDeckSpec } from '../deck/preview';
import { buildDeckFixture, NoFixtureMaterialError, type DeckFixtureFrom } from '../deck/deckFixture';
import {
  DeckReviseError, MAX_INSTRUCTION_CHARS, reviseDeck, validateReviseInputSpec, type DeckReviseSourceRef,
} from '../deck/deckRevise';
import { describeReviseTarget, validateReviseTarget, type DeckReviseTarget } from '../deck/DeckSpec';
import { NOOP_DECK_REVISE_EMITTER, createDeckReviseEmitter } from '../deck/deckReviseEvents';
import log from '../lib/logger';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { getRunPersistenceService } from '../pipeline/runPersistence';
import { workGraphPlanFromDefinition } from '../pipeline/workGraphPlan';
import { workSourceAccountRepo } from '../repositories';
import { buildDeckReviseDefinition, DECK_REVISE_PIPELINE_ID } from '../deck/deckRevisePipeline';
import { claimDeckReviseRequest } from '../deck/deckReviseRequests';
import { deckReviseChannelKey } from '../deck/deckReviseEvents';
import { getPresentationDocumentStore, PRESENTATION_DOCUMENT_TYPE } from '../deck/presentationDocuments';
import type { DeckSpec } from '../deck/DeckSpec';

export const deckRouter = Router();

const ID = /^[A-Za-z0-9][A-Za-z0-9_.@:-]{0,200}$/;
/** A client-chosen revise id: short, and safe in a log line and a channel payload. */
const REQUEST_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;
/** A document id rides a WS channel name (`pipeline:run:deck-revise:<id>`, ≤ 100 chars). */
const DOCUMENT_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;

/** A deck bigger than this is a document, not a deck — refused before rendering. */
export const MAX_SPEC_BYTES = 512 * 1024;

/**
 * POST /api/deck/preview
 * Renders a spec without keeping it. 400 when it is not a deck, 413 when it is
 * too big to be one — the editor can act on the difference.
 */
deckRouter.post('/preview', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const body = (req.body ?? {}) as { spec?: unknown };
  const spec = body.spec ?? body;
  const size = Buffer.byteLength(JSON.stringify(spec ?? null));
  if (size > MAX_SPEC_BYTES) {
    throw new AppError(413, `the spec is ${size} bytes; the cap is ${MAX_SPEC_BYTES}`);
  }
  const outcome = previewDeckSpec(spec);
  if (!outcome.ok) {
    res.status(outcome.status).json({ error: outcome.errors[0], errors: outcome.errors });
    return;
  }
  res.status(200).json({
    title: outcome.preview.title,
    slideCount: outcome.preview.slides.length,
    slides: outcome.preview.slides,
    warnings: outcome.preview.warnings,
  });
}));

/**
 * POST /api/deck/fixture
 * A DeckSpec derived from a real project document and/or a real recording's
 * transcript. `source` says whether the model wrote it; when it did not, the
 * reason is returned with it.
 */
deckRouter.post('/fixture', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const body = (req.body ?? {}) as { from?: unknown; title?: unknown };
  const raw = (body.from ?? {}) as Record<string, unknown>;
  const from: DeckFixtureFrom = {};
  for (const key of ['documentId', 'recordingId', 'personId'] as const) {
    const value = raw[key];
    if (value === undefined || value === null) continue;
    if (typeof value !== 'string' || !ID.test(value)) throw new ValidationError(`from.${key} is invalid`);
    from[key] = value;
  }
  if (!from.documentId && !from.recordingId) {
    throw new ValidationError('from.documentId or from.recordingId is required');
  }
  const title = typeof body.title === 'string' ? body.title.slice(0, 120) : undefined;

  try {
    const result = await buildDeckFixture(from, title, {});
    log.info({ from, source: result.source, slides: result.spec.slides.length, userId: req.user?.sub }, 'deck fixture built');
    res.status(200).json({
      spec: result.spec,
      source: result.source,
      ...(result.reason ? { reason: result.reason } : {}),
      warnings: result.warnings,
      transcriptIds: result.transcriptIds,
      material: {
        ...(result.material.document ? { document: { documentId: result.material.document.documentId, title: result.material.document.title, characters: result.material.document.text.length, ...(result.material.document.unreadable ? { unreadable: result.material.document.unreadable } : {}) } } : {}),
        ...(result.material.transcript ? { transcript: { transcriptId: result.material.transcript.transcriptId, title: result.material.transcript.title, segments: result.material.transcript.segments.length } } : {}),
      },
    });
  } catch (err) {
    if (err instanceof NoFixtureMaterialError) throw new ValidationError(err.message);
    throw err;
  }
}));

/**
 * POST /api/deck/revise
 *   { spec, instruction, slideId?, target?: { field, index? }, sources?: [{ kind, id }],
 *     documentId?, requestId? }
 *   → { spec, summary, changedSlideIds, warnings, source: 'model', requestId }
 *
 * `target` narrows a slide-scoped edit to one field of the slide — 'title',
 * 'eyebrow', 'bullets' (index = one bullet), 'columns' (index = 0|1), 'chart',
 * 'quote', 'image', 'notes' — or the deck's 'subtitle'. It must fit the
 * slide's layout; everything outside it is kept byte-for-byte.
 *
 * `documentId` (the presentation being edited) turns on live progress:
 * `pipeline.deck.revise.started|phase|completed|failed` on the gateway channel
 * `pipeline:run:deck-revise:<documentId>` (see `deck/deckReviseEvents.ts`).
 * `requestId` names this revise in those events; one is made when omitted.
 *
 * The editor's agent composer. Applies an instruction to one slide (`slideId`)
 * or the whole deck, grounded in the real text of the deck's sources, and
 * returns the edited spec without keeping it. When the model cannot produce a
 * valid edit the answer is 502/503 with the reason — never an edit it did not
 * make. See `deck/deckRevise.ts`.
 */
deckRouter.post('/revise', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  // Kept for back-compat. It saves nothing, so it is not how a composer edit
  // changes a document any more — that is a `deck-revise` run.
  res.setHeader('Deprecation', 'true');
  res.setHeader('Link', '</api/deck/revise-run>; rel="successor-version"');
  const body = (req.body ?? {}) as {
    spec?: unknown; instruction?: unknown; slideId?: unknown; target?: unknown; sources?: unknown; documentId?: unknown; requestId?: unknown;
  };
  const size = Buffer.byteLength(JSON.stringify(body.spec ?? null));
  if (size > MAX_SPEC_BYTES) throw new AppError(413, `the spec is ${size} bytes; the cap is ${MAX_SPEC_BYTES}`);

  const instruction = typeof body.instruction === 'string' ? body.instruction.trim() : '';
  if (!instruction) throw new ValidationError('instruction is required');
  if (instruction.length > MAX_INSTRUCTION_CHARS) throw new ValidationError(`instruction is longer than ${MAX_INSTRUCTION_CHARS} characters`);

  const checked = validateReviseInputSpec(body.spec);
  if (!checked.ok) {
    res.status(400).json({ error: checked.errors[0], errors: checked.errors });
    return;
  }
  const spec = checked.spec;

  let slideId: string | undefined;
  if (body.slideId !== undefined && body.slideId !== null) {
    if (typeof body.slideId !== 'string' || !spec.slides.some((slide) => slide.id === body.slideId)) {
      throw new ValidationError('slideId does not name a slide in this deck');
    }
    slideId = body.slideId;
  }

  let target: DeckReviseTarget | undefined;
  if (body.target !== undefined && body.target !== null) {
    const checkedTarget = validateReviseTarget(body.target, spec, slideId);
    if (!checkedTarget.ok) throw new ValidationError(checkedTarget.error);
    target = checkedTarget.target;
  }

  let documentId: string | undefined;
  if (body.documentId !== undefined && body.documentId !== null) {
    if (typeof body.documentId !== 'string' || !DOCUMENT_ID.test(body.documentId)) throw new ValidationError('documentId is invalid');
    documentId = body.documentId;
  }
  let requestId: string = randomUUID();
  if (body.requestId !== undefined && body.requestId !== null) {
    if (typeof body.requestId !== 'string' || !REQUEST_ID.test(body.requestId)) throw new ValidationError('requestId is invalid (letters, digits, - and _, at most 64)');
    requestId = body.requestId;
  }

  const sources: DeckReviseSourceRef[] = [];
  if (body.sources !== undefined) {
    if (!Array.isArray(body.sources)) throw new ValidationError('sources must be an array');
    for (const [index, raw] of body.sources.entries()) {
      const entry = (raw ?? {}) as { kind?: unknown; id?: unknown };
      if (entry.kind !== 'document' && entry.kind !== 'transcript' && entry.kind !== 'recording') {
        throw new ValidationError(`sources[${index}].kind must be 'document', 'transcript' or 'recording'`);
      }
      if (typeof entry.id !== 'string' || !ID.test(entry.id)) throw new ValidationError(`sources[${index}].id is invalid`);
      sources.push({ kind: entry.kind, id: entry.id });
    }
  }

  const userId = req.user?.sub ?? 'unknown';
  const events = documentId
    ? createDeckReviseEmitter({ documentId, requestId, userId, ...(slideId ? { slideId } : {}), ...(target ? { target } : {}) })
    : NOOP_DECK_REVISE_EMITTER;
  events.started();
  try {
    const result = await reviseDeck(
      { spec, instruction, ...(slideId ? { slideId } : {}), ...(target ? { target } : {}), sources },
      { onPhase: (phase) => events.phase(phase) },
    );
    events.completed(result.changedSlideIds);
    log.info({ scope: target ? 'field' : slideId ? 'slide' : 'deck', ...(target ? { target: describeReviseTarget(target) } : {}), changed: result.changedSlideIds.length, requestId, documentId, userId }, 'deck revised');
    res.status(200).json({ ...result, requestId });
  } catch (err) {
    if (err instanceof DeckReviseError) {
      // The event carries the plain sentence, not `reason`: a model's reason
      // can quote the deck, and the channel is not per-document authorized.
      events.failed(err.status, err.message);
      log.warn({ status: err.status, reason: err.reason, requestId, documentId, userId }, 'deck revise refused');
      res.status(err.status).json({ error: err.message, reason: err.reason, requestId, ...(err.errors.length ? { errors: err.errors } : {}) });
      return;
    }
    events.failed(500, 'The revise failed unexpectedly, so nothing was changed.');
    throw err;
  }
}));

/** A run's spec rides the run context and its step records; keep it well under a DDB item. */
export const MAX_RUN_SPEC_BYTES = 128 * 1024;
const REVISION_ID = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/;

/**
 * POST /api/deck/revise-run
 *   { documentId, instruction, slideId?, target?, sources?, requestId?, baseRevisionId?, spec? }
 *   → 202 { runId, pipelineId:'deck-revise', documentId, requestId, replay, channels }
 *
 * A composer edit of a presentation as a pipeline run. Answers as soon as the
 * run exists; the run reads the deck, has the model make the edit and saves it
 * as a new revision conditioned on the base (see deck/deckRevisePipeline.ts).
 * Idempotent by `requestId` per person and document, across replicas.
 * Contract: realtime-examples docs/design/documents-editor-handoff/REVIEW.md,
 * "Composer edits run as pipelines".
 */
deckRouter.post('/revise-run', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const body = (req.body ?? {}) as {
    documentId?: unknown; instruction?: unknown; slideId?: unknown; target?: unknown; sources?: unknown;
    requestId?: unknown; baseRevisionId?: unknown; spec?: unknown; requestedByName?: unknown;
  };
  if (typeof body.documentId !== 'string' || !DOCUMENT_ID.test(body.documentId)) throw new ValidationError('documentId is required and must be a document id');
  const documentId = body.documentId;
  const instruction = typeof body.instruction === 'string' ? body.instruction.trim() : '';
  if (!instruction) throw new ValidationError('instruction is required');
  if (instruction.length > MAX_INSTRUCTION_CHARS) throw new ValidationError(`instruction is longer than ${MAX_INSTRUCTION_CHARS} characters`);
  let requestId: string = randomUUID();
  if (body.requestId !== undefined && body.requestId !== null) {
    if (typeof body.requestId !== 'string' || !REQUEST_ID.test(body.requestId)) throw new ValidationError('requestId is invalid (letters, digits, - and _, at most 64)');
    requestId = body.requestId;
  }
  let baseRevisionId: string | null | undefined;
  if (body.baseRevisionId === null) baseRevisionId = null;
  else if (body.baseRevisionId !== undefined) {
    if (typeof body.baseRevisionId !== 'string' || !REVISION_ID.test(body.baseRevisionId)) throw new ValidationError('baseRevisionId is invalid');
    baseRevisionId = body.baseRevisionId;
  }

  const row = await getPresentationDocumentStore().get(documentId);
  if (!row || (row.type && row.type !== PRESENTATION_DOCUMENT_TYPE)) {
    res.status(404).json({ error: 'That presentation does not exist.', documentId });
    return;
  }

  // The spec the edit is made on: the editor's unsaved one, or the head's.
  let spec: DeckSpec | undefined;
  if (body.spec !== undefined && body.spec !== null) {
    const size = Buffer.byteLength(JSON.stringify(body.spec));
    if (size > MAX_RUN_SPEC_BYTES) throw new AppError(413, `the spec is ${size} bytes; a composer edit takes at most ${MAX_RUN_SPEC_BYTES}`);
    const checked = validateReviseInputSpec(body.spec);
    if (!checked.ok) { res.status(400).json({ error: checked.errors[0], errors: checked.errors }); return; }
    spec = checked.spec;
  } else {
    const head = row.revisions?.length ? row.revisions[row.revisions.length - 1] : undefined;
    const checked = head?.spec ? validateReviseInputSpec(head.spec) : undefined;
    if (!checked?.ok) throw new ValidationError('this presentation has no saved deck to edit; send spec');
  }
  const against = spec ?? (validateReviseInputSpec(row.revisions![row.revisions!.length - 1]!.spec) as { spec: DeckSpec }).spec;

  let slideId: string | undefined;
  if (body.slideId !== undefined && body.slideId !== null) {
    if (typeof body.slideId !== 'string' || !against.slides.some((slide) => slide.id === body.slideId)) {
      throw new ValidationError('slideId does not name a slide in this deck');
    }
    slideId = body.slideId;
  }
  let target: DeckReviseTarget | undefined;
  if (body.target !== undefined && body.target !== null) {
    const checkedTarget = validateReviseTarget(body.target, against, slideId);
    if (!checkedTarget.ok) throw new ValidationError(checkedTarget.error);
    target = checkedTarget.target;
  }
  const sources: DeckReviseSourceRef[] = [];
  if (body.sources !== undefined) {
    if (!Array.isArray(body.sources)) throw new ValidationError('sources must be an array');
    for (const [index, raw] of body.sources.entries()) {
      const entry = (raw ?? {}) as { kind?: unknown; id?: unknown };
      if (entry.kind !== 'document' && entry.kind !== 'transcript' && entry.kind !== 'recording') {
        throw new ValidationError(`sources[${index}].kind must be 'document', 'transcript' or 'recording'`);
      }
      if (typeof entry.id !== 'string' || !ID.test(entry.id)) throw new ValidationError(`sources[${index}].id is invalid`);
      sources.push({ kind: entry.kind, id: entry.id });
    }
  }

  const userId = req.user!.sub;
  const channels = { document: `pipeline:run:${deckReviseChannelKey(documentId)}`, run: '' };
  const answer = (runId: string, replay: boolean) => res.status(202).json({
    runId, pipelineId: DECK_REVISE_PIPELINE_ID, documentId, requestId, replay,
    channels: { ...channels, run: `pipeline:run:${runId}` },
  });

  const bridge = getPipelineBridge();
  if (!bridge?.trigger) { res.status(503).json({ error: 'pipeline engine is not available' }); return; }

  const claim = await claimDeckReviseRequest({ userId, documentId, requestId });
  if (claim.status === 'existing') { answer(claim.runId, true); return; }
  if (claim.status === 'in-flight') {
    res.status(409).json({ error: 'This request is still being started; retry in a moment.', code: 'request-in-flight', requestId });
    return;
  }

  try {
    // The joins the work-graph consumer re-checks before it accepts this
    // person's run (pipeline source) and the presentation it declares
    // (document source) — as the chat commands do.
    const organizationId = req.user!.orgId;
    if (organizationId) {
      await Promise.all([
        workSourceAccountRepo.ensureVerifiedPrincipal({ source: 'pipeline', organizationId, subject: userId }),
        workSourceAccountRepo.ensureVerifiedSourcePrincipal({ source: 'document', producerServiceId: 'websocket-gateway', organizationId, subject: userId }),
      ]);
    }
    const definition = buildDeckReviseDefinition();
    const plan = workGraphPlanFromDefinition(definition);
    // Named after the deck; the projection appends the instruction itself.
    const workGraph = plan ? { ...plan, title: `Edit ${(row.title ?? 'presentation').replace(/\s+/g, ' ').slice(0, 70)}` } : undefined;
    const requestedByName = typeof body.requestedByName === 'string' && body.requestedByName.trim() ? body.requestedByName.trim().slice(0, 80) : userId;
    const out = await Promise.resolve(bridge.trigger({
      pipelineId: DECK_REVISE_PIPELINE_ID,
      definition: definition as never,
      triggerPayload: {
        documentId, instruction, requestId, requestedBy: userId, requestedByName,
        ...(slideId ? { slideId } : {}), ...(target ? { target } : {}), sources,
        ...(spec ? { spec } : {}),
        ...(baseRevisionId !== undefined ? { baseRevisionId } : {}),
      },
      triggeredBy: { userId },
    } as never));
    const runId = (out as { runId: string }).runId;
    await claim.commit(runId);
    getRunPersistenceService()?.registerTrigger({
      runId, pipelineId: DECK_REVISE_PIPELINE_ID, userId, triggerType: 'manual', at: new Date().toISOString(),
      ...(organizationId ? { organizationId } : {}),
      ...(workGraph ? { workGraph } : {}),
    });
    log.info({ runId, documentId, requestId, userId, scope: target ? 'field' : slideId ? 'slide' : 'deck' }, 'deck revise run started');
    answer(runId, false);
  } catch (err) {
    await claim.release().catch(() => undefined);
    throw err;
  }
}));
