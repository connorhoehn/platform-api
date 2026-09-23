// /api/deck — the editor's two fast paths, neither of which writes anything.
//
//   POST /api/deck/preview  { spec }        → the slides, as SVG, plus warnings
//   POST /api/deck/fixture  { from, title } → a DeckSpec derived from real local material
//   POST /api/deck/revise   { spec, instruction, slideId?, sources? } → the spec with the agent's edit
//
// Persisting is a separate, deliberate act: POST /api/documents/:id/revisions.
// Keeping them apart is what lets the editor redraw on every keystroke without
// leaving a trail of revisions nobody asked for.

import { Router, type Request, type Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { asyncHandler, AppError, ValidationError } from '../middleware/error-handler';
import { previewDeckSpec } from '../deck/preview';
import { buildDeckFixture, NoFixtureMaterialError, type DeckFixtureFrom } from '../deck/deckFixture';
import {
  DeckReviseError, MAX_INSTRUCTION_CHARS, reviseDeck, validateReviseInputSpec, type DeckReviseSourceRef,
} from '../deck/deckRevise';
import log from '../lib/logger';

export const deckRouter = Router();

const ID = /^[A-Za-z0-9][A-Za-z0-9_.@:-]{0,200}$/;

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
 *   { spec, instruction, slideId?, sources?: [{ kind, id }] }
 *   → { spec, summary, changedSlideIds, warnings, source: 'model' }
 *
 * The editor's agent composer. Applies an instruction to one slide (`slideId`)
 * or the whole deck, grounded in the real text of the deck's sources, and
 * returns the edited spec without keeping it. When the model cannot produce a
 * valid edit the answer is 502/503 with the reason — never an edit it did not
 * make. See `deck/deckRevise.ts`.
 */
deckRouter.post('/revise', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const body = (req.body ?? {}) as { spec?: unknown; instruction?: unknown; slideId?: unknown; sources?: unknown };
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

  try {
    const result = await reviseDeck({ spec, instruction, ...(slideId ? { slideId } : {}), sources });
    log.info({ scope: slideId ? 'slide' : 'deck', changed: result.changedSlideIds.length, userId: req.user?.sub }, 'deck revised');
    res.status(200).json(result);
  } catch (err) {
    if (err instanceof DeckReviseError) {
      log.warn({ status: err.status, reason: err.reason, userId: req.user?.sub }, 'deck revise refused');
      res.status(err.status).json({ error: err.message, reason: err.reason, ...(err.errors.length ? { errors: err.errors } : {}) });
      return;
    }
    throw err;
  }
}));
