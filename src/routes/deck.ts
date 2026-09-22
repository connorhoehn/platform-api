// /api/deck — the editor's two fast paths, neither of which writes anything.
//
//   POST /api/deck/preview  { spec }        → the slides, as SVG, plus warnings
//   POST /api/deck/fixture  { from, title } → a DeckSpec derived from real local material
//
// Persisting is a separate, deliberate act: POST /api/documents/:id/revisions.
// Keeping them apart is what lets the editor redraw on every keystroke without
// leaving a trail of revisions nobody asked for.

import { Router, type Request, type Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { asyncHandler, AppError, ValidationError } from '../middleware/error-handler';
import { previewDeckSpec } from '../deck/preview';
import { buildDeckFixture, NoFixtureMaterialError, type DeckFixtureFrom } from '../deck/deckFixture';
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
