import { filterDocumentLifecycleActions } from '../middleware/document-policy';
/**
 * Document review-lifecycle routes.
 *
 *   GET  /api/documents/:documentId/lifecycle
 *   POST /api/documents/:documentId/lifecycle/transitions
 *   POST /api/documents/:documentId/lifecycle/ownership
 *
 * One transition endpoint, driven by an `action`, rather than a verb per
 * state change. The allowed-transition table lives in
 * `src/lib/document-lifecycle.ts`; this file only marshals HTTP.
 *
 * Error bodies
 * ------------
 * Failures carry a machine-readable `code` alongside the human `error` string.
 * This exists because 409 is genuinely three different outcomes here — an
 * illegal transition, outstanding reviews, and a lost compare-and-swap — and a
 * client had no way to tell them apart except by regexing the prose, which
 * breaks the moment a message is reworded. `error` is unchanged; `code` is
 * additive. The HTTP statuses are unchanged too. See `LifecycleErrorCode`.
 *
 * Auth on the GET
 * ---------------
 * Still `optionalAuth`, matching the other document GETs, but an anonymous
 * caller now gets the redacted view: status and version, without the actor
 * ids, review roster and free-text reasons that the full view carries. See
 * `toPublicView`.
 */

import { Router, type Request, type Response } from 'express';
import { requireAuth, optionalAuth } from '../middleware/auth';
import { asyncHandler, AppError } from '../middleware/error-handler';
import {
  loadContext,
  performTransition,
  claimOwnership,
  toView,
  toPublicView,
  availableActionsFor,
} from '../services/documentLifecycleService';
import {
  TRANSITIONS,
  DOCUMENT_STATUSES,
  LIFECYCLE_ACTIONS,
  lifecycleErrorCode,
  LifecycleValidationError,
} from '../lib/document-lifecycle';

export const documentLifecycleRouter = Router({ mergeParams: true });

/**
 * Run a handler, emitting `{ error, code }` for lifecycle failures.
 *
 * Applied here rather than in the shared `errorHandler` so the response shape
 * of every other endpoint in the service is untouched by this change.
 * Anything without a lifecycle code is re-thrown to the central handler.
 */
function withErrorCodes(
  fn: (req: Request, res: Response) => Promise<void>,
): (req: Request, res: Response) => Promise<void> {
  return async (req, res) => {
    try {
      await fn(req, res);
    } catch (err) {
      const code = lifecycleErrorCode(err);
      if (code && err instanceof AppError) {
        res.status(err.status).json({ error: err.message, code });
        return;
      }
      throw err;
    }
  };
}

// GET /api/documents/:documentId/lifecycle
documentLifecycleRouter.get(
  '/',
  optionalAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId } = req.params;
    const ctx = await loadContext(documentId);
    const userId = req.user?.sub;

    if (!userId) {
      // No `history`, `outstandingReviews`, `ownerId` or `updatedBy` for an
      // unauthenticated caller — those name individuals and carry free-text
      // review commentary, none of which a status badge needs.
      res.status(200).json({
        lifecycle: toPublicView(ctx),
        availableActions: [],
        outstandingReviews: [],
      });
      return;
    }

    res.status(200).json({
      lifecycle: toView(ctx),
      availableActions: filterDocumentLifecycleActions(req, availableActionsFor(ctx, userId)),
      outstandingReviews: ctx.outstanding,
    });
  }),
);

// POST /api/documents/:documentId/lifecycle/transitions
documentLifecycleRouter.post(
  '/transitions',
  requireAuth,
  asyncHandler(
    withErrorCodes(async (req: Request, res: Response): Promise<void> => {
      const { documentId } = req.params;
      const userId = req.user!.sub;
      const { action, reason } = (req.body ?? {}) as {
        action?: string;
        reason?: string;
      };

      if (!action) {
        throw new LifecycleValidationError(
          `action is required (one of: ${LIFECYCLE_ACTIONS.join(', ')})`,
          'action_required',
        );
      }

      const result = await performTransition({
        documentId,
        action,
        userId,
        ...(reason !== undefined ? { reason } : {}),
      });

      const ctx = await loadContext(documentId);
      res.status(200).json({
        lifecycle: result.lifecycle,
        from: result.from,
        to: result.to,
        action: result.action,
        availableActions: filterDocumentLifecycleActions(req, availableActionsFor(ctx, userId)),
      });
    }),
  ),
);

/**
 * POST /api/documents/:documentId/lifecycle/ownership
 *
 * Explicitly claim a document that has no owner on record.
 *
 * Deliberately its own endpoint rather than a flag on a transition: claiming
 * ownership is a permission grant, it should not ride along on an unrelated
 * action, and it must be impossible to trigger by merely reading a document.
 * Idempotent for the existing owner (200, `claimed:false`); 409
 * `owner_already_established` for anyone else; 503 when we cannot tell.
 *
 * FOR CLIENT AUTHORS: do NOT call this automatically in response to a 403
 * `owner_not_established`. Auto-retrying the claim would rebuild exactly the
 * trust-on-first-use hole this endpoint exists to remove — the first person to
 * open a document would silently become its owner again, just via two requests
 * instead of one. This must be a deliberate user action ("Take ownership of
 * this document"), shown as such.
 */
documentLifecycleRouter.post(
  '/ownership',
  requireAuth,
  asyncHandler(
    withErrorCodes(async (req: Request, res: Response): Promise<void> => {
      const { documentId } = req.params;
      const userId = req.user!.sub;

      const result = await claimOwnership({ documentId, userId });
      const ctx = await loadContext(documentId);

      res.status(200).json({
        lifecycle: result.lifecycle,
        claimed: result.claimed,
        availableActions: filterDocumentLifecycleActions(req, availableActionsFor(ctx, userId)),
      });
    }),
  ),
);

/**
 * GET /api/documents/lifecycle-schema
 *
 * The transition table as data, so a frontend can render the flow (and its
 * own client-side guards) without hard-coding a second copy of the rules.
 * Mounted on a static segment ahead of the `:documentId` mount.
 */
export const lifecycleSchemaRouter = Router();

lifecycleSchemaRouter.get(
  '/',
  asyncHandler(async (_req: Request, res: Response): Promise<void> => {
    res.status(200).json({
      statuses: DOCUMENT_STATUSES,
      actions: LIFECYCLE_ACTIONS,
      transitions: TRANSITIONS,
      legacyStatusAliases: { final: 'approved' },
    });
  }),
);
