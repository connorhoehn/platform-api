/**
 * Document review-lifecycle routes.
 *
 *   GET  /api/documents/:documentId/lifecycle
 *   POST /api/documents/:documentId/lifecycle/transitions
 *
 * One transition endpoint, driven by an `action`, rather than a verb per
 * state change. The allowed-transition table lives in
 * `src/lib/document-lifecycle.ts`; this file only marshals HTTP.
 *
 * The GET is deliberately `optionalAuth`, matching the other document GETs —
 * an anonymous reader still gets the status, and an authenticated one also
 * gets `availableActions`, which is what a frontend needs to decide whether
 * to render Submit / Approve / Reopen.
 */

import { Router, type Request, type Response } from 'express';
import { requireAuth, optionalAuth } from '../middleware/auth';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import {
  loadContext,
  performTransition,
  toView,
  availableActionsFor,
} from '../services/documentLifecycleService';
import {
  TRANSITIONS,
  DOCUMENT_STATUSES,
  LIFECYCLE_ACTIONS,
} from '../lib/document-lifecycle';

export const documentLifecycleRouter = Router({ mergeParams: true });

// GET /api/documents/:documentId/lifecycle
documentLifecycleRouter.get(
  '/',
  optionalAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId } = req.params;
    const ctx = await loadContext(documentId);
    const userId = req.user?.sub;

    res.status(200).json({
      lifecycle: toView(ctx),
      availableActions: availableActionsFor(ctx, userId),
      outstandingReviews: ctx.outstanding,
    });
  }),
);

// POST /api/documents/:documentId/lifecycle/transitions
documentLifecycleRouter.post(
  '/transitions',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId } = req.params;
    const userId = req.user!.sub;
    const { action, reason } = (req.body ?? {}) as {
      action?: string;
      reason?: string;
    };

    if (!action) {
      throw new ValidationError(
        `action is required (one of: ${LIFECYCLE_ACTIONS.join(', ')})`,
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
      availableActions: availableActionsFor(ctx, userId),
    });
  }),
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
