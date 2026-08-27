// platform-api/src/routes/workItems.ts
//
// The server half of: utterance -> work item -> AI proposal -> acceptance.
//
// THE ANTI-PATTERN THIS FILE EXISTS TO AVOID
//
// This repo already has a proposals queue — DocTypeProposalsPage — whose
// "promote" action records a decision and never applies the change. Its accepted
// rows are indistinguishable from its pending ones except by a status string
// that corresponds to nothing in the world. A queue whose accept does nothing is
// worse than no queue, because it manufactures the belief that the work is done.
//
// So this router will not mark anything accepted on request. `POST .../applied`
// is a RECEIPT endpoint: it is called by the client AFTER the mutation has
// landed in the Y.Doc, and it refuses to record success without evidence of what
// changed. The status transition is downstream of the edit, never a substitute
// for it.
//
// WHY THE CLIENT PERFORMS THE MUTATION
//
// There is no server-side Y.Doc anywhere in this system. The gateway relays CRDT
// bytes opaquely; nothing in this process can read or author a document. That is
// a real constraint, not a gap to be papered over with a headless doc service, so
// the split is explicit: the browser owns applying and the compare-and-set, the
// server owns the queue, the drafting, and the ledger.

import { Router, Request, Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { sectionItemRepo } from '../repositories';
import type { SectionItemFields } from '../repositories/SectionItemRepository';
import { publishSignal } from '../lib/event-catalog';
import {
  asyncHandler,
  ValidationError,
  NotFoundError,
  ConflictError,
} from '../middleware/error-handler';
import {
  INBOX_SECTION_ID,
  attachStateForTier,
  tierFromContext,
  type AttachCandidate,
  type AppliedReceipt,
  type CapturedContext,
} from '../types/workItem';
import {
  draftProposal,
  ProposalDraftError,
  ProposalProviderError,
  type DocumentSnapshot,
} from '../lib/proposalDrafter';
import log from '../lib/logger';

export const workItemsRouter = Router({ mergeParams: true });

/** Cap on stored free text, so one runaway transcript cannot bloat a row. */
const MAX_UTTERANCE = 4000;

function publish(documentId: string, type: string, payload: Record<string, unknown>): void {
  publishSignal(`doc:${documentId}`, 'doc:section_updated', {
    type,
    documentId,
    ...payload,
  }).catch((err: unknown) => {
    log.error(
      { event: 'doc:section_updated', err: err instanceof Error ? err.message : String(err) },
      'signal publish failed',
    );
  });
}

/**
 * Can `userId` see this item?
 *
 * A medium-confidence item is attached to a real section but shown only to the
 * person who spoke it, until they confirm. Gemini keeps unreviewed AI output
 * private the same way. The alternative — showing a guess to the whole
 * document — turns a maybe into an apparent fact.
 */
function visibleTo(item: SectionItemFields, userId: string): boolean {
  if (item.attachState !== 'author_only') return true;
  return item.createdBy === userId;
}

async function loadOr404(documentId: string, itemId: string): Promise<SectionItemFields> {
  const item = await sectionItemRepo.findByItemId(documentId, itemId);
  if (!item) throw new NotFoundError(`work item ${itemId} not found in document ${documentId}`);
  return item;
}

// ---------------------------------------------------------------------------
// POST /api/documents/:documentId/work-items
// ---------------------------------------------------------------------------

/**
 * Raise a work item from a spoken remark.
 *
 * The confidence tier decides the ONE place the item goes, and there is
 * deliberately no path here that writes more than one row. Fireflies attaches an
 * ambiguous note to every matching deal; the result is that a user who finds it
 * in the wrong place cannot tell whether it is also in the right one. Ambiguity
 * here produces exactly one inbox row carrying its ranked alternatives as data.
 */
workItemsRouter.post(
  '/',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId } = req.params;
    const userId = req.user!.sub;
    const body = req.body as {
      text?: string;
      utterance?: string;
      utteranceId?: string;
      captureId?: string;
      context?: CapturedContext;
      candidates?: AttachCandidate[];
      assignee?: string;
      priority?: string;
    };

    const utterance = (body.utterance ?? '').trim();
    if (!utterance) throw new ValidationError('utterance is required');
    if (utterance.length > MAX_UTTERANCE) {
      throw new ValidationError(`utterance exceeds ${MAX_UTTERANCE} characters`);
    }
    const context = body.context;
    if (!context || typeof context !== 'object') {
      throw new ValidationError('context (a ContextFrame) is required');
    }
    // The frame names the entity it was captured against. If that disagrees with
    // the URL, something has mis-routed and we must not guess which is right.
    if (context.documentId && context.documentId !== documentId) {
      throw new ValidationError(
        `context frame targets document ${context.documentId}, not ${documentId}`,
      );
    }

    const tier = tierFromContext(context);
    const attachState = attachStateForTier(tier);
    // `_inbox` is not a fallback target — it is the absence of one. An item here
    // is addressed to a person, not to a place in the document.
    const sectionId = tier === 'low' ? INBOX_SECTION_ID : context.sectionId ?? INBOX_SECTION_ID;

    const candidates = tier === 'low' ? body.candidates ?? [] : undefined;

    const item = await sectionItemRepo.createItem({
      documentId,
      sectionId,
      text: (body.text ?? utterance).trim().slice(0, MAX_UTTERANCE),
      status: 'open',
      source: 'voice',
      utterance,
      ...(body.utteranceId ? { utteranceId: body.utteranceId } : {}),
      ...(body.captureId ? { captureId: body.captureId } : {}),
      capturedContext: context,
      confidenceTier: tier,
      attachState,
      ...(candidates ? { candidates } : {}),
      createdBy: userId,
      // The speaker owns the follow-up unless someone says otherwise. Without
      // this the item is invisible to `GET /api/items/mine` and the queue has no
      // reader.
      assignee: body.assignee ?? userId,
      ...(body.priority ? { priority: body.priority } : {}),
      proposalStatus: 'none',
    });

    log.info(
      {
        event: 'workitem.created',
        documentId,
        itemId: item.itemId,
        tier,
        attachState,
        sectionId,
        contextTier: context.tier,
        contextSplit: context.contextSplit,
      },
      'work item raised from utterance',
    );

    publish(documentId, 'workitem:created', { sectionId, itemId: item.itemId, item });
    res.status(201).json({ item });
  }),
);

// ---------------------------------------------------------------------------
// GET /api/documents/:documentId/work-items
// ---------------------------------------------------------------------------

workItemsRouter.get(
  '/',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId } = req.params;
    const userId = req.user!.sub;
    const all = await sectionItemRepo.getItemsForDocument(documentId);
    const items = all.filter((i) => i.source === 'voice' && visibleTo(i, userId));
    res.status(200).json({
      items,
      inbox: items.filter((i) => i.attachState === 'inbox'),
    });
  }),
);

// ---------------------------------------------------------------------------
// POST /api/documents/:documentId/work-items/:itemId/propose
// ---------------------------------------------------------------------------

/**
 * Draft the AI proposal.
 *
 * `snapshot` is required and comes from the browser, because that is the only
 * process that can read the document. It doubles as the compare baseline: every
 * `FieldChange.from` this produces is a value the model was actually shown.
 */
workItemsRouter.post(
  '/:itemId/propose',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId, itemId } = req.params;
    const { snapshot, model } = req.body as { snapshot?: DocumentSnapshot; model?: string };

    if (!snapshot || !Array.isArray(snapshot.sections) || typeof snapshot.meta !== 'object') {
      throw new ValidationError(
        'snapshot { meta, sections[] } is required — the server has no Y.Doc to read',
      );
    }

    const item = await loadOr404(documentId, itemId);
    if (item.proposalStatus === 'applied') {
      throw new ConflictError('this work item has already been applied');
    }

    const targetSectionId =
      item.sectionId === INBOX_SECTION_ID ? undefined : item.sectionId;

    await sectionItemRepo.updateItemFields(documentId, item.sectionId, itemId, {
      proposalStatus: 'drafting',
      proposalError: '',
    });

    try {
      const proposal = await draftProposal({
        utterance: item.utterance ?? item.text,
        targetSectionId,
        snapshot,
        ...(model ? { model } : {}),
      });

      if (!proposal) {
        // Not an edit request. That is a normal outcome for a spoken remark and
        // must not be dressed up as an error — the item stays a plain task.
        const updated = await sectionItemRepo.updateItemFields(documentId, item.sectionId, itemId, {
          proposalStatus: 'none',
          notes: 'No document edit was requested.',
        });
        res.status(200).json({ item: updated, proposal: null });
        return;
      }

      const updated = await sectionItemRepo.updateItemFields(documentId, item.sectionId, itemId, {
        proposal,
        proposalStatus: 'ready',
      });
      publish(documentId, 'workitem:proposal_ready', {
        sectionId: item.sectionId,
        itemId,
        proposal,
      });
      res.status(200).json({ item: updated, proposal });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      // A refused proposal is data, not a 500. The reason is stored and shown,
      // because "the model tried to edit a different section" is exactly the
      // kind of thing a person needs to see rather than have retried silently.
      await sectionItemRepo.updateItemFields(documentId, item.sectionId, itemId, {
        proposalStatus: 'failed',
        proposalError: message.slice(0, 500),
      });
      log.warn({ event: 'workitem.proposal.failed', documentId, itemId, err: message }, 'draft failed');
      if (err instanceof ProposalProviderError) {
        // 502, not 500: nothing in this service is broken. Something upstream
        // of it is, and the person clicking "Draft a change" needs to be told
        // that rather than shown a generic failure they will read as a bug in
        // their own remark. Seen for real when the pod's CLI credentials
        // expired — every draft returned a bare 500 with "unhandled error".
        res.status(502).json({ error: 'the drafting model is unavailable', detail: message });
        return;
      }
      if (err instanceof ProposalDraftError) {
        res.status(422).json({ error: 'proposal could not be drafted', detail: message });
        return;
      }
      throw err;
    }
  }),
);

// ---------------------------------------------------------------------------
// POST /api/documents/:documentId/work-items/:itemId/applied
// ---------------------------------------------------------------------------

/**
 * Record that an accepted proposal actually mutated the document.
 *
 * Called by the browser AFTER the write. The receipt is mandatory and is checked
 * for substance: a field change must say what it observed and what it wrote, and
 * a prose change must say how many edits landed. Without that check this
 * endpoint would degrade into exactly the "promote does nothing" button this
 * whole feature is a correction for.
 */
workItemsRouter.post(
  '/:itemId/applied',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId, itemId } = req.params;
    const userId = req.user!.sub;
    const { receipt } = req.body as { receipt?: Partial<AppliedReceipt> };

    const item = await loadOr404(documentId, itemId);
    if (!item.proposal) throw new ValidationError('this work item has no proposal to apply');
    if (item.proposalStatus === 'applied') {
      throw new ConflictError('this proposal has already been applied');
    }
    if (!receipt) throw new ValidationError('receipt is required');

    if (item.proposal.kind === 'field') {
      if (!('wrote' in receipt)) {
        throw new ValidationError('receipt.wrote is required for a field proposal');
      }
      if (receipt.wrote !== item.proposal.change.to) {
        throw new ConflictError(
          'receipt.wrote does not match the proposed value — refusing to record an apply that did something else',
        );
      }
    } else if (!receipt.editsApplied || receipt.editsApplied < 1) {
      throw new ValidationError(
        'receipt.editsApplied must be at least 1 — an apply that changed nothing is not an apply',
      );
    }

    const full: AppliedReceipt = {
      at: new Date().toISOString(),
      by: userId,
      ...(('observedFrom' in receipt) ? { observedFrom: receipt.observedFrom } : {}),
      ...(('wrote' in receipt) ? { wrote: receipt.wrote } : {}),
      ...(receipt.suggestionKey ? { suggestionKey: receipt.suggestionKey } : {}),
      ...(receipt.editsApplied ? { editsApplied: receipt.editsApplied } : {}),
    };

    const updated = await sectionItemRepo.updateItemFields(documentId, item.sectionId, itemId, {
      proposalStatus: 'applied',
      applied: full,
      status: 'done',
      ackedBy: userId,
      ackedAt: full.at,
    });

    log.info(
      { event: 'workitem.applied', documentId, itemId, kind: item.proposal.kind, by: userId },
      'proposal applied to document',
    );
    publish(documentId, 'workitem:applied', { sectionId: item.sectionId, itemId, receipt: full });
    res.status(200).json({ item: updated });
  }),
);

// ---------------------------------------------------------------------------
// POST /api/documents/:documentId/work-items/:itemId/reject
// ---------------------------------------------------------------------------

workItemsRouter.post(
  '/:itemId/reject',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId, itemId } = req.params;
    const userId = req.user!.sub;
    const { reason } = req.body as { reason?: string };

    const item = await loadOr404(documentId, itemId);
    if (item.proposalStatus === 'applied') {
      throw new ConflictError('this proposal has already been applied');
    }

    const updated = await sectionItemRepo.updateItemFields(documentId, item.sectionId, itemId, {
      proposalStatus: 'rejected',
      status: 'rejected',
      rejectedReason: (reason ?? '').slice(0, 500),
      ackedBy: userId,
      ackedAt: new Date().toISOString(),
    });
    publish(documentId, 'workitem:rejected', { sectionId: item.sectionId, itemId });
    res.status(200).json({ item: updated });
  }),
);

// ---------------------------------------------------------------------------
// POST /api/documents/:documentId/work-items/:itemId/reattach
// ---------------------------------------------------------------------------

/**
 * Move a work item to a different section.
 *
 * A real, non-destructive move — the item keeps its id, its utterance, its
 * context frame and its whole history. Compare the prior art: Granola's only fix
 * for a misfiled note is to delete it, and Otter's reassign is one-way and drops
 * the original association. Both make the correction cost more than the mistake,
 * so people stop correcting.
 *
 * `via: 'heal'` is for the case where the stored anchor no longer resolved and
 * the client re-found the target by matching the quote. It lands as `healed`
 * rather than `attached` on purpose: a silently re-homed anchor is a wrong
 * attachment in disguise, so it must carry a badge until a human agrees with it.
 */
workItemsRouter.post(
  '/:itemId/reattach',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId, itemId } = req.params;
    const userId = req.user!.sub;
    const { toSectionId, via } = req.body as { toSectionId?: string; via?: 'human' | 'heal' };

    if (!toSectionId) throw new ValidationError('toSectionId is required');
    const item = await loadOr404(documentId, itemId);

    const moved = await sectionItemRepo.moveItem({
      documentId,
      fromSectionId: item.sectionId,
      toSectionId,
      itemId,
      by: userId,
      via: via === 'heal' ? 'heal' : 'human',
      attachState: via === 'heal' ? 'healed' : 'attached',
    });
    if (!moved) throw new NotFoundError(`work item ${itemId} disappeared during the move`);

    publish(documentId, 'workitem:reattached', {
      sectionId: toSectionId,
      fromSectionId: item.sectionId,
      itemId,
      item: moved,
    });
    res.status(200).json({ item: moved });
  }),
);

// ---------------------------------------------------------------------------
// POST /api/documents/:documentId/work-items/:itemId/confirm
// ---------------------------------------------------------------------------

/**
 * The speaker confirms a medium-confidence attachment, making it visible to
 * everyone. The complement of reattach: "yes, this is the right place."
 */
workItemsRouter.post(
  '/:itemId/confirm',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId, itemId } = req.params;
    const item = await loadOr404(documentId, itemId);
    if (item.sectionId === INBOX_SECTION_ID) {
      throw new ValidationError('an inbox item has no attachment to confirm — use reattach');
    }
    const updated = await sectionItemRepo.updateItemFields(documentId, item.sectionId, itemId, {
      attachState: 'attached',
    });
    publish(documentId, 'workitem:confirmed', { sectionId: item.sectionId, itemId });
    res.status(200).json({ item: updated });
  }),
);
