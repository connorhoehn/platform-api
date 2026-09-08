import { Router, Request, Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { sectionItemRepo } from '../repositories';
import type { SectionItemPatch } from '../repositories/SectionItemRepository';
import { publishSignal } from '../lib/event-catalog';
import {
  asyncHandler,
  ValidationError,
  ForbiddenError,
  NotFoundError,
  ConflictError,
} from '../middleware/error-handler';
import log from '../lib/logger';

export const sectionItemsRouter = Router({ mergeParams: true });
export const myItemsRouter = Router();

/**
 * A caller-supplied `itemId`, validated — or null when none was supplied.
 *
 * Only ever ONE path segment: it is the range key of the row and it goes into
 * URLs, so a slash or a newline in it produces an item that can be created and
 * then never addressed again.
 */
function callerItemId(raw: unknown): string | null {
  if (raw === undefined || raw === null) return null;
  if (typeof raw !== 'string') throw new ValidationError('itemId must be a string');
  const id = raw.trim();
  if (id === '') throw new ValidationError('itemId must not be empty');
  if (id.length > 128) throw new ValidationError('itemId must be at most 128 characters');
  if (!/^[A-Za-z0-9_.:-]+$/.test(id)) {
    throw new ValidationError(
      'itemId may contain only letters, digits and _ . : - (it is a URL path segment)',
    );
  }
  return id;
}

// POST /api/documents/:documentId/sections/:sectionId/items
sectionItemsRouter.post('/', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, sectionId } = req.params;
  const { text, assignee, assigneeId, priority, dueDate, category } = req.body as {
    text?: string;
    assignee?: string;
    assigneeId?: string;
    priority?: string;
    dueDate?: string;
    category?: string;
  };

  // Allow empty text — items are created inline and the user types text after

  // ---------------------------------------------------------------------
  // A caller-chosen id, and why this create has to be IDEMPOTENT
  // ---------------------------------------------------------------------
  //
  // A mirrored HITL callout is keyed on the callout's own `id` so that the row
  // an inbox answers and the block in the document body are the same callout.
  // That identity is minted in the document, not here — so the writer supplies
  // it.
  //
  // Which makes a second POST for the same id not an error but the NORMAL
  // case: the projection is made by whichever clients have the page open (see
  // `onCalloutChanged` in ui-components), so opening a document with a callout
  // in it POSTs the same row from every tab. `createItem` is a plain `PutItem`
  // and would happily overwrite — and overwriting is the one outcome that must
  // never happen here, because a row rewritten with a fresh `status: open`
  // would UN-ANSWER a callout somebody had already answered and hand a
  // finished question back to an agent that had moved on.
  //
  // So: an id that already exists returns the row that is there, untouched,
  // with 200 rather than 201. The status code is the only way a caller can
  // tell it created something, and no caller here needs to.
  const itemId = callerItemId((req.body as { itemId?: unknown }).itemId);
  if (itemId) {
    const existing = await sectionItemRepo.getOne(documentId, sectionId, itemId);
    if (existing) {
      res.status(200).json({ item: existing });
      return;
    }
  }

  const item = await sectionItemRepo.createItem({
    documentId,
    sectionId,
    ...(itemId ? { itemId } : {}),
    text: (text ?? '').trim(),
    // `assignee` is the label that renders; `assigneeId` is the identity that
    // GET /api/items/mine is keyed on. A writer that supplies only the label
    // produces a row nobody can find — which is exactly what used to happen.
    ...(assignee ? { assignee } : {}),
    ...(assigneeId ? { assigneeId } : {}),
    ...(priority ? { priority } : {}),
    ...(dueDate ? { dueDate } : {}),
    ...(category ? { category } : {}),
  });

  publishSignal(`doc:${documentId}`, 'doc:section_updated', {
    type: 'section:item:created',
    documentId,
    sectionId,
    item,
  }).catch((err: unknown) => {
    log.error({ event: 'doc:section_updated', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed');
  });

  res.status(201).json({ item });
}));

// GET /api/documents/:documentId/sections/:sectionId/items
sectionItemsRouter.get('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, sectionId } = req.params;
  const items = await sectionItemRepo.getItemsForSection(documentId, sectionId);
  res.status(200).json({ items });
}));

// PATCH /api/documents/:documentId/sections/:sectionId/items/:itemId
sectionItemsRouter.patch('/:itemId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, sectionId, itemId } = req.params;
  const { text, status, assignee, assigneeId, priority, dueDate, notes } = req.body as {
    text?: string;
    status?: string;
    assignee?: string;
    assigneeId?: string;
    priority?: string;
    dueDate?: string;
    notes?: string;
  };

  const updates: SectionItemPatch = {};
  if (text !== undefined) updates.text = text;
  if (status !== undefined) updates.status = status;
  // Both assignee fields are GSI hash keys, so "unassign" is a REMOVE (null),
  // never an empty string — DynamoDB rejects an empty key attribute. The
  // previous truthiness test made unassigning a silent no-op.
  if (assignee !== undefined) updates.assignee = assignee === '' ? null : assignee;
  if (assigneeId !== undefined) updates.assigneeId = assigneeId === '' ? null : assigneeId;
  if (priority !== undefined) updates.priority = priority;
  if (dueDate !== undefined) updates.dueDate = dueDate;
  if (notes !== undefined) updates.notes = notes;

  if (Object.keys(updates).length === 0) {
    throw new ValidationError('No fields to update');
  }

  const result = await sectionItemRepo.updateItemFields(documentId, sectionId, itemId, updates);

  // `null` is the storage instruction for "remove this attribute"; on the wire
  // it would land in a peer's item as a null where every reader expects a
  // string. Peers get the cleared value they can render.
  const broadcastUpdates = Object.fromEntries(
    Object.entries(updates).map(([k, v]) => [k, v === null ? '' : v]),
  );

  publishSignal(`doc:${documentId}`, 'doc:section_updated', {
    type: 'section:item:updated',
    documentId,
    sectionId,
    itemId,
    updates: broadcastUpdates,
  }).catch((err: unknown) => {
    log.error({ event: 'doc:section_updated', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed');
  });

  res.status(200).json({ item: result });
}));

// DELETE /api/documents/:documentId/sections/:sectionId/items/:itemId
sectionItemsRouter.delete('/:itemId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, sectionId, itemId } = req.params;

  await sectionItemRepo.deleteItemById(documentId, sectionId, itemId);

  publishSignal(`doc:${documentId}`, 'doc:section_updated', {
    type: 'section:item:deleted',
    documentId,
    sectionId,
    itemId,
  }).catch((err: unknown) => {
    log.error({ event: 'doc:section_updated', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed');
  });

  res.status(204).send();
}));

// POST /api/documents/:documentId/sections/:sectionId/items/:itemId/ack
sectionItemsRouter.post('/:itemId/ack', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, sectionId, itemId } = req.params;
  const userId = req.user!.sub;

  const result = await sectionItemRepo.ackItem(
    documentId,
    sectionId,
    itemId,
    userId,
    new Date().toISOString(),
  );

  publishSignal(`doc:${documentId}`, 'doc:section_updated', {
    type: 'section:item:acked',
    documentId,
    sectionId,
    itemId,
    ackedBy: userId,
  }).catch((err: unknown) => {
    log.error({ event: 'doc:section_updated', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed');
  });

  res.status(200).json({ item: result });
}));

// ---------------------------------------------------------------------------
// Answering a HITL callout from OUTSIDE the document
// ---------------------------------------------------------------------------
//
// A `macro:hitl` callout is an agent's question sitting in a document body.
// The body is a Y.Doc, and there is no server-side Y.Doc anywhere in this
// system — the gateway relays CRDT bytes opaquely and this service never
// materialises a document — so nothing that has not OPENED the page can read
// or write that callout. A cross-document inbox built on the body alone is
// empty by construction.
//
// So the callout mirrors itself into a section item, exactly the way
// `macro:diagram` puts its payload in a queryable place and keeps only a
// reference in the fence. `sectionId` is an opaque path segment here — no
// handler in this file looks it up against a real section, and the partition
// key is a plain `${documentId}:${sectionId}` concatenation — so a page-shaped
// document with no sections files the row under a sentinel (`_macro`, the same
// move `_inbox` already makes for unattached work items).
//
// The mirror row carries:
//
//   text        the question
//   assignee    who is being asked, as a HUMAN LABEL
//   assigneeId  who is being asked, as an IDENTITY — and this is the whole
//               reason the row exists. `GET /api/items/mine` is keyed on the
//               identity, so a row carrying only a display name is a row
//               nobody can find. A mirror row that predates the split put the
//               identity in `assignee`; both are accepted below, but only one
//               of them is a name. See `ASSIGNEE_KEY_MISMATCH` in the
//               frontend's inboxModel for the failure this avoids.
//   category    'hitl', so this endpoint can refuse to be pointed at a task
//   status      'pending' while it waits; 'done' once answered
//   notes       the answer
//   ackedBy/At  who answered and when
//
// WHAT THE ROW CANNOT CARRY, and therefore what this endpoint cannot check:
//
//   * the declared OPTIONS. A section item has no field for a choice list, so
//     the answer is validated as a non-empty string and nothing more. A remote
//     surface that has not read the body must offer free prose rather than
//     drawing named buttons it would be inventing.
//   * the callout's `expiresAt`. There is no field for it either, so "the
//     asker stopped waiting" is not a clock this endpoint can read. It is
//     expressed the way the row CAN express it: an asker that gives up moves
//     the row out of `pending`, and the guard below then refuses the answer.
//     A callout whose asker has expired but whose row is still pending will be
//     answered here, and that is a fact about the asker's housekeeping rather
//     than a check that was skipped.
//
// Auth mirrors the review-request PATCH: only the person a question is
// addressed to may answer it. A row with no assignee is answerable by NOBODY
// rather than by anybody — an unaddressed question is one everyone assumes
// someone else took, and letting a passer-by close it is worse than leaving it
// open.

/** The `category` a mirrored HITL callout carries. */
export const HITL_ITEM_CATEGORY = 'hitl';

/** Statuses that still mean "waiting". `open` is `createItem`'s default. */
const OPEN_STATUSES = new Set(['pending', 'open']);

// POST /api/documents/:documentId/sections/:sectionId/items/:itemId/answer
sectionItemsRouter.post('/:itemId/answer', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, sectionId, itemId } = req.params;
  const userId = req.user!.sub;
  const { answer } = req.body as { answer?: unknown };

  if (typeof answer !== 'string' || answer.trim() === '') {
    throw new ValidationError('answer is required and must be a non-empty string');
  }

  const item = await sectionItemRepo.getOne(documentId, sectionId, itemId);
  if (!item) {
    throw new NotFoundError('Callout not found');
  }
  if (item.category !== HITL_ITEM_CATEGORY) {
    throw new ValidationError(
      `Item ${itemId} is not a HITL callout (category is ${item.category ?? 'unset'}). ` +
        'Use PATCH for an ordinary section item.',
    );
  }
  // The identity, with the legacy field as the fallback for mirror rows
  // written before `assignee` was split into a label and an id. Comparing a
  // sub against `assignee` alone would 403 the very person the callout is
  // addressed to as soon as that field holds "Alice Chen".
  const addressedTo = item.assigneeId ?? item.assignee;
  if (!addressedTo) {
    throw new ForbiddenError(
      'This callout is addressed to nobody, so nobody may answer it. ' +
        'Set an assignee on the callout first.',
    );
  }
  if (addressedTo !== userId) {
    throw new ForbiddenError('Only the person this callout is addressed to may answer it');
  }
  if (!OPEN_STATUSES.has(item.status ?? '')) {
    throw new ConflictError(
      `This callout is ${item.status ?? 'closed'} and is no longer waiting on an answer.`,
    );
  }

  const answeredAt = new Date().toISOString();
  // One UpdateItem, not an ack followed by a patch: a row that recorded who
  // answered but never went terminal would sit in the asker's queue forever
  // while looking answered to the person who answered it.
  const result = await sectionItemRepo.updateItemFields(documentId, sectionId, itemId, {
    status: 'done',
    notes: answer.trim(),
    ackedBy: userId,
    ackedAt: answeredAt,
  });

  // The bridge back to the document. An editor with the page OPEN hears this
  // and can apply the same minimal text edit into the macro body that
  // answering in the page would have made (`answerHitlEdit` in
  // ui-components/doc-macros). When nobody has it open the body catches up the
  // next time somebody does — the row is what the asker reads either way.
  publishSignal(`doc:${documentId}`, 'doc:section_updated', {
    type: 'section:item:answered',
    documentId,
    sectionId,
    itemId,
    answer: answer.trim(),
    answeredBy: userId,
    answeredAt,
  }).catch((err: unknown) => {
    log.error({ event: 'doc:section_updated', err: err instanceof Error ? err.message : String(err) }, 'signal publish failed');
  });

  res.status(200).json({ item: result });
}));

// GET /api/items/mine?status=pending
//
// Keyed on the caller's auth subject, which is an IDENTITY — never on the
// display name that renders in the assignee badge. Those two were the same
// field until the label/identity split, which is why this endpoint returned an
// empty list for every user, always. See `getItemsAssignedTo`.
myItemsRouter.get('/mine', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const userId = req.user!.sub;
  const status = req.query['status'] as string | undefined;
  const items = await sectionItemRepo.getItemsAssignedTo(userId, status);
  res.status(200).json({ items });
}));
