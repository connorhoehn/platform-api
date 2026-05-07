import { Router, Request, Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { sectionItemRepo } from '../repositories';
import { broadcastService } from '../services/broadcast';
import {
  asyncHandler,
  ValidationError,
} from '../middleware/error-handler';

export const sectionItemsRouter = Router({ mergeParams: true });
export const myItemsRouter = Router();

// POST /api/documents/:documentId/sections/:sectionId/items
sectionItemsRouter.post('/', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, sectionId } = req.params;
  const { text, assignee, priority, dueDate, category } = req.body as {
    text?: string;
    assignee?: string;
    priority?: string;
    dueDate?: string;
    category?: string;
  };

  // Allow empty text — items are created inline and the user types text after

  const item = await sectionItemRepo.createItem({
    documentId,
    sectionId,
    text: (text ?? '').trim(),
    ...(assignee ? { assignee } : {}),
    ...(priority ? { priority } : {}),
    ...(dueDate ? { dueDate } : {}),
    ...(category ? { category } : {}),
  });

  // Broadcast real-time event (non-fatal)
  void broadcastService.emit(`doc:${documentId}`, 'social:post' as any, {
    type: 'section:item:created',
    documentId,
    sectionId,
    item,
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
  const { text, status, assignee, priority, dueDate, notes } = req.body as {
    text?: string;
    status?: string;
    assignee?: string;
    priority?: string;
    dueDate?: string;
    notes?: string;
  };

  const updates: Record<string, unknown> = {};
  if (text !== undefined) updates.text = text;
  if (status !== undefined) updates.status = status;
  if (assignee) updates.assignee = assignee;
  if (priority !== undefined) updates.priority = priority;
  if (dueDate !== undefined) updates.dueDate = dueDate;
  if (notes !== undefined) updates.notes = notes;

  if (Object.keys(updates).length === 0) {
    throw new ValidationError('No fields to update');
  }

  const result = await sectionItemRepo.updateItemFields(documentId, sectionId, itemId, updates);

  // Broadcast real-time event (non-fatal)
  void broadcastService.emit(`doc:${documentId}`, 'social:post' as any, {
    type: 'section:item:updated',
    documentId,
    sectionId,
    itemId,
    updates,
  });

  res.status(200).json({ item: result });
}));

// DELETE /api/documents/:documentId/sections/:sectionId/items/:itemId
sectionItemsRouter.delete('/:itemId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { documentId, sectionId, itemId } = req.params;

  await sectionItemRepo.deleteItemById(documentId, sectionId, itemId);

  // Broadcast real-time event (non-fatal)
  void broadcastService.emit(`doc:${documentId}`, 'social:post' as any, {
    type: 'section:item:deleted',
    documentId,
    sectionId,
    itemId,
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

  // Broadcast real-time event (non-fatal)
  void broadcastService.emit(`doc:${documentId}`, 'social:post' as any, {
    type: 'section:item:acked',
    documentId,
    sectionId,
    itemId,
    ackedBy: userId,
  });

  res.status(200).json({ item: result });
}));

// GET /api/items/mine?status=pending
myItemsRouter.get('/mine', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const userId = req.user!.sub;
  const status = req.query['status'] as string | undefined;
  const items = await sectionItemRepo.getItemsByAssignee(userId, status);
  res.status(200).json({ items });
}));
