// platform-api/src/pipeline/image/imageAccess.ts
//
// Loop 29c (realtime-examples access sweep, NFR #174): who may read a stored
// image by id. An image a PERSON uploaded (`uploadedBy` = their user id) is
// theirs: anyone else gets 404, as if it did not exist. Images a pipeline step
// produced carry no `uploadedBy` and stay addressable by their random UUID
// (the run's own pages show them to whoever may read the run); the media
// walker's catalog images ('media-walker') belong to the catalog surface.
//
//   no bearer → 401, not the uploader → 404.

import type { NextFunction, Request, Response } from 'express';
import { AppError, NotFoundError } from '../../middleware/error-handler';
import { markAccess, type AccessMarkedHandler } from '../../middleware/resource-access';
import { getImageBlobStore } from './visionServices';

const SHARED_UPLOADERS = new Set(['media-walker']);

export function mayReadImage(meta: { uploadedBy?: string } | null | undefined, userId: string): boolean {
  if (!meta) return false;
  const by = meta.uploadedBy;
  return !by || SHARED_UPLOADERS.has(by) || by === userId;
}

/**
 * The caller may read the image `idOf` names (default `:imageId`). An absent
 * optional id passes (a preview given `imageUrl` instead). A missing image is
 * left to the route (its own 404).
 */
export function requireImageAccess(
  idOf: (req: Request) => unknown = (req) => req.params?.imageId,
  opts: { optional?: boolean; getMetadata?: (id: string) => Promise<{ uploadedBy?: string } | null> } = {},
): AccessMarkedHandler {
  return markAccess((req: Request, _res: Response, next: NextFunction) => {
    void (async () => {
      const raw = idOf(req);
      const imageId = typeof raw === 'string' ? raw : '';
      if (!imageId) { if (opts.optional) { next(); return; } throw new NotFoundError('image not found'); }
      if (!req.headers.authorization?.startsWith('Bearer ')) throw new AppError(401, 'Authorization required');
      const meta = await (opts.getMetadata ?? ((id: string) => getImageBlobStore().getMetadata(id)))(imageId);
      if (!meta) { next(); return; }
      if (!mayReadImage(meta, req.user?.sub ?? '')) throw new NotFoundError('image not found');
      next();
    })().catch(next);
  }, 'image:uploader');
}
