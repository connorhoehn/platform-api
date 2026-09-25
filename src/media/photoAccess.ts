// platform-api/src/media/photoAccess.ts
//
// Who may read a catalog photo — and therefore its edits, masks, versions
// and its live `photo:<id>` channel. No new ACL model: a photo is readable
// by whoever may read its image (`mayReadImage` — the walker's uploads are
// shared, a user's own uploads are theirs). The same answer serves the HTTP
// routes (404 to a stranger) and the gateway's subscribe check.

import type { NextFunction, Request, Response } from 'express';
import { AppError, NotFoundError } from '../middleware/error-handler';
import { markAccess, type AccessMarkedHandler } from '../middleware/resource-access';
import { mayReadImage } from '../pipeline/image/imageAccess';
import { getImageBlobStore } from '../pipeline/image/visionServices';
import { mediaCatalogRepo } from '../repositories';
import type { MediaCatalogRow } from '../repositories/MediaCatalogRepository';

export const FILE_ID = /^[0-9a-f]{64}$/;

export interface PhotoAccessDeps {
  getFile(fileId: string): Promise<Pick<MediaCatalogRow, 'fileId' | 'imageId'> | null>;
  getImageMeta(imageId: string): Promise<{ uploadedBy?: string } | null>;
}

export const defaultPhotoAccessDeps: PhotoAccessDeps = {
  getFile: (fileId) => mediaCatalogRepo.getFile(fileId),
  getImageMeta: (imageId) => getImageBlobStore().getMetadata(imageId),
};

export async function decidePhotoAccess(
  fileId: string,
  userId: string,
  deps: PhotoAccessDeps = defaultPhotoAccessDeps,
): Promise<{ allow: boolean; reason?: string }> {
  if (!FILE_ID.test(fileId)) return { allow: false, reason: 'not a photo id' };
  const row = await deps.getFile(fileId);
  if (!row) return { allow: false, reason: 'photo not found' };
  if (!row.imageId) return { allow: true };
  const meta = await deps.getImageMeta(row.imageId);
  // An image row gone from the store leaves the catalog row readable, like a walker upload.
  if (!meta) return { allow: true };
  return mayReadImage(meta, userId) ? { allow: true } : { allow: false, reason: 'not a reader of the photo' };
}

/** Route guard: the caller may read the photo `:fileId` names (else 404). */
export function requirePhotoRead(deps: PhotoAccessDeps = defaultPhotoAccessDeps): AccessMarkedHandler {
  return markAccess((req: Request, _res: Response, next: NextFunction) => {
    void (async () => {
      if (!req.headers.authorization?.startsWith('Bearer ')) throw new AppError(401, 'Authorization required');
      const fileId = String(req.params?.fileId ?? '');
      const decision = await decidePhotoAccess(fileId, req.user?.sub ?? '', deps);
      if (!decision.allow) throw new NotFoundError('photo not found');
      next();
    })().catch(next);
  }, 'media:photo-read');
}

/**
 * The gateway channel for a photo's live edits. Channel names cap at 50
 * characters, so the 64-hex id rides as 43 base64url characters.
 */
export function photoChannelOf(fileId: string): string {
  return `photo:${Buffer.from(fileId, 'hex').toString('base64url')}`;
}

export function fileIdOfPhotoChannel(channel: string): string | null {
  if (!channel.startsWith('photo:')) return null;
  const key = channel.slice('photo:'.length);
  if (!/^[A-Za-z0-9_-]{43}$/.test(key)) return null;
  const hex = Buffer.from(key, 'base64url').toString('hex');
  return FILE_ID.test(hex) ? hex : null;
}
