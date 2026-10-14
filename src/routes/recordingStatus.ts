import type { NextFunction, Request, Response, Router } from 'express';
import type { CallRecording } from '../repositories/CallRecordingRepository';
import { requireAuth } from '../middleware/auth';
import { markAccess } from '../middleware/resource-access';
import { asyncHandler, NotFoundError, ValidationError } from '../middleware/error-handler';
import { askLobbyAuthority } from '../lib/lobbyAuthority';
import { getRunPersistenceService } from '../pipeline/runPersistence';
import { recordingStatus, waitForRecordingChange, type RecordingEventSource } from '../recordings/recordingStatus';

/** Long polling uses the native pipeline bus rather than a new chat channel.
 * No transcript, summary, participant list or pipeline context is exposed. */
export function installRecordingStatusRoute(router: Router, options: {
  authorize(req: Request, id: string): Promise<CallRecording>;
  source?: () => RecordingEventSource | null;
  timeoutMs?: number;
}) {
  const read = async (req: Request): Promise<CallRecording> => {
    const row = await options.authorize(req, String(req.params.recordingId));
    // Unlike a short request, this may have waited across a host revocation.
    // Ask current host authority before every response, including Work lobbies.
    if (req.user?.authKind === 'orgiq-hs256') {
      const bearer = req.headers.authorization?.slice(7) ?? '';
      if (!row.lobbyName || !await askLobbyAuthority(bearer, row.lobbyName,
        { ...(row.documentContext ? { documentSourceId: row.documentContext.sourceId } : {}) })) {
        throw new NotFoundError('recording not found');
      }
    }
    return row;
  };
  // The access guard, at registration: participant + current host authority
  // before the handler parses anything or subscribes to the event bus, so a
  // refused caller allocates no listener, timer or subscription.
  const guard = markAccess((req: Request, res: Response, next: NextFunction) => {
    read(req).then((row) => { res.locals.recordingStatusRow = row; next(); }, next);
  }, 'recording:read+lobby-authority');
  router.get('/recordings/:recordingId/status', requireAuth, guard,
    asyncHandler(async (req, res, next) => {
      // The socket may have closed while the guard's authority read was pending.
      if (res.destroyed) return;
      const after = req.query.after;
      if (after !== undefined && (typeof after !== 'string' || !/^[a-f0-9]{64}$/.test(after))) throw new ValidationError('Invalid recording revision');
      const row = res.locals.recordingStatusRow as CallRecording;
      if (after === recordingStatus(row).revision) {
        const abort = new AbortController();
        const close = () => abort.abort();
        res.once('close', close);
        try {
          await waitForRecordingChange({ row, revision: after, read: () => read(req), signal: abort.signal,
            source: options.source ? options.source() : getRunPersistenceService()?.getEventBus() ?? null,
            timeoutMs: options.timeoutMs });
        } finally { res.removeListener('close', close); }
        if (abort.signal.aborted || res.destroyed) return;
      }
      next();
    }),
    // Re-verify bearer signature and expiry after waiting, before writing headers.
    requireAuth,
    asyncHandler(async (req, res) => {
      const row = await read(req);
      if (res.destroyed) return;
      res.setHeader('Cache-Control', 'private, no-store');
      res.json(recordingStatus(row));
    }));
}
