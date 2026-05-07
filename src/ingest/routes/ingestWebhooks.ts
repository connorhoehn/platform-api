// platform-api/src/ingest/routes/ingestWebhooks.ts
//
// Public webhook endpoint for ingest source change notifications.
// Accepts push notifications from external sources (S3, Drive, etc.) and
// triggers an immediate sync. Mounted at /api/ingest/webhooks in app.ts
// BEFORE the auth middleware so external systems can call without a JWT.

import { Router, type Request, type Response } from 'express';
import { createHmac, timingSafeEqual } from 'crypto';
import { withContext } from '../../lib/logger';
import { ingestSourceRepo } from '../repositories';
import { getIngestEngine } from '../engine-singleton';
import type { IngestSource } from '../types';

const log = withContext({ route: 'ingestWebhooks' });

// ─── Dependency interfaces ────────────────────────────────────────────────

export interface WebhookSourceRepository {
  get(sourceId: string): Promise<IngestSource | null>;
}

export interface WebhookSyncProducer {
  send(msg: { sourceId: string }): Promise<{ deduped: boolean }>;
}

export interface WebhookIngestHandle {
  syncProducer: WebhookSyncProducer;
}

export interface WebhookDeps {
  sourceRepository: WebhookSourceRepository;
  ingestHandle: WebhookIngestHandle;
}

// ─── Types ────────────────────────────────────────────────────────────────

export interface FileChange {
  path: string;
  action: 'modified' | 'deleted';
}

// ─── Helpers ──────────────────────────────────────────────────────────────

/**
 * Verify HMAC-SHA256 signature with `sha256=` prefix. Returns true when
 * the digest matches or when no secret is configured (skip validation).
 */
function verifyHmac(secret: string, bodyString: string, headerValue: string | undefined): boolean {
  if (!headerValue || typeof headerValue !== 'string') return false;
  const prefix = 'sha256=';
  const withoutPrefix = headerValue.startsWith(prefix)
    ? headerValue.slice(prefix.length)
    : headerValue;
  const expected = createHmac('sha256', secret).update(bodyString).digest('hex');
  if (expected.length !== withoutPrefix.length) return false;
  try {
    return timingSafeEqual(Buffer.from(expected, 'utf8'), Buffer.from(withoutPrefix, 'utf8'));
  } catch {
    return false;
  }
}

/** Extract FileChange list from Google Drive change notification payload. */
function parseDrivePayload(obj: Record<string, unknown>): FileChange[] {
  if (!Array.isArray(obj.changes)) return [];
  const files: FileChange[] = [];
  for (const change of obj.changes) {
    const c = change as Record<string, unknown>;
    const name = (c.file as Record<string, unknown> | undefined)?.name;
    if (typeof name !== 'string') continue;
    files.push({ path: name, action: c.removed === true ? 'deleted' : 'modified' });
  }
  return files;
}

/** Extract FileChange list from S3 event payload. */
function parseS3Payload(obj: Record<string, unknown>): FileChange[] {
  if (!Array.isArray(obj.Records)) return [];
  const files: FileChange[] = [];
  for (const record of obj.Records) {
    const r = record as Record<string, unknown>;
    const key = (r.s3 as Record<string, unknown> | undefined)?.object
      ? ((r.s3 as Record<string, unknown>).object as Record<string, unknown>).key
      : undefined;
    if (typeof key !== 'string') continue;
    const eventName = typeof r.eventName === 'string' ? r.eventName : '';
    const action: FileChange['action'] = eventName.startsWith('ObjectRemoved') ? 'deleted' : 'modified';
    files.push({ path: key, action });
  }
  return files;
}

/** Extract FileChange list from generic `{ files: [{path, action}] }` payload. */
function parseGenericPayload(obj: Record<string, unknown>): FileChange[] {
  if (!Array.isArray(obj.files)) return [];
  const files: FileChange[] = [];
  for (const f of obj.files) {
    const file = f as Record<string, unknown>;
    if (typeof file.path !== 'string') continue;
    const action = file.action === 'deleted' ? 'deleted' : 'modified';
    files.push({ path: file.path, action });
  }
  return files;
}

/** Extract changed files from the webhook body regardless of payload format. */
function extractChangedFiles(body: unknown): FileChange[] {
  if (!body || typeof body !== 'object') return [];
  const obj = body as Record<string, unknown>;
  if (obj.kind === 'drive#change' || Array.isArray(obj.changes)) return parseDrivePayload(obj);
  if (Array.isArray(obj.Records)) return parseS3Payload(obj);
  return parseGenericPayload(obj);
}

// ─── Factory ──────────────────────────────────────────────────────────────

/**
 * Factory that creates the ingest webhook router with injected dependencies.
 * Accepts a sourceRepository and an ingestHandle (for testability).
 */
export function createIngestWebhookRoutes(deps: WebhookDeps): Router {
  const router = Router();

  router.post('/:sourceId', async (req: Request, res: Response) => {
    const sourceId = req.params.sourceId;

    // 1. Look up source
    const source = await deps.sourceRepository.get(sourceId);
    if (!source) {
      return res.status(404).json({ error: `source ${sourceId} not found` });
    }

    // 2. Validate webhook signature if source has a webhookSecret configured
    const webhookSecret = (source.config as Record<string, unknown>).webhookSecret;
    if (typeof webhookSecret === 'string' && webhookSecret.length > 0) {
      const bodyString = JSON.stringify(req.body);
      const signature = req.headers['x-webhook-signature'] as string | undefined;
      if (!verifyHmac(webhookSecret, bodyString, signature)) {
        log.error({ sourceId }, 'ingest webhook signature verification failed');
        return res.status(403).json({ error: 'invalid webhook signature' });
      }
    }

    // 3. Parse changed files from body
    const files = extractChangedFiles(req.body);
    if (files.length === 0) {
      return res.status(200).json({ status: 'no_changes' });
    }

    // 4. Trigger sync via the injected handle
    const { deduped } = await deps.ingestHandle.syncProducer.send({ sourceId });

    log.info({ sourceId, filesChanged: files.length, deduped }, 'ingest webhook accepted');

    return res.status(200).json({
      status: deduped ? 'sync_in_progress' : 'sync_triggered',
      filesChanged: files.length,
      files,
    });
  });

  return router;
}

// ─── Singleton wired with real dependencies ───────────────────────────────

export const ingestWebhooksRouter = createIngestWebhookRoutes({
  sourceRepository: {
    get: (sourceId) => ingestSourceRepo.getById(sourceId),
  },
  ingestHandle: {
    syncProducer: {
      send: async ({ sourceId }) => {
        const engine = getIngestEngine();
        if (!engine) return { deduped: false };
        void engine.triggerSync(sourceId, 'webhook').catch((err: unknown) => {
          log.error(
            { err: err instanceof Error ? err.message : String(err), sourceId },
            'webhook-triggered sync failed',
          );
        });
        return { deduped: false };
      },
    },
  },
});
