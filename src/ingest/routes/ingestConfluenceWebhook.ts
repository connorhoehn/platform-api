// platform-api/src/ingest/routes/ingestConfluenceWebhook.ts
//
// Confluence-specific webhook receiver. Mounted (in app.ts) at
//   POST /api/ingest/webhook/confluence/:sourceId
// BEFORE auth so Atlassian's webhook callbacks (which carry no JWT) can
// reach it. Confluence "page updated" events are normalized into the
// existing `IngestEvent` shape and trigger a re-fetch of the changed page.

import { Router, type Request, type Response } from 'express';
import { withContext } from '../../lib/logger';
import { ingestSourceRepo } from '../repositories';
import { getIngestEngine } from '../engine-singleton';
import type { IngestEvent, IngestSource } from '../types';
import { verifyWebhookSignature } from './verify-webhook';

const log = withContext({ route: 'ingestConfluenceWebhook' });

// ─── Dependency interfaces ────────────────────────────────────────────────

export interface ConfluenceWebhookSourceRepository {
  get(sourceId: string): Promise<IngestSource | null>;
}

export interface ConfluenceWebhookSyncProducer {
  /**
   * Trigger a re-fetch for a single page on the given source. Implementations
   * can either fan-out to a full source sync or do a targeted single-page
   * fetch — the contract here only requires that `pageId` makes its way back
   * through the ingest pipeline.
   */
  triggerPageRefetch(args: { sourceId: string; pageId: string }): Promise<{ deduped: boolean }>;
}

export interface ConfluenceWebhookEventBus {
  publish?(event: IngestEvent): void;
}

export interface ConfluenceWebhookDeps {
  sourceRepository: ConfluenceWebhookSourceRepository;
  syncProducer: ConfluenceWebhookSyncProducer;
  eventBus?: ConfluenceWebhookEventBus;
  /** Override clock for deterministic tests. */
  now?: () => Date;
}

// ─── Payload normalization ────────────────────────────────────────────────

/**
 * Confluence webhook event shape (subset we care about):
 *   {
 *     "event": "page_updated",
 *     "page": { "id": "12345", "title": "Foo", "version": { "number": 7 } },
 *     "userKey": "...", "timestamp": 1714940000000
 *   }
 *
 * Atlassian Cloud also delivers Connect-style payloads with
 * `eventType: 'page_updated'` and `content.id` — accept both shapes.
 */
export interface ConfluencePageUpdate {
  pageId: string;
  title?: string;
  versionNumber?: number;
  eventType: 'page_updated' | 'page_created' | 'page_removed' | 'unknown';
}

export function normalizeConfluencePayload(body: unknown): ConfluencePageUpdate | null {
  if (!body || typeof body !== 'object') return null;
  const obj = body as Record<string, unknown>;

  // Try classic shape: { event, page: { id } }
  const classicEvent = typeof obj.event === 'string' ? obj.event : undefined;
  const classicPage = obj.page as Record<string, unknown> | undefined;
  // Try connect-style: { eventType, content: { id, title, version: { number } } }
  const connectEvent = typeof obj.eventType === 'string' ? obj.eventType : undefined;
  const connectContent = obj.content as Record<string, unknown> | undefined;

  const eventStr = classicEvent ?? connectEvent ?? '';
  const pageObj = classicPage ?? connectContent;
  if (!pageObj || typeof pageObj !== 'object') return null;

  const id = (pageObj as Record<string, unknown>).id;
  if (id === undefined || id === null) return null;
  const pageId = String(id);

  const title = typeof (pageObj as Record<string, unknown>).title === 'string'
    ? ((pageObj as Record<string, unknown>).title as string)
    : undefined;

  const version = (pageObj as Record<string, unknown>).version as
    | Record<string, unknown>
    | undefined;
  const versionNumber =
    version && typeof version.number === 'number' ? (version.number as number) : undefined;

  let eventType: ConfluencePageUpdate['eventType'] = 'unknown';
  if (eventStr.includes('updated')) eventType = 'page_updated';
  else if (eventStr.includes('created')) eventType = 'page_created';
  else if (eventStr.includes('removed') || eventStr.includes('deleted')) eventType = 'page_removed';

  return { pageId, title, versionNumber, eventType };
}

// ─── Factory ──────────────────────────────────────────────────────────────

export function createConfluenceWebhookRoutes(deps: ConfluenceWebhookDeps): Router {
  const router = Router();
  const now = deps.now ?? (() => new Date());

  router.post('/:sourceId', async (req: Request, res: Response) => {
    const sourceId = req.params.sourceId;

    const source = await deps.sourceRepository.get(sourceId);
    if (!source) {
      return res.status(404).json({ error: `source ${sourceId} not found` });
    }

    const verify = verifyWebhookSignature(req, source);
    if (!verify.ok) {
      log.error(
        { sourceId, reason: verify.reason },
        'confluence webhook verification failed',
      );
      return res.status(verify.status ?? 403).json({ error: verify.reason });
    }

    const update = normalizeConfluencePayload(req.body);
    if (!update) {
      return res.status(400).json({ error: 'unrecognized confluence webhook payload' });
    }

    // Emit a normalized IngestEvent so the rest of the pipeline (UI, audit)
    // sees Confluence pushes the same way it sees S3/Drive pushes.
    const evt: IngestEvent = {
      type: 'sync.file.fetched',
      sourceId,
      filePath: update.pageId,
      index: 0,
      total: 1,
      timestamp: now().toISOString(),
    };
    deps.eventBus?.publish?.(evt);

    if (update.eventType === 'page_removed') {
      // No re-fetch needed for deletes; the operator surface tracks this via
      // the audit event we just published.
      log.info({ sourceId, pageId: update.pageId }, 'confluence page removed');
      return res.status(200).json({ status: 'page_removed', pageId: update.pageId });
    }

    const { deduped } = await deps.syncProducer.triggerPageRefetch({
      sourceId,
      pageId: update.pageId,
    });

    log.info(
      { sourceId, pageId: update.pageId, deduped, eventType: update.eventType },
      'confluence webhook accepted',
    );

    return res.status(200).json({
      status: deduped ? 'sync_in_progress' : 'sync_triggered',
      pageId: update.pageId,
      eventType: update.eventType,
    });
  });

  return router;
}

// ─── Singleton wired with real dependencies ───────────────────────────────

export const confluenceWebhookRouter = createConfluenceWebhookRoutes({
  sourceRepository: {
    get: (sourceId) => ingestSourceRepo.getById(sourceId),
  },
  syncProducer: {
    triggerPageRefetch: async ({ sourceId, pageId }) => {
      const engine = getIngestEngine();
      if (!engine) return { deduped: false };
      // Ingest engine doesn't (yet) expose targeted single-page fetch;
      // route through the regular per-source sync, which will pick up the
      // changed page on its next list() pass via Confluence's version field.
      void engine.triggerSync(sourceId, 'webhook').catch((err: unknown) => {
        log.error(
          { err: err instanceof Error ? err.message : String(err), sourceId, pageId },
          'confluence webhook-triggered sync failed',
        );
      });
      return { deduped: false };
    },
  },
});
