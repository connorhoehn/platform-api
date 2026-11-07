// platform-api/src/media/backfillOrg.ts
//
// Catalog rows written before org scoping carry no `orgId` and are never
// returned (MediaCatalogRepository). Decision of 7 November 2026: at startup,
// when exactly one org is configured for the walker, stamp it onto those rows;
// otherwise leave them hidden and log how many there are, once. A re-walk is
// the fallback.
//
// The walker's org is `MEDIA_WALKER_ORG_ID`; under the insecure dev switch
// with no setting it is `default-org`, the same fallback the media routes use.

import { skipAuthEnabled } from '../lib/insecureDevMode';
import type { MediaCatalogRow } from '../repositories/MediaCatalogRepository';

export interface BackfillRepo {
  listUnscoped(limit: number, cursor?: Record<string, unknown>): Promise<{ items: MediaCatalogRow[]; cursor: Record<string, unknown> | undefined }>;
  adoptRow(row: MediaCatalogRow, orgId: string): Promise<void>;
}
export interface BackfillLog { info(o: object, m: string): void; warn(o: object, m: string): void }

export const BACKFILL_BATCH = 100;
export const BACKFILL_MAX_BATCHES = 200;

/** The one org the walker posts as, or null when none or several are configured. */
export function resolveWalkerOrg(env: NodeJS.ProcessEnv = process.env): string | null {
  const configured = (env.MEDIA_WALKER_ORG_ID ?? '').split(',').map((s) => s.trim()).filter(Boolean);
  if (configured.length === 1) return configured[0];
  if (configured.length === 0 && skipAuthEnabled(env)) return 'default-org';
  return null;
}

export async function backfillMediaOrg(
  repo: BackfillRepo, logger: BackfillLog, env: NodeJS.ProcessEnv = process.env,
): Promise<{ stamped: number; hidden: number }> {
  const orgId = resolveWalkerOrg(env);
  let stamped = 0, hidden = 0, cursor: Record<string, unknown> | undefined, batches = 0;
  try {
    do {
      const page = await repo.listUnscoped(BACKFILL_BATCH, cursor);
      cursor = page.cursor;
      batches++;
      if (!orgId) { hidden += page.items.length; }
      else {
        for (const row of page.items) await repo.adoptRow(row, orgId);
        stamped += page.items.length;
        if (page.items.length > 0) logger.info({ orgId, stamped, batch: batches }, '[media] backfilled orgId on catalog rows');
      }
    } while (cursor && batches < BACKFILL_MAX_BATCHES);
  } catch (e) {
    logger.warn({ err: e instanceof Error ? e.message : String(e), stamped }, '[media] orgId backfill stopped early; rows left as they are');
    return { stamped, hidden };
  }
  const more = cursor ? ' (stopped at the batch cap; the rest follow on the next start)' : '';
  if (!orgId) {
    if (hidden > 0) logger.warn({ hidden }, `[media] ${hidden} catalog rows have no orgId and stay hidden: set MEDIA_WALKER_ORG_ID to one org to stamp them, or re-walk${more}`);
  } else if (stamped > 0 || cursor) {
    logger.info({ orgId, stamped }, `[media] orgId backfill done${more}`);
  }
  return { stamped, hidden };
}
