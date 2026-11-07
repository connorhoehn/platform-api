// platform-api/src/repositories/MediaCatalogRepository.ts
//
// The catalog: one row per file the walker has seen, PER ORGANISATION, keyed by
// CONTENT hash within it so a file moved or renamed on the drive is the same
// row, and a second catalog of the same drive (HoehnPhotosOrganizer's) can be
// reconciled by the same key. Two indexes answer the two reads the page makes:
// newest first, and everything in one burst.
//
// Org scoping (security review, 6 November 2026: the catalog was one shared
// stream any authenticated user of any org could read). The table's hash key
// attribute is still `fileId`, but it now holds `<orgId>#<contentHash>`, so
// two orgs cataloguing the same photo get two rows; `contentId` keeps the bare
// hash and is what the API calls `fileId`. The time index's partition
// (`bucket`) is the org, and burst reads filter on `orgId`. Rows written
// before this (bucket `all`, no `orgId`) belong to no org and are never
// returned; re-walk to rebuild them.

import { BatchWriteCommand, DynamoDBDocumentClient, QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export const MEDIA_CATALOG_TABLE = tableName('media-catalog');

/** The stored hash key: the org, then the content hash. */
export function catalogKey(orgId: string, contentId: string): string {
  return `${orgId}#${contentId}`;
}

export interface MediaCatalogRow {
  /** Stored: `catalogKey(orgId, contentId)`. Returned by the API: the content hash. */
  fileId: string;
  /** The content hash (sha256 hex). */
  contentId: string;
  orgId: string;
  /** The time index's partition: the org. */
  bucket: string;
  root: string;
  path: string;
  relPath: string;
  sizeBytes: number;
  mimeType: string;
  kind: 'image' | 'video' | 'other';
  width: number | null;
  height: number | null;
  capturedAt: string | null;
  modifiedAt: string;
  /** `capturedAt ?? modifiedAt` — the time index's sort key, always present. */
  shotAt: string;
  cameraId: string | null;
  dhash: string | null;
  burstId: string;
  burstSize: number;
  isRepresentative: boolean;
  /** The stored thumbnail, when the walker sent one. */
  imageId: string | null;
  scene: string | null;
  sceneConfidence: number | null;
  caption: string | null;
  catalogedAt: string;
  runId: string | null;
}

export interface CatalogPage {
  items: MediaCatalogRow[];
  cursor: string | null;
}

function encodeCursor(key: Record<string, unknown> | undefined): string | null {
  return key ? Buffer.from(JSON.stringify(key)).toString('base64url') : null;
}
function decodeCursor(cursor: string | null | undefined): Record<string, unknown> | undefined {
  if (!cursor) return undefined;
  try { return JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as Record<string, unknown>; }
  catch { return undefined; }
}

/** The API's view: `fileId` is the content hash, not the stored org-prefixed key. */
function toPublicRow(row: MediaCatalogRow): MediaCatalogRow {
  return { ...row, fileId: row.contentId ?? row.fileId };
}

export class MediaCatalogRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(MEDIA_CATALOG_TABLE, docClient);
  }

  /** Whole rows in, 25 at a time — the DynamoDB batch limit. Last write wins per fileId. */
  async upsertMany(rows: readonly MediaCatalogRow[]): Promise<number> {
    let written = 0;
    for (let i = 0; i < rows.length; i += 25) {
      const chunk = rows.slice(i, i + 25);
      let pending: Record<string, unknown>[] = chunk.map((r) => ({ PutRequest: { Item: r } }));
      // Unprocessed items come back on throughput pressure; DDB-local never does, but real tables do.
      for (let attempt = 0; attempt < 4 && pending.length > 0; attempt++) {
        const out = await this.docClient.send(new BatchWriteCommand({ RequestItems: { [this.tableName]: pending } }));
        const left = (out.UnprocessedItems?.[this.tableName] ?? []) as Record<string, unknown>[];
        written += pending.length - left.length;
        pending = left;
        if (pending.length > 0) await new Promise((r) => setTimeout(r, 50 * (attempt + 1)));
      }
      if (pending.length > 0) throw new Error(`media-catalog: ${pending.length} rows unprocessed after retries`);
    }
    return written;
  }

  /** One file of `orgId`'s catalog by content hash; null when that org has no such row. */
  async getFile(orgId: string, contentId: string): Promise<MediaCatalogRow | null> {
    if (!orgId) return null;
    const row = await this.getItem<MediaCatalogRow>({ fileId: catalogKey(orgId, contentId) });
    return row && row.orgId === orgId ? toPublicRow(row) : null;
  }

  /** `orgId`'s catalog, newest first by shot time. */
  async listRecent(orgId: string, opts: { limit?: number; cursor?: string | null } = {}): Promise<CatalogPage> {
    if (!orgId) return { items: [], cursor: null };
    const out = await this.docClient.send(new QueryCommand({
      TableName: this.tableName,
      IndexName: 'time-index',
      KeyConditionExpression: '#b = :b',
      ExpressionAttributeNames: { '#b': 'bucket' },
      ExpressionAttributeValues: { ':b': orgId },
      ScanIndexForward: false,
      Limit: Math.min(Math.max(opts.limit ?? 60, 1), 500),
      ...(decodeCursor(opts.cursor) ? { ExclusiveStartKey: decodeCursor(opts.cursor) } : {}),
    }));
    const items = ((out.Items ?? []) as MediaCatalogRow[]).filter((r) => r.orgId === orgId).map(toPublicRow);
    return { items, cursor: encodeCursor(out.LastEvaluatedKey as Record<string, unknown> | undefined) };
  }

  /** Every frame of one burst in `orgId`'s catalog, in shot order. */
  async listBurst(orgId: string, burstId: string): Promise<MediaCatalogRow[]> {
    if (!orgId) return [];
    const out = await this.docClient.send(new QueryCommand({
      TableName: this.tableName,
      IndexName: 'burst-index',
      KeyConditionExpression: '#g = :g',
      // Burst ids derive from content, so two orgs can share one.
      FilterExpression: '#o = :o',
      ExpressionAttributeNames: { '#g': 'burstId', '#o': 'orgId' },
      ExpressionAttributeValues: { ':g': burstId, ':o': orgId },
      ScanIndexForward: true,
    }));
    return ((out.Items ?? []) as MediaCatalogRow[]).filter((r) => r.orgId === orgId).map(toPublicRow);
  }

  /** One bounded page of rows that carry no `orgId` (written before org scoping), for the startup backfill. */
  async listUnscoped(limit: number, cursor?: Record<string, unknown>): Promise<{ items: MediaCatalogRow[]; cursor: Record<string, unknown> | undefined }> {
    const out = await this.docClient.send(new ScanCommand({
      TableName: this.tableName,
      FilterExpression: 'attribute_not_exists(orgId)',
      Limit: limit,
      ...(cursor ? { ExclusiveStartKey: cursor } : {}),
    }));
    return { items: (out.Items ?? []) as MediaCatalogRow[], cursor: out.LastEvaluatedKey as Record<string, unknown> | undefined };
  }

  /** Re-key an org-less row into `orgId`'s catalog: write the scoped row, then drop the old one. */
  async adoptRow(row: MediaCatalogRow, orgId: string): Promise<void> {
    const contentId = row.contentId ?? row.fileId;
    await this.putItem({ ...row, fileId: catalogKey(orgId, contentId), contentId, orgId, bucket: orgId });
    await this.deleteItem({ fileId: row.fileId });
  }
}
