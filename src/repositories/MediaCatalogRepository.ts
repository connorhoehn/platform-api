// platform-api/src/repositories/MediaCatalogRepository.ts
//
// The catalog: one row per file the walker has seen, keyed by CONTENT hash so
// a file moved or renamed on the drive is the same row, and a second catalog
// of the same drive (HoehnPhotosOrganizer's) can be reconciled by the same
// key. Two indexes answer the two reads the page makes: newest first, and
// everything in one burst.

import { BatchWriteCommand, DynamoDBDocumentClient, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export const MEDIA_CATALOG_TABLE = tableName('media-catalog');
/** Every row shares one partition on the time index: the catalog is browsed as one stream. */
export const CATALOG_BUCKET = 'all';

export interface MediaCatalogRow {
  fileId: string;
  bucket: typeof CATALOG_BUCKET;
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

  async getFile(fileId: string): Promise<MediaCatalogRow | null> {
    return this.getItem<MediaCatalogRow>({ fileId });
  }

  /** Newest first by shot time. */
  async listRecent(opts: { limit?: number; cursor?: string | null } = {}): Promise<CatalogPage> {
    const out = await this.docClient.send(new QueryCommand({
      TableName: this.tableName,
      IndexName: 'time-index',
      KeyConditionExpression: '#b = :b',
      ExpressionAttributeNames: { '#b': 'bucket' },
      ExpressionAttributeValues: { ':b': CATALOG_BUCKET },
      ScanIndexForward: false,
      Limit: Math.min(Math.max(opts.limit ?? 60, 1), 500),
      ...(decodeCursor(opts.cursor) ? { ExclusiveStartKey: decodeCursor(opts.cursor) } : {}),
    }));
    return { items: (out.Items ?? []) as MediaCatalogRow[], cursor: encodeCursor(out.LastEvaluatedKey as Record<string, unknown> | undefined) };
  }

  /** Every frame of one burst, in shot order. */
  async listBurst(burstId: string): Promise<MediaCatalogRow[]> {
    const out = await this.docClient.send(new QueryCommand({
      TableName: this.tableName,
      IndexName: 'burst-index',
      KeyConditionExpression: '#g = :g',
      ExpressionAttributeNames: { '#g': 'burstId' },
      ExpressionAttributeValues: { ':g': burstId },
      ScanIndexForward: true,
    }));
    return (out.Items ?? []) as MediaCatalogRow[];
  }
}
