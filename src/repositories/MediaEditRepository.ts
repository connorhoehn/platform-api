// platform-api/src/repositories/MediaEditRepository.ts
//
// A photo's kept edits (realtime-examples docs/design/media-photo-editor/PLAN.md §2).
// One NEW table, `media-edits` (HASH fileId, RANGE sk, no GSI), created on
// first use, so no index is ever added to a populated table:
//
//   HEAD          { currentVersion, editCount, pick, masks[], updatedAt, updatedBy }
//   v#00000001…   one immutable item per kept version: { version, parentVersion, ops, summary, source, … }
//   sug#<id>      { version } — the suggestion a version kept, so a replayed Keep answers the same version
//
// A version is its own item (the #188 lesson: an array of versions on one
// row grows to the 400 KB cap). One Keep = one TransactWriteItems: the
// version item only if absent + the HEAD only if still at the version the
// writer read (the compare-and-set) + the suggestion claim only if absent.
// A cancelled transaction is re-read: a claim that now exists means this
// Keep already landed (idempotent replay), otherwise it is a real conflict.
// Nothing is ever overwritten; Restore writes a new version with old ops.

import {
  GetCommand,
  QueryCommand,
  TransactWriteCommand,
  UpdateCommand,
  BatchGetCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import {
  CreateTableCommand,
  DescribeTableCommand,
  waitUntilTableExists,
  type DynamoDBClient,
} from '@aws-sdk/client-dynamodb';
import { ddbClient, docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';

export const MEDIA_EDITS_TABLE = tableName('media-edits');
export const HEAD_SK = 'HEAD';
/** Masks per photo — they ride on HEAD, so the list is capped. */
export const MAX_MASKS = 8;
/** Ops per version: a panel has ~20 controls × a few scopes. */
export const MAX_OPS = 120;

export type PhotoEditSource = 'user' | 'assistant' | 'restore';

export interface PhotoMaskRecord {
  maskId: string;
  label: string;
  kind: 'people' | 'subject' | 'sky' | 'brush';
  subjects?: number;
  /** Outline(s), x/y normalized 0..1 of the photo. */
  polygons: Array<Array<[number, number]>>;
  /** sha256 of the rasterized 8-bit PNG; the blob is `mask-<blobHash>` in the image store. */
  blobHash: string;
  /** How the mask was made. `seed` = hand-drawn for the sample set; no segmentation step exists yet. */
  origin: 'seed' | 'brush' | 'segmentation';
}

export interface PhotoEditHead {
  fileId: string;
  currentVersion: number;
  editCount: number;
  pick: 'pick' | 'reject' | null;
  masks: PhotoMaskRecord[];
  updatedAt: string;
  updatedBy?: string;
}

export interface PhotoEditVersionItem {
  fileId: string;
  version: number;
  parentVersion: number | null;
  ops: Array<Record<string, unknown>>;
  summary: string;
  source: PhotoEditSource;
  suggestionId?: string;
  runId?: string;
  restoredFrom?: number;
  renderedImageId?: string;
  createdBy: string;
  createdAt: string;
}

export class PhotoEditConflict extends Error {
  constructor(public readonly fileId: string, public readonly currentVersion: number) {
    super(`photo ${fileId} is at version ${currentVersion}`);
    this.name = 'PhotoEditConflict';
  }
}

export const versionSk = (v: number) => `v#${String(v).padStart(8, '0')}`;
const sugSk = (id: string) => `sug#${id}`;

function cancelled(err: unknown): boolean {
  const name = (err as { name?: string } | null)?.name;
  return name === 'TransactionCanceledException' || name === 'ConditionalCheckFailedException';
}

export async function ensureMediaEditsTable(client: DynamoDBClient = ddbClient, table = MEDIA_EDITS_TABLE): Promise<void> {
  try {
    await client.send(new DescribeTableCommand({ TableName: table }));
    return;
  } catch (err) {
    if ((err as { name?: string })?.name !== 'ResourceNotFoundException') throw err;
  }
  try {
    await client.send(new CreateTableCommand({
      TableName: table,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [
        { AttributeName: 'fileId', AttributeType: 'S' },
        { AttributeName: 'sk', AttributeType: 'S' },
      ],
      KeySchema: [
        { AttributeName: 'fileId', KeyType: 'HASH' },
        { AttributeName: 'sk', KeyType: 'RANGE' },
      ],
    }));
  } catch (err) {
    if ((err as { name?: string })?.name !== 'ResourceInUseException') throw err;
  }
  await waitUntilTableExists({ client, maxWaitTime: 60 }, { TableName: table });
}

export interface AppendVersionInput {
  fileId: string;
  ops: Array<Record<string, unknown>>;
  /** The version the writer saw. Undefined = append on top of whatever is current (retried). */
  parentVersion?: number;
  summary: string;
  source: PhotoEditSource;
  suggestionId?: string;
  runId?: string;
  restoredFrom?: number;
  userId: string;
  now?: string;
}

export interface AppendResult {
  version: PhotoEditVersionItem;
  /** True when this call wrote nothing because the suggestion was already kept. */
  replay: boolean;
}

function toHead(fileId: string, item: Record<string, unknown> | undefined): PhotoEditHead {
  return {
    fileId,
    currentVersion: Number(item?.currentVersion ?? 0),
    editCount: Number(item?.editCount ?? 0),
    pick: (item?.pick as PhotoEditHead['pick']) ?? null,
    masks: (item?.masks as PhotoMaskRecord[] | undefined) ?? [],
    updatedAt: String(item?.updatedAt ?? ''),
    ...(item?.updatedBy ? { updatedBy: String(item.updatedBy) } : {}),
  };
}

function stripKeys(item: Record<string, unknown>): PhotoEditVersionItem {
  const { sk: _sk, ...rest } = item;
  return rest as unknown as PhotoEditVersionItem;
}

export class MediaEditRepository {
  private ready: Promise<void> | null = null;

  constructor(
    private readonly client: DynamoDBDocumentClient = docClient,
    private readonly table: string = MEDIA_EDITS_TABLE,
    private readonly ensure: () => Promise<void> = () => ensureMediaEditsTable(ddbClient, table),
  ) {}

  private tableReady(): Promise<void> {
    if (!this.ready) this.ready = this.ensure().catch((err) => { this.ready = null; throw err; });
    return this.ready;
  }

  async getHead(fileId: string): Promise<PhotoEditHead> {
    await this.tableReady();
    const out = await this.client.send(new GetCommand({ TableName: this.table, Key: { fileId, sk: HEAD_SK }, ConsistentRead: true }));
    return toHead(fileId, out.Item);
  }

  /** HEAD rows for many photos (the filmstrip badges). Missing = an unedited photo. */
  async getHeads(fileIds: readonly string[]): Promise<PhotoEditHead[]> {
    await this.tableReady();
    const ids = [...new Set(fileIds)];
    const found = new Map<string, Record<string, unknown>>();
    for (let i = 0; i < ids.length; i += 100) {
      let keys: Record<string, unknown>[] = ids.slice(i, i + 100).map((fileId) => ({ fileId, sk: HEAD_SK }));
      for (let attempt = 0; attempt < 4 && keys.length > 0; attempt++) {
        const out = await this.client.send(new BatchGetCommand({ RequestItems: { [this.table]: { Keys: keys } } }));
        for (const it of out.Responses?.[this.table] ?? []) found.set(String(it.fileId), it);
        keys = (out.UnprocessedKeys?.[this.table]?.Keys ?? []) as Record<string, unknown>[];
      }
    }
    return ids.map((id) => toHead(id, found.get(id)));
  }

  async getVersion(fileId: string, version: number): Promise<PhotoEditVersionItem | null> {
    await this.tableReady();
    const out = await this.client.send(new GetCommand({ TableName: this.table, Key: { fileId, sk: versionSk(version) }, ConsistentRead: true }));
    return out.Item ? stripKeys(out.Item) : null;
  }

  /** Versions newest first, one Query per page. */
  async listVersions(fileId: string, opts: { limit?: number; before?: number } = {}): Promise<{ versions: PhotoEditVersionItem[]; next: number | null }> {
    await this.tableReady();
    const limit = Math.min(Math.max(opts.limit ?? 50, 1), 100);
    const out = await this.client.send(new QueryCommand({
      TableName: this.table,
      KeyConditionExpression: opts.before
        ? 'fileId = :f AND sk BETWEEN :lo AND :hi'
        : 'fileId = :f AND begins_with(sk, :p)',
      ExpressionAttributeValues: opts.before
        ? { ':f': fileId, ':lo': versionSk(0), ':hi': versionSk(opts.before - 1) }
        : { ':f': fileId, ':p': 'v#' },
      ScanIndexForward: false,
      Limit: limit,
      ConsistentRead: true,
    }));
    const versions = (out.Items ?? []).map(stripKeys);
    const last = versions[versions.length - 1];
    return { versions, next: out.LastEvaluatedKey && last ? last.version : null };
  }

  private async claimed(fileId: string, suggestionId: string): Promise<PhotoEditVersionItem | null> {
    const out = await this.client.send(new GetCommand({ TableName: this.table, Key: { fileId, sk: sugSk(suggestionId) }, ConsistentRead: true }));
    if (!out.Item) return null;
    return this.getVersion(fileId, Number(out.Item.version));
  }

  /**
   * Keep: write version current+1. With `parentVersion`, a HEAD that moved
   * since is a PhotoEditConflict (the caller re-reads); without it, the
   * append retries on top of whatever is current (jittered, up to 20).
   */
  async appendVersion(input: AppendVersionInput): Promise<AppendResult> {
    await this.tableReady();
    const attempts = input.parentVersion === undefined ? 20 : 1;
    for (let attempt = 0; attempt < attempts; attempt++) {
      if (input.suggestionId) {
        const prior = await this.claimed(input.fileId, input.suggestionId);
        if (prior) return { version: prior, replay: true };
      }
      const head = await this.getHead(input.fileId);
      const current = head.currentVersion;
      if (input.parentVersion !== undefined && input.parentVersion !== current) {
        throw new PhotoEditConflict(input.fileId, current);
      }
      const now = input.now ?? new Date().toISOString();
      const version: PhotoEditVersionItem = {
        fileId: input.fileId,
        version: current + 1,
        parentVersion: current === 0 ? null : current,
        ops: input.ops,
        summary: input.summary,
        source: input.source,
        ...(input.suggestionId ? { suggestionId: input.suggestionId } : {}),
        ...(input.runId ? { runId: input.runId } : {}),
        ...(input.restoredFrom !== undefined ? { restoredFrom: input.restoredFrom } : {}),
        createdBy: input.userId,
        createdAt: now,
      };
      try {
        await this.client.send(new TransactWriteCommand({
          TransactItems: [
            { Put: { TableName: this.table, Item: { ...version, sk: versionSk(version.version) }, ConditionExpression: 'attribute_not_exists(sk)' } },
            {
              Update: {
                TableName: this.table,
                Key: { fileId: input.fileId, sk: HEAD_SK },
                UpdateExpression: 'SET currentVersion = :n, editCount = :c, updatedAt = :t, updatedBy = :u',
                ConditionExpression: 'attribute_not_exists(sk) OR currentVersion = :cur',
                ExpressionAttributeValues: { ':n': version.version, ':c': input.ops.length, ':t': now, ':u': input.userId, ':cur': current },
              },
            },
            ...(input.suggestionId ? [{
              Put: {
                TableName: this.table,
                Item: { fileId: input.fileId, sk: sugSk(input.suggestionId), version: version.version, createdAt: now },
                ConditionExpression: 'attribute_not_exists(sk)',
              },
            }] : []),
          ],
        }));
        return { version, replay: false };
      } catch (err) {
        if (!cancelled(err)) throw err;
        if (input.suggestionId) {
          const prior = await this.claimed(input.fileId, input.suggestionId);
          if (prior) return { version: prior, replay: true };
        }
        if (input.parentVersion !== undefined) {
          throw new PhotoEditConflict(input.fileId, (await this.getHead(input.fileId)).currentVersion);
        }
        await new Promise((r) => setTimeout(r, 5 + Math.random() * 20 * (attempt + 1)));
      }
    }
    throw new PhotoEditConflict(input.fileId, (await this.getHead(input.fileId)).currentVersion);
  }

  async setPick(fileId: string, pick: PhotoEditHead['pick'], userId: string): Promise<void> {
    await this.tableReady();
    await this.client.send(new UpdateCommand({
      TableName: this.table,
      Key: { fileId, sk: HEAD_SK },
      UpdateExpression: 'SET pick = :p, updatedAt = :t, updatedBy = :u, currentVersion = if_not_exists(currentVersion, :z), editCount = if_not_exists(editCount, :z)',
      ExpressionAttributeValues: { ':p': pick, ':t': new Date().toISOString(), ':u': userId, ':z': 0 },
    }));
  }

  /** Replace the photo's masks (the seed, a brush). Versions are untouched. */
  async setMasks(fileId: string, masks: PhotoMaskRecord[], userId: string): Promise<void> {
    await this.tableReady();
    await this.client.send(new UpdateCommand({
      TableName: this.table,
      Key: { fileId, sk: HEAD_SK },
      UpdateExpression: 'SET masks = :m, updatedAt = :t, updatedBy = :u, currentVersion = if_not_exists(currentVersion, :z), editCount = if_not_exists(editCount, :z)',
      ExpressionAttributeValues: { ':m': masks.slice(0, MAX_MASKS), ':t': new Date().toISOString(), ':u': userId, ':z': 0 },
    }));
  }

  /** The one attribute a version gains after it is written: its render. Set once. */
  async setRendered(fileId: string, version: number, imageId: string): Promise<void> {
    await this.tableReady();
    try {
      await this.client.send(new UpdateCommand({
        TableName: this.table,
        Key: { fileId, sk: versionSk(version) },
        UpdateExpression: 'SET renderedImageId = :i',
        ConditionExpression: 'attribute_exists(sk) AND attribute_not_exists(renderedImageId)',
        ExpressionAttributeValues: { ':i': imageId },
      }));
    } catch (err) {
      if (!cancelled(err)) throw err;
    }
  }
}
