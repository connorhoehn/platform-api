// A presentation's revisions, one DynamoDB item each (realtime-examples NFR #188).
//
// Until now every revision lived in ONE `revisions[]` array on the document's
// `crdt-documents` row. The row diets (#4, #36, #156) moved the bytes off it,
// but each save still added an entry, so a busy deck marched toward the 400 KB
// item cap and the 300 KB guard would eventually refuse every save. Here each
// revision is its own item in `presentation-revisions`:
//
//   documentId (HASH)  rev (RANGE) = `rev#<8-digit version>`   revision = StoredRevision
//
// A separate table rather than a sort key on `crdt-documents` (whose key is
// `documentId` alone and cannot grow a range key) — and no GSI anywhere: a GSI
// added to a populated DynamoDB Local table can break every write (NFR notes).
//
// The head row keeps what the Documents list reads (`title`, `outline`,
// `pageCount`, `revisionPreviews`) plus `revisionCount` and a `headRevision`
// summary. An append is ONE TransactWriteItems: the item, conditioned on the
// version not existing, and the head row, conditioned on the count this writer
// read — so two racing writers never share a version and never skip one.
//
// Rows written before this keep their array until `scripts/migrate-revision-items.ts`
// copies it out (verified) and removes the field; every reader merges both.

import {
  CreateTableCommand, DescribeTableCommand, waitUntilTableExists, type DynamoDBClient,
} from '@aws-sdk/client-dynamodb';
import { GetCommand, QueryCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { tableName } from '../lib/ddb-table-name';
import type { PresentationDocumentRow, StoredRevision } from './presentationDocuments';

export const PRESENTATION_REVISIONS_TABLE = tableName(process.env.DYNAMODB_PRESENTATION_REVISIONS_TABLE || 'presentation-revisions');

/** One stored item. `revision` is a native map, so the blob sweeper's deep walk finds every key it names. */
export interface RevisionItem {
  documentId: string;
  rev: string;
  version: number;
  revision: StoredRevision;
}

/** The head row's one-line summary of its newest revision (the list chip, a conflict answer). */
export interface HeadRevisionSummary {
  id: string;
  label: string;
  slideCount: number;
  createdAt: string;
  createdBy: string;
}

export function revKey(version: number): string {
  return `rev#${String(version).padStart(8, '0')}`;
}

/** `v12` → 12. Every id the store has written is `v<n>`, n = its position. */
export function versionOf(revisionId: string | undefined): number | null {
  const match = /^v(\d+)$/.exec(revisionId ?? '');
  return match ? Number(match[1]) : null;
}

export function summarizeRevision(revision: StoredRevision): HeadRevisionSummary {
  return {
    id: revision.id,
    label: revision.label,
    slideCount: revision.slideCount,
    createdAt: revision.createdAt,
    createdBy: revision.createdBy,
  };
}

/** How many revisions the row stands for, wherever they live. */
export function revisionCountOf(row: PresentationDocumentRow | null): number {
  if (!row) return 0;
  const counted = (row as { revisionCount?: unknown }).revisionCount;
  return typeof counted === 'number' && Number.isSafeInteger(counted) && counted >= 0
    ? counted
    : row.revisions?.length ?? 0;
}

/** Idempotent: create the table (no GSI, no TTL) on first use. */
export async function ensurePresentationRevisionsTable(client: DynamoDBClient, table = PRESENTATION_REVISIONS_TABLE): Promise<void> {
  try {
    await client.send(new DescribeTableCommand({ TableName: table }));
    return;
  } catch (error) {
    if ((error as { name?: string })?.name !== 'ResourceNotFoundException') throw error;
  }
  try {
    await client.send(new CreateTableCommand({
      TableName: table,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [
        { AttributeName: 'documentId', AttributeType: 'S' },
        { AttributeName: 'rev', AttributeType: 'S' },
      ],
      KeySchema: [
        { AttributeName: 'documentId', KeyType: 'HASH' },
        { AttributeName: 'rev', KeyType: 'RANGE' },
      ],
    }));
  } catch (createError) {
    if ((createError as { name?: string })?.name !== 'ResourceInUseException') throw createError;
  }
  await waitUntilTableExists({ client, maxWaitTime: 60 }, { TableName: table });
}

/** One revision item, or null. */
export async function getRevisionItem(docClient: DynamoDBDocumentClient, documentId: string, version: number, table = PRESENTATION_REVISIONS_TABLE): Promise<StoredRevision | null> {
  const result = await docClient.send(new GetCommand({ TableName: table, Key: { documentId, rev: revKey(version) }, ConsistentRead: true }));
  return (result.Item as RevisionItem | undefined)?.revision ?? null;
}

/**
 * One page of revision items, newest first: a single Query (`Limit`, and
 * `ExclusiveStartKey` = the item of `beforeVersion`). `more` says whether an
 * older item exists past the page.
 */
export async function queryRevisionPage(
  docClient: DynamoDBDocumentClient,
  documentId: string,
  opts: { limit: number; beforeVersion?: number },
  table = PRESENTATION_REVISIONS_TABLE,
): Promise<{ revisions: StoredRevision[]; more: boolean }> {
  const result = await docClient.send(new QueryCommand({
    TableName: table,
    KeyConditionExpression: 'documentId = :d',
    ExpressionAttributeValues: { ':d': documentId },
    ScanIndexForward: false,
    Limit: opts.limit,
    // Read-your-writes across replicas (Loop 31 ppt): a save on one replica,
    // then this page from another, must list the new head — and agree with
    // the head row's `revisionCount`, which is read consistently too.
    ConsistentRead: true,
    ...(opts.beforeVersion !== undefined ? { ExclusiveStartKey: { documentId, rev: revKey(opts.beforeVersion) } } : {}),
  }));
  return {
    revisions: ((result.Items ?? []) as RevisionItem[]).map((item) => item.revision),
    more: !!result.LastEvaluatedKey,
  };
}

/** Every revision item of a document, oldest first (paged through `LastEvaluatedKey`). */
export async function queryAllRevisionItems(docClient: DynamoDBDocumentClient, documentId: string, table = PRESENTATION_REVISIONS_TABLE): Promise<RevisionItem[]> {
  const items: RevisionItem[] = [];
  let startKey: Record<string, unknown> | undefined;
  do {
    const page = await docClient.send(new QueryCommand({
      TableName: table,
      KeyConditionExpression: 'documentId = :d',
      ExpressionAttributeValues: { ':d': documentId },
      ConsistentRead: true,
      ...(startKey ? { ExclusiveStartKey: startKey } : {}),
    }));
    items.push(...((page.Items ?? []) as RevisionItem[]));
    startKey = page.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (startKey);
  return items;
}

/**
 * The row's revisions from both places, oldest first: an item wins over the
 * array entry of the same id (the item is the copy the migration verified, or
 * a revision the array never held).
 */
export function mergeRevisions(arrayRevisions: readonly StoredRevision[] | undefined, items: readonly RevisionItem[]): StoredRevision[] {
  const byVersion = new Map<number, StoredRevision>();
  (arrayRevisions ?? []).forEach((revision, index) => byVersion.set(versionOf(revision.id) ?? index + 1, revision));
  for (const item of items) byVersion.set(item.version, item.revision);
  return [...byVersion.entries()].sort((a, b) => a[0] - b[0]).map(([, revision]) => revision);
}

/**
 * JSON with object keys sorted at every depth. A DynamoDB map comes back in no
 * particular key order, so "the copy is the same bytes" is checked on this form.
 */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map((entry) => canonicalJson(entry === undefined ? null : entry)).join(',')}]`;
  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, entry]) => entry !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`).join(',')}}`;
  }
  return JSON.stringify(value);
}
