// Moves a presentation row's `revisions[]` array into `presentation-revisions`
// items (realtime-examples NFR #188). Driven by scripts/migrate-revision-items.ts.
//
// Guarantees, per row:
//   - COPY first: each array entry becomes item `rev#<version>`, put only if
//     that item does not exist (never overwrites);
//   - VERIFY every copy: each item is read back (consistent read) and its
//     revision compared to the array entry as canonical JSON (keys sorted —
//     DynamoDB returns a map in no set order), byte for byte;
//   - only then REMOVE the array from the head row, in one UpdateItem
//     conditioned on the array still having the length that was copied and
//     the row's `updatedAt` being the one read, so a save that lands mid-pass
//     wins and the row simply waits for the next pass. `revisionCount` and
//     `headRevision` are set only if absent (a revision already written as an
//     item keeps its own).
// Idempotent and resumable: an item already there and equal counts as
// copied; a re-run finds nothing to do. No row, item or blob is deleted —
// the one removal is the array field, after its verified copy.

import { GetCommand, PutCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DOCUMENTS_TABLE, estimateItemBytes, type PresentationDocumentRow, type StoredRevision } from './presentationDocuments';
import {
  PRESENTATION_REVISIONS_TABLE, canonicalJson, revKey, summarizeRevision, versionOf, type RevisionItem,
} from './presentationRevisionItems';

/** An item this big is refused by DynamoDB (400 KB); leave the entry on the row instead. */
const MAX_ITEM_BYTES = 380 * 1024;

export type MigrationOutcome =
  | { documentId: string; action: 'skip'; reason: string }
  | { documentId: string; action: 'keep'; reason: string; copied: number }
  | { documentId: string; action: 'would-migrate'; revisions: number; toCopy: number; bytesBefore: number; bytesAfter: number }
  | { documentId: string; action: 'migrated'; revisions: number; copied: number; bytesBefore: number; bytesAfter: number }
  | { documentId: string; action: 'raced'; copied: number };

function conditionFailed(error: unknown): boolean {
  return (error as { name?: string })?.name === 'ConditionalCheckFailedException';
}

async function readItem(docClient: DynamoDBDocumentClient, table: string, documentId: string, version: number): Promise<RevisionItem | null> {
  const result = await docClient.send(new GetCommand({ TableName: table, Key: { documentId, rev: revKey(version) }, ConsistentRead: true }));
  return (result.Item as RevisionItem | undefined) ?? null;
}

/** The head row as it will be once the array is gone. */
function rowAfter(row: PresentationDocumentRow, head: StoredRevision): Record<string, unknown> {
  const { revisions: _revisions, ...rest } = row;
  return {
    ...rest,
    revisionCount: row.revisionCount ?? row.revisions!.length,
    headRevision: row.headRevision ?? summarizeRevision(head),
  };
}

export async function migrateRevisionRow(
  docClient: DynamoDBDocumentClient,
  row: PresentationDocumentRow,
  opts: { apply: boolean; table?: string; pauseMs?: number },
): Promise<MigrationOutcome> {
  const table = opts.table ?? PRESENTATION_REVISIONS_TABLE;
  const documentId = row.documentId;
  const revisions = row.revisions;
  if (!Array.isArray(revisions) || revisions.length === 0) return { documentId, action: 'skip', reason: 'no revisions array' };
  // Ids must be v1..vN in order: the item key is the version, and a row whose
  // array says otherwise is not one this store wrote.
  for (let index = 0; index < revisions.length; index += 1) {
    if (versionOf(revisions[index]?.id) !== index + 1) {
      return { documentId, action: 'keep', reason: `entry ${index} is ${JSON.stringify(revisions[index]?.id)}, not v${index + 1}`, copied: 0 };
    }
  }
  const tooBig = revisions.find((revision, index) => estimateItemBytes({ documentId, rev: revKey(index + 1), version: index + 1, revision } as unknown as Record<string, unknown>) > MAX_ITEM_BYTES);
  if (tooBig) return { documentId, action: 'keep', reason: `${tooBig.id} is over ${MAX_ITEM_BYTES / 1024} KB as an item`, copied: 0 };

  const bytesBefore = estimateItemBytes(row as unknown as Record<string, unknown>);
  const bytesAfter = estimateItemBytes(rowAfter(row, revisions[revisions.length - 1]!));

  // What is already there (a resumed pass): equal = copied; different = stop.
  const existing = await Promise.all(revisions.map((_revision, index) => readItem(docClient, table, documentId, index + 1)));
  for (let index = 0; index < revisions.length; index += 1) {
    const item = existing[index];
    if (item && canonicalJson(item.revision) !== canonicalJson(revisions[index])) {
      return { documentId, action: 'keep', reason: `item ${revKey(index + 1)} exists and differs from the array entry`, copied: 0 };
    }
  }
  const toCopy = revisions.map((revision, index) => ({ revision, version: index + 1 })).filter((entry) => !existing[entry.version - 1]);
  if (!opts.apply) return { documentId, action: 'would-migrate', revisions: revisions.length, toCopy: toCopy.length, bytesBefore, bytesAfter };

  // 1. Copy — never over an item that exists.
  let copied = 0;
  for (const entry of toCopy) {
    try {
      await docClient.send(new PutCommand({
        TableName: table,
        Item: { documentId, rev: revKey(entry.version), version: entry.version, revision: entry.revision },
        ConditionExpression: 'attribute_not_exists(rev)',
      }));
      copied += 1;
    } catch (error) {
      if (!conditionFailed(error)) throw error;
      // Someone (another pass) put it first — the verify below judges it.
    }
  }

  // 2. Verify every copy, byte for byte on canonical JSON.
  for (let index = 0; index < revisions.length; index += 1) {
    const item = await readItem(docClient, table, documentId, index + 1);
    if (!item || canonicalJson(item.revision) !== canonicalJson(revisions[index])) {
      return { documentId, action: 'keep', reason: `copy of v${index + 1} did not verify`, copied };
    }
  }

  if (opts.pauseMs) await new Promise((resolve) => setTimeout(resolve, opts.pauseMs));

  // 3. Remove the array — only while it is still exactly what was copied.
  const head = summarizeRevision(revisions[revisions.length - 1]!);
  try {
    await docClient.send(new UpdateCommand({
      TableName: DOCUMENTS_TABLE,
      Key: { documentId },
      UpdateExpression: 'REMOVE revisions SET revisionCount = if_not_exists(revisionCount, :count), headRevision = if_not_exists(headRevision, :head)',
      ConditionExpression: row.updatedAt !== undefined
        ? 'size(revisions) = :count AND updatedAt = :updatedAt'
        : 'size(revisions) = :count AND attribute_not_exists(updatedAt)',
      ExpressionAttributeValues: {
        ':count': revisions.length,
        ':head': head,
        ...(row.updatedAt !== undefined ? { ':updatedAt': row.updatedAt } : {}),
      },
    }));
  } catch (error) {
    if (!conditionFailed(error)) throw error;
    return { documentId, action: 'raced', copied };
  }
  return { documentId, action: 'migrated', revisions: revisions.length, copied, bytesBefore, bytesAfter };
}
