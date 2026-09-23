// platform-api/src/documents/lifecycleBatch.ts
//
// Many lifecycle rows in one BatchGetItem per 100 ids — the Work column's
// backfill for a whole scope (PLAN §2.1). Read-only; the lifecycle
// repository stays the only writer of `document-lifecycle`.

import { BatchGetCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import { normalizeStatus } from '../lib/document-lifecycle';

const BATCH_GET_LIMIT = 100;

export async function lifecycleMany(
  documentIds: readonly string[],
  client: DynamoDBDocumentClient = docClient,
  table: string = tableName('document-lifecycle'),
): Promise<Map<string, { status?: string; ownerId?: string } | null>> {
  const out = new Map<string, { status?: string; ownerId?: string } | null>();
  const wanted = [...new Set(documentIds)].filter(Boolean);
  for (const id of wanted) out.set(id, null);
  for (let i = 0; i < wanted.length; i += BATCH_GET_LIMIT) {
    let keys = wanted.slice(i, i + BATCH_GET_LIMIT).map((documentId) => ({ documentId }));
    for (let attempt = 0; attempt < 2 && keys.length > 0; attempt++) {
      const res = await client.send(new BatchGetCommand({
        RequestItems: { [table]: { Keys: keys, ConsistentRead: true, ProjectionExpression: 'documentId, #s, ownerId', ExpressionAttributeNames: { '#s': 'status' } } },
      }));
      for (const row of (res.Responses?.[table] ?? []) as Array<Record<string, unknown>>) {
        const id = row.documentId;
        if (typeof id !== 'string') continue;
        out.set(id, {
          ...(typeof row.status === 'string' ? { status: normalizeStatus(row.status) } : {}),
          ...(typeof row.ownerId === 'string' ? { ownerId: row.ownerId } : {}),
        });
      }
      keys = (res.UnprocessedKeys?.[table]?.Keys ?? []) as Array<{ documentId: string }>;
    }
  }
  return out;
}
