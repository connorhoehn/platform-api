// scripts/backfill-revision-slides.ts
//
// Strips the legacy `revisionSlides` map from presentation rows (NFR row
// diet, Loop 26). The logic and its guarantees live in
// src/deck/revisionSlidesBackfill.ts: an entry goes only when the revision
// holds the same slides byte for byte and still names every blob the entry
// named; each write is conditioned on the row's `updatedAt` and revision
// count, so a save that lands mid-pass wins and the row waits for the next
// pass. Rows, items and blobs are never deleted.
//
// Dry run by default. Logs bytes before/after per row, the largest row and
// the table's total.
//
//   LOCAL_DDB_ENDPOINT=http://localhost:18001 npx tsx scripts/backfill-revision-slides.ts            # dry run
//   LOCAL_DDB_ENDPOINT=http://localhost:18001 npx tsx scripts/backfill-revision-slides.ts --apply
//   … --document <id>      one row only
//   … --pause-ms 3000      wait between reading a row and writing it (to exercise the race)
//   … --json               one JSON summary on stdout

import { DescribeTableCommand, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { ScanCommand } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../src/lib/aws-clients';
import { DdbImageBlobStore } from '../src/pipeline/image/ImageBlobStore';
import {
  DOCUMENTS_TABLE, DdbPresentationDocumentStore, estimateItemBytes, type PresentationDocumentRow,
} from '../src/deck/presentationDocuments';
import { applyRevisionSlidesStrip, planRevisionSlidesStrip } from '../src/deck/revisionSlidesBackfill';

const args = process.argv.slice(2);
const opt = (name: string): string | undefined => {
  const i = args.indexOf(`--${name}`);
  return i === -1 ? undefined : args[i + 1];
};
const APPLY = args.includes('--apply');
const ONLY = opt('document');
const PAUSE_MS = Number(opt('pause-ms') ?? 0);
const JSON_OUT = args.includes('--json');
const say = (line: string) => { if (!JSON_OUT) process.stdout.write(`${line}\n`); };
const kb = (bytes: number) => `${(bytes / 1024).toFixed(1)} KB`;

async function scanAll(): Promise<Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  let ExclusiveStartKey: Record<string, unknown> | undefined;
  do {
    const page = await docClient.send(new ScanCommand({ TableName: DOCUMENTS_TABLE, ExclusiveStartKey, ...(ONLY ? { FilterExpression: 'documentId = :id', ExpressionAttributeValues: { ':id': ONLY } } : {}) }));
    rows.push(...(page.Items ?? []));
    ExclusiveStartKey = page.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return rows;
}

async function tableSizeBytes(): Promise<number | null> {
  const endpoint = process.env.LOCAL_DDB_ENDPOINT || process.env.DYNAMODB_ENDPOINT;
  const client = new DynamoDBClient({ ...(endpoint ? { endpoint } : {}), region: process.env.AWS_REGION ?? 'us-east-1' });
  const out = await client.send(new DescribeTableCommand({ TableName: DOCUMENTS_TABLE })).catch(() => null);
  return out?.Table?.TableSizeBytes ?? null;
}

async function main() {
  // Reads blobs from the durable DynamoDB copy (where every manifest and
  // post-#4 slide is written first). A blob only on a pod's volume reads as
  // missing, and that entry is kept — never stripped on a guess.
  const store = new DdbPresentationDocumentStore({ blobStore: new DdbImageBlobStore({ ttlDays: null }), mirror: null });
  const describedBefore = await tableSizeBytes();
  const rows = await scanAll();
  const sizes = new Map<string, number>(rows.map((row) => [String(row.documentId), estimateItemBytes(row)]));
  const totalBefore = [...sizes.values()].reduce((a, b) => a + b, 0);
  const largestBefore = [...sizes.entries()].sort((a, b) => b[1] - a[1])[0];
  const legacyRows = rows.filter((row) => row.revisionSlides !== undefined) as unknown as PresentationDocumentRow[];
  say(`${APPLY ? 'APPLY' : 'DRY RUN'} — table ${DOCUMENTS_TABLE}: ${rows.length} rows, ${kb(totalBefore)} estimated${describedBefore !== null ? ` (DescribeTable ${kb(describedBefore)})` : ''}; largest ${largestBefore?.[0]} ${kb(largestBefore?.[1] ?? 0)}; ${legacyRows.length} rows carry revisionSlides`);

  const results: Array<Record<string, unknown>> = [];
  for (const row of legacyRows) {
    const plan = await planRevisionSlidesStrip(store, row);
    let outcome = plan.strip.length ? 'would-strip' : 'nothing-to-do';
    if (APPLY && plan.strip.length) {
      if (PAUSE_MS) await new Promise((resolve) => setTimeout(resolve, PAUSE_MS));
      outcome = await applyRevisionSlidesStrip(docClient, row, plan);
    }
    if (outcome === 'stripped' || outcome === 'would-strip') sizes.set(row.documentId, plan.bytesAfter);
    results.push({ documentId: row.documentId, title: row.title, outcome, bytesBefore: plan.bytesBefore, bytesAfter: plan.bytesAfter, legacyBytes: plan.legacyBytes, stripped: plan.strip.length, kept: plan.kept });
    say(`  ${row.documentId} "${row.title ?? ''}": ${kb(plan.bytesBefore)} → ${kb(plan.bytesAfter)} (legacy ${kb(plan.legacyBytes)}; ${plan.strip.length} entries ${APPLY ? '' : 'to '}strip, ${plan.kept.length} kept) ${outcome}`);
    for (const kept of plan.kept) say(`      kept ${kept.revisionId}: ${kept.reason}`);
  }

  const totalAfter = [...sizes.values()].reduce((a, b) => a + b, 0);
  const largestAfter = [...sizes.entries()].sort((a, b) => b[1] - a[1])[0];
  const describedAfter = APPLY ? await tableSizeBytes() : null;
  const summary = {
    mode: APPLY ? 'apply' : 'dry-run',
    rows: rows.length,
    legacyRows: legacyRows.length,
    raced: results.filter((r) => r.outcome === 'raced').length,
    totalBytes: { before: totalBefore, after: totalAfter },
    describeTableBytes: { before: describedBefore, after: describedAfter },
    largest: { before: largestBefore, after: largestAfter },
    results,
  };
  if (JSON_OUT) process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  else say(`${APPLY ? 'after' : 'would be'}: ${kb(totalAfter)} estimated total; largest ${largestAfter?.[0]} ${kb(largestAfter?.[1] ?? 0)}${describedAfter !== null ? `; DescribeTable ${kb(describedAfter)}` : ''}; raced ${summary.raced}`);
}

main().catch((error) => { process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`); process.exit(1); });
