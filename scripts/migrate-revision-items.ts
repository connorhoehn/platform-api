// scripts/migrate-revision-items.ts
//
// Moves every presentation row's `revisions[]` array into one
// `presentation-revisions` item per revision (realtime-examples NFR #188).
// The logic and its guarantees live in src/deck/revisionItemsMigration.ts:
// copy (never over an existing item) → verify every copy byte for byte on
// canonical JSON → remove ONLY the array field, conditioned on the array and
// `updatedAt` being what was copied. Idempotent and resumable; a row a save
// raced is left for the next pass. No row, item or blob is deleted.
//
// Dry run by default.
//
//   LOCAL_DDB_ENDPOINT=http://localhost:18001 npx tsx scripts/migrate-revision-items.ts            # dry run
//   LOCAL_DDB_ENDPOINT=http://localhost:18001 npx tsx scripts/migrate-revision-items.ts --apply
//   … --document <id>      one row only
//   … --pause-ms 3000      wait between verifying and removing the array (to exercise the race)
//   … --json               one JSON summary on stdout

import { ScanCommand } from '@aws-sdk/lib-dynamodb';
import { docClient, ddbClient } from '../src/lib/aws-clients';
import { DOCUMENTS_TABLE, estimateItemBytes, type PresentationDocumentRow } from '../src/deck/presentationDocuments';
import { PRESENTATION_REVISIONS_TABLE, ensurePresentationRevisionsTable } from '../src/deck/presentationRevisionItems';
import { migrateRevisionRow, type MigrationOutcome } from '../src/deck/revisionItemsMigration';

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
    const page = await docClient.send(new ScanCommand({
      TableName: DOCUMENTS_TABLE,
      ExclusiveStartKey,
      ...(ONLY ? { FilterExpression: 'documentId = :id', ExpressionAttributeValues: { ':id': ONLY } } : {}),
    }));
    rows.push(...(page.Items ?? []));
    ExclusiveStartKey = page.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return rows;
}

async function main() {
  await ensurePresentationRevisionsTable(ddbClient);
  const rows = await scanAll();
  const sizes = new Map<string, number>(rows.map((row) => [String(row.documentId), estimateItemBytes(row)]));
  const largestBefore = [...sizes.entries()].sort((a, b) => b[1] - a[1])[0];
  const candidates = rows.filter((row) => Array.isArray(row.revisions) && (row.revisions as unknown[]).length) as unknown as PresentationDocumentRow[];
  say(`${APPLY ? 'APPLY' : 'DRY RUN'} — ${DOCUMENTS_TABLE} → ${PRESENTATION_REVISIONS_TABLE}: ${rows.length} rows, ${candidates.length} carry a revisions array; largest ${largestBefore?.[0]} ${kb(largestBefore?.[1] ?? 0)}`);

  const results: MigrationOutcome[] = [];
  for (const row of candidates) {
    const outcome = await migrateRevisionRow(docClient, row, { apply: APPLY, pauseMs: PAUSE_MS });
    results.push(outcome);
    if (outcome.action === 'migrated' || outcome.action === 'would-migrate') sizes.set(row.documentId, outcome.bytesAfter);
    const detail = outcome.action === 'migrated' || outcome.action === 'would-migrate'
      ? `${outcome.revisions} revisions, ${kb(outcome.bytesBefore)} → ${kb(outcome.bytesAfter)}${outcome.action === 'would-migrate' ? `, ${outcome.toCopy} to copy` : `, ${outcome.copied} copied`}`
      : 'reason' in outcome ? outcome.reason : `${outcome.copied} copied`;
    say(`  ${row.documentId} "${row.title ?? ''}": ${outcome.action} — ${detail}`);
  }
  const largestAfter = [...sizes.entries()].sort((a, b) => b[1] - a[1])[0];
  const count = (action: MigrationOutcome['action']) => results.filter((r) => r.action === action).length;
  const summary = {
    mode: APPLY ? 'apply' : 'dry-run',
    rows: rows.length,
    candidates: candidates.length,
    migrated: count('migrated'),
    wouldMigrate: count('would-migrate'),
    kept: count('keep'),
    raced: count('raced'),
    itemsCopied: results.reduce((sum, r) => sum + ('copied' in r ? r.copied : 0), 0),
    largest: { before: largestBefore, after: largestAfter },
    results,
  };
  if (JSON_OUT) process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  else say(`${APPLY ? 'after' : 'would be'}: largest ${largestAfter?.[0]} ${kb(largestAfter?.[1] ?? 0)}; migrated ${summary.migrated}, would migrate ${summary.wouldMigrate}, kept ${summary.kept}, raced ${summary.raced}, items copied ${summary.itemsCopied}`);
}

main().catch((error) => { process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`); process.exit(1); });
