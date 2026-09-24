// scripts/recover-deck-slides.ts
//
// Census + same-content recovery for presentation slides whose bytes are gone
// (NFR #163). The logic and its proofs live in src/deck/slideRecovery.ts: a
// missing slide is re-rendered from its revision's stored spec and restored
// only when its recorded sha256 matches, or (legacy, no hash) its recorded
// size matches and the renderer reproduces every checkable legacy slide byte
// for byte. Restores go under the slide's own key, durable copy first, never
// over an existing key. `--copy-at-risk` also copies blobs only the volume
// holds into the durable DynamoDB copy. Rows are never written; nothing is
// deleted.
//
// Dry run by default. --document limits what is restored; the census and the
// determinism probe always cover every row.
//   DYNAMODB_ENDPOINT=http://localhost:<fwd> IMAGE_BLOB_STORE=fs IMAGE_BLOB_DIR=<volume dir> \
//     npx tsx scripts/recover-deck-slides.ts [--apply] [--copy-at-risk] [--document <id>] [--json]

import { ScanCommand } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../src/lib/aws-clients';
import { DdbImageBlobStore } from '../src/pipeline/image/ImageBlobStore';
import { getAssetBlobStore } from '../src/pipeline/image/visionServices';
import { ChunkedBlobStore } from '../src/deck/chunkedBlobStore';
import { DOCUMENTS_TABLE, DdbPresentationDocumentStore, contentHash, type PresentationDocumentRow } from '../src/deck/presentationDocuments';
import { applySlideRecovery, censusSlides } from '../src/deck/slideRecovery';

const args = process.argv.slice(2);
const APPLY = args.includes('--apply');
const COPY_AT_RISK = args.includes('--copy-at-risk');
const ONLY = (() => { const i = args.indexOf('--document'); return i === -1 ? undefined : args[i + 1]; })();
const JSON_OUT = args.includes('--json');
const say = (line: string) => { if (!JSON_OUT) process.stdout.write(`${line}\n`); };

async function scanAll(): Promise<PresentationDocumentRow[]> {
  const rows: PresentationDocumentRow[] = [];
  let ExclusiveStartKey: Record<string, unknown> | undefined;
  do {
    const page = await docClient.send(new ScanCommand({ TableName: DOCUMENTS_TABLE, ExclusiveStartKey }));
    rows.push(...((page.Items ?? []) as unknown as PresentationDocumentRow[]));
    ExclusiveStartKey = page.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return rows;
}

async function main() {
  const primary = getAssetBlobStore();
  const mirror = primary instanceof DdbImageBlobStore ? null : new ChunkedBlobStore(new DdbImageBlobStore({ ttlDays: null }));
  const stores = { primary, mirror };
  const store = new DdbPresentationDocumentStore({ blobStore: primary, mirror });
  const census = await censusSlides(store, stores, await scanAll(), ONLY);
  const revisionsHit = new Set(census.missing.map((m) => `${m.documentId}/${m.revisionId}`)).size;

  say(`${APPLY ? 'APPLY' : 'DRY RUN'} — ${census.rows} rows, ${census.revisions} revisions; ${census.missing.length} slides unreadable in ${revisionsHit} revisions; ${census.missingPptx.length} .pptx unreadable; ${census.atRisk.length} blobs only on the volume (no durable copy)`);
  say(`probe: ${census.probe.equal}/${census.probe.slides} readable slides re-render byte-equal (legacy, no sha256: ${census.probe.legacyEqual}/${census.probe.legacySlides})`);
  for (const r of census.plan) say(`  restore ${r.documentId}/${r.revisionId}/${r.slideId} → ${r.key} (${r.body.byteLength} B, sha256 ${contentHash(r.body).slice(0, 12)}…, proof ${r.proof})`);
  for (const u of census.unrecoverable) say(`  cannot restore ${u.documentId}/${u.revisionId}/${u.slideId}: ${u.reason}`);
  for (const a of census.atRisk) say(`  at risk ${a.what} ${a.key} (${a.documentId}/${a.revisionId})`);

  let applied: Awaited<ReturnType<typeof applySlideRecovery>> | null = null;
  if (APPLY) {
    applied = await applySlideRecovery(census, stores, { copyAtRisk: COPY_AT_RISK });
    say(`restored ${applied.restored.length}, copied to the durable store ${applied.copied.length}, skipped ${applied.skipped.length}`);
  }
  if (JSON_OUT) {
    process.stdout.write(`${JSON.stringify({
      mode: APPLY ? 'apply' : 'dry-run', ...census,
      plan: census.plan.map(({ body, ...rest }) => ({ ...rest, bytes: body.byteLength, renderedSha256: contentHash(body) })),
      unrecoverable: census.unrecoverable.map((u) => { const { body: _b, ...rest } = u as typeof u & { body?: Buffer }; return rest; }),
      applied,
    }, null, 2)}\n`);
  }
}

main().catch((error) => { process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`); process.exit(1); });
