// Strips the legacy `revisionSlides` map from presentation rows — the row diet
// for rows written before NFR #4.
//
// `revisionSlides[revisionId]` repeated `revisions[].slides` (and, before the
// diet, every slide's SVG a second time). Nothing has written it since #4
// (`ebb24ef`), and every reader takes `revisions[]` first (inline or through
// the `slidesRef` manifest, #36), so on the largest local row it is 111 KB of
// 259 KB that buys nothing but the distance to DynamoDB's 400 KB item limit.
//
// Lossless, entry by entry. An entry is removed only when:
//   - the row still has that revision, and its slide list resolves (inline or
//     from its manifest);
//   - every legacy slide has a slide with the same id there whose bytes are
//     the same (inline markup, or the blob it names) — compared, not assumed;
//   - every deck blob key the entry names is still named by the revision or
//     its manifest, so the blob sweeper (#48) never loses a reference because
//     of this (no blob becomes unreferenced).
// Anything else keeps its entry and is reported with the reason.
//
// Never races a writer. The write is one UpdateCommand conditioned on the
// row being exactly as read: same `updatedAt`, same revision count, and the
// legacy map still present. A save that lands between the read and the write
// changes `updatedAt`, the condition fails, and the row is left for the next
// pass (idempotent: a stripped row has nothing left to plan). Rows and blobs
// are never deleted — only the one attribute (or some of its entries).

import { UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { collectBlobReferences } from './blobSweeper';
import {
  DOCUMENTS_TABLE, estimateItemBytes, resolveRevisionSlides,
  type PresentationDocumentRow, type PresentationDocumentStore, type StoredSlide,
} from './presentationDocuments';

export interface KeptEntry { revisionId: string; reason: string }

export interface StripPlan {
  documentId: string;
  /** Revision ids whose legacy entry is redundant and will be removed. */
  strip: string[];
  kept: KeptEntry[];
  bytesBefore: number;
  bytesAfter: number;
  /** The legacy map's own weight, as DynamoDB counts it. */
  legacyBytes: number;
}

export type StripOutcome = 'stripped' | 'raced' | 'nothing-to-do';

type Store = PresentationDocumentStore;

async function slideBytes(store: Store, slide: StoredSlide | undefined): Promise<Buffer | null> {
  if (!slide) return null;
  if (typeof slide.svg === 'string') return Buffer.from(slide.svg, 'utf8');
  if (!slide.asset?.key || !store.readAsset) return null;
  return store.readAsset(slide.asset).catch(() => null);
}

/** What stripping this row would remove, and what it would keep and why. Reads only. */
export async function planRevisionSlidesStrip(store: Store, row: PresentationDocumentRow): Promise<StripPlan> {
  const legacy = row.revisionSlides && typeof row.revisionSlides === 'object' && !Array.isArray(row.revisionSlides)
    ? row.revisionSlides
    : {};
  const strip: string[] = [];
  const kept: KeptEntry[] = [];
  for (const [revisionId, entries] of Object.entries(legacy)) {
    const revision = row.revisions?.find((candidate) => candidate.id === revisionId);
    if (!revision) { kept.push({ revisionId, reason: 'no revision with this id on the row' }); continue; }
    const resolved = await resolveRevisionSlides(store, revision);
    if (!resolved) { kept.push({ revisionId, reason: 'the revision\'s slide list cannot be read' }); continue; }
    if (!Array.isArray(entries)) { kept.push({ revisionId, reason: 'entry is not a list' }); continue; }
    const referenced = collectBlobReferences({ revision, resolved });
    let reason: string | undefined;
    for (const slide of entries) {
      const match = resolved.find((candidate) => candidate?.id === slide?.id);
      if (!match) { reason = `slide ${slide?.id} is only in the legacy entry`; break; }
      const [was, now] = await Promise.all([slideBytes(store, slide), slideBytes(store, match)]);
      if (!now) { reason = `slide ${slide.id} has no readable bytes outside the legacy entry`; break; }
      if (was && !was.equals(now)) { reason = `slide ${slide.id} differs from the revision's copy`; break; }
      const orphaned = [...collectBlobReferences(slide)].filter((key) => !referenced.has(key));
      if (orphaned.length) { reason = `slide ${slide.id} is the only reference to ${orphaned.join(', ')}`; break; }
    }
    if (reason) kept.push({ revisionId, reason }); else strip.push(revisionId);
  }
  const bytesBefore = estimateItemBytes(row as unknown as Record<string, unknown>);
  const remaining = Object.fromEntries(Object.entries(legacy).filter(([id]) => !strip.includes(id)));
  const { revisionSlides: _drop, ...rest } = row;
  const after = Object.keys(remaining).length ? { ...rest, revisionSlides: remaining } : rest;
  return {
    documentId: row.documentId,
    strip,
    kept,
    bytesBefore,
    bytesAfter: estimateItemBytes(after as unknown as Record<string, unknown>),
    legacyBytes: Object.keys(legacy).length ? estimateItemBytes({ revisionSlides: legacy }) : 0,
  };
}

/**
 * Removes the planned entries, only if the row is still exactly as it was read
 * (`updatedAt`, revision count, legacy map present). A lost condition is
 * `raced`: nothing was written, run again later.
 */
export async function applyRevisionSlidesStrip(
  docClient: DynamoDBDocumentClient,
  row: PresentationDocumentRow,
  plan: StripPlan,
): Promise<StripOutcome> {
  if (!plan.strip.length) return 'nothing-to-do';
  const legacy = row.revisionSlides ?? {};
  const remaining = Object.fromEntries(Object.entries(legacy).filter(([id]) => !plan.strip.includes(id)));
  const values: Record<string, unknown> = { ':count': row.revisions?.length ?? 0 };
  const conditions = ['attribute_exists(documentId)', 'attribute_exists(revisionSlides)', 'size(revisions) = :count'];
  if (row.updatedAt === undefined) conditions.push('attribute_not_exists(updatedAt)');
  else { conditions.push('updatedAt = :updatedAt'); values[':updatedAt'] = row.updatedAt; }
  const keep = Object.keys(remaining).length > 0;
  if (keep) values[':remaining'] = remaining;
  try {
    await docClient.send(new UpdateCommand({
      TableName: DOCUMENTS_TABLE,
      Key: { documentId: row.documentId },
      UpdateExpression: keep ? 'SET revisionSlides = :remaining' : 'REMOVE revisionSlides',
      ConditionExpression: conditions.join(' AND '),
      ExpressionAttributeValues: values,
    }));
    return 'stripped';
  } catch (error) {
    if ((error as { name?: string }).name === 'ConditionalCheckFailedException') return 'raced';
    throw error;
  }
}
