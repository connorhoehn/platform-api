// Slide census and same-content recovery (NFR #165).
//
// `9d9fff04…/v1` lost every slide's bytes: a pre-#4 revision whose slides were
// blobs only (`deck-<doc>-v1-<slide>.svg`, no inline `svg`) written to the
// file store before its volume was mounted (realtime-examples NFR #70,
// `6be0b31c`) and before the durable DynamoDB mirror existed — a pod
// replacement took the only copy. The row kept naming the keys, every reader
// 404'd, and nothing noticed. Neither the #156 row-diet backfill nor the #48
// sweeper touched it (the row still carries `revisionSlides`; every key it
// names is marked).
//
// This module finds such slides and brings them back only with the SAME bytes:
//   - census: every revision's slides (inline, #36 manifest, legacy
//     `revisionSlides`) are readable (inline markup, the primary store, or the
//     durable copy) — or listed as missing;
//   - at risk: a referenced blob only the primary (volume) holds, with no
//     durable copy — the state that made this loss possible — is listed, and
//     can be copied to the durable store byte for byte;
//   - restore: a missing slide is re-rendered from the revision's own stored
//     spec and planned only when provably identical: its recorded `sha256`
//     matches, or (a legacy slide with no hash) its recorded size matches AND
//     the renderer reproduces every checkable legacy slide byte for byte.
// Writes go under the slide's own recorded key, durable copy first, and only
// when neither store holds the key. Rows are never written; nothing is deleted.

import type { AssetBlobStore, BlobAsset } from '../pipeline/image/ImageBlobStore';
import {
  contentHash, resolveRevisionSlides, resolveRevisionSpec,
  type PresentationDocumentRow, type PresentationDocumentStore, type StoredSlide,
} from './presentationDocuments';
import { renderDeckPreview } from './preview';
import { SVG_CONTENT_TYPE } from './renderSlideSvg';

export interface MissingSlide { documentId: string; revisionId: string; slideId: string; key?: string; sizeBytes?: number; sha256?: string }
export interface PlannedRestore extends MissingSlide { key: string; body: Buffer; proof: 'sha256' | 'size+probe' }
export interface UnrecoverableSlide extends MissingSlide { reason: string }
export interface AtRiskBlob { documentId: string; revisionId: string; key: string; what: 'slide' | 'pptx' | 'spec' | 'slides-manifest' }

export interface SlideCensus {
  rows: number;
  revisions: number;
  missing: MissingSlide[];
  missingPptx: string[];
  atRisk: AtRiskBlob[];
  probe: { slides: number; equal: number; legacySlides: number; legacyEqual: number };
  legacyTrusted: boolean;
  plan: PlannedRestore[];
  unrecoverable: UnrecoverableSlide[];
}

export interface SlideRecoveryStores {
  primary: AssetBlobStore;
  /** The durable copy. Null when the primary IS the durable store. */
  mirror: AssetBlobStore | null;
}

const read = async (store: AssetBlobStore | null, key: string | undefined): Promise<Buffer | null> =>
  store && key ? (await store.getBlob(key).catch(() => null))?.body ?? null : null;

/** Reads only. `onlyDocument` limits the plan; the census and probe cover every row. */
export async function censusSlides(
  store: PresentationDocumentStore,
  stores: SlideRecoveryStores,
  rows: PresentationDocumentRow[],
  onlyDocument?: string,
): Promise<SlideCensus> {
  const bytesOf = async (asset: BlobAsset | undefined) => (await read(stores.primary, asset?.key)) ?? (await read(stores.mirror, asset?.key));
  const census: SlideCensus = {
    rows: rows.length, revisions: 0, missing: [], missingPptx: [], atRisk: [],
    probe: { slides: 0, equal: 0, legacySlides: 0, legacyEqual: 0 }, legacyTrusted: false, plan: [], unrecoverable: [],
  };
  const candidates: PlannedRestore[] = [];
  const noteAtRisk = async (documentId: string, revisionId: string, key: string | undefined, what: AtRiskBlob['what']) => {
    if (!key || !stores.mirror) return;
    if (census.atRisk.some((entry) => entry.key === key)) return;
    if ((await read(stores.primary, key)) && !(await read(stores.mirror, key))) census.atRisk.push({ documentId, revisionId, key, what });
  };

  for (const row of rows) {
    if (!row || typeof row !== 'object') continue;
    const revisionIds = new Set([...(Array.isArray(row.revisions) ? row.revisions : []).map((r) => r.id), ...Object.keys(row.revisionSlides ?? {})]);
    for (const revisionId of revisionIds) {
      census.revisions += 1;
      const revision = row.revisions?.find((r) => r.id === revisionId);
      const listed: StoredSlide[] = [
        ...(revision ? (await resolveRevisionSlides(store, revision)) ?? [] : []),
        ...(Array.isArray(row.revisionSlides?.[revisionId]) ? row.revisionSlides![revisionId]! : []),
      ];
      const byId = new Map<string, StoredSlide>();
      const bytes = new Map<string, Buffer>();
      for (const slide of listed) {
        if (!slide?.id) continue;
        if (!byId.has(slide.id)) byId.set(slide.id, slide);
        if (bytes.has(slide.id)) continue;
        const body = typeof slide.svg === 'string' ? Buffer.from(slide.svg, 'utf8') : await bytesOf(slide.asset);
        if (body) bytes.set(slide.id, body);
        if (typeof slide.svg !== 'string') await noteAtRisk(row.documentId, revisionId, slide.asset?.key, 'slide');
      }
      if (revision?.asset?.key) {
        if (!(await bytesOf(revision.asset))) census.missingPptx.push(`${row.documentId}/${revisionId}`);
        await noteAtRisk(row.documentId, revisionId, revision.asset.key, 'pptx');
      }
      await noteAtRisk(row.documentId, revisionId, revision?.specRef?.key, 'spec');
      await noteAtRisk(row.documentId, revisionId, revision?.slidesRef?.key, 'slides-manifest');

      const spec = revision ? await resolveRevisionSpec(store, revision).catch(() => null) : null;
      let rendered: Map<string, Buffer> | null = null;
      try {
        rendered = spec ? new Map(renderDeckPreview(spec).slides.map((s) => [s.id, Buffer.from(s.svg, 'utf8')])) : null;
      } catch { rendered = null; }

      for (const [slideId, slide] of byId) {
        const stored = bytes.get(slideId);
        const now = rendered?.get(slideId);
        if (stored && now) {
          census.probe.slides += 1;
          const equal = now.equals(stored);
          if (equal) census.probe.equal += 1;
          if (!slide.sha256) { census.probe.legacySlides += 1; if (equal) census.probe.legacyEqual += 1; }
        }
        if (stored) continue;
        const entry: MissingSlide = { documentId: row.documentId, revisionId, slideId, key: slide.asset?.key, sizeBytes: slide.asset?.sizeBytes, sha256: slide.sha256 };
        census.missing.push(entry);
        if (onlyDocument && row.documentId !== onlyDocument) continue;
        if (!spec) { census.unrecoverable.push({ ...entry, reason: 'no readable spec' }); continue; }
        if (!now) { census.unrecoverable.push({ ...entry, reason: 'the spec renders no slide with this id' }); continue; }
        if (!slide.asset?.key) { census.unrecoverable.push({ ...entry, reason: 'no recorded key to restore under' }); continue; }
        if (slide.sha256) {
          const hash = contentHash(now);
          if (hash === slide.sha256) candidates.push({ ...entry, key: slide.asset.key, body: now, proof: 'sha256' });
          else census.unrecoverable.push({ ...entry, reason: `render hashes to ${hash.slice(0, 12)}…, recorded ${slide.sha256.slice(0, 12)}…` });
        } else if (now.byteLength === Number(slide.asset.sizeBytes)) {
          candidates.push({ ...entry, key: slide.asset.key, body: now, proof: 'size+probe' });
        } else {
          census.unrecoverable.push({ ...entry, reason: `render is ${now.byteLength} B, recorded ${slide.asset.sizeBytes} B` });
        }
      }
    }
  }

  census.legacyTrusted = census.probe.legacySlides > 0 && census.probe.legacyEqual === census.probe.legacySlides;
  for (const candidate of candidates) {
    if (candidate.proof === 'sha256' || census.legacyTrusted) census.plan.push(candidate);
    else census.unrecoverable.push({ ...candidate, reason: `size matches, but the renderer reproduces only ${census.probe.legacyEqual}/${census.probe.legacySlides} legacy slides byte for byte` });
  }
  return census;
}

/**
 * Writes the planned slides (durable copy first) and copies at-risk blobs to
 * the durable store. Never overwrites a key a store already holds; never deletes.
 */
export async function applySlideRecovery(
  census: Pick<SlideCensus, 'plan' | 'atRisk'>,
  stores: SlideRecoveryStores,
  opts: { copyAtRisk?: boolean } = {},
): Promise<{ restored: string[]; copied: string[]; skipped: string[] }> {
  const out = { restored: [] as string[], copied: [] as string[], skipped: [] as string[] };
  for (const restore of census.plan) {
    if ((await read(stores.primary, restore.key)) || (await read(stores.mirror, restore.key))) { out.skipped.push(restore.key); continue; }
    const meta = { contentType: SVG_CONTENT_TYPE, createdBy: 'slide-recovery', label: `restore ${restore.documentId} ${restore.revisionId} ${restore.slideId}` };
    if (stores.mirror) await stores.mirror.putBlob(restore.key, restore.body, meta);
    await stores.primary.putBlob(restore.key, restore.body, meta);
    out.restored.push(restore.key);
  }
  if (opts.copyAtRisk && stores.mirror) {
    for (const risk of census.atRisk) {
      if (await read(stores.mirror, risk.key)) { out.skipped.push(risk.key); continue; }
      const blob = await stores.primary.getBlob(risk.key).catch(() => null);
      if (!blob) { out.skipped.push(risk.key); continue; }
      await stores.mirror.putBlob(risk.key, blob.body, { contentType: blob.asset.contentType, createdBy: 'slide-recovery', label: `durable copy ${risk.documentId} ${risk.revisionId}` });
      out.copied.push(risk.key);
    }
  }
  return out;
}
