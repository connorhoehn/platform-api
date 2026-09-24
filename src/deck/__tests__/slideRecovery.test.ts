// NFR #163 — `9d9fff04…/v1` lost every slide's bytes. Reproduces that row's
// shape (a pre-#4 revision whose slides are blobs only, named twice: in
// `revisions[].slides` and the legacy `revisionSlides`), with both blob stores
// empty, and pins what each actor does with it:
//   - the #156 row-diet backfill keeps the legacy entry (it is not the cause);
//   - the #48 sweeper marks every key the row names (it is not the cause);
//   - the census finds the loss, and recovery re-renders from the stored spec
//     only when the bytes are provably the same, never over an existing key;
//   - a blob only the volume holds is flagged and copied to the durable store.

jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn(async () => ({})) }, ddbClient: { send: jest.fn() } }));

import { GetCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import { DdbPresentationDocumentStore, contentHash, type PresentationDocumentRow, type StoredSlide } from '../presentationDocuments';
import { renderDeckPreview } from '../preview';
import { planRevisionSlidesStrip } from '../revisionSlidesBackfill';
import { collectBlobReferences, sweepDeckBlobs } from '../blobSweeper';
import { applySlideRecovery, censusSlides } from '../slideRecovery';
import { sampleDeck } from './fixtures';

const DAY = 24 * 60 * 60 * 1000;

function setup(rows: PresentationDocumentRow[]) {
  const primary = new InMemoryImageBlobStore();
  const mirror = new InMemoryImageBlobStore();
  const docClient = {
    async send(command: unknown) {
      if (command instanceof GetCommand) {
        const id = (command as unknown as { input: { Key: { documentId: string } } }).input.Key.documentId;
        return { Item: rows.find((row) => row.documentId === id) };
      }
      return {};
    },
  } as unknown as DynamoDBDocumentClient;
  const store = new DdbPresentationDocumentStore({ docClient, blobStore: primary, mirror });
  return { primary, mirror, store, stores: { primary, mirror } };
}

/** A pre-#4 legacy revision: blob-only slides (no inline svg, no sha256), spec inline. */
function legacyRow(documentId: string, opts: { inlineSvg?: boolean } = {}): { row: PresentationDocumentRow; bodies: Map<string, Buffer> } {
  const spec = { ...sampleDeck(), title: `Deck ${documentId}` };
  const rendered = renderDeckPreview(spec).slides;
  const bodies = new Map<string, Buffer>();
  const slides: StoredSlide[] = rendered.map((slide, index) => {
    const key = `deck-${documentId}-v1-${slide.id}.svg`;
    const body = Buffer.from(slide.svg, 'utf8');
    bodies.set(key, body);
    return {
      id: slide.id, index: index + 1, title: slide.title, previewHandle: `${documentId}.v1.${slide.id}`,
      asset: { key, contentType: 'image/svg+xml', sizeBytes: body.byteLength },
      ...(opts.inlineSvg ? { svg: slide.svg } : {}),
    };
  });
  const row = {
    documentId, type: 'presentation', title: spec.title, updatedAt: '2026-09-22T02:39:10.636Z',
    revisions: [{ id: 'v1', label: 'v1', createdAt: '2026-09-22T02:39:10.636Z', createdBy: 'dev-frank', slideCount: slides.length, slides, spec,
      asset: { key: `deck-${documentId}-v1.pptx`, contentType: 'application/octet-stream', sizeBytes: 10 } }],
    revisionSlides: { v1: slides.map((slide) => ({ ...slide })) },
  } as unknown as PresentationDocumentRow;
  return { row, bodies };
}

describe('NFR #163: a legacy revision whose only slide copies were on a lost volume', () => {
  it('the row-diet backfill keeps the legacy entry — it has no bytes to prove the strip lossless', async () => {
    const { row } = legacyRow('lost');
    const { store } = setup([row]);
    const plan = await planRevisionSlidesStrip(store, row);
    expect(plan.strip).toEqual([]);
    expect(plan.kept).toEqual([{ revisionId: 'v1', reason: expect.stringContaining('no readable bytes outside the legacy entry') }]);
  });

  it('the sweeper marks every key the row names, and deletes none of them once restored', async () => {
    const { row, bodies } = legacyRow('lost');
    const refs = collectBlobReferences(row);
    for (const key of bodies.keys()) expect(refs.has(key)).toBe(true);
    let now = 0;
    const mirror = new InMemoryImageBlobStore({ now: () => now });
    const primary = new InMemoryImageBlobStore({ now: () => now });
    for (const [key, body] of bodies) { await mirror.putBlob(key, body, { contentType: 'image/svg+xml' }); await primary.putBlob(key, body, { contentType: 'image/svg+xml' }); }
    await mirror.putBlob('deck-orphan-v1-x.svg', Buffer.from('x'), { contentType: 'image/svg+xml' });
    now = 3 * DAY;
    const result = await sweepDeckBlobs({ stores: [mirror, primary], rows: async function* rows() { yield row as unknown as Record<string, unknown>; }, now: () => now });
    expect(result.deleted).toBe(1);
    for (const key of bodies.keys()) expect(await mirror.getBlob(key)).not.toBeNull();
  });

  it('census finds the lost slides; recovery restores the same bytes under the recorded keys and the slides serve again', async () => {
    const lost = legacyRow('lost');
    const intact = legacyRow('intact', { inlineSvg: true }); // the determinism probe's evidence
    const { store, stores, primary, mirror } = setup([lost.row, intact.row]);
    const census = await censusSlides(store, stores, [lost.row, intact.row]);
    expect(census.missing.map((m) => m.slideId)).toEqual(lost.row.revisions![0]!.slides!.map((s) => s.id));
    expect(census.missingPptx).toEqual(['lost/v1', 'intact/v1']);
    expect(census.legacyTrusted).toBe(true);
    expect(census.plan).toHaveLength(bodies(lost).size);
    for (const restore of census.plan) {
      expect(restore.proof).toBe('size+probe');
      expect(restore.body.equals(lost.bodies.get(restore.key)!)).toBe(true);
    }
    const applied = await applySlideRecovery(census, stores);
    expect(applied.restored).toHaveLength(census.plan.length);
    for (const [key, body] of lost.bodies) {
      if (key.endsWith('.pptx')) continue;
      expect((await mirror.getBlob(key))!.body.equals(body)).toBe(true);
      expect((await primary.getBlob(key))!.body.equals(body)).toBe(true);
    }
    const first = lost.row.revisions![0]!.slides![0]!;
    const served = await store.slidePreview('lost', 'v1', first.id);
    expect(served?.body.equals(lost.bodies.get(first.asset.key)!)).toBe(true);
    expect((await censusSlides(store, stores, [lost.row, intact.row])).missing).toEqual([]);
  });

  it('refuses a legacy restore when nothing proves the renderer is deterministic', async () => {
    const lost = legacyRow('lost');
    const { store, stores } = setup([lost.row]);
    const census = await censusSlides(store, stores, [lost.row]);
    expect(census.plan).toEqual([]);
    expect(census.unrecoverable.every((u) => /size matches, but the renderer reproduces only 0\/0/.test(u.reason))).toBe(true);
  });

  it('refuses when the render differs from the recorded size or hash', async () => {
    const lost = legacyRow('lost');
    const intact = legacyRow('intact', { inlineSvg: true });
    const slides = lost.row.revisions![0]!.slides!;
    slides[0] = { ...slides[0]!, asset: { ...slides[0]!.asset, sizeBytes: 1 } };
    slides[1] = { ...slides[1]!, sha256: contentHash('something else') };
    lost.row.revisionSlides = { v1: slides.map((s) => ({ ...s })) };
    const { store, stores } = setup([lost.row, intact.row]);
    const census = await censusSlides(store, stores, [lost.row, intact.row]);
    expect(census.plan.map((p) => p.slideId)).not.toContain(slides[0]!.id);
    expect(census.plan.map((p) => p.slideId)).not.toContain(slides[1]!.id);
    expect(census.unrecoverable.map((u) => u.reason)).toEqual(expect.arrayContaining([expect.stringMatching(/render is \d+ B, recorded 1 B/), expect.stringMatching(/render hashes to/)]));
  });

  it('never overwrites a key a store already holds', async () => {
    const lost = legacyRow('lost');
    const intact = legacyRow('intact', { inlineSvg: true });
    const { store, stores, mirror } = setup([lost.row, intact.row]);
    const census = await censusSlides(store, stores, [lost.row, intact.row]);
    const key = census.plan[0]!.key;
    await mirror.putBlob(key, Buffer.from('already here'), { contentType: 'image/svg+xml' });
    const applied = await applySlideRecovery(census, stores);
    expect(applied.skipped).toContain(key);
    expect((await mirror.getBlob(key))!.body.toString()).toBe('already here');
  });

  it('flags a blob only the volume holds and copies it, byte for byte, to the durable store', async () => {
    const lost = legacyRow('vol');
    const { store, stores, primary, mirror } = setup([lost.row]);
    for (const [key, body] of lost.bodies) await primary.putBlob(key, body, { contentType: 'image/svg+xml' });
    const census = await censusSlides(store, stores, [lost.row]);
    expect(census.missing).toEqual([]);
    expect(census.atRisk.map((a) => a.key).sort()).toEqual([...lost.bodies.keys()].sort());
    const applied = await applySlideRecovery(census, stores, { copyAtRisk: true });
    expect(applied.copied).toHaveLength(lost.bodies.size);
    for (const [key, body] of lost.bodies) expect((await mirror.getBlob(key))!.body.equals(body)).toBe(true);
    expect((await censusSlides(store, stores, [lost.row])).atRisk).toEqual([]);
  });
});

function bodies(deck: { bodies: Map<string, Buffer> }): Map<string, Buffer> {
  return new Map([...deck.bodies].filter(([key]) => key.endsWith('.svg')));
}
