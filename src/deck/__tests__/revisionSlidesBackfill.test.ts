// The legacy `revisionSlides` strip: lossless per entry, conditional on the
// row being exactly as read, idempotent.

import { GetCommand, PutCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import { DdbPresentationDocumentStore, type PresentationDocumentRow, type StoredSlide } from '../presentationDocuments';
import { applyRevisionSlidesStrip, planRevisionSlidesStrip } from '../revisionSlidesBackfill';

const svg = (text: string) => `<svg xmlns="http://www.w3.org/2000/svg">${text}</svg>`;

/** A one-row DynamoDB: Put, Get, the store's SET update, and the backfill's conditional SET/REMOVE. */
function fakeClient() {
  let row: Record<string, unknown> | null = null;
  const client = {
    async send(command: unknown) {
      if (command instanceof GetCommand) return { Item: row ? JSON.parse(JSON.stringify(row)) : undefined };
      if (command instanceof PutCommand) row = JSON.parse(JSON.stringify((command as unknown as { input: { Item: Record<string, unknown> } }).input.Item));
      if (command instanceof UpdateCommand && row) {
        const input = (command as unknown as { input: { UpdateExpression: string; ConditionExpression?: string; ExpressionAttributeValues: Record<string, unknown> } }).input;
        const values = input.ExpressionAttributeValues;
        if (input.ConditionExpression?.includes('updatedAt = :updatedAt') && row.updatedAt !== values[':updatedAt']) {
          throw Object.assign(new Error('The conditional request failed'), { name: 'ConditionalCheckFailedException' });
        }
        if (input.ConditionExpression?.includes('attribute_exists(revisionSlides)') && row.revisionSlides === undefined) {
          throw Object.assign(new Error('The conditional request failed'), { name: 'ConditionalCheckFailedException' });
        }
        const next: Record<string, unknown> = { ...row };
        if (input.UpdateExpression === 'REMOVE revisionSlides') delete next.revisionSlides;
        else if (input.UpdateExpression === 'SET revisionSlides = :remaining') next.revisionSlides = values[':remaining'];
        else for (const [key, value] of Object.entries(values)) if (key !== ':expectedCount') next[key.slice(1)] = value;
        row = JSON.parse(JSON.stringify(next));
      }
      return {};
    },
  } as unknown as DynamoDBDocumentClient;
  return { client, row: () => row as unknown as PresentationDocumentRow, set: (next: PresentationDocumentRow) => { row = JSON.parse(JSON.stringify(next)); } };
}

/** A deck at v2 whose row also carries the pre-#4 legacy map, markup and all. */
async function legacyDeck() {
  const blobStore = new InMemoryImageBlobStore();
  const mirror = new InMemoryImageBlobStore();
  const fake = fakeClient();
  let tick = 0;
  const store = new DdbPresentationDocumentStore({ docClient: fake.client, blobStore, mirror, now: () => new Date(Date.UTC(2026, 8, 23, 10, 0, tick++)) });
  const slidesV1 = [{ id: 'cover', title: 'Cover', svg: svg('cover') }, { id: 'plan', title: 'Plan', svg: svg('plan v1') }];
  const slidesV2 = [{ id: 'cover', title: 'Cover', svg: svg('cover') }, { id: 'plan', title: 'Plan', svg: svg('plan v2') }];
  await store.appendRevision({ documentId: 'deck-1', title: 'Deck', createdBy: 'u', pptx: Buffer.from('PK1'), slides: slidesV1 });
  await store.appendRevision({ documentId: 'deck-1', title: 'Deck', createdBy: 'u', pptx: Buffer.from('PK2'), slides: slidesV2 });
  const row = fake.row();
  // What a pre-#4 save wrote beside `revisions[]`: every slide's record again, with its markup.
  const legacyOf = (revisionId: string, source: typeof slidesV1): StoredSlide[] => {
    const own = row.revisions!.find((r) => r.id === revisionId)!;
    return source.map((slide, index) => ({
      id: slide.id, index, title: slide.title, previewHandle: `deck-1.${revisionId}.${slide.id}`,
      asset: own.slides?.[index]?.asset ?? { key: `deck-svg-legacy-${revisionId}-${slide.id}.svg`, contentType: 'image/svg+xml', sizeBytes: 1 },
      svg: slide.svg,
    }));
  };
  const v1 = row.revisions![0]!;
  const v1Slides = (await store.readAsset(v1.slidesRef!))!;
  const v1Records = JSON.parse(v1Slides.toString('utf8')) as StoredSlide[];
  fake.set({
    ...row,
    revisionSlides: {
      v1: slidesV1.map((slide, index) => ({ ...v1Records[index]!, svg: slide.svg })),
      v2: legacyOf('v2', slidesV2),
    },
  });
  return { store, fake };
}

describe('revisionSlides backfill', () => {
  it('strips an entry the revision already holds byte for byte, and every slide still serves', async () => {
    const { store, fake } = await legacyDeck();
    const before = fake.row();
    const plan = await planRevisionSlidesStrip(store, before);
    expect(plan.strip.sort()).toEqual(['v1', 'v2']);
    expect(plan.kept).toEqual([]);
    expect(plan.bytesAfter).toBeLessThan(plan.bytesBefore);
    expect(plan.legacyBytes).toBeGreaterThan(0);
    expect(await applyRevisionSlidesStrip(fake.client, before, plan)).toBe('stripped');
    const after = fake.row();
    expect(after.revisionSlides).toBeUndefined();
    expect(after.revisions).toEqual(before.revisions);
    for (const [revisionId, text] of [['v1', 'plan v1'], ['v2', 'plan v2']] as const) {
      const served = await store.slidePreview('deck-1', revisionId, 'plan');
      expect(served?.body.toString('utf8')).toBe(svg(text));
    }
    // Idempotent: nothing left to plan or write.
    const again = await planRevisionSlidesStrip(store, after);
    expect(again.strip).toEqual([]);
    expect(await applyRevisionSlidesStrip(fake.client, after, again)).toBe('nothing-to-do');
  });

  it('keeps an entry it cannot prove redundant, with the reason', async () => {
    const { store, fake } = await legacyDeck();
    const row = fake.row();
    const legacy = row.revisionSlides!;
    fake.set({
      ...row,
      revisionSlides: {
        ...legacy,
        v2: legacy.v2!.map((slide) => (slide.id === 'plan' ? { ...slide, svg: svg('edited by hand') } : slide)),
        v9: legacy.v1!,
        v1: [...legacy.v1!, { id: 'extra', index: 2, title: 'Extra', previewHandle: 'x', asset: { key: 'deck-svg-only-here.svg', contentType: 'image/svg+xml', sizeBytes: 1 }, svg: svg('extra') }],
      },
    });
    const plan = await planRevisionSlidesStrip(store, fake.row());
    expect(plan.strip).toEqual([]);
    expect(Object.fromEntries(plan.kept.map((k) => [k.revisionId, k.reason]))).toEqual({
      v1: 'slide extra is only in the legacy entry',
      v2: 'slide plan differs from the revision\'s copy',
      v9: 'no revision with this id on the row',
    });
    expect(await applyRevisionSlidesStrip(fake.client, fake.row(), plan)).toBe('nothing-to-do');
  });

  it('removes only the provable entries and keeps the rest', async () => {
    const { store, fake } = await legacyDeck();
    const row = fake.row();
    fake.set({ ...row, revisionSlides: { ...row.revisionSlides!, v9: row.revisionSlides!.v1! } });
    const plan = await planRevisionSlidesStrip(store, fake.row());
    expect(plan.strip.sort()).toEqual(['v1', 'v2']);
    expect(await applyRevisionSlidesStrip(fake.client, fake.row(), plan)).toBe('stripped');
    expect(Object.keys(fake.row().revisionSlides!)).toEqual(['v9']);
  });

  it('never writes over a save that landed after the read: the condition fails and the new revision stays', async () => {
    const { store, fake } = await legacyDeck();
    const read = fake.row();
    const plan = await planRevisionSlidesStrip(store, read);
    // A revise lands between the backfill's read and its write.
    await store.appendRevision({ documentId: 'deck-1', title: 'Deck', createdBy: 'u', pptx: Buffer.from('PK3'), slides: [{ id: 'cover', title: 'Cover', svg: svg('cover v3') }] });
    expect(await applyRevisionSlidesStrip(fake.client, read, plan)).toBe('raced');
    const now = fake.row();
    expect(now.revisions!.map((r) => r.id)).toEqual(['v1', 'v2', 'v3']);
    expect(Object.keys(now.revisionSlides!).sort()).toEqual(['v1', 'v2']);
    // The next pass reads the new row and strips it.
    const next = await planRevisionSlidesStrip(store, now);
    expect(await applyRevisionSlidesStrip(fake.client, now, next)).toBe('stripped');
    expect(fake.row().revisions!.map((r) => r.id)).toEqual(['v1', 'v2', 'v3']);
    expect(fake.row().revisionSlides).toBeUndefined();
  });
});
