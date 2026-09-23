// NFR #36: a presentation row stops growing with every revision's slide list.
// Only the head keeps its slide records inline; an older revision's list moves
// to a content-addressed manifest (`deck-slides-<hash>.json`, `slidesRef`),
// the same way older specs moved (NFR #4). Old rows read unchanged and migrate
// on their next save; the conditional save is untouched.

jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn(async () => ({})) }, ddbClient: { send: jest.fn() } }));

import { GetCommand, PutCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import type { DeckSpec } from '../DeckSpec';
import {
  DdbPresentationDocumentStore, PresentationRevisionConflictError, estimateItemBytes, resolveRevisionSlides,
  resolveRevisionSpec, slideListContentKey, withRevisionSlides,
  type PresentationDocumentRow, type StoredRevision,
} from '../presentationDocuments';

/** crdt-documents with the store's one conditional update: `size(revisions) = :expectedCount`. */
function fakeClient(initial: PresentationDocumentRow | null = null) {
  let row: PresentationDocumentRow | null = initial ? JSON.parse(JSON.stringify(initial)) : null;
  let raceNext = false;
  const client = {
    async send(command: unknown) {
      const input = (command as { input: Record<string, any> }).input;
      if (command instanceof GetCommand) return { Item: row ? JSON.parse(JSON.stringify(row)) : undefined };
      if (command instanceof PutCommand) {
        if (row) throw Object.assign(new Error('exists'), { name: 'ConditionalCheckFailedException' });
        row = JSON.parse(JSON.stringify(input.Item));
        return {};
      }
      if (command instanceof UpdateCommand && row) {
        const values = input.ExpressionAttributeValues as Record<string, unknown>;
        if (raceNext || (row.revisions?.length ?? 0) !== values[':expectedCount']) {
          raceNext = false;
          throw Object.assign(new Error('The conditional request failed'), { name: 'ConditionalCheckFailedException' });
        }
        const next: Record<string, unknown> = { ...row };
        for (const [key, value] of Object.entries(values)) if (key !== ':expectedCount') next[key.slice(1)] = value;
        row = JSON.parse(JSON.stringify(next));
      }
      return {};
    },
  } as unknown as DynamoDBDocumentClient;
  return { client, written: () => row, raceNextUpdate: () => { raceNext = true; } };
}

const NOTES = 'Speaker notes that run long, the way a real talk track does. '.repeat(40).slice(0, 1900);

function fortySlideSpec(rev: number): DeckSpec {
  return {
    title: `Audit deck r${rev}`, theme: 'product',
    slides: Array.from({ length: 40 }, (_v, index) => ({
      id: `s${index}`, layout: 'bullets' as const, title: `Slide ${index} title`,
      bullets: ['First point on this slide', 'Second point on this slide'],
      // Each save edits one slide's notes, the way a composer edit does.
      notes: `${index === rev % 40 ? `r${rev} ` : ''}${NOTES}`.slice(0, 1900),
      sources: [{ documentId: 'doc-sprint-review', title: 'Sprint review' }],
    })),
  };
}

function renderedSlides(spec: DeckSpec, rev: number) {
  return spec.slides.map((slide, index) => ({
    id: slide.id, title: slide.title, svg: `<svg>${'p'.repeat(1500)}${index}${index === rev % 40 ? `r${rev}` : ''}</svg>`,
  }));
}

function setup(initial: PresentationDocumentRow | null = null) {
  const blobStore = new InMemoryImageBlobStore();
  const mirror = new InMemoryImageBlobStore();
  const table = fakeClient(initial);
  const store = new DdbPresentationDocumentStore({ docClient: table.client, blobStore, mirror });
  return { blobStore, mirror, table, store };
}

async function save(store: DdbPresentationDocumentStore, rev: number, extra: { expectedHead?: string | null } = {}) {
  const spec = fortySlideSpec(rev);
  return store.appendRevision({ documentId: 'audit-nfr36', title: 'Audit deck', createdBy: 'u', pptx: Buffer.from(`PK${rev}`), slides: renderedSlides(spec, rev), spec, ...extra });
}

describe('20 saves of a 40-slide deck with long notes (NFR #36)', () => {
  it('keeps the row under 150 KB, and every revision’s slides and spec still reopen', async () => {
    const { blobStore, mirror, table, store } = setup();
    const puts = jest.spyOn(mirror, 'putBlob');
    const sizes: number[] = [];
    for (let rev = 1; rev <= 20; rev += 1) {
      await save(store, rev);
      sizes.push(estimateItemBytes(table.written() as never));
    }
    // Before NFR #36 the same scenario measured 94,552 B after one save and
    // 290,679 B after twenty (~10.3 KB of slide records per save).
    // eslint-disable-next-line no-console
    console.log(`NFR36 row bytes: first=${sizes[0]} last=${sizes[19]} perSave=${Math.round((sizes[19]! - sizes[9]!) / 10)}`);
    expect(sizes[19]!).toBeLessThan(150 * 1024);
    expect((sizes[19]! - sizes[9]!) / 10).toBeLessThan(1024);

    const row = table.written()!;
    expect(row.revisions).toHaveLength(20);
    // Only the head carries its slide list (and its spec) inline.
    expect(row.revisions!.filter((revision) => revision.slides)).toHaveLength(1);
    expect(row.revisions![19]!.slides).toHaveLength(40);
    for (const revision of row.revisions!.slice(0, 19)) {
      expect(revision.slidesRef!.key).toBe(slideListContentKey(revision.slidesRef!.sha256));
      // Durable copy and volume both hold the manifest.
      expect(await mirror.getBlob(revision.slidesRef!.key)).not.toBeNull();
      expect(await blobStore.getBlob(revision.slidesRef!.key)).not.toBeNull();
    }
    // Each manifest written once: 19 moved lists, never twice.
    expect(puts.mock.calls.filter(([key]) => String(key).startsWith('deck-slides-'))).toHaveLength(19);

    for (const [position, revision] of row.revisions!.entries()) {
      const slides = (await resolveRevisionSlides(store, revision))!;
      expect(slides).toHaveLength(40);
      expect(slides[0]!.previewHandle).toBe(`audit-nfr36.${revision.id}.s0`);
      expect((await resolveRevisionSpec(store, revision))!.title).toBe(`Audit deck r${position + 1}`);
      const edited = (position + 1) % 40;
      expect((await store.slidePreview('audit-nfr36', revision.id, `s${edited}`))!.body.toString('utf8')).toContain(`r${position + 1}</svg>`);
    }
    // The list route's view: every revision with its own slides back in place.
    const listed = await withRevisionSlides(store, row);
    expect(listed.revisions!.every((revision) => revision.slides?.length === 40)).toBe(true);
  });

  it('reads a moved list from the durable copy when the volume lost it', async () => {
    const { table, mirror } = setup();
    const writer = new DdbPresentationDocumentStore({ docClient: table.client, blobStore: new InMemoryImageBlobStore(), mirror });
    await save(writer, 1);
    await save(writer, 2);
    const reader = new DdbPresentationDocumentStore({ docClient: table.client, blobStore: new InMemoryImageBlobStore(), mirror });
    const v1 = table.written()!.revisions![0]!;
    expect(v1.slides).toBeUndefined();
    expect(await resolveRevisionSlides(reader, v1)).toHaveLength(40);
    expect((await reader.slidePreview('audit-nfr36', 'v1', 's1'))!.body.toString('utf8')).toContain('r1</svg>');
  });

  it('says a list is unreadable (null) rather than empty when its manifest is gone everywhere', async () => {
    const { table, store } = setup();
    await save(store, 1);
    await save(store, 2);
    const reader = new DdbPresentationDocumentStore({ docClient: table.client, blobStore: new InMemoryImageBlobStore(), mirror: new InMemoryImageBlobStore() });
    expect(await resolveRevisionSlides(reader, table.written()!.revisions![0]!)).toBeNull();
    expect(await reader.slidePreview('audit-nfr36', 'v1', 's0')).toBeNull();
  });
});

describe('rows written before NFR #36', () => {
  function legacyRow(): PresentationDocumentRow {
    const revision = (id: string): StoredRevision => ({
      id, label: id, createdAt: '2026-09-20T10:00:00.000Z', createdBy: 'u', slideCount: 2,
      asset: { key: `deck-audit-legacy-${id}.pptx`, contentType: 'application/x', sizeBytes: 2 },
      slides: ['cover', 'body'].map((slideId, index) => ({
        id: slideId, index: index + 1, title: slideId, previewHandle: `audit-legacy.${id}.${slideId}`,
        asset: { key: `old-${id}-${slideId}.svg`, contentType: 'image/svg+xml', sizeBytes: 10 },
        svg: `<svg>${id} ${slideId}</svg>`,
      })),
    });
    return { documentId: 'audit-legacy', title: 'Legacy', type: 'presentation', revisions: [revision('v1'), revision('v2')] };
  }

  it('read unchanged, then the next save moves the old inline lists out (and only then)', async () => {
    const { table, store } = setup(legacyRow());
    // Read as it is: every revision's inline list, markup included.
    const before = table.written()!;
    expect(await resolveRevisionSlides(store, before.revisions![0]!)).toEqual(legacyRow().revisions![0]!.slides);
    expect((await store.slidePreview('audit-legacy', 'v1', 'body'))).toMatchObject({ from: 'inline' });
    expect(await withRevisionSlides(store, before)).toBe(before);
    expect(table.written()).toEqual(legacyRow());

    await store.appendRevision({ documentId: 'audit-legacy', title: 'Legacy', createdBy: 'u', pptx: Buffer.from('PK3'),
      slides: [{ id: 'cover', title: 'cover', svg: '<svg>v3 cover</svg>' }] });
    const after = table.written()!;
    expect(after.revisions!.map((revision) => !!revision.slides)).toEqual([false, false, true]);
    // Lossless: the moved list is exactly the old inline one, markup and all.
    expect(await resolveRevisionSlides(store, after.revisions![0]!)).toEqual(legacyRow().revisions![0]!.slides);
    expect((await store.slidePreview('audit-legacy', 'v1', 'body'))).toMatchObject({ from: 'inline' });
    expect((await store.slidePreview('audit-legacy', 'v2', 'cover'))!.body.toString('utf8')).toBe('<svg>v2 cover</svg>');
  });
});

describe('the save is still conditional', () => {
  it('a save whose head moved is refused with a conflict, and the row is untouched', async () => {
    const { table, store } = setup();
    await save(store, 1);
    await save(store, 2);
    const snapshot = JSON.stringify(table.written());
    await expect(save(store, 3, { expectedHead: 'v1' })).rejects.toBeInstanceOf(PresentationRevisionConflictError);
    table.raceNextUpdate();
    await expect(save(store, 3, { expectedHead: 'v2' })).rejects.toBeInstanceOf(PresentationRevisionConflictError);
    expect(JSON.stringify(table.written())).toBe(snapshot);
  });
});
