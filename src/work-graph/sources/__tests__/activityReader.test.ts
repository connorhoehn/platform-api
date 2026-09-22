// What the reader makes of a presentation row: the slides each revision is
// actually made of, the documents that revision cited, and the bytes behind one
// slide. The row shape is the one `deck/presentationDocuments.ts` writes.

jest.mock('../../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { GetCommand, QueryCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { InMemoryImageBlobStore } from '../../../pipeline/image/ImageBlobStore';
import type { PresentationDocumentRow, StoredSlide } from '../../../deck/presentationDocuments';
import { DynamoWorkActivityReader } from '../activityReader';

type Row = Record<string, unknown>;

const SVG = '<svg xmlns="http://www.w3.org/2000/svg"><title>slide</title></svg>';

function slide(id: string, index: number, title: string, key: string): StoredSlide {
  return {
    id,
    index,
    title,
    previewHandle: `doc-deck.v1.${id}`,
    asset: { key, contentType: 'image/svg+xml', sizeBytes: SVG.length },
  };
}

const deckRow: PresentationDocumentRow & Row = {
  documentId: 'doc-deck',
  title: 'Sprint review',
  type: 'presentation',
  pageCount: 2,
  outline: [{ id: 'cover', title: 'Sprint review' }, { id: 'risks', title: 'Open risks' }],
  revisions: [
    {
      id: 'v1',
      label: 'v1',
      createdAt: '2026-09-21T13:27:00.000Z',
      createdBy: 'connor',
      slideCount: 2,
      asset: { key: 'deck-v1.pptx', contentType: 'application/vnd.openxmlformats-officedocument.presentationml.presentation', sizeBytes: 10 },
      slides: [slide('cover', 1, 'Sprint review', 'deck-v1-cover.svg'), slide('risks', 2, 'Open risks', 'deck-v1-risks.svg')],
    },
  ],
  revisionSlides: {
    v2: [slide('cover', 1, 'Sprint review', 'deck-v2-cover.svg'), slide('owners', 2, 'Mitigations and owners', 'deck-v2-owners.svg')],
  },
};

function readerFor(row: Row | undefined, blobs = new InMemoryImageBlobStore()): DynamoWorkActivityReader {
  const client = {
    async send(command: unknown) {
      if (command instanceof GetCommand) return { Item: row };
      if (command instanceof QueryCommand) return { Count: 0 };
      if (command instanceof ScanCommand) return { Items: [] };
      return {};
    },
  } as unknown as DynamoDBDocumentClient;
  // Deliberately a store with only `get` and `appendRevision`: the reader must
  // still read the row's markup before the blob when the store is an older one
  // that has no `slidePreview` of its own.
  return new DynamoWorkActivityReader(client, {
    presentations: () => ({
      get: async (documentId: string) => (documentId === row?.documentId ? row as unknown as PresentationDocumentRow : null),
      appendRevision: async () => { throw new Error('not used'); },
    }),
    blobs: () => blobs,
  });
}

describe('artifact facts from a presentation row', () => {
  it('reports each revision with its own slides, from either place the row records them', async () => {
    const facts = await readerFor(deckRow).artifact('doc-deck');
    expect(facts?.mediaKind).toBe('presentation');
    expect(facts?.revisions?.map((revision) => revision.id)).toEqual(['v1']);
    expect(facts?.revisions?.[0]?.slides).toEqual([
      { id: 'cover', index: 1, title: 'Sprint review' },
      { id: 'risks', index: 2, title: 'Open risks' },
    ]);
    expect(facts?.revisions?.[0]?.slideCount).toBe(2);

    // The same row with the slides only under `revisionSlides`.
    const keyed = await readerFor({
      ...deckRow,
      revisions: [{ ...(deckRow.revisions ?? [])[0], id: 'v2', slides: [] }],
    }).artifact('doc-deck');
    expect(keyed?.revisions?.[0]?.slides.map((entry) => entry.title))
      .toEqual(['Sprint review', 'Mitigations and owners']);
  });

  it('carries the documents a revision cited, and nothing when it cited none', async () => {
    const cited = await readerFor({
      ...deckRow,
      revisions: [{
        ...(deckRow.revisions ?? [])[0],
        sources: [
          { documentId: 'doc-release', title: 'Release plan' },
          // Neither half is usable on its own, so this one is dropped.
          { documentId: 'doc-unnamed' },
        ],
      }],
    }).artifact('doc-deck');
    expect(cited?.revisions?.[0]?.sources).toEqual([{ documentId: 'doc-release', title: 'Release plan' }]);

    const facts = await readerFor(deckRow).artifact('doc-deck');
    expect(facts?.revisions?.[0]?.sources).toBeUndefined();
  });

  it('says nothing about revisions for a row that records none', async () => {
    const facts = await readerFor({
      documentId: 'doc-deck',
      title: 'Sprint review',
      type: 'presentation',
      pageCount: 12,
      revisionPreviews: { v1: { contentType: 'image/svg+xml', body: SVG } },
    }).artifact('doc-deck');
    expect(facts?.revisions).toBeUndefined();
    expect(facts?.pageCount).toBe(12);
  });

  it('numbers and names a slide the source left untitled rather than dropping it', async () => {
    const facts = await readerFor({
      ...deckRow,
      revisions: [{ ...(deckRow.revisions ?? [])[0], slides: [{ id: 'cover' }, { id: 'risks', title: 'Open risks' }] }],
    }).artifact('doc-deck');
    expect(facts?.revisions?.[0]?.slides).toEqual([
      { id: 'cover', index: 1, title: 'Slide 1' },
      { id: 'risks', index: 2, title: 'Open risks' },
    ]);
  });
});

describe('the bytes behind one slide', () => {
  it('serves the slide the revision record names, from the blob store', async () => {
    const blobs = new InMemoryImageBlobStore();
    await blobs.putBlob('deck-v1-risks.svg', Buffer.from(SVG, 'utf8'), { contentType: 'image/svg+xml' });
    const preview = await readerFor(deckRow, blobs).revisionPreview('doc-deck', 'v1', 'risks');
    expect(preview?.contentType).toBe('image/svg+xml');
    expect(preview?.body.toString('utf8')).toBe(SVG);

    // A slide under `revisionSlides` is reachable the same way.
    await blobs.putBlob('deck-v2-owners.svg', Buffer.from(SVG, 'utf8'), { contentType: 'image/svg+xml' });
    expect((await readerFor(deckRow, blobs).revisionPreview('doc-deck', 'v2', 'owners'))?.body.toString('utf8')).toBe(SVG);
  });

  it('is absent for a slide the record does not name, or whose bytes are gone', async () => {
    const blobs = new InMemoryImageBlobStore();
    expect(await readerFor(deckRow, blobs).revisionPreview('doc-deck', 'v1', 'nowhere')).toBeNull();
    expect(await readerFor(deckRow, blobs).revisionPreview('doc-deck', 'v9', 'cover')).toBeNull();
    // Named, but never stored.
    expect(await readerFor(deckRow, blobs).revisionPreview('doc-deck', 'v1', 'cover')).toBeNull();
  });

  it('refuses markup that could execute inside the viewer that opened it', async () => {
    const blobs = new InMemoryImageBlobStore();
    await blobs.putBlob(
      'deck-v1-cover.svg',
      Buffer.from('<svg xmlns="http://www.w3.org/2000/svg"><script>fetch("/x")</script></svg>', 'utf8'),
      { contentType: 'image/svg+xml' },
    );
    expect(await readerFor(deckRow, blobs).revisionPreview('doc-deck', 'v1', 'cover')).toBeNull();
  });
});

// The row the `--decks` seed leaves behind: a hyphenated document id, slide ids
// like `title-slide`, `revisions[].slides[].asset` AND `revisionSlides` both
// populated, and one inline cover per revision.
const seededRow: Row = {
  documentId: 'active-now-2026-09-21-sprint-review',
  title: 'Sprint review',
  type: 'presentation',
  revisions: [
    {
      id: 'v1',
      label: 'v1',
      createdAt: '2026-09-21T13:27:00.000Z',
      createdBy: 'dev-connor',
      slideCount: 2,
      asset: { key: 'deck-active-now-2026-09-21-sprint-review-v1.pptx', contentType: 'application/octet-stream', sizeBytes: 10 },
      slides: [
        slide('title-slide', 1, 'Sprint review', 'deck-active-now-2026-09-21-sprint-review-v1-title-slide.svg'),
        slide('sources', 2, 'Sources', 'deck-active-now-2026-09-21-sprint-review-v1-sources.svg'),
      ],
    },
  ],
  revisionSlides: {
    v1: [
      slide('title-slide', 1, 'Sprint review', 'deck-active-now-2026-09-21-sprint-review-v1-title-slide.svg'),
      slide('sources', 2, 'Sources', 'deck-active-now-2026-09-21-sprint-review-v1-sources.svg'),
    ],
  },
  revisionPreviews: { v1: { contentType: 'image/svg+xml', body: SVG } },
};

describe('the row the deck seed leaves behind', () => {
  it('serves the slide the work-graph anchor names', async () => {
    const blobs = new InMemoryImageBlobStore();
    await blobs.putBlob(
      'deck-active-now-2026-09-21-sprint-review-v1-title-slide.svg',
      Buffer.from(SVG, 'utf8'),
      { contentType: 'image/svg+xml' },
    );
    const reader = readerFor(seededRow, blobs);
    // Every anchor the snapshot offers resolves to bytes or to an honest 404,
    // never to a slide the lookup cannot even find.
    const facts = await reader.artifact('active-now-2026-09-21-sprint-review');
    expect(facts?.revisions?.[0]?.slides.map((entry) => entry.id)).toEqual(['title-slide', 'sources']);
    expect((await reader.revisionPreview('active-now-2026-09-21-sprint-review', 'v1', 'title-slide'))?.body.toString('utf8'))
      .toBe(SVG);
  });

  it('serves a slide from the row\'s own markup when no blob file exists', async () => {
    // The live row: every slide carries inline `svg`, and the blob volume is
    // empty after a restart. The row is what survived, so the row answers.
    const inline = {
      ...seededRow,
      revisions: [{
        ...(seededRow.revisions as Row[])[0],
        slides: [
          { ...slide('title-slide', 1, 'Sprint review', 'deck-active-now-2026-09-21-sprint-review-v2-title-slide.svg'), svg: SVG },
        ],
      }],
      revisionSlides: {},
    };
    const reader = readerFor(inline, new InMemoryImageBlobStore());
    const preview = await reader.revisionPreview('active-now-2026-09-21-sprint-review', 'v1', 'title-slide');
    expect(preview?.contentType).toBe('image/svg+xml');
    expect(preview?.body.toString('utf8')).toBe(SVG);
  });

  it('uses the store\'s own slide lookup when it has one', async () => {
    // The production store reads the row first and the blob second; the reader
    // must defer to it rather than going straight to a blob key.
    const asked: string[] = [];
    const reader = new DynamoWorkActivityReader(
      { async send() { return {}; } } as unknown as DynamoDBDocumentClient,
      {
        presentations: () => ({
          get: async () => null,
          appendRevision: async () => { throw new Error('not used'); },
          slidePreview: async (_documentId: string, revisionId: string, slideId: string) => {
            asked.push(`${revisionId}/${slideId}`);
            return { contentType: 'image/svg+xml', body: Buffer.from(SVG, 'utf8'), from: 'inline' as const };
          },
        }),
        blobs: () => { throw new Error('the blob store must not be reached'); },
      },
    );
    const preview = await reader.revisionPreview('active-now-2026-09-21-sprint-review', 'v2', 'title-slide');
    expect(preview?.body.toString('utf8')).toBe(SVG);
    expect(asked).toEqual(['v2/title-slide']);
  });

  it('keeps the cover working when the slide bytes did not survive the restart', async () => {
    // Exactly the live defect: the row still names every slide asset, but the
    // blob volume is empty. The inline cover lives in the row, so it stays.
    const reader = readerFor(seededRow, new InMemoryImageBlobStore());
    expect(await reader.revisionPreview('active-now-2026-09-21-sprint-review', 'v1', 'title-slide')).toBeNull();
    expect((await reader.revisionPreview('active-now-2026-09-21-sprint-review', 'v1'))?.body.toString('utf8')).toBe(SVG);
  });

  it('finds a slide the revision record omits but `revisionSlides` still has', async () => {
    const blobs = new InMemoryImageBlobStore();
    await blobs.putBlob(
      'deck-active-now-2026-09-21-sprint-review-v1-sources.svg',
      Buffer.from(SVG, 'utf8'),
      { contentType: 'image/svg+xml' },
    );
    const partial = { ...seededRow, revisions: [{ ...(seededRow.revisions as Row[])[0], slides: [] }] };
    const reader = readerFor(partial, blobs);
    expect((await reader.artifact('active-now-2026-09-21-sprint-review'))?.revisions?.[0]?.slides.map((entry) => entry.id))
      .toEqual(['title-slide', 'sources']);
    expect((await reader.revisionPreview('active-now-2026-09-21-sprint-review', 'v1', 'sources'))?.body.toString('utf8'))
      .toBe(SVG);
  });
});

describe('which step a run is inside', () => {
  it('names the running step from the run\'s own step summaries, newest first', async () => {
    const run = {
      runId: 'run-9',
      status: 'running',
      startedAt: '2026-09-21T13:33:00.000Z',
      lastEventAt: '2026-09-21T13:42:00.000Z',
      triggeredBy: 'connor',
      pipelineId: 'deck-pipeline',
      stepsSummary: {
        sources: { status: 'completed', startedAt: '2026-09-21T13:33:00.000Z' },
        // A forked branch is filed as `deck:2`; the step it names is `deck`.
        'deck:2': { status: 'running', startedAt: '2026-09-21T13:40:00.000Z' },
      },
    };
    const client = {
      async send(command: unknown) {
        if (command instanceof ScanCommand) return { Items: [run] };
        return {};
      },
    } as unknown as DynamoDBDocumentClient;
    const facts = await new DynamoWorkActivityReader(client).pipelineRun('run-9');
    expect(facts?.runningStep).toEqual({ stepId: 'deck' });
    expect(facts?.status).toBe('running');
  });

  it('names no running step when every step the run recorded has finished', async () => {
    const client = {
      async send(command: unknown) {
        if (command instanceof ScanCommand) {
          return {
            Items: [{
              runId: 'run-9',
              status: 'completed',
              startedAt: '2026-09-21T13:33:00.000Z',
              stepsSummary: { deck: { status: 'completed', startedAt: '2026-09-21T13:40:00.000Z' } },
            }],
          };
        }
        return {};
      },
    } as unknown as DynamoDBDocumentClient;
    expect((await new DynamoWorkActivityReader(client).pipelineRun('run-9'))?.runningStep).toBeUndefined();
  });
});
