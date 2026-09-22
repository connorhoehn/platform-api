// The exact row a rendered deck writes into `crdt-documents`, and the blob keys
// behind it. This is the contract the Documents page, the work-graph projection
// and the document routes all read, so it is asserted field by field.

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { GetCommand, PutCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import {
  DdbPresentationDocumentStore,
  blobKeySegment,
  deckPreviewHandle,
  nextRevisionId,
  parseDeckPreviewHandle,
  pptxBlobKey,
  slideBlobKey,
  type PresentationDocumentRow,
} from '../presentationDocuments';
import { PPTX_CONTENT_TYPE } from '../renderPptx';
import { sampleDeck } from './fixtures';

interface Sent { command: unknown; input: Record<string, unknown> }

function fakeClient(rowFor: (documentId: string) => PresentationDocumentRow | null): { client: DynamoDBDocumentClient; sent: Sent[] } {
  const sent: Sent[] = [];
  const client = {
    async send(command: unknown) {
      const input = (command as { input: Record<string, unknown> }).input;
      sent.push({ command, input });
      if (command instanceof GetCommand) {
        const key = input.Key as { documentId: string };
        return { Item: rowFor(key.documentId) ?? undefined };
      }
      return {};
    },
  } as unknown as DynamoDBDocumentClient;
  return { client, sent };
}

const slides = [
  { id: 'cover', title: 'Cover', svg: '<svg>cover</svg>' },
  { id: 'sources', title: 'Sources', svg: '<svg>sources</svg>' },
];

describe('appendRevision — the first revision', () => {
  it('writes a presentation row the Documents page and the work graph can both read', async () => {
    const blobStore = new InMemoryImageBlobStore();
    const { client, sent } = fakeClient(() => null);
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore, now: () => new Date('2026-09-21T10:00:00.000Z') });

    const { revision } = await store.appendRevision({
      documentId: 'doc-1', title: 'Q4 platform review', createdBy: 'user-frank',
      pptx: Buffer.from('PKdeck'), slides,
    });

    const put = sent.find((entry) => entry.command instanceof PutCommand)!;
    expect(put.input.TableName).toBe('test-crdt-documents');
    const item = put.input.Item as Record<string, unknown>;
    expect(item).toMatchObject({
      documentId: 'doc-1',
      title: 'Q4 platform review',
      type: 'presentation',
      status: 'draft',
      createdBy: 'user-frank',
      createdAt: '2026-09-21T10:00:00.000Z',
      updatedAt: '2026-09-21T10:00:00.000Z',
      pageCount: 2,
    });
    // `outline` is what the work-graph activity reader turns into slide anchors.
    expect(item.outline).toEqual([{ id: 'cover', title: 'Cover' }, { id: 'sources', title: 'Sources' }]);
    // `revisionPreviews[revisionId]` is what the existing preview route serves:
    // one image per revision, which for a deck is its cover slide.
    expect(item.revisionPreviews).toEqual({ v1: { contentType: 'image/svg+xml', body: '<svg>cover</svg>', encoding: 'utf8' } });
    expect((item.revisionSlides as Record<string, unknown[]>).v1).toHaveLength(2);

    expect(revision).toMatchObject({ id: 'v1', label: 'v1', slideCount: 2, createdBy: 'user-frank' });
    expect(revision.asset).toEqual({ key: 'deck-doc-1-v1.pptx', contentType: PPTX_CONTENT_TYPE, sizeBytes: 6 });
    expect(revision.slides[0]).toMatchObject({
      id: 'cover', index: 1, title: 'Cover', previewHandle: 'doc-1.v1.cover',
      asset: { key: 'deck-doc-1-v1-cover.svg', contentType: 'image/svg+xml' },
    });

    // The bytes really are in the store, under the keys the row points at.
    expect((await blobStore.getBlob('deck-doc-1-v1.pptx'))!.body.toString('utf8')).toBe('PKdeck');
    expect((await blobStore.getBlob('deck-doc-1-v1-cover.svg'))!.body.toString('utf8')).toBe('<svg>cover</svg>');
  });
});

describe('appendRevision — a second revision', () => {
  it('appends rather than replacing, and keeps every revision’s preview', async () => {
    const blobStore = new InMemoryImageBlobStore();
    const existing: PresentationDocumentRow = {
      documentId: 'doc-1',
      title: 'Q4 platform review',
      type: 'presentation',
      revisions: [{
        id: 'v1', label: 'v1', createdAt: '2026-09-20T10:00:00.000Z', createdBy: 'user-frank', slideCount: 2,
        asset: { key: 'deck-doc-1-v1.pptx', contentType: PPTX_CONTENT_TYPE, sizeBytes: 6 },
        slides: [],
      }],
      revisionPreviews: { v1: { contentType: 'image/svg+xml', body: '<svg>old</svg>', encoding: 'utf8' } },
      revisionSlides: { v1: [] },
    };
    const { client, sent } = fakeClient(() => existing);
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore, now: () => new Date('2026-09-21T10:00:00.000Z') });

    const { revision } = await store.appendRevision({
      documentId: 'doc-1', title: 'Q4 platform review', createdBy: 'user-frank',
      pptx: Buffer.from('PKdeck2'), slides,
    });

    expect(revision.id).toBe('v2');
    const update = sent.find((entry) => entry.command instanceof UpdateCommand)!;
    const values = update.input.ExpressionAttributeValues as Record<string, unknown>;
    expect((values[':revisions'] as unknown[]).map((entry) => (entry as { id: string }).id)).toEqual(['v1', 'v2']);
    expect(Object.keys(values[':revisionPreviews'] as object)).toEqual(['v1', 'v2']);
    expect((values[':revisionPreviews'] as Record<string, { body: string }>).v1.body).toBe('<svg>old</svg>');
    expect(Object.keys(values[':revisionSlides'] as object)).toEqual(['v1', 'v2']);
    expect(values[':pageCount']).toBe(2);
    expect(values[':type']).toBe('presentation');
  });
});

describe('a revision names what it stands on', () => {
  it('derives sources from the spec’s citations, and calls a transcript one only when it was declared', async () => {
    const blobStore = new InMemoryImageBlobStore();
    const { client } = fakeClient(() => null);
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore, now: () => new Date('2026-09-21T10:00:00.000Z') });
    const spec = sampleDeck();
    // Spelled like any other document id — the seeded transcript is.
    spec.slides[1]!.sources = [{ documentId: 'active-now-2026-09-21-planning-transcript', title: 'Planning sync' }];

    const { revision } = await store.appendRevision({
      documentId: 'doc-1', title: 'Q4 platform review', createdBy: 'user-frank',
      pptx: Buffer.from('PKdeck'), slides, spec,
      transcriptIds: ['active-now-2026-09-21-planning-transcript'],
    });

    expect(revision.spec).toEqual(spec);
    expect(revision.sources).toEqual([
      { documentId: 'doc-sprint-review', title: 'Sprint review', kind: 'document' },
      { documentId: 'active-now-2026-09-21-planning-transcript', title: 'Planning sync', kind: 'transcript' },
      { documentId: 'doc-roadmap', title: 'Q4 roadmap', kind: 'document', anchor: 'h2-q4' },
    ]);
  });

  it('calls an undeclared source a document, however it is spelled', async () => {
    const blobStore = new InMemoryImageBlobStore();
    const { client } = fakeClient(() => null);
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore, now: () => new Date('2026-09-21T10:00:00.000Z') });
    const spec = sampleDeck();
    spec.slides[0]!.sources = [{ documentId: 'transcript-of-a-meeting-that-is-a-document', title: 'Notes' }];
    const { revision } = await store.appendRevision({
      documentId: 'doc-1', title: 'T', createdBy: 'u', pptx: Buffer.from('PK'), slides, spec,
    });
    expect(revision.sources!.find((source) => source.documentId.startsWith('transcript'))!.kind).toBe('document');
  });

  it('takes sources the caller declares over the spec’s own', async () => {
    const blobStore = new InMemoryImageBlobStore();
    const { client } = fakeClient(() => null);
    const store = new DdbPresentationDocumentStore({ docClient: client, blobStore, now: () => new Date('2026-09-21T10:00:00.000Z') });
    const { revision } = await store.appendRevision({
      documentId: 'doc-1', title: 'T', createdBy: 'u', pptx: Buffer.from('PK'), slides,
      spec: sampleDeck(),
      sources: [{ documentId: 'doc-x', title: 'X', kind: 'document' }],
    });
    expect(revision.sources).toEqual([{ documentId: 'doc-x', title: 'X', kind: 'document' }]);
  });
});

describe('handles and keys', () => {
  it('round-trips a handle whose documentId contains dots', () => {
    const handle = deckPreviewHandle('doc.with.dots', 'v2', 'cover');
    expect(handle).toBe('doc.with.dots.v2.cover');
    expect(parseDeckPreviewHandle(handle)).toEqual({ documentId: 'doc.with.dots', revisionId: 'v2', slideId: 'cover' });
  });

  it('refuses a handle that names fewer than three things', () => {
    expect(parseDeckPreviewHandle('doc.v1')).toBeNull();
    expect(parseDeckPreviewHandle('')).toBeNull();
  });

  it('keeps blob keys to characters a filesystem store accepts', () => {
    expect(blobKeySegment('doc/../etc/passwd')).toBe('doc----etc-passwd');
    expect(pptxBlobKey('doc/1', 'v1')).toBe('deck-doc-1-v1.pptx');
    expect(slideBlobKey('doc 1', 'v2', 'cover')).toBe('deck-doc-1-v2-cover.svg');
  });

  it('numbers the next revision from what the document already has', () => {
    expect(nextRevisionId(null)).toBe('v1');
    expect(nextRevisionId({ documentId: 'd', revisions: [] })).toBe('v1');
    expect(nextRevisionId({ documentId: 'd', revisions: [{ id: 'v1' } as never, { id: 'v2' } as never] })).toBe('v3');
  });
});
