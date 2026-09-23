// What the reader makes of a presentation row: the slides each revision is
// actually made of, the documents that revision cited, and the bytes behind one
// slide. The row shape is the one `deck/presentationDocuments.ts` writes.

jest.mock('../../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { GetCommand, QueryCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

/** pipeline-runs' `runId-index` GSI over a mock's rows; undefined for any other command. */
function runIndexItems(command: unknown, rows: Array<Record<string, unknown>>): { Items: Array<Record<string, unknown>> } | undefined {
  if (!(command instanceof QueryCommand) || command.input.IndexName !== 'runId-index') return undefined;
  const runId = command.input.ExpressionAttributeValues?.[':r'];
  return { Items: rows.filter((row) => row.runId === runId) };
}
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
    expect(facts?.revisions?.[0]?.heading).toBeUndefined();

    // NFR #93: the revision's own heading reaches the facts, for the pane subtitle.
    const titled = await readerFor({
      ...deckRow,
      revisions: [{ ...(deckRow.revisions ?? [])[0], heading: 'Audit E2E' }],
    }).artifact('doc-deck');
    expect(titled?.revisions?.[0]?.heading).toBe('Audit E2E');

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

describe('an older revision whose slide list moved to a manifest (NFR #36)', () => {
  const MANIFEST = 'deck-slides-00000000000000000000000000000000000000aa.json';
  const moved: Row = {
    ...deckRow,
    revisionSlides: undefined,
    revisions: [
      { ...(deckRow.revisions ?? [])[0], slides: undefined, slidesRef: { key: MANIFEST, contentType: 'application/json', sizeBytes: 1, sha256: '00000000000000000000000000000000000000aa' } },
      { ...(deckRow.revisions ?? [])[0], id: 'v2', slides: [slide('cover', 1, 'Head cover', 'deck-v1-cover.svg')] },
    ],
  };
  function readerWithAssets(blobs: InMemoryImageBlobStore): DynamoWorkActivityReader {
    const client = { async send(command: unknown) { return command instanceof GetCommand ? { Item: moved } : command instanceof QueryCommand ? { Count: 0 } : {}; } } as unknown as DynamoDBDocumentClient;
    return new DynamoWorkActivityReader(client, {
      presentations: () => ({
        get: async () => moved as unknown as PresentationDocumentRow,
        appendRevision: async () => { throw new Error('not used'); },
        readAsset: async (asset) => (await blobs.getBlob(asset.key))?.body ?? null,
      }),
      blobs: () => blobs,
    });
  }

  it('names each revision’s own slides, reading the older list from its manifest, and serves its slide', async () => {
    const blobs = new InMemoryImageBlobStore();
    await blobs.putBlob(MANIFEST, Buffer.from(JSON.stringify(deckRow.revisions![0]!.slides)), { contentType: 'application/json' });
    await blobs.putBlob('deck-v1-risks.svg', Buffer.from(SVG), { contentType: 'image/svg+xml' });
    const reader = readerWithAssets(blobs);
    const facts = await reader.artifact('doc-deck');
    expect(facts?.revisions?.map((revision) => revision.slides.map((entry) => entry.title)))
      .toEqual([['Sprint review', 'Open risks'], ['Head cover']]);
    expect((await reader.revisionPreview('doc-deck', 'v1', 'risks'))?.body.toString('utf8')).toBe(SVG);
  });
});

describe('a deck with many revisions whose older lists moved off the row', () => {
  it('names all 25 revisions, the older 24 from their manifests, in order', async () => {
    const blobs = new InMemoryImageBlobStore();
    const revisions: Row[] = [];
    for (let index = 1; index <= 25; index += 1) {
      const key = `deck-slides-${String(index).padStart(40, '0')}.json`;
      const slides = [slide('cover', 1, `Cover r${index}`, `deck-v${index}-cover.svg`), slide('risks', 2, 'Open risks', `deck-v${index}-risks.svg`)];
      if (index < 25) {
        await blobs.putBlob(key, Buffer.from(JSON.stringify(slides)), { contentType: 'application/json' });
        revisions.push({ ...(deckRow.revisions ?? [])[0], id: `v${index}`, label: `v${index}`, slides: undefined, slidesRef: { key, contentType: 'application/json', sizeBytes: 1, sha256: key.slice(12, 52) } });
      } else {
        revisions.push({ ...(deckRow.revisions ?? [])[0], id: `v${index}`, label: `v${index}`, slides });
      }
    }
    const row: Row = { ...deckRow, revisionSlides: undefined, revisions };
    const client = { async send(command: unknown) { return command instanceof GetCommand ? { Item: row } : command instanceof QueryCommand ? { Count: 0 } : {}; } } as unknown as DynamoDBDocumentClient;
    const reader = new DynamoWorkActivityReader(client, {
      presentations: () => ({
        get: async () => row as unknown as PresentationDocumentRow,
        appendRevision: async () => { throw new Error('not used'); },
        readAsset: async (asset) => (await blobs.getBlob(asset.key))?.body ?? null,
      }),
      blobs: () => blobs,
    });
    const facts = await reader.artifact('doc-deck');
    expect(facts?.revisions?.map((revision) => revision.id)).toEqual(Array.from({ length: 25 }, (_, i) => `v${i + 1}`));
    expect(facts?.revisions?.every((revision) => revision.slides.length === 2)).toBe(true);
    expect(facts?.revisions?.[0]?.slides[0]?.title).toBe('Cover r1');
    expect(facts?.revisions?.[24]?.slides[0]?.title).toBe('Cover r25');
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

  it('serves a revision cover that names its slide (NFR #4 row diet) from that slide\'s blob', async () => {
    const blobs = new InMemoryImageBlobStore();
    await blobs.putBlob('deck-v1-risks.svg', Buffer.from(SVG, 'utf8'), { contentType: 'image/svg+xml' });
    const row = { ...deckRow, revisionPreviews: { v1: { contentType: 'image/svg+xml', slideId: 'risks' } } };
    const cover = await readerFor(row, blobs).revisionPreview('doc-deck', 'v1');
    expect(cover?.body.toString('utf8')).toBe(SVG);
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

describe('reading a day of runs', () => {
  const run = (runId: string, steps: Record<string, unknown>): Row => ({
    runId,
    pipelineId: 'deck-pipeline',
    status: 'running',
    startedAt: '2026-09-22T13:33:00.000Z',
    lastEventAt: '2026-09-22T13:42:00.000Z',
    triggeredBy: 'dev-frank',
    stepsSummary: steps,
  });

  function scanningReader(rows: Row[]): { reader: DynamoWorkActivityReader; scans: number; queries: number } {
    const counter = { scans: 0, queries: 0 };
    const client = {
      async send(command: unknown) {
        if (command instanceof ScanCommand) {
          counter.scans += 1;
          return { Items: rows };
        }
        const byRunId = runIndexItems(command, rows);
        if (byRunId) { counter.queries += 1; return byRunId; }
        return {};
      },
    } as unknown as DynamoDBDocumentClient;
    return { reader: new DynamoWorkActivityReader(client), get scans() { return counter.scans; }, get queries() { return counter.queries; } };
  }

  it('reads every run and every step of them with one indexed read per run, never a scan', async () => {
    // The shape that made frank's graph crawl: one run with many step nodes,
    // each of which used to scan `pipeline-runs` for the very same row.
    const rows = [
      run('run-9', { sources: { status: 'completed', startedAt: '2026-09-22T13:33:00.000Z' }, deck: { status: 'running', startedAt: '2026-09-22T13:40:00.000Z' } }),
      run('run-12', { deck: { status: 'completed', startedAt: '2026-09-22T13:20:00.000Z' } }),
    ];
    const harness = scanningReader(rows);
    const resources = ['run-9', 'run-9::sources', 'run-9::deck', 'run-12', 'run-12::deck', 'run-9'];

    const facts = await harness.reader.pipelineRuns(resources);
    expect(harness.scans).toBe(0);
    expect(harness.queries).toBe(2);
    expect([...facts.keys()].sort()).toEqual(['run-12', 'run-12::deck', 'run-9', 'run-9::deck', 'run-9::sources']);
    expect(facts.get('run-9')?.runningStep).toEqual({ stepId: 'deck' });
    expect(facts.get('run-9::deck')?.step?.stepId).toBe('deck');
    expect(facts.get('run-12')?.status).toBe('running');
  });

  it('discloses nothing for a run id two rows both claim, or one no row has', async () => {
    const harness = scanningReader([run('run-9', {}), run('run-9', {})]);
    const facts = await harness.reader.pipelineRuns(['run-9', 'run-404']);
    expect(facts.size).toBe(0);
  });

  it('asks for nothing when the graph names no runs', async () => {
    const harness = scanningReader([]);
    expect((await harness.reader.pipelineRuns([])).size).toBe(0);
    expect(harness.scans).toBe(0);
    expect(harness.queries).toBe(0);
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
        return runIndexItems(command, [run]) ?? {};
      },
    } as unknown as DynamoDBDocumentClient;
    const facts = await new DynamoWorkActivityReader(client).pipelineRun('run-9');
    expect(facts?.runningStep).toEqual({ stepId: 'deck' });
    expect(facts?.status).toBe('running');
  });

  it('names no running step when every step the run recorded has finished', async () => {
    const client = {
      async send(command: unknown) {
        const items = [{
          runId: 'run-9',
          status: 'completed',
          startedAt: '2026-09-21T13:33:00.000Z',
          stepsSummary: { deck: { status: 'completed', startedAt: '2026-09-21T13:40:00.000Z' } },
        }];
        if (command instanceof ScanCommand) return { Items: items };
        return runIndexItems(command, items) ?? {};
      },
    } as unknown as DynamoDBDocumentClient;
    expect((await new DynamoWorkActivityReader(client).pipelineRun('run-9'))?.runningStep).toBeUndefined();
  });
});

describe('runs past the first page of the scan (a table without the runId index)', () => {
  it('pages the run scan, so a run in the second 1 MB page still has facts', async () => {
    // DynamoDB stops a Scan page at 1 MB READ, before the filter; with every
    // run row carrying its checkpoint, 114 of 122 rows fit in the first page.
    const later = { runId: 'run-late', pipelineId: 'deck-revise', status: 'completed', startedAt: '2026-09-23T02:59:16.000Z', triggeredBy: 'dev-frank', stepsSummary: {} };
    let calls = 0;
    const client = {
      async send(command: unknown) {
        if (command instanceof QueryCommand && command.input.IndexName === 'runId-index') {
          const err = new Error('The table does not have the specified index: runId-index');
          err.name = 'ValidationException';
          throw err;
        }
        if (!(command instanceof ScanCommand)) return {};
        calls += 1;
        return (command.input as { ExclusiveStartKey?: unknown }).ExclusiveStartKey
          ? { Items: [later], ScannedCount: 8 }
          : { Items: [], ScannedCount: 114, LastEvaluatedKey: { pipelineId: 'x', runId: 'y' } };
      },
    } as unknown as DynamoDBDocumentClient;
    const reader = new DynamoWorkActivityReader(client);
    expect((await reader.pipelineRuns(['run-late'])).get('run-late')?.status).toBe('completed');
    expect(calls).toBe(2);
    calls = 0;
    expect((await reader.pipelineRun('run-late'))?.status).toBe('completed');
    expect(calls).toBe(2);
  });
});

describe('what a composer edit did, from its own step outputs', () => {
  const spec = { slides: [{ id: 'cover', title: 'audit-deck-2355' }, { id: 'transcript', title: 'Transcript' }] };
  const trigger = { status: 'completed', output: { instruction: 'Make the title name what the transcript decided', documentId: 'deck-1', slideId: 'transcript', target: { field: 'title' }, baseRevisionId: 'v6' } };
  const load = { status: 'completed', startedAt: '2026-09-23T02:59:17.000Z', output: { spec, baseRevisionId: 'v6', specFrom: 'head', title: 'audit-deck-2355' } };
  const propose = { status: 'completed', startedAt: '2026-09-23T02:59:18.000Z', output: { summary: 'Changed title to reflect the key decision.', changedSlideIds: ['transcript'] } };
  const write = { status: 'completed', startedAt: '2026-09-23T02:59:30.000Z', output: { written: true, revisionId: 'v7', baseRevisionId: 'v6', summary: 'Changed title to reflect the key decision.', changedSlideIds: ['transcript'] } };

  function readerOf(row: Row): DynamoWorkActivityReader {
    const client = {
      async send(command: unknown) { return command instanceof ScanCommand ? { Items: [row] } : runIndexItems(command, [row]) ?? {}; },
    } as unknown as DynamoDBDocumentClient;
    return new DynamoWorkActivityReader(client);
  }

  it('names the instruction, the scope as a reader sees it, and the revision the write saved', async () => {
    const row = { runId: 'ee808930', pipelineId: 'deck-revise', status: 'completed', startedAt: '2026-09-23T02:59:16.000Z', triggeredBy: 'dev-frank', stepsSummary: { trigger, load, propose, write } };
    const facts = await readerOf(row).pipelineRuns(['ee808930', 'ee808930::write']);
    expect(facts.get('ee808930')?.edit).toEqual({
      instruction: 'Make the title name what the transcript decided',
      scope: 'Slide 2 “Transcript” · title',
      documentId: 'deck-1',
      title: 'audit-deck-2355',
      baseRevisionId: 'v6',
      specFrom: 'head',
      summary: 'Changed title to reflect the key decision.',
      changedSlides: 1,
      revisionId: 'v7',
    });
    expect(facts.get('ee808930::write')?.edit?.revisionId).toBe('v7');
  });

  it('carries no revision for an edit whose write never ran, and nothing for a run that is not an edit', async () => {
    const failed = { runId: 'c1caab64', pipelineId: 'deck-revise', status: 'failed', startedAt: '2026-09-23T02:58:27.000Z', triggeredBy: 'dev-frank', error: 'load: deck-revise revision-conflict: v6 was saved after the one this edit started from, so nothing was changed.', stepsSummary: { trigger: { status: 'completed', output: { instruction: 'Shorter title', documentId: 'deck-1' } }, load: { status: 'failed', startedAt: '2026-09-23T02:58:28.000Z' } } };
    const facts = await readerOf(failed).pipelineRuns(['c1caab64']);
    expect(facts.get('c1caab64')?.edit).toEqual({ instruction: 'Shorter title', scope: 'Whole deck', documentId: 'deck-1' });
    const other = { runId: 'r2', pipelineId: 'x', status: 'completed', startedAt: '2026-09-23T02:58:27.000Z', triggeredBy: 'dev-frank', stepsSummary: { trigger: { status: 'completed', output: { prompt: 'hi' } } } };
    expect((await readerOf(other).pipelineRuns(['r2'])).get('r2')?.edit).toBeUndefined();
  });
});

describe('readableError — a stored failure as the pane shows it', () => {
  it('drops a leading run id, however many times it was prefixed, and leaves the rest', () => {
    const { readableError } = jest.requireActual('../activityReader') as typeof import('../activityReader');
    expect(readableError('a10af720-facc-4643-9dae-d4046a5b8509: claude exited 1: Error: Input must be provided'))
      .toBe('claude exited 1: Error: Input must be provided');
    expect(readableError('A10AF720-FACC-4643-9DAE-D4046A5B8509:a76da4dd-f53d-45fa-bc80-831f46a17ac1 : boom')).toBe('boom');
    expect(readableError('Deck not found: 6cdfcbaf-95f6-4205-933a-b0a98f497ea1')).toBe('Deck not found: 6cdfcbaf-95f6-4205-933a-b0a98f497ea1');
    expect(readableError('a10af720-facc-4643-9dae-d4046a5b8509: ')).toBeUndefined();
    expect(readableError(undefined)).toBeUndefined();
  });
});

describe('why a paused run waits (NFR #126)', () => {
  const read = async (row: Record<string, unknown>) => {
    const items = [{ runId: 'run-9', startedAt: '2026-09-21T13:33:00.000Z', triggeredBy: 'connor', pipelineId: 'deck-revise', stepsSummary: {}, ...row }];
    const client = {
      async send(command: unknown) {
        if (command instanceof ScanCommand) return { Items: items };
        return runIndexItems(command, items) ?? {};
      },
    } as unknown as DynamoDBDocumentClient;
    return new DynamoWorkActivityReader(client).pipelineRun('run-9');
  };

  it('carries the reason and the step from the row while the row is paused', async () => {
    const facts = await read({ status: 'paused_at_breakpoint', pause: { reason: 'paused_at_breakpoint', stepId: 'write' } });
    expect(facts?.status).toBe('waiting');
    expect(facts?.pause).toEqual({ reason: 'paused_at_breakpoint', step: 'write' });
    expect((await read({ status: 'awaiting_approval' }))?.pause).toEqual({ reason: 'awaiting_approval' });
  });

  it('says nothing once the row runs or finishes, and drops a reason the status contradicts', async () => {
    expect((await read({ status: 'running', pause: { reason: 'paused_at_breakpoint', stepId: 'write' } }))?.pause).toBeUndefined();
    expect((await read({ status: 'completed', pause: { reason: 'awaiting_approval', stepId: 'approve' } }))?.pause).toBeUndefined();
    expect((await read({ status: 'awaiting_approval', pause: { reason: 'paused_at_breakpoint', stepId: 'write' } }))?.pause).toEqual({ reason: 'awaiting_approval' });
  });
});
