// The four steps, end to end over a fake blob store and repository. What is
// pinned: thumbnails leave the context and land in the store under a
// content-keyed id; bursts are grouped before classification; a missing
// vision client skips scenes instead of failing the run; the catalog rows
// carry burst, scene and representative, and a rerun writes the same rows.

const stored = new Map<string, { body: Buffer; meta: Record<string, unknown> }>();
const upserted: Record<string, unknown>[][] = [];
let visionClient: { analyze: jest.Mock } | null = null;

jest.mock('../../repositories', () => ({
  mediaCatalogRepo: { upsertMany: jest.fn(async (rows: Record<string, unknown>[]) => { upserted.push(rows); return rows.length; }) },
}));
jest.mock('../../pipeline/image/visionServices', () => ({
  getImageBlobStore: () => ({
    put: jest.fn(async (id: string, body: Buffer, meta: Record<string, unknown>) => { stored.set(id, { body, meta }); return id; }),
    get: jest.fn(async (id: string) => stored.get(id) ?? null),
  }),
  getVisionClient: () => { if (!visionClient) throw new Error('ANTHROPIC_API_KEY is not set'); return visionClient; },
}));
jest.mock('../../pipeline/bridge-types', () => ({ getPipelineBridge: () => null }));
jest.mock('../../pipeline/runPersistence', () => ({ getRunPersistenceService: () => null }));

import { buildMediaCatalogHandlers, buildMediaCatalogDefinition, KINDS, sceneFromOutput, thumbnailImageId, DEFAULT_SCENE_LABELS, type MediaProbe } from '../mediaCatalogPipeline';
import { lintSystemPipeline } from '../../pipeline/systemPipelineLint';
import type { PipelineNode } from 'distributed-core/applications/pipeline';

const handlers = buildMediaCatalogHandlers();
const node = (data: Record<string, unknown> = {}) => ({ id: 'n', type: 'n', position: { x: 0, y: 0 }, data }) as unknown as PipelineNode;
const helpers = { runId: 'run-7' } as never;
const run = (kind: string, ctx: Record<string, unknown>, data?: Record<string, unknown>) => handlers.get(kind)!.execute(node(data), ctx, helpers);

const ID = (n: number) => n.toString(16).padStart(64, '0');
const at = (sec: number) => new Date(Date.UTC(2026, 8, 14, 12, 0, sec)).toISOString();
const PNG = Buffer.from('89504e470d0a1a0a', 'hex');
const probe = (n: number, sec: number, over: Partial<MediaProbe> = {}): MediaProbe => ({
  path: `/Volumes/nvme/trip/IMG_${n}.jpg`, relPath: `trip/IMG_${n}.jpg`, fileId: ID(n), sizeBytes: 1000 + n, mimeType: 'image/jpeg', kind: 'image',
  width: 6000, height: 4000, capturedAt: at(sec), modifiedAt: at(sec), cameraId: 'Sony ILCE-7M4', dhash: 'ffff0000ffff0000',
  thumbnail: { base64: PNG.toString('base64'), mimeType: 'image/jpeg', width: 512, height: 341 }, ...over,
});

/** Run the pipeline the way the executor would: each step's output merges into the context. */
async function runAll(files: MediaProbe[], classifyData?: Record<string, unknown>) {
  let ctx: Record<string, unknown> = { batch: { batchId: 'b1', root: '/Volumes/nvme', files }, orgId: 'org-a' };
  for (const [kind, data] of [[KINDS.ingest], [KINDS.burstGroup], [KINDS.classify, classifyData], [KINDS.upsert]] as Array<[string, Record<string, unknown> | undefined]>) {
    const out = await run(kind, ctx, data);
    if (out.status !== 'completed') throw new Error(`${kind}: ${(out as { error?: string }).error}`);
    ctx = { ...ctx, ...(out as { output: Record<string, unknown> }).output };
  }
  return ctx;
}

beforeEach(() => { stored.clear(); upserted.length = 0; visionClient = null; });

describe('the definition', () => {
  it('passes the system-pipeline lint and makes classification optional', () => {
    const def = buildMediaCatalogDefinition();
    expect(lintSystemPipeline(def)).toEqual([]);
    const classify = (def.nodes as Array<{ id: string; data: { optional?: boolean } }>).find((n) => n.id === 'classify');
    expect(classify?.data.optional).toBe(true);
  });
});

describe('media:upsert', () => {
  it('refuses to write rows that belong to no organisation', async () => {
    const out = await run(KINDS.upsert, { ingested: [], bursts: [], root: '/v' });
    expect(out.status).toBe('failed');
    expect((out as { error?: string }).error).toMatch(/orgId/);
  });
});

describe('media:ingest', () => {
  it('stores each thumbnail under a content-keyed id and strips it from the context', async () => {
    const out = await run(KINDS.ingest, { batch: { batchId: 'b1', root: '/v', files: [probe(1, 0), probe(2, 1, { thumbnail: null, kind: 'video', mimeType: 'video/mp4' })] } });
    expect(out.status).toBe('completed');
    const o = (out as { output: { ingested: Array<Record<string, unknown>>; thumbnailsStored: number } }).output;
    expect(o.thumbnailsStored).toBe(1);
    expect(o.ingested[0].imageId).toBe(thumbnailImageId(ID(1)));
    expect(o.ingested[0]).not.toHaveProperty('thumbnail');
    expect(o.ingested[1].imageId).toBeNull();
    expect(stored.get(thumbnailImageId(ID(1)))?.meta.uploadedBy).toBe('media-walker');
    // The posted batch (thumbnails and all) does not ride the context past this step.
    expect((o as unknown as { batch: Record<string, unknown> }).batch).toEqual({ batchId: 'b1', root: '/v', fileCount: 2 });
  });

  it('refuses a context with no batch', async () => {
    expect((await run(KINDS.ingest, {})).status).toBe('failed');
  });
});

describe('the whole run', () => {
  it('without a vision client: bursts and rows, no scenes, and the run completes', async () => {
    const ctx = await runAll([probe(1, 0), probe(2, 1), probe(3, 60)]);
    expect(ctx.skipped).toMatch(/no vision client/);
    expect(ctx.burstCount).toBe(1);
    const rows = upserted[0] as Array<Record<string, unknown>>;
    expect(rows).toHaveLength(3);
    const byId = new Map(rows.map((r) => [r.contentId, r]));
    expect(byId.get(ID(1))?.burstSize).toBe(2);
    expect(byId.get(ID(2))?.burstId).toBe(byId.get(ID(1))?.burstId);
    expect(byId.get(ID(3))?.burstSize).toBe(1);
    expect(rows.every((r) => r.scene === null)).toBe(true);
    expect(rows.every((r) => r.runId === 'run-7' && r.bucket === 'org-a' && r.orgId === 'org-a')).toBe(true);
    // Stored under the org: the same photo in another org is another row.
    expect(rows.every((r) => r.fileId === `org-a#${r.contentId}`)).toBe(true);
    // The middle frame of a 2-frame burst is the first; exactly one representative per burst.
    expect(rows.filter((r) => r.isRepresentative).map((r) => r.contentId).sort()).toEqual([ID(1), ID(3)]);
  });

  it('with a vision client: one call per burst, the verdict applied to every frame of it', async () => {
    visionClient = { analyze: jest.fn(async () => ({ output: { label: 'landscape', confidence: 0.91, caption: 'A ridge at dusk.' }, rawText: '', usage: {}, latencyMs: 1, cacheHit: false })) };
    const ctx = await runAll([probe(1, 0), probe(2, 1), probe(3, 60)]);
    expect(visionClient.analyze).toHaveBeenCalledTimes(2);
    expect(ctx.classified).toBe(2);
    const rows = upserted[0] as Array<Record<string, unknown>>;
    expect(rows.map((r) => r.scene)).toEqual(['landscape', 'landscape', 'landscape']);
    expect(rows[0].sceneConfidence).toBe(0.91);
    const call = visionClient.analyze.mock.calls[0][0] as { userPrompt: string; imageBytes: Buffer };
    expect(call.userPrompt).toContain(DEFAULT_SCENE_LABELS[0]);
    expect(Buffer.compare(call.imageBytes, PNG)).toBe(0);
  });

  it('a frame the model fails on leaves its burst without a scene; the rest are written', async () => {
    let n = 0;
    visionClient = { analyze: jest.fn(async () => { if (n++ === 0) throw new Error('overloaded'); return { output: { label: 'food', confidence: 0.6, caption: null }, rawText: '', usage: {}, latencyMs: 1, cacheHit: false }; }) };
    const ctx = await runAll([probe(1, 0), probe(3, 60)]);
    expect(ctx.failed).toBe(1);
    const rows = upserted[0] as Array<Record<string, unknown>>;
    expect(rows.map((r) => r.scene)).toEqual([null, 'food']);
  });

  it('rerunning the same batch writes the same rows (content-keyed, stable burst ids)', async () => {
    await runAll([probe(1, 0), probe(2, 1)]);
    await runAll([probe(1, 0), probe(2, 1)]);
    const strip = (rows: Record<string, unknown>[]) => rows.map(({ catalogedAt: _c, ...r }) => r);
    expect(strip(upserted[1])).toEqual(strip(upserted[0]));
  });
});

describe('sceneFromOutput', () => {
  const labels = ['landscape', 'food', 'other'];
  it('reads a structured verdict, or finds the label in the raw text, or gives up', () => {
    expect(sceneFromOutput({ label: 'food', confidence: 1.4 }, '', labels)).toEqual({ label: 'food', confidence: 1, caption: null });
    expect(sceneFromOutput(null, 'This looks like a LANDSCAPE at dusk.', labels)).toEqual({ label: 'landscape', confidence: null, caption: null });
    expect(sceneFromOutput({ label: 'castle' }, 'a castle', labels)).toBeNull();
  });
});
