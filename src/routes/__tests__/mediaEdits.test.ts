const signals: Array<{ channel: string; event: string; payload: Record<string, unknown> }> = [];
jest.mock('../../lib/event-catalog', () => ({
  publishSignal: async (channel: string, event: string, payload: Record<string, unknown>) => { signals.push({ channel, event, payload }); },
}));
const catalog = new Map<string, { fileId: string; imageId: string | null; width: number; height: number }>();
jest.mock('../../repositories', () => ({ mediaCatalogRepo: { getFile: async (id: string) => catalog.get(id) ?? null } }));
const metas = new Map<string, { uploadedBy?: string }>();
const blobs = new Map<string, Buffer>();
jest.mock('../../pipeline/image/visionServices', () => ({
  getImageBlobStore: () => ({
    getMetadata: async (id: string) => metas.get(id) ?? (blobs.has(id) ? {} : null),
    put: async (id: string, body: Buffer) => { blobs.set(id, body); return id; },
    get: async () => null,
  }),
}));
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));

import express from 'express';
import sharp from 'sharp';
import { createMediaEditsRouter, validateOps } from '../mediaEdits';
import { MediaEditRepository } from '../../repositories/MediaEditRepository';
import { fileIdOfPhotoChannel, photoChannelOf } from '../../media/photoAccess';
import { errorHandler } from '../../middleware/error-handler';

const MINE = '1'.repeat(64);
const THEIRS = '2'.repeat(64);

/** The repository over an in-memory item map (same conditions as the repository test's fake). */
function memoryRepo(): MediaEditRepository {
  const repo = Object.create(MediaEditRepository.prototype) as MediaEditRepository;
  const heads = new Map<string, any>();
  const versions = new Map<string, any>();
  const sugs = new Map<string, number>();
  Object.assign(repo, {
    getHead: async (f: string) => ({ fileId: f, currentVersion: 0, editCount: 0, pick: null, masks: [], updatedAt: '', ...heads.get(f) }),
    getHeads: async (ids: string[]) => ids.map((f) => ({ fileId: f, currentVersion: heads.get(f)?.currentVersion ?? 0, masks: [] })),
    getVersion: async (f: string, v: number) => versions.get(`${f}|${v}`) ?? null,
    listVersions: async (f: string) => ({ versions: [...versions.values()].filter((v) => v.fileId === f).sort((a, b) => b.version - a.version), next: null }),
    setMasks: async (f: string, masks: unknown[]) => { heads.set(f, { ...heads.get(f), masks }); },
    appendVersion: async (input: any) => {
      const s = sugs.get(`${input.fileId}|${input.suggestionId}`);
      if (s) return { version: versions.get(`${input.fileId}|${s}`), replay: true };
      const cur = heads.get(input.fileId)?.currentVersion ?? 0;
      if (input.parentVersion !== undefined && input.parentVersion !== cur) {
        const { PhotoEditConflict } = jest.requireActual('../../repositories/MediaEditRepository');
        throw new PhotoEditConflict(input.fileId, cur);
      }
      const v = { fileId: input.fileId, version: cur + 1, parentVersion: cur || null, ops: input.ops, summary: input.summary, source: input.source, suggestionId: input.suggestionId, restoredFrom: input.restoredFrom, createdBy: input.userId, createdAt: 'now' };
      versions.set(`${input.fileId}|${v.version}`, v);
      heads.set(input.fileId, { ...heads.get(input.fileId), currentVersion: v.version });
      if (input.suggestionId) sugs.set(`${input.fileId}|${input.suggestionId}`, v.version);
      return { version: v, replay: false };
    },
  });
  return repo;
}

async function withServer(fn: (call: (method: string, path: string, body?: unknown) => Promise<{ status: number; json: any }>) => Promise<void>) {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => { (req as any).user = { sub: 'dev-connor' }; next(); });
  app.use('/edits', createMediaEditsRouter(memoryRepo()));
  app.use(errorHandler);
  const server = app.listen(0);
  const port = (server.address() as { port: number }).port;
  try {
    await fn(async (method, path, body) => {
      const res = await fetch(`http://127.0.0.1:${port}${path}`, {
        method, headers: { 'content-type': 'application/json', authorization: 'Bearer t' }, ...(body ? { body: JSON.stringify(body) } : {}),
      });
      return { status: res.status, json: await res.json().catch(() => null) };
    });
  } finally {
    server.close();
  }
}

beforeEach(() => {
  signals.length = 0;
  catalog.clear(); metas.clear(); blobs.clear();
  catalog.set(MINE, { fileId: MINE, imageId: 'img-mine', width: 600, height: 400 });
  catalog.set(THEIRS, { fileId: THEIRS, imageId: 'img-theirs', width: 600, height: 400 });
  metas.set('img-mine', { uploadedBy: 'dev-connor' });
  metas.set('img-theirs', { uploadedBy: 'someone-else' });
});

describe('media edits routes', () => {
  it('a photo channel fits the gateway 50-char cap and round-trips', () => {
    const ch = photoChannelOf(MINE);
    expect(ch.length).toBeLessThanOrEqual(50);
    expect(fileIdOfPhotoChannel(ch)).toBe(MINE);
    expect(fileIdOfPhotoChannel('photo:nope')).toBeNull();
  });

  it('validates ops against the adjust-layer kinds', () => {
    expect(validateOps([{ kind: 'exposure', stops: 0.2 }, { kind: 'curve', channel: 'rgb', points: [[0, 0], [255, 255]] }])).toHaveLength(2);
    expect(() => validateOps([{ kind: 'posterize', value: 1 }])).toThrow(/kind/);
    expect(() => validateOps([{ kind: 'exposure', stops: 'x' }])).toThrow(/finite/);
  });

  it('Keep → 201 + one signal; replay → 200 same version, no signal; stale parent → 409', async () => {
    await withServer(async (call) => {
      const body = { ops: [{ kind: 'shadows', amount: 18 }], parentVersion: 0, suggestionId: 'sug-1', summary: 'Lift shadows', source: 'assistant' };
      const first = await call('POST', `/edits/${MINE}/versions`, body);
      expect(first.status).toBe(201);
      expect(first.json.version).toMatchObject({ version: 1, source: 'assistant' });
      expect(signals).toEqual([expect.objectContaining({ channel: photoChannelOf(MINE), event: 'photo:edit_kept', payload: expect.objectContaining({ version: 1, suggestionId: 'sug-1' }) })]);
      const again = await call('POST', `/edits/${MINE}/versions`, body);
      expect(again).toMatchObject({ status: 200, json: { replay: true, version: { version: 1 } } });
      expect(signals).toHaveLength(1);
      const stale = await call('POST', `/edits/${MINE}/versions`, { ...body, suggestionId: 'sug-2' });
      expect(stale).toMatchObject({ status: 409, json: { currentVersion: 1 } });
      const restore = await call('POST', `/edits/${MINE}/restore`, { version: 0, parentVersion: 1 });
      expect(restore.json.version).toMatchObject({ version: 2, ops: [], source: 'restore', restoredFrom: 0 });
      const got = await call('GET', `/edits/${MINE}`);
      expect(got.json.versions.map((v: any) => v.version)).toEqual([2, 1]);
      expect(got.json.channel).toBe(photoChannelOf(MINE));
    });
  });

  it("someone else's photo is a 404 and is left out of the head batch", async () => {
    await withServer(async (call) => {
      expect((await call('GET', `/edits/${THEIRS}`)).status).toBe(404);
      expect((await call('POST', `/edits/${THEIRS}/versions`, { ops: [], parentVersion: 0, suggestionId: 's' })).status).toBe(404);
      const heads = await call('GET', `/edits?fileIds=${MINE},${THEIRS}`);
      expect(heads.json.heads.map((h: any) => h.fileId)).toEqual([MINE]);
    });
  });

  it('masks are rasterized and stored by content hash (same outline → same blob)', async () => {
    await withServer(async (call) => {
      const mask = { maskId: 'left', label: 'Person on left', kind: 'subject', polygons: [[[0.1, 0.2], [0.3, 0.2], [0.3, 0.9], [0.1, 0.9]]] };
      const out = await call('PUT', `/edits/${MINE}/masks`, { masks: [mask, { ...mask, maskId: 'copy', label: 'Copy' }] });
      expect(out.status).toBe(200);
      const [a, b] = out.json.masks;
      expect(a.blobHash).toMatch(/^[0-9a-f]{64}$/);
      expect(b.blobHash).toBe(a.blobHash);
      expect(blobs.size).toBe(1);
      const png = blobs.get(`mask-${a.blobHash}`)!;
      const { data, info } = await sharp(png).raw().toBuffer({ resolveWithObject: true });
      const at = (x: number, y: number) => data[(Math.round(y * info.height) * info.width + Math.round(x * info.width)) * info.channels];
      expect(at(0.2, 0.5)).toBe(255);
      expect(at(0.7, 0.5)).toBe(0);
    });
  });
});
