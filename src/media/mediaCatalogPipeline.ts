// platform-api/src/media/mediaCatalogPipeline.ts
//
// The media catalog as a run, not a script: a host walker probes files where
// the bytes are and posts a batch; this pipeline stores the thumbnails,
// groups the frames into bursts, classifies one frame per burst, and writes
// the catalog. See .planning/MEDIA-CATALOG-AND-VIDEO-SURFACES-2026-09-14.md
// in realtime-examples for why the walk is a host process (the drive is on
// the host; this service is in a pod) and why burst detection runs before
// classification (one vision call per moment, not per frame).
//
// Trigger payload — one batch, lands in context as `ctx.batch`:
//   { batchId, root, files: MediaProbe[] }
// A probe is everything the walker can know without a model: hash, size,
// mime, dimensions, EXIF time and camera, a difference hash, and a ≤512px
// JPEG thumbnail as base64 for image files.

import type { StepHandler, StepOutcome, PipelineNode } from 'distributed-core/applications/pipeline';
import { mediaCatalogRepo } from '../repositories';
import { CATALOG_BUCKET, type MediaCatalogRow } from '../repositories/MediaCatalogRepository';
import { getImageBlobStore, getVisionClient } from '../pipeline/image/visionServices';
import { buildPresetSchema } from '../pipeline/image/visionSchemaPresets';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { getRunPersistenceService } from '../pipeline/runPersistence';
import { groupBursts, shotAt, type Burst } from './burstGroup';
import log from '../lib/logger';

export const MEDIA_CATALOG_PIPELINE_ID = 'media-catalog';

export const KINDS = {
  ingest: 'media:ingest',
  burstGroup: 'media:burst-group',
  classify: 'media:classify',
  upsert: 'catalog:upsert',
} as const;

/** What the walker sends per file. */
export interface MediaProbe {
  path: string;
  relPath: string;
  fileId: string;
  sizeBytes: number;
  mimeType: string;
  kind: 'image' | 'video' | 'other';
  width: number | null;
  height: number | null;
  capturedAt: string | null;
  modifiedAt: string;
  cameraId: string | null;
  dhash: string | null;
  thumbnail?: { base64: string; mimeType: string; width: number; height: number } | null;
}

export interface MediaBatch {
  batchId: string;
  root: string;
  files: MediaProbe[];
}

/** A probe after ingest: the thumbnail is in the blob store, not the context. */
export type IngestedFile = Omit<MediaProbe, 'thumbnail'> & { imageId: string | null };

export interface SceneVerdict { label: string; confidence: number | null; caption: string | null }

/** The scene vocabulary a photo library sorts by. A node may override `labels`. */
export const DEFAULT_SCENE_LABELS = [
  'landscape', 'cityscape', 'portrait', 'group of people', 'pet or animal', 'food',
  'event or party', 'sports', 'vehicle', 'interior', 'night', 'document or screenshot', 'other',
] as const;

/** `111LEICA/L1111005.DNG` → `111LEICA/L1111005`: what a RAW shares with the JPEG beside it. */
export function stemOf(relPath: string): string {
  const slash = relPath.lastIndexOf('/');
  const dot = relPath.lastIndexOf('.');
  return dot > slash + 1 ? relPath.slice(0, dot) : relPath;
}

/** Thumbnails are keyed by content hash, so a rerun over the same file overwrites, never duplicates. */
export function thumbnailImageId(fileId: string): string {
  return `media-${fileId}`;
}

const fail = (error: string): StepOutcome => ({ status: 'failed', error });
const ok = (output: Record<string, unknown>): StepOutcome => ({ status: 'completed', output });

function batchOf(ctx: Record<string, unknown>): MediaBatch | null {
  const b = ctx.batch as Partial<MediaBatch> | undefined;
  if (!b || typeof b !== 'object' || !Array.isArray(b.files)) return null;
  return { batchId: String(b.batchId ?? ''), root: String(b.root ?? ''), files: b.files as MediaProbe[] };
}

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

function ingestHandler(): StepHandler {
  return {
    kind: KINDS.ingest,
    async execute(_n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const batch = batchOf(ctx);
      if (!batch) return fail('batch.files is required');
      const store = getImageBlobStore();
      const uploadedAt = new Date().toISOString();
      const ingested: IngestedFile[] = [];
      let stored = 0;
      for (const f of batch.files) {
        const { thumbnail, ...rest } = f;
        let imageId: string | null = null;
        if (thumbnail?.base64) {
          const body = Buffer.from(thumbnail.base64, 'base64');
          imageId = thumbnailImageId(f.fileId);
          await store.put(imageId, body, {
            mimeType: thumbnail.mimeType || 'image/jpeg',
            sizeBytes: body.byteLength,
            width: thumbnail.width,
            height: thumbnail.height,
            uploadedAt,
            uploadedBy: 'media-walker',
          });
          stored++;
        }
        ingested.push({ ...rest, imageId });
      }
      // `batch` is REPLACED with a slim record: the trigger payload carried
      // every thumbnail's base64, and a step output merges into the context
      // root, so leaving `ctx.batch` as posted put ~30 KB per file into every
      // checkpoint after this one until the run persistence truncated them.
      return ok({ ingested, batch: { batchId: batch.batchId, root: batch.root, fileCount: batch.files.length }, batchId: batch.batchId, root: batch.root, thumbnailsStored: stored });
    },
  };
}

function burstGroupHandler(): StepHandler {
  return {
    kind: KINDS.burstGroup,
    async execute(n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const files = ctx.ingested as IngestedFile[] | undefined;
      if (!Array.isArray(files)) return fail('ingested files are required (run media:ingest first)');
      const data = (n.data ?? {}) as { windowMs?: number; maxHamming?: number };
      const bursts = groupBursts(files.map((f) => ({ ...f, stem: stemOf(f.relPath) })), { windowMs: data.windowMs, maxHamming: data.maxHamming });
      const multi = bursts.filter((b) => b.fileIds.length > 1);
      return ok({ bursts, burstCount: multi.length, framesInBursts: multi.reduce((s, b) => s + b.fileIds.length, 0) });
    },
  };
}

/** Pull `{label, confidence, caption}` out of whatever the vision client returned. */
export function sceneFromOutput(output: unknown, rawText: string, labels: readonly string[]): SceneVerdict | null {
  const o = (output && typeof output === 'object' ? output : null) as Record<string, unknown> | null;
  const pick = (v: unknown) => (typeof v === 'string' && labels.includes(v) ? v : null);
  let label = o ? pick(o.label) ?? pick(o.classification) ?? pick(o.category) : null;
  if (!label) {
    const lower = rawText.toLowerCase();
    label = labels.find((l) => lower.includes(l)) ?? null;
  }
  if (!label) return null;
  const conf = o && typeof o.confidence === 'number' ? Math.max(0, Math.min(1, o.confidence)) : null;
  const caption = o && typeof o.caption === 'string' ? o.caption : null;
  return { label, confidence: conf, caption };
}

function classifyHandler(): StepHandler {
  return {
    kind: KINDS.classify,
    async execute(n: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const files = ctx.ingested as IngestedFile[] | undefined;
      const bursts = ctx.bursts as Burst[] | undefined;
      if (!Array.isArray(files) || !Array.isArray(bursts)) return fail('ingested files and bursts are required');
      const data = (n.data ?? {}) as { labels?: string[]; model?: string; maxOutputTokens?: number };
      const labels = Array.isArray(data.labels) && data.labels.length > 0 ? data.labels : [...DEFAULT_SCENE_LABELS];
      let client: ReturnType<typeof getVisionClient>;
      try {
        client = getVisionClient();
      } catch (err) {
        // No model is not a failed run: the catalog is still worth having
        // without scenes, and the node is `optional` for exactly this.
        log.warn({ reason: (err as Error).message }, 'media:classify skipped — no vision client');
        return ok({ scenes: {}, classified: 0, skipped: `no vision client: ${(err as Error).message}` });
      }
      const store = getImageBlobStore();
      const byId = new Map(files.map((f) => [f.fileId, f]));
      const scenes: Record<string, SceneVerdict> = {};
      let classified = 0;
      let failed = 0;
      for (const b of bursts) {
        const rep = byId.get(b.representativeFileId);
        if (!rep?.imageId) continue;
        const row = await store.get(rep.imageId);
        if (!row) continue;
        try {
          const res = await client.analyze({
            model: data.model ?? 'claude-haiku-4-5-20251001',
            systemPrompt: 'You are a precise photo-library scene classifier.',
            userPrompt: `Classify this photo as one of: ${labels.join(', ')}. Return the chosen label with a confidence 0-1 and a brief caption.`,
            imageBytes: row.body,
            imageMimeType: row.meta.mimeType,
            outputSchema: buildPresetSchema('classification-enum', { labels }),
            maxOutputTokens: data.maxOutputTokens ?? 256,
          });
          const verdict = sceneFromOutput(res.output, res.rawText, labels);
          if (!verdict) { failed++; continue; }
          // One verdict for the whole burst: the frames are one moment.
          for (const id of b.fileIds) scenes[id] = verdict;
          classified++;
        } catch (err) {
          failed++;
          log.warn({ fileId: rep.fileId, error: (err as Error).message }, 'media:classify frame failed');
        }
      }
      return ok({ scenes, classified, failed });
    },
  };
}

function upsertHandler(): StepHandler {
  return {
    kind: KINDS.upsert,
    async execute(_n: PipelineNode, ctx: Record<string, unknown>, helpers?: { runId?: string }): Promise<StepOutcome> {
      const files = ctx.ingested as IngestedFile[] | undefined;
      const bursts = (ctx.bursts as Burst[] | undefined) ?? [];
      const scenes = (ctx.scenes as Record<string, SceneVerdict> | undefined) ?? {};
      const root = typeof ctx.root === 'string' ? ctx.root : '';
      if (!Array.isArray(files)) return fail('ingested files are required');
      const burstOf = new Map<string, Burst>();
      for (const b of bursts) for (const id of b.fileIds) burstOf.set(id, b);
      const catalogedAt = new Date().toISOString();
      const rows: MediaCatalogRow[] = files.map((f) => {
        const b = burstOf.get(f.fileId) ?? { burstId: `burst-${f.fileId.slice(0, 16)}`, fileIds: [f.fileId], representativeFileId: f.fileId };
        const s = scenes[f.fileId] ?? null;
        return {
          fileId: f.fileId,
          bucket: CATALOG_BUCKET,
          root,
          path: f.path,
          relPath: f.relPath,
          sizeBytes: f.sizeBytes,
          mimeType: f.mimeType,
          kind: f.kind,
          width: f.width,
          height: f.height,
          capturedAt: f.capturedAt,
          modifiedAt: f.modifiedAt,
          shotAt: new Date(shotAt(f)).toISOString(),
          cameraId: f.cameraId,
          dhash: f.dhash,
          burstId: b.burstId,
          burstSize: b.fileIds.length,
          isRepresentative: b.representativeFileId === f.fileId,
          imageId: f.imageId,
          scene: s?.label ?? null,
          sceneConfidence: s?.confidence ?? null,
          caption: s?.caption ?? null,
          catalogedAt,
          runId: helpers?.runId ?? null,
        };
      });
      const upserted = await mediaCatalogRepo.upsertMany(rows);
      return ok({ upserted });
    },
  };
}

export function buildMediaCatalogHandlers(): Map<string, StepHandler> {
  const m = new Map<string, StepHandler>();
  for (const h of [ingestHandler(), burstGroupHandler(), classifyHandler(), upsertHandler()]) m.set(h.kind, h);
  return m;
}

// ---------------------------------------------------------------------------
// The definition
// ---------------------------------------------------------------------------

const IO_RETRY = { maxAttempts: 3, backoffMs: 1000, backoffMultiplier: 2, retryUnknown: true } as const;
const pos = (x: number) => ({ x, y: 120 });
const edge = (a: string, b: string) => ({ id: `${a}->${b}`, source: a, sourceHandle: 'out', target: b, targetHandle: 'in' });

export function buildMediaCatalogDefinition(): Record<string, unknown> {
  return {
    id: MEDIA_CATALOG_PIPELINE_ID,
    name: 'Media catalog',
    description: 'For each batch the drive walker posts: store the thumbnails, group frames into bursts, classify one frame per burst, and write the catalog.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'webhook', label: 'Walker batch' } },
      { id: 'ingest', type: KINDS.ingest, position: pos(200), data: { type: KINDS.ingest, label: 'Store the thumbnails', retryPolicy: IO_RETRY } },
      { id: 'bursts', type: KINDS.burstGroup, position: pos(400), data: { type: KINDS.burstGroup, label: 'Group bursts', windowMs: 3000, maxHamming: 10, retryPolicy: { maxAttempts: 2, backoffMs: 200, backoffMultiplier: 1, retryUnknown: false } } },
      // Optional: a catalog without scenes is still a catalog. No model, no failure.
      { id: 'classify', type: KINDS.classify, position: pos(600), data: { type: KINDS.classify, label: 'Classify the scene (one frame per burst)', labels: [...DEFAULT_SCENE_LABELS], optional: true, retryPolicy: IO_RETRY } },
      { id: 'upsert', type: KINDS.upsert, position: pos(800), data: { type: KINDS.upsert, label: 'Write the catalog', retryPolicy: IO_RETRY } },
    ],
    edges: [edge('trigger', 'ingest'), edge('ingest', 'bursts'), edge('bursts', 'classify'), edge('classify', 'upsert')],
  };
}

/** Start one run per posted batch. Mirrors `startSystemRun` in recordingPipelines. */
export async function startMediaCatalogRun(batch: MediaBatch, userId: string): Promise<string> {
  const bridge = getPipelineBridge();
  if (!bridge?.trigger) throw new Error('pipeline engine is not available');
  const out = await Promise.resolve(bridge.trigger({
    pipelineId: MEDIA_CATALOG_PIPELINE_ID,
    definition: buildMediaCatalogDefinition() as never,
    triggerPayload: { batch },
    triggeredBy: { userId },
  } as never));
  const runId = (out as { runId: string }).runId;
  getRunPersistenceService()?.registerTrigger({ runId, pipelineId: MEDIA_CATALOG_PIPELINE_ID, userId, triggerType: 'webhook', at: new Date().toISOString() });
  return runId;
}
