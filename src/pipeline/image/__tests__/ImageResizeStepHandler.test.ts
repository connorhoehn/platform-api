// platform-api/src/pipeline/image/__tests__/ImageResizeStepHandler.test.ts

import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageResizeStepHandler } from '../ImageResizeStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_resize',
    type: 'image:resize' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image:resize', ...data } as never,
  } as unknown as PipelineNode;
}

function makeHelpers(): {
  helpers: StepHandlerHelpers;
  emitted: Array<{ event: string; payload: unknown }>;
} {
  const emitted: Array<{ event: string; payload: unknown }> = [];
  const helpers: StepHandlerHelpers = {
    emit: (async (event: string, payload: unknown) => {
      emitted.push({ event, payload });
    }) as never,
    runId: 'run_resize',
    ownerNodeId: 'node_test',
    stepId: 'step_resize',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

/**
 * Build a real 100×80 RGB PNG for Sharp to process.
 */
async function makeTestImage(): Promise<{ bytes: Buffer; meta: ImageMeta }> {
  const width = 100;
  const height = 80;
  const pixels = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 3;
      pixels[i] = Math.floor((x / width) * 255);
      pixels[i + 1] = Math.floor((y / height) * 255);
      pixels[i + 2] = 128;
    }
  }
  const bytes = await sharp(pixels, { raw: { width, height, channels: 3 } })
    .png()
    .toBuffer();
  return {
    bytes,
    meta: {
      mimeType: 'image/png',
      sizeBytes: bytes.length,
      width,
      height,
      uploadedAt: '2026-06-06T00:00:00.000Z',
    },
  };
}

describe('ImageResizeStepHandler.execute', () => {
  // ── Happy path ──────────────────────────────────────────────────────────

  it('resizes to target width and preserves aspect ratio (fit=inside)', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_resize_1', bytes, meta);

    const handler = createImageResizeStepHandler({ imageBlobStore: blob });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ width: 50 }),
      { scatter: { parentImageId: 'src_resize_1' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.imageId).toBeDefined();
    expect(out.parentImageId).toBe('src_resize_1');
    expect(out.width).toBe(50);
    // 100×80 scaled to width=50 → height=40 (aspect preserved)
    expect(out.height).toBe(40);
    expect(out.mimeType).toBe('image/png');
    expect(out.materialized).toBe(true);

    // Event emitted
    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
    const payload = produced!.payload as Record<string, unknown>;
    expect(payload.imageId).toBe(out.imageId);
    expect(payload.stepId).toBe('step_resize');
  });

  it('resizes to exact dimensions with fit=fill (ignores aspect ratio)', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_fill', bytes, meta);

    const handler = createImageResizeStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ width: 60, height: 60, fit: 'fill' }),
      { scatter: { parentImageId: 'src_fill' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.width).toBe(60);
    expect(out.height).toBe(60);
  });

  it('resizes with fit=contain, resulting image fits within the target box', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_contain', bytes, meta);

    const handler = createImageResizeStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ width: 80, height: 80, fit: 'contain', background: '#000000' }),
      { scatter: { parentImageId: 'src_contain' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    // The source 100×80 fits within 80×80 — width capped at 80
    expect(out.width).toBe(80);
    expect(out.height).toBe(80);
  });

  it('withoutEnlargement=true prevents upscaling when source is smaller', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_noenl', bytes, meta);

    const handler = createImageResizeStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ width: 500, height: 500, fit: 'inside', withoutEnlargement: true }),
      { scatter: { parentImageId: 'src_noenl' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    // Source is 100×80 — withoutEnlargement keeps it at original size
    expect(out.width).toBeLessThanOrEqual(100);
    expect(out.height).toBeLessThanOrEqual(80);
  });

  it('height-only resize scales proportionally', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_honly', bytes, meta);

    const handler = createImageResizeStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ height: 40 }),
      { trigger: { imageRef: 'src_honly' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.height).toBe(40);
    // 100×80 scaled to height=40 → width=50
    expect(out.width).toBe(50);
  });

  it('stores resized image in blob store with correct metadata', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_store', bytes, meta);

    const handler = createImageResizeStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ width: 50 }),
      { scatter: { parentImageId: 'src_store' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();
    expect(stored!.meta.width).toBe(50);
    expect(stored!.meta.mimeType).toBe('image/png');
  });

  it('falls back to trigger.imageRef when scatter.parentImageId is absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_fb', bytes, meta);

    const handler = createImageResizeStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ width: 50 }),
      { trigger: { imageRef: 'src_fb' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).parentImageId).toBe('src_fb');
  });

  it('falls back to trigger.imageId when imageRef is also absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_fb2', bytes, meta);

    const handler = createImageResizeStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ width: 50 }),
      { trigger: { imageId: 'src_fb2' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).parentImageId).toBe('src_fb2');
  });

  // ── Failure paths ───────────────────────────────────────────────────────

  it('fails when both width and height are omitted', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageResizeStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),   // no width or height
      { scatter: { parentImageId: 'any' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/both width and height are omitted/);
  });

  it('fails when source imageId cannot be resolved', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageResizeStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ width: 50 }),
      {},   // empty context
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/failed to resolve source imageId/);
  });

  it('fails when source image is missing from blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageResizeStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ width: 50 }),
      { scatter: { parentImageId: 'ghost' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/not found in blob store/);
  });

  it('fails when blob-store put throws', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_putfail', bytes, meta);

    const throwingBlob = {
      get: blob.get.bind(blob),
      getMetadata: blob.getMetadata.bind(blob),
      put: async () => { throw new Error('DDB write timeout'); },
    };

    const handler = createImageResizeStepHandler({ imageBlobStore: throwingBlob });
    const outcome = await handler.execute(
      makeNode({ width: 50 }),
      { scatter: { parentImageId: 'src_putfail' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/blob-store put failed/);
  });
});
