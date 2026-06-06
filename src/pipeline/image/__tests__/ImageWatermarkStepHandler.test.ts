// platform-api/src/pipeline/image/__tests__/ImageWatermarkStepHandler.test.ts

import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageWatermarkStepHandler } from '../ImageWatermarkStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_watermark',
    type: 'image:watermark' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image:watermark', ...data } as never,
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
    runId: 'run_watermark',
    ownerNodeId: 'node_test',
    stepId: 'step_watermark',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

/**
 * Build a real 100×80 RGB PNG for the source image.
 */
async function makeSourceImage(): Promise<{ bytes: Buffer; meta: ImageMeta }> {
  const width = 100;
  const height = 80;
  const pixels = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 3;
      pixels[i] = Math.floor((x / width) * 200);
      pixels[i + 1] = Math.floor((y / height) * 200);
      pixels[i + 2] = 100;
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

/**
 * Build a small 20×20 RGBA PNG for the watermark/overlay.
 */
async function makeOverlayImage(width = 20, height = 20): Promise<{ bytes: Buffer; meta: ImageMeta }> {
  const pixels = Buffer.alloc(width * height * 4);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 4;
      pixels[i] = 255;     // R
      pixels[i + 1] = 0;   // G
      pixels[i + 2] = 0;   // B
      pixels[i + 3] = 200; // A (semi-opaque)
    }
  }
  const bytes = await sharp(pixels, { raw: { width, height, channels: 4 } })
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

describe('ImageWatermarkStepHandler.execute', () => {
  // ── Happy path ──────────────────────────────────────────────────────────

  it('composites watermark and emits pipeline:step:image:produced', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes: srcBytes, meta: srcMeta } = await makeSourceImage();
    const { bytes: ovlBytes, meta: ovlMeta } = await makeOverlayImage();
    await blob.put('src_wm_1', srcBytes, srcMeta);
    await blob.put('ovl_wm_1', ovlBytes, ovlMeta);

    const handler = createImageWatermarkStepHandler({ imageBlobStore: blob });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ overlayImageId: 'ovl_wm_1' }),
      { scatter: { parentImageId: 'src_wm_1' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.imageId).toBeDefined();
    expect(out.parentImageId).toBe('src_wm_1');
    // Dimensions preserved (watermark composite keeps source dimensions)
    expect(out.width).toBe(100);
    expect(out.height).toBe(80);
    expect(out.mimeType).toBe('image/png');
    expect(out.materialized).toBe(true);

    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();

    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
    const payload = produced!.payload as Record<string, unknown>;
    expect(payload.imageId).toBe(out.imageId);
    expect(payload.stepId).toBe('step_watermark');
  });

  it('default gravity=southeast places overlay without error', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes: srcBytes, meta: srcMeta } = await makeSourceImage();
    const { bytes: ovlBytes, meta: ovlMeta } = await makeOverlayImage();
    await blob.put('src_grav', srcBytes, srcMeta);
    await blob.put('ovl_grav', ovlBytes, ovlMeta);

    const handler = createImageWatermarkStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ overlayImageId: 'ovl_grav', gravity: 'southeast' }),
      { scatter: { parentImageId: 'src_grav' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).width).toBe(100);
  });

  it('gravity=center places overlay without error', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes: srcBytes, meta: srcMeta } = await makeSourceImage();
    const { bytes: ovlBytes, meta: ovlMeta } = await makeOverlayImage();
    await blob.put('src_center', srcBytes, srcMeta);
    await blob.put('ovl_center', ovlBytes, ovlMeta);

    const handler = createImageWatermarkStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ overlayImageId: 'ovl_center', gravity: 'center' }),
      { scatter: { parentImageId: 'src_center' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
  });

  it('opacity=1.0 (fully opaque) produces completed output', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes: srcBytes, meta: srcMeta } = await makeSourceImage();
    const { bytes: ovlBytes, meta: ovlMeta } = await makeOverlayImage();
    await blob.put('src_opaque', srcBytes, srcMeta);
    await blob.put('ovl_opaque', ovlBytes, ovlMeta);

    const handler = createImageWatermarkStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ overlayImageId: 'ovl_opaque', opacity: 1.0 }),
      { scatter: { parentImageId: 'src_opaque' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
  });

  it('opacity=0.0 (fully transparent overlay) still produces completed output', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes: srcBytes, meta: srcMeta } = await makeSourceImage();
    const { bytes: ovlBytes, meta: ovlMeta } = await makeOverlayImage();
    await blob.put('src_transparent', srcBytes, srcMeta);
    await blob.put('ovl_transparent', ovlBytes, ovlMeta);

    const handler = createImageWatermarkStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ overlayImageId: 'ovl_transparent', opacity: 0.0 }),
      { scatter: { parentImageId: 'src_transparent' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
  });

  it('falls back to trigger.imageRef when scatter.parentImageId is absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes: srcBytes, meta: srcMeta } = await makeSourceImage();
    const { bytes: ovlBytes, meta: ovlMeta } = await makeOverlayImage();
    await blob.put('src_fb', srcBytes, srcMeta);
    await blob.put('ovl_fb', ovlBytes, ovlMeta);

    const handler = createImageWatermarkStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ overlayImageId: 'ovl_fb' }),
      { trigger: { imageRef: 'src_fb' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).parentImageId).toBe('src_fb');
  });

  // ── Failure paths ───────────────────────────────────────────────────────

  it('fails when overlayImageId is missing from config', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageWatermarkStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),   // no overlayImageId
      { scatter: { parentImageId: 'any' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/overlayImageId is required/);
  });

  it('fails when source imageId cannot be resolved', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageWatermarkStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ overlayImageId: 'ovl_any' }),
      {},
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/failed to resolve source imageId/);
  });

  it('fails when source image is missing from blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageWatermarkStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ overlayImageId: 'ovl_any' }),
      { scatter: { parentImageId: 'ghost_src' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/not found in blob store/);
  });

  it('fails when overlay image is missing from blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeSourceImage();
    await blob.put('src_miss_ovl', bytes, meta);

    const handler = createImageWatermarkStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ overlayImageId: 'ghost_ovl' }),
      { scatter: { parentImageId: 'src_miss_ovl' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/not found in blob store/);
  });

  it('fails when overlay dimensions exceed source dimensions', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes: srcBytes, meta: srcMeta } = await makeSourceImage();
    // Overlay larger than source (200×200 > 100×80)
    const { bytes: ovlBytes, meta: ovlMeta } = await makeOverlayImage(200, 200);
    await blob.put('src_dimcheck', srcBytes, srcMeta);
    await blob.put('ovl_dimcheck', ovlBytes, ovlMeta);

    const handler = createImageWatermarkStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ overlayImageId: 'ovl_dimcheck' }),
      { scatter: { parentImageId: 'src_dimcheck' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/exceed source dimensions/);
  });

  it('fails when blob-store put throws', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes: srcBytes, meta: srcMeta } = await makeSourceImage();
    const { bytes: ovlBytes, meta: ovlMeta } = await makeOverlayImage();
    await blob.put('src_putfail', srcBytes, srcMeta);
    await blob.put('ovl_putfail', ovlBytes, ovlMeta);

    const throwingBlob = {
      get: blob.get.bind(blob),
      getMetadata: blob.getMetadata.bind(blob),
      put: async () => { throw new Error('DDB write timeout'); },
    };

    const handler = createImageWatermarkStepHandler({ imageBlobStore: throwingBlob });
    const outcome = await handler.execute(
      makeNode({ overlayImageId: 'ovl_putfail' }),
      { scatter: { parentImageId: 'src_putfail' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/blob-store put failed/);
  });
});
