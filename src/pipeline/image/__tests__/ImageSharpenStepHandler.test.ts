// platform-api/src/pipeline/image/__tests__/ImageSharpenStepHandler.test.ts

import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageSharpenStepHandler } from '../ImageSharpenStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_sharpen',
    type: 'image:sharpen' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image:sharpen', ...data } as never,
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
    runId: 'run_sharpen',
    ownerNodeId: 'node_test',
    stepId: 'step_sharpen',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

/**
 * Build a real 100×80 RGB PNG with a gradient for sharpening to have
 * meaningful edges to act on.
 */
async function makeTestImage(): Promise<{ bytes: Buffer; meta: ImageMeta }> {
  const width = 100;
  const height = 80;
  const pixels = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 3;
      // Hard step edge at x=50 for sharpening to enhance
      const v = x < 50 ? 50 : 200;
      pixels[i] = v;
      pixels[i + 1] = v;
      pixels[i + 2] = v;
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

describe('ImageSharpenStepHandler.execute', () => {
  // ── Happy path ──────────────────────────────────────────────────────────

  it('applies sharpening and emits pipeline:step:image:produced', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_sharpen_1', bytes, meta);

    const handler = createImageSharpenStepHandler({ imageBlobStore: blob });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ sigma: 1.0 }),
      { scatter: { parentImageId: 'src_sharpen_1' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.imageId).toBeDefined();
    expect(out.parentImageId).toBe('src_sharpen_1');
    expect(out.width).toBe(100);
    expect(out.height).toBe(80);
    expect(out.mimeType).toBe('image/png');
    expect(out.materialized).toBe(true);

    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
    expect((produced!.payload as Record<string, unknown>).imageId).toBe(out.imageId);
  });

  it('default params (no sigma/m1/m2 specified) succeeds', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_defaults', bytes, meta);

    const handler = createImageSharpenStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'src_defaults' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.width).toBe(100);
    expect(out.height).toBe(80);
  });

  it('sigma=0 is a no-op pass-through — produces same dimensions', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_noop', bytes, meta);

    const handler = createImageSharpenStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ sigma: 0 }),
      { scatter: { parentImageId: 'src_noop' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.width).toBe(100);
    expect(out.height).toBe(80);
    // Still produces a new imageId (re-encoded, not the same blob)
    expect(out.imageId).not.toBe('src_noop');
  });

  it('sigma below minimum is clamped to 0.5 (no error)', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_clamp_low', bytes, meta);

    const handler = createImageSharpenStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ sigma: 0.1 }),
      { scatter: { parentImageId: 'src_clamp_low' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
  });

  it('sigma above maximum is clamped to 3.0 (no error)', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_clamp_high', bytes, meta);

    const handler = createImageSharpenStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ sigma: 99 }),
      { scatter: { parentImageId: 'src_clamp_high' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
  });

  it('stores sharpened image in blob store with correct metadata', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_store', bytes, meta);

    const handler = createImageSharpenStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ sigma: 1.5 }),
      { scatter: { parentImageId: 'src_store' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();
    expect(stored!.meta.width).toBe(100);
    expect(stored!.meta.height).toBe(80);
    expect(stored!.meta.mimeType).toBe('image/png');
  });

  it('high sigma produces different output than low sigma', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    const handler = createImageSharpenStepHandler({ imageBlobStore: blob });

    await blob.put('src_sigma_low', bytes, meta);
    await blob.put('src_sigma_high', bytes, meta);

    const outLow = await handler.execute(
      makeNode({ sigma: 0.5 }),
      { scatter: { parentImageId: 'src_sigma_low' } },
      makeHelpers().helpers,
    );
    const outHigh = await handler.execute(
      makeNode({ sigma: 3.0 }),
      { scatter: { parentImageId: 'src_sigma_high' } },
      makeHelpers().helpers,
    );

    expect(outLow.status).toBe('completed');
    expect(outHigh.status).toBe('completed');

    const lowId = (outLow.output as Record<string, unknown>).imageId as string;
    const highId = (outHigh.output as Record<string, unknown>).imageId as string;
    const lowBuf = (await blob.get(lowId))!.body;
    const highBuf = (await blob.get(highId))!.body;

    // Different sigma → different pixel values → different buffer content
    expect(lowBuf.equals(highBuf)).toBe(false);
  });

  // ── Fallback resolution ─────────────────────────────────────────────────

  it('falls back to trigger.imageRef when scatter.parentImageId is absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_fb', bytes, meta);

    const handler = createImageSharpenStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
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

    const handler = createImageSharpenStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      { trigger: { imageId: 'src_fb2' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).parentImageId).toBe('src_fb2');
  });

  // ── Failure paths ───────────────────────────────────────────────────────

  it('fails when source imageId cannot be resolved', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageSharpenStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ sigma: 1.0 }),
      {},
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/failed to resolve source imageId/);
  });

  it('fails when source image is missing from blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageSharpenStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ sigma: 1.0 }),
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

    const handler = createImageSharpenStepHandler({ imageBlobStore: throwingBlob });
    const outcome = await handler.execute(
      makeNode({ sigma: 1.0 }),
      { scatter: { parentImageId: 'src_putfail' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/blob-store put failed/);
  });
});
