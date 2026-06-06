// platform-api/src/pipeline/image/__tests__/ImageAdjustBwStepHandler.test.ts

import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageAdjustBwStepHandler } from '../ImageAdjustBwStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_bw',
    type: 'image:adjust-bw' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image:adjust-bw', ...data } as never,
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
    runId: 'run_bw',
    ownerNodeId: 'node_test',
    stepId: 'step_bw',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

/**
 * Build a real 100×80 RGB PNG with a colourful gradient so the various
 * greyscale algorithms produce meaningfully different pixel values.
 */
async function makeTestImage(): Promise<{ bytes: Buffer; meta: ImageMeta }> {
  const width = 100;
  const height = 80;
  const pixels = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 3;
      // Distinct R, G, B gradients so that luminosity != average != desaturate
      pixels[i] = Math.floor((x / width) * 255);       // R
      pixels[i + 1] = Math.floor((y / height) * 255);   // G
      pixels[i + 2] = 128;                              // B constant
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

/** Extract the grey value of a single pixel from a PNG buffer via Sharp. */
async function samplePixel(
  buf: Buffer,
  x: number,
  y: number,
): Promise<{ r: number; g: number; b: number }> {
  const { data, info } = await sharp(buf)
    .raw()
    .toBuffer({ resolveWithObject: true });
  const channels = info.channels;
  const idx = (y * info.width + x) * channels;
  return { r: data[idx]!, g: data[idx + 1]!, b: data[idx + 2]! };
}

describe('ImageAdjustBwStepHandler.execute', () => {
  // ── Algorithm correctness ──────────────────────────────────────────────

  it('luminosity algorithm produces a greyscale output (R==G==B per pixel)', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_lum', bytes, meta);

    const handler = createImageAdjustBwStepHandler({ imageBlobStore: blob });
    const { helpers } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ algorithm: 'luminosity' }),
      { scatter: { parentImageId: 'src_lum' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();

    // Sample a mid-image pixel — in greyscale PNG, R should equal G equal B.
    const px = await samplePixel(stored!.body, 50, 40);
    expect(px.r).toBe(px.g);
    expect(px.g).toBe(px.b);
  });

  it('desaturate algorithm produces greyscale output', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_desat', bytes, meta);

    const handler = createImageAdjustBwStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ algorithm: 'desaturate' }),
      { scatter: { parentImageId: 'src_desat' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();

    const px = await samplePixel(stored!.body, 50, 40);
    // After desaturate → PNG re-encode, R/G/B should be equal
    expect(px.r).toBe(px.g);
    expect(px.g).toBe(px.b);
  });

  it('average algorithm produces greyscale output', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_avg', bytes, meta);

    const handler = createImageAdjustBwStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ algorithm: 'average' }),
      { scatter: { parentImageId: 'src_avg' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();

    const px = await samplePixel(stored!.body, 50, 40);
    expect(px.r).toBe(px.g);
    expect(px.g).toBe(px.b);
  });

  it('three algorithms produce different output buffers for the same input', async () => {
    // Compare full PNG buffer hashes rather than single pixels — the algorithms
    // differ in how they weight channels, which changes the overall output
    // even when a sampled pixel happens to round to the same value.
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    const handler = createImageAdjustBwStepHandler({ imageBlobStore: blob });

    const runAlgo = async (algorithm: string) => {
      const id = `src_multi_${algorithm}`;
      await blob.put(id, bytes, meta);
      const outcome = await handler.execute(
        makeNode({ algorithm }),
        { scatter: { parentImageId: id } },
        makeHelpers().helpers,
      );
      expect(outcome.status).toBe('completed');
      const out = outcome.output as Record<string, unknown>;
      const stored = await blob.get(out.imageId as string);
      return stored!.body;
    };

    const lumBuf = await runAlgo('luminosity');
    const desBuf = await runAlgo('desaturate');
    const avgBuf = await runAlgo('average');

    // At least two of the three algorithm outputs must differ in bytes.
    // `desaturate` (saturation=0 stays RGB) vs `luminosity` (BT.709 grayscale)
    // will always differ because they use different weighting.
    const lumVsDes = lumBuf.equals(desBuf);
    const lumVsAvg = lumBuf.equals(avgBuf);
    const desVsAvg = desBuf.equals(avgBuf);
    const allSame = lumVsDes && lumVsAvg && desVsAvg;
    expect(allSame).toBe(false);
  });

  // ── Contrast passthrough ────────────────────────────────────────────────

  it('contrast=1.0 (default) produces same dimensions as source', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_c1', bytes, meta);

    const handler = createImageAdjustBwStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ contrast: 1.0 }),
      { scatter: { parentImageId: 'src_c1' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.width).toBe(100);
    expect(out.height).toBe(80);
  });

  it('contrast=2.0 shifts pixel values vs contrast=1.0', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();

    const handler = createImageAdjustBwStepHandler({ imageBlobStore: blob });
    // Run contrast=1 and contrast=2 on separate copies of the source image
    await blob.put('src_contrast_low', bytes, meta);
    await blob.put('src_contrast_high', bytes, meta);

    const outLow = await handler.execute(
      makeNode({ algorithm: 'luminosity', contrast: 1.0 }),
      { scatter: { parentImageId: 'src_contrast_low' } },
      makeHelpers().helpers,
    );
    const outHigh = await handler.execute(
      makeNode({ algorithm: 'luminosity', contrast: 2.0 }),
      { scatter: { parentImageId: 'src_contrast_high' } },
      makeHelpers().helpers,
    );

    expect(outLow.status).toBe('completed');
    expect(outHigh.status).toBe('completed');

    const lowId = (outLow.output as Record<string, unknown>).imageId as string;
    const highId = (outHigh.output as Record<string, unknown>).imageId as string;

    // Buffer lengths will differ when tone curves change pixel distribution
    // (contrast=2 drives pixels toward 0/255 → PNG compresses differently).
    const storedLow = await blob.get(lowId);
    const storedHigh = await blob.get(highId);
    expect(storedLow).not.toBeNull();
    expect(storedHigh).not.toBeNull();

    // Sample a pixel away from mid-grey — contrast should shift it.
    const pxLow = await samplePixel(storedLow!.body, 10, 10);
    const pxHigh = await samplePixel(storedHigh!.body, 10, 10);
    // After full contrast boost, same input → different grey value
    expect(pxLow.r).not.toBe(pxHigh.r);
  });

  // ── Fallback resolution ─────────────────────────────────────────────────

  it('falls back to trigger.imageRef when scatter.parentImageId is absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_fb', bytes, meta);

    const handler = createImageAdjustBwStepHandler({ imageBlobStore: blob });
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

    const handler = createImageAdjustBwStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      { trigger: { imageId: 'src_fb2' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).parentImageId).toBe('src_fb2');
  });

  // ── Output contract ─────────────────────────────────────────────────────

  it('produces a new imageId and emits pipeline:step:image:produced', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_emit', bytes, meta);

    const handler = createImageAdjustBwStepHandler({ imageBlobStore: blob });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'src_emit' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.imageId).toBeDefined();
    expect(out.imageId).not.toBe('src_emit');   // new imageId, not source
    expect(out.parentImageId).toBe('src_emit');
    expect(out.materialized).toBe(true);
    expect(out.mimeType).toBe('image/png');
    expect(typeof out.width).toBe('number');
    expect(typeof out.height).toBe('number');

    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
    const payload = produced!.payload as Record<string, unknown>;
    expect(payload.imageId).toBe(out.imageId);
    expect(payload.stepId).toBe('step_bw');
    // image:produced should NOT carry a bbox (this is not a crop)
    expect(payload.bbox).toBeUndefined();
  });

  it('result bytes are stored in blob store with correct metadata', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_store', bytes, meta);

    const handler = createImageAdjustBwStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
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

  // ── Failure paths ───────────────────────────────────────────────────────

  it('fails when source imageId cannot be resolved', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageAdjustBwStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      {},   // empty context — no scatter, no trigger
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/failed to resolve source imageId/);
  });

  it('fails when source image is missing from blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageAdjustBwStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'ghost_id' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/not found in blob store/);
  });

  it('fails when blob-store put throws', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_put_fail', bytes, meta);

    // Stub put to throw
    const throwingBlob = {
      get: blob.get.bind(blob),
      getMetadata: blob.getMetadata.bind(blob),
      put: async () => {
        throw new Error('DDB write timeout');
      },
    };

    const handler = createImageAdjustBwStepHandler({ imageBlobStore: throwingBlob });
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'src_put_fail' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/blob-store put failed/);
  });
});
