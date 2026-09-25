// platform-api/src/pipeline/image/__tests__/ImageAdjustLayerStepHandler.test.ts

import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageAdjustLayerStepHandler } from '../ImageAdjustLayerStepHandler';
import type { AdjustLayer } from '../ImageAdjustLayerStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_layer',
    type: 'image:adjust-layer' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image:adjust-layer', ...data } as never,
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
    runId: 'run_layer',
    ownerNodeId: 'node_test',
    stepId: 'step_layer',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

/**
 * Build a real 100×80 RGB PNG with a colourful gradient. The gradient has
 * distinct R/G/B contributions so brightness/saturation/hue adjustments
 * produce measurably different results.
 */
async function makeTestImage(): Promise<{ bytes: Buffer; meta: ImageMeta }> {
  const width = 100;
  const height = 80;
  const pixels = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 3;
      pixels[i] = Math.floor((x / width) * 200) + 50;      // R: 50–250
      pixels[i + 1] = Math.floor((y / height) * 200) + 30; // G: 30–230
      pixels[i + 2] = 100;                                  // B: constant
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

/** Sample the RGB of a single pixel from a PNG buffer. */
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

// Helper: store a copy of the source under a unique id and run handler
async function applyLayers(
  layers: AdjustLayer[],
  srcId: string,
  blob: InMemoryImageBlobStore,
): Promise<{ imageId: string; width: number; height: number; mimeType: string }> {
  const handler = createImageAdjustLayerStepHandler({ imageBlobStore: blob });
  const outcome = await handler.execute(
    makeNode({ layers }),
    { scatter: { parentImageId: srcId } },
    makeHelpers().helpers,
  );
  expect(outcome.status).toBe('completed');
  return outcome.output as { imageId: string; width: number; height: number; mimeType: string };
}

describe('ImageAdjustLayerStepHandler.execute', () => {
  // ── Individual layer kinds ──────────────────────────────────────────────

  it('brightness layer changes pixel luminance', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_br_orig', bytes, meta);
    await blob.put('src_br_bright', bytes, meta);

    const outOrig = await applyLayers([], 'src_br_orig', blob);
    const outBright = await applyLayers(
      [{ kind: 'brightness', value: 1.8 }],
      'src_br_bright',
      blob,
    );

    const pxOrig = await samplePixel((await blob.get(outOrig.imageId))!.body, 50, 40);
    const pxBright = await samplePixel((await blob.get(outBright.imageId))!.body, 50, 40);

    // Brightened pixel should have higher values
    expect(pxBright.r).toBeGreaterThan(pxOrig.r);
  });

  it('contrast layer stretches tonal range', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_ct_orig', bytes, meta);
    await blob.put('src_ct_high', bytes, meta);

    const outOrig = await applyLayers([], 'src_ct_orig', blob);
    const outHigh = await applyLayers(
      [{ kind: 'contrast', value: 2.0 }],
      'src_ct_high',
      blob,
    );

    // Contrast should move bright pixels toward white — sample upper-right area
    const pxOrig = await samplePixel((await blob.get(outOrig.imageId))!.body, 90, 10);
    const pxHigh = await samplePixel((await blob.get(outHigh.imageId))!.body, 90, 10);
    expect(pxHigh.r).not.toBe(pxOrig.r);
  });

  it('saturation=0 layer produces greyscale (R==G==B)', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_sat0', bytes, meta);

    const out = await applyLayers([{ kind: 'saturation', value: 0 }], 'src_sat0', blob);
    const px = await samplePixel((await blob.get(out.imageId))!.body, 50, 40);
    expect(px.r).toBe(px.g);
    expect(px.g).toBe(px.b);
  });

  it('saturation=2 layer boosts chroma vs passthrough', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_sat2_orig', bytes, meta);
    await blob.put('src_sat2_boost', bytes, meta);

    const outOrig = await applyLayers([], 'src_sat2_orig', blob);
    const outBoost = await applyLayers(
      [{ kind: 'saturation', value: 2 }],
      'src_sat2_boost',
      blob,
    );

    // Channel spread should be wider after saturation boost
    const pxOrig = await samplePixel((await blob.get(outOrig.imageId))!.body, 50, 40);
    const pxBoost = await samplePixel((await blob.get(outBoost.imageId))!.body, 50, 40);
    const spreadOrig = Math.max(pxOrig.r, pxOrig.g, pxOrig.b) - Math.min(pxOrig.r, pxOrig.g, pxOrig.b);
    const spreadBoost = Math.max(pxBoost.r, pxBoost.g, pxBoost.b) - Math.min(pxBoost.r, pxBoost.g, pxBoost.b);
    expect(spreadBoost).toBeGreaterThanOrEqual(spreadOrig);
  });

  it('hue rotation shifts colour channel relationships', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_hue_orig', bytes, meta);
    await blob.put('src_hue_rot', bytes, meta);

    const outOrig = await applyLayers([], 'src_hue_orig', blob);
    const outRot = await applyLayers([{ kind: 'hue', degrees: 90 }], 'src_hue_rot', blob);

    const pxOrig = await samplePixel((await blob.get(outOrig.imageId))!.body, 50, 40);
    const pxRot = await samplePixel((await blob.get(outRot.imageId))!.body, 50, 40);
    // A 90° hue rotation should change channel values
    expect(JSON.stringify(pxOrig)).not.toBe(JSON.stringify(pxRot));
  });

  it('blur layer smooths pixel values (blurred output differs from source)', async () => {
    const blob = new InMemoryImageBlobStore();
    // Build a checkerboard pattern: alternating white/black pixels.
    // Pixels at (0,0)=white, (1,0)=black, etc. After blur, all pixels
    // should be grey (intermediate values), not pure 0 or 255.
    const width = 100;
    const height = 80;
    const pxHF = Buffer.alloc(width * height * 3);
    for (let y = 0; y < height; y++) {
      for (let x = 0; x < width; x++) {
        const i = (y * width + x) * 3;
        const v = ((x ^ y) & 1) === 0 ? 255 : 0;
        pxHF[i] = v; pxHF[i + 1] = v; pxHF[i + 2] = v;
      }
    }
    const hfBytes = await sharp(pxHF, { raw: { width, height, channels: 3 } }).png().toBuffer();
    const hfMeta: ImageMeta = { mimeType: 'image/png', sizeBytes: hfBytes.length, width, height, uploadedAt: '2026-06-06T00:00:00.000Z' };
    await blob.put('src_blur_hf', hfBytes, hfMeta);

    const outBlur = await applyLayers([{ kind: 'blur', radiusPx: 5 }], 'src_blur_hf', blob);
    const stored = await blob.get(outBlur.imageId);
    expect(stored).not.toBeNull();

    // Sample a pixel that was originally pure white (255,255,255) in the
    // checkerboard — after blur it must be intermediate (not 255).
    // Pixel (0,0) is white in our pattern ((0^0)&1 === 0 → 255).
    const pxBlurred = await samplePixel(stored!.body, 0, 0);
    // After 5px gaussian blur from neighbouring black pixels, value < 255
    expect(pxBlurred.r).toBeLessThan(255);
    expect(pxBlurred.r).toBeGreaterThan(0);
  });

  // ── Multiple layers compose in order ───────────────────────────────────

  it('multiple layers compose: brightness then saturation', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_multi_orig', bytes, meta);
    await blob.put('src_multi_comp', bytes, meta);

    const outOrig = await applyLayers([], 'src_multi_orig', blob);
    const outComp = await applyLayers(
      [
        { kind: 'brightness', value: 1.5 },
        { kind: 'saturation', value: 0.5 },
      ],
      'src_multi_comp',
      blob,
    );

    const pxOrig = await samplePixel((await blob.get(outOrig.imageId))!.body, 50, 40);
    const pxComp = await samplePixel((await blob.get(outComp.imageId))!.body, 50, 40);
    // Composed result must differ from the no-op passthrough
    expect(JSON.stringify(pxOrig)).not.toBe(JSON.stringify(pxComp));
  });

  it('all five layer kinds each change the image relative to a passthrough', async () => {
    // Verify that each individual layer kind actually modifies the output
    // by comparing against an empty-layers baseline.
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();

    const storeAndApply = async (id: string, layers: AdjustLayer[]) => {
      await blob.put(id, bytes, meta);
      const out = await applyLayers(layers, id, blob);
      return (await blob.get(out.imageId))!.body;
    };

    await blob.put('src_baseline', bytes, meta);
    const baseline = (await blob.get(
      (await applyLayers([], 'src_baseline', blob)).imageId,
    ))!.body;

    const brightBuf = await storeAndApply('src_kind_br', [{ kind: 'brightness', value: 0.3 }]);
    const contBuf   = await storeAndApply('src_kind_ct', [{ kind: 'contrast', value: 2.5 }]);
    const satBuf    = await storeAndApply('src_kind_st', [{ kind: 'saturation', value: 0 }]);
    const hueBuf    = await storeAndApply('src_kind_hu', [{ kind: 'hue', degrees: 180 }]);
    const blurBuf   = await storeAndApply('src_kind_bl', [{ kind: 'blur', radiusPx: 5 }]);

    expect(brightBuf.equals(baseline)).toBe(false);
    expect(contBuf.equals(baseline)).toBe(false);
    expect(satBuf.equals(baseline)).toBe(false);
    expect(hueBuf.equals(baseline)).toBe(false);
    expect(blurBuf.equals(baseline)).toBe(false);
  });

  // ── Empty layers array ─────────────────────────────────────────────────

  it('empty layers array still produces a new imageId', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_empty', bytes, meta);

    const handler = createImageAdjustLayerStepHandler({ imageBlobStore: blob });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ layers: [] }),
      { scatter: { parentImageId: 'src_empty' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.imageId).toBeDefined();
    expect(out.imageId).not.toBe('src_empty');
    expect(out.parentImageId).toBe('src_empty');
    expect(out.mimeType).toBe('image/png');
    expect(out.materialized).toBe(true);

    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
  });

  // ── Output contract ────────────────────────────────────────────────────

  it('emits pipeline:step:image:produced without bbox field', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_evt', bytes, meta);

    const handler = createImageAdjustLayerStepHandler({ imageBlobStore: blob });
    const { helpers, emitted } = makeHelpers();
    await handler.execute(
      makeNode({ layers: [{ kind: 'brightness', value: 1.2 }] }),
      { scatter: { parentImageId: 'src_evt' } },
      helpers,
    );

    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
    const payload = produced!.payload as Record<string, unknown>;
    expect(payload.imageId).toBeDefined();
    expect(payload.bbox).toBeUndefined();
    expect(payload.stepId).toBe('step_layer');
  });

  // ── Failure paths ──────────────────────────────────────────────────────

  it('fails when source imageId cannot be resolved', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageAdjustLayerStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ layers: [] }),
      {},
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/failed to resolve source imageId/);
  });

  it('fails when source image is missing from blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageAdjustLayerStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ layers: [] }),
      { scatter: { parentImageId: 'ghost' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/not found in blob store/);
  });

  it('fails with unknown layer kind', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_bad_layer', bytes, meta);

    const handler = createImageAdjustLayerStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      // Cast to any to inject bad layer
      makeNode({ layers: [{ kind: 'posterize', value: 2 }] as never }), // 'sharpen' became a real kind (photo editor)
      { scatter: { parentImageId: 'src_bad_layer' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/unknown layer kind/);
  });

  it('fails when blob-store put throws', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_layer_put_fail', bytes, meta);

    const throwingBlob = {
      get: blob.get.bind(blob),
      getMetadata: blob.getMetadata.bind(blob),
      put: async () => {
        throw new Error('DDB write timeout');
      },
    };

    const handler = createImageAdjustLayerStepHandler({ imageBlobStore: throwingBlob });
    const outcome = await handler.execute(
      makeNode({ layers: [] }),
      { scatter: { parentImageId: 'src_layer_put_fail' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/blob-store put failed/);
  });
});
