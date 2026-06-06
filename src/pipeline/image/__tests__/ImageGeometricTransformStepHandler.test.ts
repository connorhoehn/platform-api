// platform-api/src/pipeline/image/__tests__/ImageGeometricTransformStepHandler.test.ts

import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageGeometricTransformStepHandler } from '../ImageGeometricTransformStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_transform',
    type: 'image:transform' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image:transform', ...data } as never,
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
    runId: 'run_transform',
    ownerNodeId: 'node_test',
    stepId: 'step_transform',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

/**
 * Build a real 100×80 RGB PNG. Uses an asymmetric pattern so rotate/flip
 * changes produce verifiably different pixel layouts.
 */
async function makeTestImage(): Promise<{ bytes: Buffer; meta: ImageMeta }> {
  const width = 100;
  const height = 80;
  const pixels = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 3;
      // Ramp on x (R), ramp on y (G), constant B — distinct in all axes
      pixels[i] = Math.floor((x / width) * 255);
      pixels[i + 1] = Math.floor((y / height) * 255);
      pixels[i + 2] = 64;
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

/** Extract a single pixel from a PNG buffer. */
async function samplePixel(buf: Buffer, x: number, y: number): Promise<{ r: number; g: number; b: number }> {
  const { data, info } = await sharp(buf).raw().toBuffer({ resolveWithObject: true });
  const ch = info.channels;
  const idx = (y * info.width + x) * ch;
  return { r: data[idx]!, g: data[idx + 1]!, b: data[idx + 2]! };
}

describe('ImageGeometricTransformStepHandler.execute', () => {
  // ── Empty operations (pass-through re-encode) ───────────────────────────

  it('empty operations array produces a re-encoded image with new imageId', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_noop', bytes, meta);

    const handler = createImageGeometricTransformStepHandler({ imageBlobStore: blob });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ operations: [] }),
      { scatter: { parentImageId: 'src_noop' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.imageId).toBeDefined();
    expect(out.imageId).not.toBe('src_noop');
    expect(out.parentImageId).toBe('src_noop');
    expect(out.width).toBe(100);
    expect(out.height).toBe(80);
    expect(out.mimeType).toBe('image/png');
    expect(out.materialized).toBe(true);

    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
  });

  // ── Rotate ─────────────────────────────────────────────────────────────

  it('rotate 90 degrees swaps width and height', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_rot90', bytes, meta);

    const handler = createImageGeometricTransformStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ operations: [{ op: 'rotate', degrees: 90 }] }),
      { scatter: { parentImageId: 'src_rot90' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    // 100×80 rotated 90° → 80×100
    expect(out.width).toBe(80);
    expect(out.height).toBe(100);
  });

  it('rotate 180 degrees preserves dimensions', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_rot180', bytes, meta);

    const handler = createImageGeometricTransformStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ operations: [{ op: 'rotate', degrees: 180 }] }),
      { scatter: { parentImageId: 'src_rot180' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.width).toBe(100);
    expect(out.height).toBe(80);
  });

  it('rotate 180° produces different pixel layout than original', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_rot180_px', bytes, meta);

    const handler = createImageGeometricTransformStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ operations: [{ op: 'rotate', degrees: 180 }] }),
      { scatter: { parentImageId: 'src_rot180_px' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();

    // Top-left pixel of rotated image should equal the bottom-right pixel
    // of the original. Source pixel at (99,79): R≈255, G≈255.
    // After 180° rotation, that pixel appears at (0,0).
    const rotPx = await samplePixel(stored!.body, 0, 0);
    // Original (99,79) R channel ≈ floor(99/100*255) = 254
    expect(rotPx.r).toBeGreaterThan(200);
  });

  // ── Flip / Flop ─────────────────────────────────────────────────────────

  it('flip (vertical) preserves dimensions', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_flip', bytes, meta);

    const handler = createImageGeometricTransformStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ operations: [{ op: 'flip' }] }),
      { scatter: { parentImageId: 'src_flip' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.width).toBe(100);
    expect(out.height).toBe(80);
  });

  it('flop (horizontal) preserves dimensions', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_flop', bytes, meta);

    const handler = createImageGeometricTransformStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ operations: [{ op: 'flop' }] }),
      { scatter: { parentImageId: 'src_flop' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.width).toBe(100);
    expect(out.height).toBe(80);
  });

  it('chained flip + flop equals 180° rotation in pixel content', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    const handler = createImageGeometricTransformStepHandler({ imageBlobStore: blob });

    await blob.put('src_chain_ff', bytes, meta);
    const outFlipFlop = await handler.execute(
      makeNode({ operations: [{ op: 'flip' }, { op: 'flop' }] }),
      { scatter: { parentImageId: 'src_chain_ff' } },
      makeHelpers().helpers,
    );

    await blob.put('src_chain_rot', bytes, meta);
    const outRot180 = await handler.execute(
      makeNode({ operations: [{ op: 'rotate', degrees: 180 }] }),
      { scatter: { parentImageId: 'src_chain_rot' } },
      makeHelpers().helpers,
    );

    expect(outFlipFlop.status).toBe('completed');
    expect(outRot180.status).toBe('completed');

    const ffId = (outFlipFlop.output as Record<string, unknown>).imageId as string;
    const r180Id = (outRot180.output as Record<string, unknown>).imageId as string;
    const ffStored = await blob.get(ffId);
    const r180Stored = await blob.get(r180Id);
    expect(ffStored).not.toBeNull();
    expect(r180Stored).not.toBeNull();

    // Sample top-left pixel — should be the same for both transforms.
    const ffPx = await samplePixel(ffStored!.body, 0, 0);
    const r180Px = await samplePixel(r180Stored!.body, 0, 0);
    expect(ffPx.r).toBe(r180Px.r);
    expect(ffPx.g).toBe(r180Px.g);
  });

  // ── Fallback resolution ─────────────────────────────────────────────────

  it('falls back to trigger.imageRef when scatter.parentImageId is absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_fb', bytes, meta);

    const handler = createImageGeometricTransformStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ operations: [] }),
      { trigger: { imageRef: 'src_fb' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).parentImageId).toBe('src_fb');
  });

  // ── Failure paths ───────────────────────────────────────────────────────

  it('fails on unknown operation kind', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_badop', bytes, meta);

    const handler = createImageGeometricTransformStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ operations: [{ op: 'skew' }] }),
      { scatter: { parentImageId: 'src_badop' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/unknown operation/);
  });

  it('fails when source imageId cannot be resolved', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageGeometricTransformStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ operations: [] }),
      {},
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/failed to resolve source imageId/);
  });

  it('fails when source image is missing from blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageGeometricTransformStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ operations: [{ op: 'flip' }] }),
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

    const handler = createImageGeometricTransformStepHandler({ imageBlobStore: throwingBlob });
    const outcome = await handler.execute(
      makeNode({ operations: [{ op: 'flip' }] }),
      { scatter: { parentImageId: 'src_putfail' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/blob-store put failed/);
  });
});
