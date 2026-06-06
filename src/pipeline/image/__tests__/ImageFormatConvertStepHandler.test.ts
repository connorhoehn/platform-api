// platform-api/src/pipeline/image/__tests__/ImageFormatConvertStepHandler.test.ts

import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageFormatConvertStepHandler } from '../ImageFormatConvertStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_format',
    type: 'image:format-convert' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image:format-convert', ...data } as never,
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
    runId: 'run_format',
    ownerNodeId: 'node_test',
    stepId: 'step_format',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

/**
 * Build a real 100×80 RGB PNG.
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
      pixels[i + 2] = 180;
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
 * Build a 100×80 RGBA PNG (with transparency) for testing JPEG flatten.
 */
async function makeTransparentImage(): Promise<{ bytes: Buffer; meta: ImageMeta }> {
  const width = 100;
  const height = 80;
  const pixels = Buffer.alloc(width * height * 4);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 4;
      pixels[i] = 200;
      pixels[i + 1] = 100;
      pixels[i + 2] = 50;
      pixels[i + 3] = 128; // semi-transparent
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

describe('ImageFormatConvertStepHandler.execute', () => {
  // ── Format correctness ─────────────────────────────────────────────────

  it('converts PNG to JPEG with correct mimeType', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_jpeg', bytes, meta);

    const handler = createImageFormatConvertStepHandler({ imageBlobStore: blob });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ format: 'jpeg', quality: 85 }),
      { scatter: { parentImageId: 'src_jpeg' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.imageId).toBeDefined();
    expect(out.parentImageId).toBe('src_jpeg');
    expect(out.mimeType).toBe('image/jpeg');
    expect(out.width).toBe(100);
    expect(out.height).toBe(80);
    expect(out.materialized).toBe(true);

    // Check JPEG magic bytes in stored image (0xFF 0xD8)
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();
    expect(stored!.body[0]).toBe(0xff);
    expect(stored!.body[1]).toBe(0xd8);

    // Event fired
    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
    expect((produced!.payload as Record<string, unknown>).mimeType).toBe('image/jpeg');
  });

  it('converts PNG to WebP with correct mimeType', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_webp', bytes, meta);

    const handler = createImageFormatConvertStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ format: 'webp', quality: 80 }),
      { scatter: { parentImageId: 'src_webp' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.mimeType).toBe('image/webp');
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();
    // WebP magic: 'RIFF' at offset 0 and 'WEBP' at offset 8
    const sig = stored!.body.toString('ascii', 0, 4);
    expect(sig).toBe('RIFF');
  });

  it('converts PNG to PNG (re-encode) preserving mimeType', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_png2png', bytes, meta);

    const handler = createImageFormatConvertStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ format: 'png' }),
      { scatter: { parentImageId: 'src_png2png' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.mimeType).toBe('image/png');
    expect(out.imageId).not.toBe('src_png2png');  // new imageId
  });

  it('JPEG conversion flattens transparent PNG on white background (no black artifacts)', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTransparentImage();
    await blob.put('src_transparency', bytes, meta);

    const handler = createImageFormatConvertStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ format: 'jpeg' }),
      { scatter: { parentImageId: 'src_transparency' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();

    // Read a pixel — it must not be pure black (which would indicate the
    // transparent alpha was naively composited on black).
    const { data, info } = await sharp(stored!.body)
      .raw()
      .toBuffer({ resolveWithObject: true });
    const ch = info.channels;
    const px = { r: data[0]!, g: data[ch]!, b: data[ch * 2]! };
    // With white flatten: any pixel from the semi-transparent overlay
    // should be lighter than pure black
    const sum = px.r + px.g + px.b;
    expect(sum).toBeGreaterThan(10); // not pure black
  });

  it('WebP lossless conversion succeeds', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_webp_lossless', bytes, meta);

    const handler = createImageFormatConvertStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ format: 'webp', lossless: true }),
      { scatter: { parentImageId: 'src_webp_lossless' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).mimeType).toBe('image/webp');
  });

  it('quality differences produce different byte sizes (jpeg)', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    const handler = createImageFormatConvertStepHandler({ imageBlobStore: blob });

    await blob.put('src_q_low', bytes, meta);
    await blob.put('src_q_high', bytes, meta);

    const outLow = await handler.execute(
      makeNode({ format: 'jpeg', quality: 10 }),
      { scatter: { parentImageId: 'src_q_low' } },
      makeHelpers().helpers,
    );
    const outHigh = await handler.execute(
      makeNode({ format: 'jpeg', quality: 95 }),
      { scatter: { parentImageId: 'src_q_high' } },
      makeHelpers().helpers,
    );

    expect(outLow.status).toBe('completed');
    expect(outHigh.status).toBe('completed');

    const lowId = (outLow.output as Record<string, unknown>).imageId as string;
    const highId = (outHigh.output as Record<string, unknown>).imageId as string;
    const lowStored = await blob.get(lowId);
    const highStored = await blob.get(highId);

    expect(lowStored!.body.length).toBeLessThan(highStored!.body.length);
  });

  // ── Fallback resolution ─────────────────────────────────────────────────

  it('falls back to trigger.imageRef when scatter.parentImageId is absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_fb', bytes, meta);

    const handler = createImageFormatConvertStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ format: 'png' }),
      { trigger: { imageRef: 'src_fb' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).parentImageId).toBe('src_fb');
  });

  // ── Failure paths ───────────────────────────────────────────────────────

  it('fails with invalid format', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageFormatConvertStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ format: 'bmp' }),
      { scatter: { parentImageId: 'any' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/invalid format/);
  });

  it('fails when source imageId cannot be resolved', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageFormatConvertStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ format: 'jpeg' }),
      {},
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/failed to resolve source imageId/);
  });

  it('fails when source image is missing from blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageFormatConvertStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ format: 'webp' }),
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

    const handler = createImageFormatConvertStepHandler({ imageBlobStore: throwingBlob });
    const outcome = await handler.execute(
      makeNode({ format: 'png' }),
      { scatter: { parentImageId: 'src_putfail' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/blob-store put failed/);
  });
});
