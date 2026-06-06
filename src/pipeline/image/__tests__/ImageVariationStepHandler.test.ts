// platform-api/src/pipeline/image/__tests__/ImageVariationStepHandler.test.ts

import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import type { BedrockRuntimeClient } from '@aws-sdk/client-bedrock-runtime';
import { createImageVariationStepHandler } from '../ImageVariationStepHandler';
import { InMemoryImageBlobStore, IMAGE_BLOB_SIZE_CAP_BYTES } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_variation',
    type: 'image:variation' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image:variation', ...data } as never,
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
    runId: 'run_variation',
    ownerNodeId: 'node_variation',
    stepId: 'step_variation',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

async function makeTestImage(w = 64, h = 64): Promise<{ bytes: Buffer; meta: ImageMeta }> {
  const pixels = Buffer.alloc(w * h * 3, 100);
  const bytes = await sharp(pixels, { raw: { width: w, height: h, channels: 3 } })
    .png()
    .toBuffer();
  return {
    bytes,
    meta: { mimeType: 'image/png', sizeBytes: bytes.length, width: w, height: h, uploadedAt: '2026-06-06T00:00:00.000Z' },
  };
}

async function makeLargePng(): Promise<Buffer> {
  const width = 600;
  const height = 600;
  const pixels = Buffer.alloc(width * height * 3);
  for (let i = 0; i < pixels.length; i++) {
    pixels[i] = (i * 131 + 37) % 256;
  }
  return sharp(pixels, { raw: { width, height, channels: 3 } })
    .png({ compressionLevel: 0 })
    .toBuffer();
}

function makeFakeBedrockClient(images: string[]): BedrockRuntimeClient {
  return {
    send: jest.fn().mockResolvedValue({
      body: Buffer.from(JSON.stringify({ images })),
    }),
  } as unknown as BedrockRuntimeClient;
}

function makeThrottlingClient(): BedrockRuntimeClient {
  const err = Object.assign(new Error('Too many requests'), {
    name: 'ThrottlingException',
  });
  return {
    send: jest.fn().mockRejectedValue(err),
  } as unknown as BedrockRuntimeClient;
}

function makeErrorClient(message: string): BedrockRuntimeClient {
  return {
    send: jest.fn().mockRejectedValue(new Error(message)),
  } as unknown as BedrockRuntimeClient;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('ImageVariationStepHandler.execute', () => {
  // ── Happy path ────────────────────────────────────────────────────────────

  it('generates variations, stores them, emits events, and returns fan-out', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_var', bytes, meta);

    const var1 = await makeTestImage();
    const var2 = await makeTestImage();
    const client = makeFakeBedrockClient([
      var1.bytes.toString('base64'),
      var2.bytes.toString('base64'),
    ]);

    const handler = createImageVariationStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const { helpers, emitted } = makeHelpers();

    const outcome = await handler.execute(
      makeNode({ numVariations: 2, similarity: 0.7 }),
      { scatter: { parentImageId: 'src_var' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    const ids = out.imageIds as string[];
    expect(ids).toHaveLength(2);
    expect(out.parentImageId).toBe('src_var');

    // Fan-out shape
    const fo = (outcome as Record<string, unknown>).fanOut as Record<string, unknown>;
    expect(fo).toBeDefined();
    expect((fo.items as unknown[]).length).toBe(2);
    expect(fo.itemAlias).toBe('generatedImage');

    // Both stored
    for (const id of ids) {
      const stored = await blob.get(id);
      expect(stored).not.toBeNull();
    }

    // Two emits
    const produced = emitted.filter((e) => e.event === 'pipeline:step:image:produced');
    expect(produced.length).toBe(2);
  });

  // ── Source fallback paths ─────────────────────────────────────────────────

  it('falls back to trigger.imageRef when scatter.parentImageId is absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_fb', bytes, meta);

    const variationBytes = await makeTestImage();
    const client = makeFakeBedrockClient([variationBytes.bytes.toString('base64')]);

    const handler = createImageVariationStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ numVariations: 1 }),
      { trigger: { imageRef: 'src_fb' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).parentImageId).toBe('src_fb');
  });

  it('falls back to trigger.imageId when imageRef is absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_fb2', bytes, meta);

    const variationBytes = await makeTestImage();
    const client = makeFakeBedrockClient([variationBytes.bytes.toString('base64')]);

    const handler = createImageVariationStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ numVariations: 1 }),
      { trigger: { imageId: 'src_fb2' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).parentImageId).toBe('src_fb2');
  });

  // ── 350KB cap-fit ─────────────────────────────────────────────────────────

  it('compresses variation images that exceed 350KB before storing', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_cap', bytes, meta);

    const largeBytes = await makeLargePng();
    expect(largeBytes.byteLength).toBeGreaterThan(IMAGE_BLOB_SIZE_CAP_BYTES);

    const client = makeFakeBedrockClient([largeBytes.toString('base64')]);
    const handler = createImageVariationStepHandler({ imageBlobStore: blob, bedrockClient: client });

    const outcome = await handler.execute(
      makeNode({ numVariations: 1 }),
      { scatter: { parentImageId: 'src_cap' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const ids = (outcome.output as Record<string, unknown>).imageIds as string[];
    const stored = await blob.get(ids[0]!);
    expect(stored).not.toBeNull();
    expect(stored!.body.byteLength).toBeLessThanOrEqual(IMAGE_BLOB_SIZE_CAP_BYTES);
  });

  // ── Missing source ────────────────────────────────────────────────────────

  it('fails when source imageId cannot be resolved', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = makeFakeBedrockClient([]);

    const handler = createImageVariationStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({}),
      {},  // empty context — no imageId anywhere
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/failed to resolve source imageId/);
    expect((client.send as jest.Mock).mock.calls).toHaveLength(0);
  });

  it('fails when resolved source image is not in blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = makeFakeBedrockClient([]);

    const handler = createImageVariationStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'ghost_image' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/not found in blob store/);
    expect((client.send as jest.Mock).mock.calls).toHaveLength(0);
  });

  // ── Empty images array ────────────────────────────────────────────────────

  it('fails with content-policy message when images array is empty', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_cp', bytes, meta);
    const client = makeFakeBedrockClient([]);

    const handler = createImageVariationStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'src_cp' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/content policy/);
  });

  // ── ThrottlingException ───────────────────────────────────────────────────

  it('maps ThrottlingException to rate-limited failure', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_throttle', bytes, meta);
    const client = makeThrottlingClient();

    const handler = createImageVariationStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'src_throttle' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/rate limited/);
  });

  // ── Generic Bedrock error ─────────────────────────────────────────────────

  it('surfaces generic Bedrock errors as failed status', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_err', bytes, meta);
    const client = makeErrorClient('model not found');

    const handler = createImageVariationStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'src_err' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/model not found/);
  });
});
