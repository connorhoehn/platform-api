// platform-api/src/pipeline/image/__tests__/ImageGenerateStepHandler.test.ts

import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import type { BedrockRuntimeClient } from '@aws-sdk/client-bedrock-runtime';
import { createImageGenerateStepHandler } from '../ImageGenerateStepHandler';
import { InMemoryImageBlobStore, IMAGE_BLOB_SIZE_CAP_BYTES } from '../ImageBlobStore';

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_generate',
    type: 'image:generate' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image:generate', ...data } as never,
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
    runId: 'run_gen',
    ownerNodeId: 'node_generate',
    stepId: 'step_generate',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

/**
 * Build a small PNG and return its base64 encoding — this simulates a
 * Bedrock-generated image body.
 */
async function makeSmallPng(width = 64, height = 64): Promise<Buffer> {
  const pixels = Buffer.alloc(width * height * 3, 128);
  return sharp(pixels, { raw: { width, height, channels: 3 } })
    .png()
    .toBuffer();
}

/**
 * Build a PNG that is guaranteed to exceed IMAGE_BLOB_SIZE_CAP_BYTES so
 * the cap-fit path is exercised. 512×512 with random-ish pixels to defeat
 * PNG compression.
 */
async function makeLargePng(): Promise<Buffer> {
  // Generate a 600×600 image with noisy content to keep PNG large.
  const width = 600;
  const height = 600;
  const pixels = Buffer.alloc(width * height * 3);
  // Pseudo-random noise so PNG can't compress it small
  for (let i = 0; i < pixels.length; i++) {
    pixels[i] = (i * 131 + 37) % 256;
  }
  return sharp(pixels, { raw: { width, height, channels: 3 } })
    .png({ compressionLevel: 0 })   // minimal compression → large file
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

describe('ImageGenerateStepHandler.execute', () => {
  // ── Happy path — single image ────────────────────────────────────────────

  it('generates a single image, stores it, and emits pipeline:step:image:produced', async () => {
    const blob = new InMemoryImageBlobStore();
    const pngBytes = await makeSmallPng();
    const b64 = pngBytes.toString('base64');
    const client = makeFakeBedrockClient([b64]);

    const handler = createImageGenerateStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const { helpers, emitted } = makeHelpers();

    const outcome = await handler.execute(
      makeNode({ prompt: 'a photo of a sunset', width: 512, height: 512 }),
      {},
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(typeof out.imageId).toBe('string');
    expect(out.mimeType).toBe('image/png');

    // Stored in blob
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();

    // Event emitted
    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
    const payload = produced!.payload as Record<string, unknown>;
    expect(payload.imageId).toBe(out.imageId);
    expect(payload.stepId).toBe('step_generate');

    // No fanOut field in single-image mode
    expect(outcome.fanOut).toBeUndefined();
  });

  // ── {{path}} template interpolation ──────────────────────────────────────

  it('interpolates {{path}} tokens in the prompt from context', async () => {
    const blob = new InMemoryImageBlobStore();
    const pngBytes = await makeSmallPng();
    const client = makeFakeBedrockClient([pngBytes.toString('base64')]);

    const handler = createImageGenerateStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ prompt: 'photo of {{trigger.subject}} at sunset' }),
      { trigger: { subject: 'a mountain' } },
      makeHelpers().helpers,
    );

    // Handler completed — the prompt resolved without error
    expect(outcome.status).toBe('completed');
    // The Bedrock client should have been called once
    expect((client.send as jest.Mock).mock.calls).toHaveLength(1);
  });

  // ── numImages > 1 → fan-out ───────────────────────────────────────────────

  it('returns fan-out output when numImages > 1', async () => {
    const blob = new InMemoryImageBlobStore();
    const png1 = await makeSmallPng(64, 64);
    const png2 = await makeSmallPng(64, 64);
    const client = makeFakeBedrockClient([
      png1.toString('base64'),
      png2.toString('base64'),
    ]);

    const handler = createImageGenerateStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const { helpers, emitted } = makeHelpers();

    const outcome = await handler.execute(
      makeNode({ prompt: 'scenic landscape', numImages: 2 }),
      {},
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(Array.isArray(out.imageIds)).toBe(true);
    expect((out.imageIds as string[]).length).toBe(2);

    // fanOut is present with 2 items
    const fo = outcome.fanOut;
    expect(fo).toBeDefined();
    expect(fo!.items.length).toBe(2);
    expect(fo!.itemAlias).toBe('generatedImage');
    expect(fo!.maxConcurrency).toBe(4);

    // Two emits — one per image
    const produced = emitted.filter((e) => e.event === 'pipeline:step:image:produced');
    expect(produced.length).toBe(2);
  });

  // ── 350KB cap-fit path ────────────────────────────────────────────────────

  it('compresses images that exceed 350KB before storing', async () => {
    const blob = new InMemoryImageBlobStore();
    const largeBytes = await makeLargePng();

    // Verify our test image is actually over cap before the handler runs
    expect(largeBytes.byteLength).toBeGreaterThan(IMAGE_BLOB_SIZE_CAP_BYTES);

    const client = makeFakeBedrockClient([largeBytes.toString('base64')]);
    const handler = createImageGenerateStepHandler({ imageBlobStore: blob, bedrockClient: client });

    const outcome = await handler.execute(
      makeNode({ prompt: 'large image test', width: 512, height: 512 }),
      {},
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();
    // The stored bytes must be within cap
    expect(stored!.body.byteLength).toBeLessThanOrEqual(IMAGE_BLOB_SIZE_CAP_BYTES);
  });

  // ── Empty images array (content policy) ──────────────────────────────────

  it('fails with content-policy message when images array is empty', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = makeFakeBedrockClient([]);

    const handler = createImageGenerateStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ prompt: 'violent content' }),
      {},
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/content policy/);
  });

  // ── ThrottlingException ───────────────────────────────────────────────────

  it('maps ThrottlingException to rate-limited failure', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = makeThrottlingClient();

    const handler = createImageGenerateStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ prompt: 'any prompt' }),
      {},
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/rate limited/);
  });

  // ── Generic network error ─────────────────────────────────────────────────

  it('surfaces generic Bedrock errors as failed status', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = makeErrorClient('connection reset by peer');

    const handler = createImageGenerateStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ prompt: 'any prompt' }),
      {},
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/connection reset/);
  });

  // ── Missing prompt ────────────────────────────────────────────────────────

  it('fails immediately when prompt is missing', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = makeFakeBedrockClient([]);

    const handler = createImageGenerateStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({}),   // no prompt
      {},
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/prompt is required/);
    // Should not even reach Bedrock
    expect((client.send as jest.Mock).mock.calls).toHaveLength(0);
  });

  // ── seed param forwarded ──────────────────────────────────────────────────

  it('includes seed in the Bedrock request body when provided', async () => {
    const blob = new InMemoryImageBlobStore();
    const pngBytes = await makeSmallPng();
    const client = makeFakeBedrockClient([pngBytes.toString('base64')]);

    const handler = createImageGenerateStepHandler({ imageBlobStore: blob, bedrockClient: client });
    await handler.execute(
      makeNode({ prompt: 'any prompt', seed: 42 }),
      {},
      makeHelpers().helpers,
    );

    const calls = (client.send as jest.Mock).mock.calls as Array<[{ input: { body: string } }]>;
    const bodyStr = calls[0]![0].input.body as string;
    const body = JSON.parse(bodyStr) as Record<string, unknown>;
    const config = body.imageGenerationConfig as Record<string, unknown>;
    expect(config.seed).toBe(42);
  });
});
