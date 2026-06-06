// platform-api/src/pipeline/image/__tests__/ImageEditPromptStepHandler.test.ts

import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import type { BedrockRuntimeClient } from '@aws-sdk/client-bedrock-runtime';
import { createImageEditPromptStepHandler } from '../ImageEditPromptStepHandler';
import { InMemoryImageBlobStore, IMAGE_BLOB_SIZE_CAP_BYTES } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_edit',
    type: 'image:edit-prompt' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image:edit-prompt', ...data } as never,
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
    runId: 'run_edit',
    ownerNodeId: 'node_edit',
    stepId: 'step_edit',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

async function makeTestImage(w = 64, h = 64): Promise<{ bytes: Buffer; meta: ImageMeta }> {
  const pixels = Buffer.alloc(w * h * 3, 150);
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

describe('ImageEditPromptStepHandler.execute', () => {
  // ── Happy path — text-driven mask (maskPrompt) ────────────────────────────

  it('edits a source image with text-driven mask and emits pipeline:step:image:produced', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_edit', bytes, meta);

    const editedBytes = await makeTestImage();
    const client = makeFakeBedrockClient([editedBytes.bytes.toString('base64')]);

    const handler = createImageEditPromptStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const { helpers, emitted } = makeHelpers();

    const outcome = await handler.execute(
      makeNode({ prompt: 'replace background with galaxy' }),
      { scatter: { parentImageId: 'src_edit' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(typeof out.imageId).toBe('string');
    expect(out.parentImageId).toBe('src_edit');
    expect(out.materialized).toBe(true);
    expect(typeof out.mimeType).toBe('string');

    // Stored in blob
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();

    // Event emitted
    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
    const payload = produced!.payload as Record<string, unknown>;
    expect(payload.imageId).toBe(out.imageId);
    expect(payload.stepId).toBe('step_edit');

    // Bedrock called once
    expect((client.send as jest.Mock).mock.calls).toHaveLength(1);

    // Request body uses INPAINTING with maskPrompt (not maskImage) because no mask was supplied
    const calls = (client.send as jest.Mock).mock.calls as Array<[{ input: { body: string } }]>;
    const bodyStr = calls[0]![0].input.body as string;
    const body = JSON.parse(bodyStr) as Record<string, unknown>;
    expect(body.taskType).toBe('INPAINTING');
    const params = body.inPaintingParams as Record<string, unknown>;
    expect(params.maskPrompt).toBeDefined();
    expect(params.maskImage).toBeUndefined();
  });

  // ── Explicit mask image ───────────────────────────────────────────────────

  it('uses maskImage when maskImagePath resolves to a blob-store image', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes: srcBytes, meta: srcMeta } = await makeTestImage();
    const { bytes: maskBytes, meta: maskMeta } = await makeTestImage();
    await blob.put('src_masked', srcBytes, srcMeta);
    await blob.put('mask_001', maskBytes, maskMeta);

    const editedBytes = await makeTestImage();
    const client = makeFakeBedrockClient([editedBytes.bytes.toString('base64')]);

    const handler = createImageEditPromptStepHandler({ imageBlobStore: blob, bedrockClient: client });

    const outcome = await handler.execute(
      makeNode({
        prompt: 'fill in with ocean waves',
        maskImagePath: 'mask_001',  // literal imageId (not a valid JSONPath)
      }),
      { scatter: { parentImageId: 'src_masked' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');

    // Bedrock request should have used maskImage instead of maskPrompt
    const calls = (client.send as jest.Mock).mock.calls as Array<[{ input: { body: string } }]>;
    const bodyStr = calls[0]![0].input.body as string;
    const body = JSON.parse(bodyStr) as Record<string, unknown>;
    const params = body.inPaintingParams as Record<string, unknown>;
    expect(params.maskImage).toBeDefined();
    expect(params.maskPrompt).toBeUndefined();
  });

  // ── Template interpolation ────────────────────────────────────────────────

  it('interpolates {{path}} tokens in the prompt', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_tpl', bytes, meta);

    const editedBytes = await makeTestImage();
    const client = makeFakeBedrockClient([editedBytes.bytes.toString('base64')]);

    const handler = createImageEditPromptStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ prompt: 'change sky to {{trigger.skyColor}}' }),
      { scatter: { parentImageId: 'src_tpl' }, trigger: { skyColor: 'purple' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((client.send as jest.Mock).mock.calls).toHaveLength(1);
  });

  // ── 350KB cap-fit ─────────────────────────────────────────────────────────

  it('compresses the edited image if it exceeds 350KB before storing', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_capfit', bytes, meta);

    const largeBytes = await makeLargePng();
    expect(largeBytes.byteLength).toBeGreaterThan(IMAGE_BLOB_SIZE_CAP_BYTES);

    const client = makeFakeBedrockClient([largeBytes.toString('base64')]);
    const handler = createImageEditPromptStepHandler({ imageBlobStore: blob, bedrockClient: client });

    const outcome = await handler.execute(
      makeNode({ prompt: 'fill area with texture' }),
      { scatter: { parentImageId: 'src_capfit' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();
    expect(stored!.body.byteLength).toBeLessThanOrEqual(IMAGE_BLOB_SIZE_CAP_BYTES);
  });

  // ── Missing source ────────────────────────────────────────────────────────

  it('fails when source imageId cannot be resolved from context', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = makeFakeBedrockClient([]);

    const handler = createImageEditPromptStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ prompt: 'any prompt' }),
      {},  // no imageId anywhere
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/failed to resolve source imageId/);
    expect((client.send as jest.Mock).mock.calls).toHaveLength(0);
  });

  it('fails when resolved source image is not in blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = makeFakeBedrockClient([]);

    const handler = createImageEditPromptStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ prompt: 'any prompt' }),
      { scatter: { parentImageId: 'ghost_src' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/not found in blob store/);
  });

  // ── Missing mask image ────────────────────────────────────────────────────

  it('fails when an explicit mask imageId is not in blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_nomask', bytes, meta);
    const client = makeFakeBedrockClient([]);

    const handler = createImageEditPromptStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ prompt: 'any prompt', maskImagePath: 'ghost_mask' }),
      { scatter: { parentImageId: 'src_nomask' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/mask image.*not found/);
  });

  // ── Missing prompt ────────────────────────────────────────────────────────

  it('fails immediately when prompt is missing', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = makeFakeBedrockClient([]);

    const handler = createImageEditPromptStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'any' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/prompt is required/);
    expect((client.send as jest.Mock).mock.calls).toHaveLength(0);
  });

  // ── ThrottlingException ───────────────────────────────────────────────────

  it('maps ThrottlingException to rate-limited failure', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_throttle', bytes, meta);
    const client = makeThrottlingClient();

    const handler = createImageEditPromptStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ prompt: 'any prompt' }),
      { scatter: { parentImageId: 'src_throttle' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/rate limited/);
  });

  // ── Generic Bedrock error ─────────────────────────────────────────────────

  it('surfaces generic Bedrock invocation errors as failed', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_err', bytes, meta);
    const client = makeErrorClient('internal service error');

    const handler = createImageEditPromptStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ prompt: 'any prompt' }),
      { scatter: { parentImageId: 'src_err' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/internal service error/);
  });

  // ── Empty images array ────────────────────────────────────────────────────

  it('fails with content-policy message when Bedrock returns an empty images array', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_cp', bytes, meta);
    const client = makeFakeBedrockClient([]);

    const handler = createImageEditPromptStepHandler({ imageBlobStore: blob, bedrockClient: client });
    const outcome = await handler.execute(
      makeNode({ prompt: 'explicit content' }),
      { scatter: { parentImageId: 'src_cp' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/content policy/);
  });
});
