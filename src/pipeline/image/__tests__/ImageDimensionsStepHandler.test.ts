import sharp from 'sharp';
import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import { createImageDimensionsStepHandler } from '../ImageDimensionsStepHandler';
import { createImageGenerateStepHandler } from '../ImageGenerateStepHandler';
import type { ImageGenClient, ImageGenRequest } from '../ImageGenClient';

function makeHelpers(): StepHandlerHelpers {
  return {
    emit: (async () => {}) as never,
    runId: 'r', ownerNodeId: 'n', stepId: 's',
    abortSignal: new AbortController().signal,
    sleep: async () => {}, isCancelled: () => false,
  } as unknown as StepHandlerHelpers;
}

function node(type: string, id: string, data: Record<string, unknown>): PipelineNode {
  return { id, type, position: { x: 0, y: 0 }, data: { type, ...data } } as unknown as PipelineNode;
}

async function seedImage(blob: InMemoryImageBlobStore, id: string, w: number, h: number) {
  const png = await sharp(Buffer.alloc(w * h * 3, 100), { raw: { width: w, height: h, channels: 3 } })
    .png().toBuffer();
  await blob.put(id, png, { mimeType: 'image/png', sizeBytes: png.length, width: w, height: h, uploadedAt: 'now' });
}

/** Captures the request so we can assert resolved+snapped dims. */
function capturingGenClient(): { client: ImageGenClient; last: () => ImageGenRequest | null } {
  let last: ImageGenRequest | null = null;
  const tiny = Buffer.from('x').toString('base64');
  return {
    client: {
      provider: 'test',
      async generate(req) { last = req; return { images: [tiny] }; },
    },
    last: () => last,
  };
}

describe('image:dimensions', () => {
  it('writes exact dims, aspect ratio, and snapped generation dims', async () => {
    const blob = new InMemoryImageBlobStore();
    await seedImage(blob, 'img1', 1920, 1080);
    const handler = createImageDimensionsStepHandler({ imageBlobStore: blob });

    const outcome = await handler.execute(
      node('image:dimensions', 'dims1', {}),
      { imageId: 'img1' },
      makeHelpers(),
    );

    expect(outcome.status).toBe('completed');
    const o = outcome.output as Record<string, number>;
    expect(o.width).toBe(1920);
    expect(o.height).toBe(1080);
    expect(o.aspectRatio).toBeCloseTo(1.7778, 4);
    // 1920x1080 (16:9) snapped under 1024 → 1024x576
    expect(o.genWidth).toBe(1024);
    expect(o.genHeight).toBe(576);
  });

  it('resolves the source via trigger.imageRef fallback', async () => {
    const blob = new InMemoryImageBlobStore();
    await seedImage(blob, 'imgA', 800, 800);
    const handler = createImageDimensionsStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      node('image:dimensions', 'd', {}),
      { trigger: { imageRef: 'imgA' } },
      makeHelpers(),
    );
    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, number>).genWidth).toBe(800);
  });

  it('fails clearly when the image is absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageDimensionsStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      node('image:dimensions', 'd', {}),
      { imageId: 'missing' },
      makeHelpers(),
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/not found/);
  });
});

describe('image:generate dynamic dimensions', () => {
  it('resolves a {{path}} width/height binding from context and snaps it', async () => {
    const blob = new InMemoryImageBlobStore();
    const { client, last } = capturingGenClient();
    const handler = createImageGenerateStepHandler({ imageBlobStore: blob, imageGenClient: client });

    await handler.execute(
      node('image:generate', 'gen', {
        prompt: 'a landscape',
        width: '{{steps.dims1.output.genWidth}}',
        height: '{{steps.dims1.output.genHeight}}',
      }),
      { steps: { dims1: { output: { genWidth: 1024, genHeight: 576 } } } },
      makeHelpers(),
    );

    expect(last()).toMatchObject({ width: 1024, height: 576 });
  });

  it('snaps a numeric width to a valid multiple of 16', async () => {
    const blob = new InMemoryImageBlobStore();
    const { client, last } = capturingGenClient();
    const handler = createImageGenerateStepHandler({ imageBlobStore: blob, imageGenClient: client });
    await handler.execute(
      node('image:generate', 'gen', { prompt: 'p', width: 500, height: 500 }),
      {},
      makeHelpers(),
    );
    expect(last()).toMatchObject({ width: 496, height: 496 });
  });

  it('falls back to 512 when a binding resolves to nothing', async () => {
    const blob = new InMemoryImageBlobStore();
    const { client, last } = capturingGenClient();
    const handler = createImageGenerateStepHandler({ imageBlobStore: blob, imageGenClient: client });
    await handler.execute(
      node('image:generate', 'gen', { prompt: 'p', width: '{{steps.missing.output.genWidth}}' }),
      {},
      makeHelpers(),
    );
    expect(last()).toMatchObject({ width: 512, height: 512 });
  });
});
