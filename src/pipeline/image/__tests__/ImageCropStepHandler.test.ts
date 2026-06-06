// platform-api/src/pipeline/image/__tests__/ImageCropStepHandler.test.ts

import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageCropStepHandler } from '../ImageCropStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_crop',
    type: 'image:crop' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image:crop', ...data } as never,
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
    runId: 'run_1',
    ownerNodeId: 'node_test',
    stepId: 'step_crop',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

/**
 * Build a real PNG with a checkerboard pattern so Sharp's extract has
 * something it can actually decode and re-encode. 100×80 px, single channel.
 */
async function makeTestImage(): Promise<{
  bytes: Buffer;
  meta: ImageMeta;
}> {
  const width = 100;
  const height = 80;
  const pixels = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 3;
      const v = ((x ^ y) & 1) === 0 ? 200 : 50;
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

describe('ImageCropStepHandler.execute', () => {
  it('crops the source image and emits pipeline:step:image:produced', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_1', bytes, meta);

    const handler = createImageCropStepHandler({ imageBlobStore: blob });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({}),
      {
        scatter: {
          parentImageId: 'src_1',
          item: { bbox: { x: 10, y: 5, width: 40, height: 30 } },
        },
      },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.imageId).toBeDefined();
    expect(out.parentImageId).toBe('src_1');
    expect(out.width).toBe(40);
    expect(out.height).toBe(30);
    expect(out.mimeType).toBe('image/png');
    expect(out.materialized).toBe(true);

    // The cropped image landed in the blob store.
    const stored = await blob.get(out.imageId as string);
    expect(stored).not.toBeNull();
    expect(stored!.meta.width).toBe(40);
    expect(stored!.meta.height).toBe(30);

    // image:produced event fired with the new imageId + bbox.
    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
    const payload = produced!.payload as Record<string, unknown>;
    expect(payload.imageId).toBe(out.imageId);
    expect(payload.stepId).toBe('step_crop');
    expect(payload.bbox).toEqual({ x: 10, y: 5, width: 40, height: 30 });
  });

  it('uses trigger.imageRef fallback when scatter.parentImageId is absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_fallback', bytes, meta);

    const handler = createImageCropStepHandler({ imageBlobStore: blob });
    const { helpers } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({}),
      {
        trigger: { imageRef: 'src_fallback' },
        scatter: { item: { bbox: { x: 0, y: 0, width: 50, height: 50 } } },
      },
      helpers,
    );
    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).parentImageId).toBe('src_fallback');
  });

  it('accepts x1/y1/x2/y2 corner-form bboxes (detection model shape)', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_2', bytes, meta);

    const handler = createImageCropStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      {
        scatter: {
          parentImageId: 'src_2',
          item: { bbox: { x1: 20, y1: 10, x2: 80, y2: 70 } },
        },
      },
      makeHelpers().helpers,
    );
    expect(outcome.status).toBe('completed');
    const bbox = (outcome.output as Record<string, unknown>).bbox as Record<string, number>;
    expect(bbox.x).toBe(20);
    expect(bbox.y).toBe(10);
    expect(bbox.width).toBe(60);
    expect(bbox.height).toBe(60);
  });

  it('clamps bboxes that overshoot the image bounds', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_3', bytes, meta);

    const handler = createImageCropStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      {
        scatter: {
          parentImageId: 'src_3',
          // Image is 100×80; bbox extends past the right + bottom edges.
          item: { bbox: { x: 80, y: 60, width: 50, height: 50 } },
        },
      },
      makeHelpers().helpers,
    );
    expect(outcome.status).toBe('completed');
    const bbox = (outcome.output as Record<string, unknown>).bbox as Record<string, number>;
    expect(bbox.width).toBeLessThanOrEqual(20);
    expect(bbox.height).toBeLessThanOrEqual(20);
  });

  it('skips persistence when materialize is false', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_4', bytes, meta);

    const handler = createImageCropStepHandler({ imageBlobStore: blob });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ materialize: false }),
      {
        scatter: {
          parentImageId: 'src_4',
          item: { bbox: { x: 0, y: 0, width: 30, height: 30 } },
        },
      },
      helpers,
    );
    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.imageId).toBeUndefined();
    expect(out.materialized).toBe(false);
    // No image:produced event when there's no new imageId to point at.
    expect(emitted.find((e) => e.event === 'pipeline:step:image:produced')).toBeUndefined();
  });

  it('fails when source image is missing', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageCropStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      {
        scatter: {
          parentImageId: 'missing',
          item: { bbox: { x: 0, y: 0, width: 10, height: 10 } },
        },
      },
      makeHelpers().helpers,
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/not found/);
  });

  it('honors a literal data.bbox over the context bboxPath (frozen-run path)', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_frozen', bytes, meta);

    const handler = createImageCropStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({
        // Literal bbox baked into the node config — no scatter context.
        bbox: { x: 5, y: 5, width: 60, height: 40 },
      }),
      {
        // Source is resolvable via the fallback trigger path.
        trigger: { imageRef: 'src_frozen' },
      },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const bbox = (outcome.output as Record<string, unknown>).bbox as Record<string, number>;
    expect(bbox).toEqual({ x: 5, y: 5, width: 60, height: 40 });
  });

  it('data.bbox literal wins over the context bboxPath when both are set', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_both', bytes, meta);

    const handler = createImageCropStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ bbox: { x: 1, y: 2, width: 30, height: 40 } }),
      {
        scatter: {
          parentImageId: 'src_both',
          // Context bbox would be DIFFERENT — handler must ignore it
          // because data.bbox is set.
          item: { bbox: { x: 50, y: 50, width: 20, height: 20 } },
        },
      },
      makeHelpers().helpers,
    );
    expect(outcome.status).toBe('completed');
    const bbox = (outcome.output as Record<string, unknown>).bbox as Record<string, number>;
    expect(bbox).toEqual({ x: 1, y: 2, width: 30, height: 40 });
  });

  it('falls back to bboxPath when data.bbox is malformed (non-fatal coercion)', async () => {
    // A malformed literal `data.bbox` returns null from coerceBBox; the
    // handler should fall through to the context path rather than failing
    // outright. (Defensive: keeps the wsg-frontend's freeze-run from
    // breaking a re-run if a future edit zeroes one of the bbox fields.)
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_fall', bytes, meta);

    const handler = createImageCropStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ bbox: { foo: 'bar' } as never }),
      {
        scatter: {
          parentImageId: 'src_fall',
          item: { bbox: { x: 0, y: 0, width: 25, height: 25 } },
        },
      },
      makeHelpers().helpers,
    );
    expect(outcome.status).toBe('completed');
    const bbox = (outcome.output as Record<string, unknown>).bbox as Record<string, number>;
    expect(bbox.width).toBe(25);
  });

  it('fails when bbox is malformed', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_5', bytes, meta);

    const handler = createImageCropStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      {
        scatter: { parentImageId: 'src_5', item: { bbox: { foo: 'bar' } } },
      },
      makeHelpers().helpers,
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/malformed|missing/);
  });

  it('fails when bbox clamps to zero area', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_6', bytes, meta);

    const handler = createImageCropStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      {
        scatter: {
          parentImageId: 'src_6',
          // bbox entirely past the image edge → clamped to zero width.
          item: { bbox: { x: 200, y: 200, width: 50, height: 50 } },
        },
      },
      makeHelpers().helpers,
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/zero area/);
  });
});
