// platform-api/src/pipeline/image/__tests__/ImageSaveStepHandler.test.ts

import sharp from 'sharp';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageSaveStepHandler, resolveTemplate } from '../ImageSaveStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_save',
    type: 'image:save' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image:save', ...data } as never,
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
    runId: 'run_save',
    ownerNodeId: 'node_test',
    stepId: 'step_save',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

async function makeTestImage(): Promise<{ bytes: Buffer; meta: ImageMeta }> {
  const width = 100;
  const height = 80;
  const pixels = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 3;
      const v = ((x ^ y) & 1) === 0 ? 200 : 50;
      pixels[i] = v; pixels[i + 1] = v; pixels[i + 2] = v;
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

// ── resolveTemplate unit tests ─────────────────────────────────────────────

describe('resolveTemplate', () => {
  it('replaces a simple token', () => {
    const ctx = { scatter: { index: 3 } } as unknown as Record<string, unknown>;
    expect(resolveTemplate('item-{{scatter.index}}.jpg', ctx)).toBe('item-3.jpg');
  });

  it('replaces multiple tokens', () => {
    const ctx = {
      scatter: { index: 1, item: { label: 'cat' } },
    } as unknown as Record<string, unknown>;
    expect(
      resolveTemplate('crop-{{scatter.item.label}}-{{scatter.index}}.jpg', ctx),
    ).toBe('crop-cat-1.jpg');
  });

  it('renders missing path as empty string (no error)', () => {
    const ctx = {} as Record<string, unknown>;
    expect(resolveTemplate('{{scatter.index}}.jpg', ctx)).toBe('.jpg');
  });

  it('handles null context value as empty string', () => {
    const ctx = { foo: null } as unknown as Record<string, unknown>;
    expect(resolveTemplate('{{foo}}.txt', ctx)).toBe('.txt');
  });

  it('leaves non-token text unchanged', () => {
    const ctx = {} as Record<string, unknown>;
    expect(resolveTemplate('static-name.jpg', ctx)).toBe('static-name.jpg');
  });

  it('handles template with no tokens', () => {
    const ctx = { a: 'b' } as unknown as Record<string, unknown>;
    expect(resolveTemplate('no-tokens-here', ctx)).toBe('no-tokens-here');
  });

  it('converts numeric values to string', () => {
    const ctx = { n: 42 } as unknown as Record<string, unknown>;
    expect(resolveTemplate('file-{{n}}.png', ctx)).toBe('file-42.png');
  });
});

// ── ImageSaveStepHandler.execute ────────────────────────────────────────────

describe('ImageSaveStepHandler.execute', () => {
  // ── Core contract ──────────────────────────────────────────────────────

  it('returns the source imageId unchanged (no new blob materialized)', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_save_1', bytes, meta);

    const handler = createImageSaveStepHandler({ imageBlobStore: blob });
    const { helpers } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'src_save_1' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    // imageId must be the SAME as the source — no new blob was written
    expect(out.imageId).toBe('src_save_1');
    expect(out.width).toBe(100);
    expect(out.height).toBe(80);
    expect(out.mimeType).toBe('image/png');
    // no parentImageId field — save is not a transform
    expect(out.parentImageId).toBeUndefined();
  });

  it('does NOT write a new row to the blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_save_count', bytes, meta);

    // Spy on put — it should never be called
    const putSpy = jest.spyOn(blob, 'put');

    const handler = createImageSaveStepHandler({ imageBlobStore: blob });
    await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'src_save_count' } },
      makeHelpers().helpers,
    );

    expect(putSpy).not.toHaveBeenCalled();
  });

  // ── Emitted event ──────────────────────────────────────────────────────

  it('emits pipeline:step:image:produced with the source imageId', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_save_emit', bytes, meta);

    const handler = createImageSaveStepHandler({ imageBlobStore: blob });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'src_save_emit' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
    const payload = produced!.payload as Record<string, unknown>;
    expect(payload.imageId).toBe('src_save_emit');
    expect(payload.stepId).toBe('step_save');
    expect(payload.width).toBe(100);
    expect(payload.height).toBe(80);
    expect(payload.mimeType).toBe('image/png');
    // No bbox on save — not a crop
    expect(payload.bbox).toBeUndefined();
  });

  // ── Filename templating ────────────────────────────────────────────────

  it('resolves filenameTemplate tokens from context', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_tmpl_1', bytes, meta);

    const handler = createImageSaveStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ filenameTemplate: 'crop-{{scatter.item.label}}-{{scatter.index}}.jpg' }),
      {
        scatter: {
          parentImageId: 'src_tmpl_1',
          index: 2,
          item: { label: 'dog' },
        },
      },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.filename).toBe('crop-dog-2.jpg');
  });

  it('renders missing template values as empty string', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_tmpl_miss', bytes, meta);

    const handler = createImageSaveStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ filenameTemplate: '{{scatter.item.label}}-{{scatter.index}}.png' }),
      { scatter: { parentImageId: 'src_tmpl_miss' } }, // no label or index
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    // Both tokens missing → empty strings
    expect(out.filename).toBe('-.png');
  });

  it('omits filename from output when filenameTemplate is not set', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_no_tmpl', bytes, meta);

    const handler = createImageSaveStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'src_no_tmpl' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.filename).toBeUndefined();
  });

  // ── Fallback source resolution ─────────────────────────────────────────

  it('falls back to trigger.imageRef when scatter.parentImageId is absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_save_fb', bytes, meta);

    const handler = createImageSaveStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      { trigger: { imageRef: 'src_save_fb' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).imageId).toBe('src_save_fb');
  });

  it('falls back to trigger.imageId when imageRef is absent', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_save_fb2', bytes, meta);

    const handler = createImageSaveStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      { trigger: { imageId: 'src_save_fb2' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).imageId).toBe('src_save_fb2');
  });

  // ── Failure paths ──────────────────────────────────────────────────────

  it('fails when source imageId cannot be resolved', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageSaveStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      {},
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/failed to resolve source imageId/);
  });

  it('fails when source image is missing from blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageSaveStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'ghost_id' } },
      makeHelpers().helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/not found in blob store/);
  });

  it('does not throw when the emit subscriber throws', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeTestImage();
    await blob.put('src_save_bad_emit', bytes, meta);

    const handler = createImageSaveStepHandler({ imageBlobStore: blob });
    const throwingHelpers: StepHandlerHelpers = {
      emit: async () => {
        throw new Error('subscriber crashed');
      },
      runId: 'run_1',
      ownerNodeId: 'node_test',
      stepId: 'step_save',
      abortSignal: new AbortController().signal,
      sleep: async () => {},
      isCancelled: () => false,
    };

    const outcome = await handler.execute(
      makeNode({}),
      { scatter: { parentImageId: 'src_save_bad_emit' } },
      throwingHelpers,
    );

    // Should still complete successfully despite emit throw
    expect(outcome.status).toBe('completed');
  });
});
