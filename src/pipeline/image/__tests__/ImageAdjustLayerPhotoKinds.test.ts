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

import { adjustRgb, applyPixelLayers, curveLut, paramsFromLayers } from '../adjustPixels';

async function flatImage(v: number, width = 40, height = 20): Promise<Buffer> {
  return sharp(Buffer.alloc(width * height * 3, v), { raw: { width, height, channels: 3 } }).png().toBuffer();
}
const meta = (w = 40, h = 20): ImageMeta => ({ mimeType: 'image/png', sizeBytes: 1, width: w, height: h, uploadedAt: new Date().toISOString() });
async function pixelAt(store: InMemoryImageBlobStore, imageId: string, x: number, y: number): Promise<number[]> {
  const got = await store.get(imageId);
  const { data, info } = await sharp(got!.body).raw().toBuffer({ resolveWithObject: true });
  const i = (y * info.width + x) * info.channels;
  return [data[i]!, data[i + 1]!, data[i + 2]!];
}

describe('adjustPixels (mirror of the editor preview)', () => {
  it('matches the preview reference values', () => {
    // The same numbers ui-components adjustRgb gives for these inputs.
    expect(adjustRgb([0.5, 0.5, 0.5], paramsFromLayers([{ kind: 'exposure', stops: 1 }]))).toEqual([1, 1, 1]);
    const warm = adjustRgb([0.4, 0.4, 0.4], paramsFromLayers([{ kind: 'temperature', kelvin: 12000 }]));
    expect(warm[0]).toBeCloseTo(0.48, 5);
    expect(warm[2]).toBeCloseTo(0.32, 5);
    const lifted = adjustRgb([0.1, 0.1, 0.1], paramsFromLayers([{ kind: 'shadows', amount: 100 }]));
    expect(lifted[0]).toBeGreaterThan(0.25);
    expect(adjustRgb([0.9, 0.9, 0.9], paramsFromLayers([{ kind: 'shadows', amount: 100 }]))[0]).toBeCloseTo(0.9, 5);
  });

  it('builds a curve LUT through its points and blends through a mask', () => {
    const lut = curveLut([[0, 0], [128, 64], [255, 255]]);
    expect([lut[0], lut[128], lut[255]]).toEqual([0, 64, 255]);
    const px = Buffer.from([100, 100, 100, 100, 100, 100]);
    applyPixelLayers(px, 2, 1, 3, [{ kind: 'exposure', stops: 1 }], new Uint8Array([255, 0]));
    expect([...px]).toEqual([200, 200, 200, 100, 100, 100]);
  });
});

describe('image:adjust-layer photo kinds', () => {
  it('renders exposure / shadows / temperature / curve / vignette / sharpen in one list', async () => {
    const store = new InMemoryImageBlobStore();
    await store.put('src', await flatImage(100), meta());
    const handler = createImageAdjustLayerStepHandler({ imageBlobStore: store });
    const layers: AdjustLayer[] = [
      { kind: 'exposure', stops: 0.5 }, { kind: 'shadows', amount: 20 }, { kind: 'temperature', kelvin: 7000 },
      { kind: 'curve', channel: 'rgb', points: [[0, 0], [255, 255]] }, { kind: 'vignette', amount: -50 }, { kind: 'sharpen', amount: 40 },
    ];
    const { helpers } = makeHelpers();
    const out = await handler.execute(makeNode({ type: 'image:adjust-layer', layers }), { imageId: 'src' }, helpers);
    expect(out.status).toBe('completed');
    const id = (out as any).output.imageId as string;
    const centre = await pixelAt(store, id, 20, 10);
    const corner = await pixelAt(store, id, 0, 0);
    expect(centre[0]).toBeGreaterThan(centre[2]!); // warmer
    expect(centre[1]).toBeGreaterThan(100);        // brighter
    expect(corner[1]).toBeLessThan(centre[1]!);    // vignetted
  });

  it('applies a masked layer inside its mask only (maskId resolved through node masks)', async () => {
    const store = new InMemoryImageBlobStore();
    await store.put('src', await flatImage(100), meta());
    const maskPng = await sharp({ create: { width: 40, height: 20, channels: 3, background: '#000' } })
      .composite([{ input: await sharp({ create: { width: 20, height: 20, channels: 3, background: '#fff' } }).png().toBuffer(), left: 0, top: 0 }])
      .png().toBuffer();
    await store.put('mask-abc', maskPng, meta());
    const handler = createImageAdjustLayerStepHandler({ imageBlobStore: store });
    const { helpers } = makeHelpers();
    const out = await handler.execute(
      makeNode({ type: 'image:adjust-layer', layers: [{ kind: 'exposure', stops: 1, maskId: 'left' }], masks: { left: 'mask-abc' } }),
      { imageId: 'src' }, helpers,
    );
    expect(out.status).toBe('completed');
    const id = (out as any).output.imageId as string;
    expect((await pixelAt(store, id, 5, 10))[0]).toBe(200);
    expect((await pixelAt(store, id, 35, 10))[0]).toBe(100);
  });

  it('fails a layer whose mask cannot be resolved', async () => {
    const store = new InMemoryImageBlobStore();
    await store.put('src', await flatImage(100), meta());
    const handler = createImageAdjustLayerStepHandler({ imageBlobStore: store });
    const { helpers } = makeHelpers();
    const out = await handler.execute(makeNode({ type: 'image:adjust-layer', layers: [{ kind: 'shadows', amount: 10, maskId: 'nope' }] }), { imageId: 'src' }, helpers);
    expect(out.status).toBe('failed');
    expect((out as any).error).toMatch(/mask 'nope'/);
  });
});
