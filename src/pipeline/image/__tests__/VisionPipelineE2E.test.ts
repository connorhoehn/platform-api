// platform-api/src/pipeline/image/__tests__/VisionPipelineE2E.test.ts
//
// End-to-end test driving the FULL vision pipeline through
// PipelineExecutor: trigger → image-source → vision-analyze → scatter →
// crop. All five custom step handlers participate; Sharp runs for real
// against an InMemoryImageBlobStore; only the Anthropic SDK is stubbed
// so we can pin a deterministic structured output.
//
// What this test proves:
//   - The handler registry's wiring (scatter + image:crop + image-source
//     + image:vision-analyze) is correctly merged into the executor's
//     customStepHandlers map.
//   - The fan-out semantics from distributed-core's PipelineExecutor
//     (StepOutcome.fanOut → arm-suffixed stepIds) work with the
//     real handler instances.
//   - The Anthropic SDK is replaceable end-to-end through the
//     VisionClientLike interface (no real network).
//   - Sharp can extract per-arm crops from a real PNG round-trip via
//     the in-memory blob store.
//   - The arm step records carry distinct imageIds and the produced
//     image bytes are actually different (one crop per detected region).

import { describe, it, expect } from '@jest/globals';
import sharp from 'sharp';
import { randomUUID } from 'crypto';
import { EventBus } from 'distributed-core';
import {
  PipelineExecutor,
  type StepHandler,
} from 'distributed-core/applications/pipeline';
import type {
  PipelineDefinition,
  PipelineEdge,
  PipelineEventMap,
  PipelineNode,
} from 'distributed-core/applications/pipeline';
import type { LLMClient } from 'distributed-core/applications/pipeline';
import { createScatterStepHandler } from '../ScatterStepHandler';
import { createImageCropStepHandler } from '../ImageCropStepHandler';
import { createImageSourceStepHandler } from '../ImageSourceStepHandler';
import { createVisionStepHandler } from '../VisionStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { ImageMeta } from '../types';
import type { VisionClientLike, VisionClientCallResult } from '../VisionClient';

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

/** Trivial pub/sub for the EventBus — captures published events for assertions. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function makeLocalPubSub(): any {
  const handlers: Map<
    string,
    (topic: string, payload: unknown, meta: Record<string, unknown>) => Promise<void>
  > = new Map();
  let subCounter = 0;
  return {
    subscribe(
      _topic: string,
      h: (topic: string, payload: unknown, meta: Record<string, unknown>) => Promise<void>,
    ) {
      subCounter++;
      const id = `sub-${subCounter}`;
      handlers.set(id, h);
      return Promise.resolve(id);
    },
    unsubscribe(id: string) {
      handlers.delete(id);
      return Promise.resolve();
    },
    async publish(topic: string, payload: unknown, meta: Record<string, unknown> = {}) {
      for (const h of handlers.values()) await h(topic, payload, meta);
    },
    subscribeAll(
      h: (topic: string, payload: unknown, meta: Record<string, unknown>) => Promise<void>,
    ) {
      subCounter++;
      const id = `sub-all-${subCounter}`;
      handlers.set(id, h);
      return Promise.resolve(id);
    },
  };
}

function makeEventBus(): EventBus<PipelineEventMap> {
  return new EventBus<PipelineEventMap>(makeLocalPubSub(), 'test-node', {
    topic: 'pipeline.e2e',
  });
}

const fakeLLMClient: LLMClient = {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  stream: () => ({ async *[Symbol.asyncIterator]() { /* none */ } }) as any,
};

/**
 * Vision client stub that returns a fixed structured output. Bypasses the
 * Anthropic SDK entirely so the test is deterministic + offline.
 */
function makeStubVisionClient(regions: Array<{ x: number; y: number; width: number; height: number; label: string }>): VisionClientLike {
  return {
    async analyze(_input, _onToken): Promise<VisionClientCallResult> {
      return {
        output: {
          caption: `Detected ${regions.length} regions.`,
          extracted: { regions },
        },
        usage: { input_tokens: 100, output_tokens: 50 },
        rawText: JSON.stringify({ regions }),
        latencyMs: 200,
        cacheHit: false,
      };
    },
  };
}

/**
 * Build a real PNG fixture so Sharp can perform actual crops. 200×150
 * RGB checkerboard so every crop region produces a different buffer.
 */
async function makeSourceImage(): Promise<{ bytes: Buffer; meta: ImageMeta }> {
  const width = 200;
  const height = 150;
  const pixels = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 3;
      // Color depends on the quadrant + a high-frequency pattern so crops
      // from different regions have measurably different content.
      const quadrantR = x < width / 2 ? 220 : 60;
      const quadrantG = y < height / 2 ? 220 : 60;
      const checker = ((x ^ y) & 3) === 0 ? 50 : 0;
      pixels[i] = quadrantR + checker;
      pixels[i + 1] = quadrantG + checker;
      pixels[i + 2] = 128 + checker;
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
 * Build the e2e pipeline definition.
 *   trigger → image-source → vision → scatter → crop
 */
function buildE2ePipeline(): PipelineDefinition {
  const now = new Date().toISOString();
  const triggerNode: PipelineNode = {
    id: 'trigger',
    type: 'trigger',
    position: { x: 0, y: 0 },
    data: { type: 'trigger', triggerType: 'manual' },
  };
  const sourceNode: PipelineNode = {
    id: 'source',
    type: 'image-source' as never,
    position: { x: 200, y: 0 },
    data: { type: 'image-source', mode: 'trigger', triggerPath: '$.trigger.imageRef' } as never,
  };
  const visionNode: PipelineNode = {
    id: 'vision',
    type: 'image:vision-analyze' as never,
    position: { x: 400, y: 0 },
    data: {
      type: 'image:vision-analyze',
      imageRefPath: '$.steps.source.imageId',
      userPrompt: 'Find regions of interest.',
      outputSchema: {
        type: 'object',
        properties: {
          regions: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                x: { type: 'number' },
                y: { type: 'number' },
                width: { type: 'number' },
                height: { type: 'number' },
                label: { type: 'string' },
              },
            },
          },
        },
      },
    } as never,
  };
  const scatterNode: PipelineNode = {
    id: 'scatter',
    type: 'scatter' as never,
    position: { x: 600, y: 0 },
    data: {
      type: 'scatter',
      itemsPath: '$.steps.vision.extracted.regions',
      itemAlias: 'region',
    } as never,
  };
  const cropNode: PipelineNode = {
    id: 'crop',
    type: 'image:crop' as never,
    position: { x: 800, y: 0 },
    data: {
      type: 'image:crop',
      sourceImagePath: '$.steps.source.imageId',
      bboxPath: '$.region',
    } as never,
  };
  const edges: PipelineEdge[] = [
    { id: 'e1', source: 'trigger', sourceHandle: 'out', target: 'source', targetHandle: 'in' },
    { id: 'e2', source: 'source', sourceHandle: 'out', target: 'vision', targetHandle: 'in' },
    { id: 'e3', source: 'vision', sourceHandle: 'out', target: 'scatter', targetHandle: 'in' },
    { id: 'e4', source: 'scatter', sourceHandle: 'out', target: 'crop', targetHandle: 'in' },
  ];
  return {
    id: 'def-vision-e2e-' + randomUUID().slice(0, 8),
    name: 'vision-e2e',
    version: 1,
    status: 'published',
    nodes: [triggerNode, sourceNode, visionNode, scatterNode, cropNode],
    edges,
    createdBy: 'test',
    createdAt: now,
    updatedAt: now,
  };
}

// ---------------------------------------------------------------------------
// E2E test
// ---------------------------------------------------------------------------

describe('Vision pipeline E2E — image-source → vision → scatter → crop', () => {
  it('produces N distinct crop arms with persisted imageIds', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeSourceImage();
    const sourceImageId = 'src_e2e_1';
    await blob.put(sourceImageId, bytes, meta);

    // Three regions, each pointing at a different quadrant of the source.
    const regions = [
      { x: 10, y: 10, width: 80, height: 60, label: 'top-left' },
      { x: 110, y: 10, width: 80, height: 60, label: 'top-right' },
      { x: 10, y: 80, width: 80, height: 60, label: 'bottom-left' },
    ];

    const customStepHandlers = new Map<string, StepHandler>();
    customStepHandlers.set('scatter', createScatterStepHandler());
    customStepHandlers.set('image:crop', createImageCropStepHandler({ imageBlobStore: blob }));
    customStepHandlers.set('image-source', createImageSourceStepHandler({ imageBlobStore: blob }));
    customStepHandlers.set(
      'image:vision-analyze',
      createVisionStepHandler({
        visionClient: makeStubVisionClient(regions),
        imageBlobStore: blob,
        repository: { persistVisionResult: async () => {} },
        pipelineIdLookup: () => 'pl_e2e',
      }),
    );

    const bus = makeEventBus();
    await bus.start();

    const events: Array<{ type: string; payload: unknown }> = [];
    bus.subscribe('pipeline:step:fanout', async (env) => {
      const payload = (env as unknown as { payload: unknown }).payload;
      events.push({ type: 'pipeline:step:fanout', payload });
    });
    bus.subscribe('pipeline:step:image:produced', async (env) => {
      const payload = (env as unknown as { payload: unknown }).payload;
      events.push({ type: 'pipeline:step:image:produced', payload });
    });

    const executor = new PipelineExecutor({
      definition: buildE2ePipeline(),
      ownerNodeId: 'test-node',
      llmClient: fakeLLMClient,
      eventBus: bus,
      speedMultiplier: 0.02,
      triggerPayload: { trigger: { imageRef: sourceImageId } },
      customStepHandlers,
    });

    const run = await executor.run();
    expect(run.status).toBe('completed');

    // Trigger / source / vision / scatter run once each under bare ids.
    expect(run.steps['trigger']).toBeDefined();
    expect(run.steps['source']?.status).toBe('completed');
    expect(run.steps['vision']?.status).toBe('completed');
    expect(run.steps['scatter']?.status).toBe('completed');

    // Three crop arms — synthesized stepIds crop:1 / crop:2 / crop:3.
    const cropArmKeys = Object.keys(run.steps)
      .filter((k) => k.startsWith('crop'))
      .sort();
    expect(cropArmKeys).toEqual(['crop:1', 'crop:2', 'crop:3']);
    for (const armKey of cropArmKeys) {
      expect(run.steps[armKey].status).toBe('completed');
    }

    // Each arm produced a distinct imageId.
    const armImageIds = cropArmKeys.map(
      (k) => (run.steps[k].output as Record<string, unknown>).imageId as string,
    );
    expect(new Set(armImageIds).size).toBe(3);

    // Each crop landed in the blob store with the expected dimensions.
    for (let i = 0; i < cropArmKeys.length; i++) {
      const stored = await blob.get(armImageIds[i]);
      expect(stored).not.toBeNull();
      expect(stored!.meta.width).toBe(regions[i].width);
      expect(stored!.meta.height).toBe(regions[i].height);
    }

    // The three crops' bytes differ — Sharp actually extracted distinct
    // regions of the source rather than re-emitting the same buffer.
    // PNG header bytes overlap; hash the FULL buffer so the dedup check
    // measures actual content difference.
    const { createHash } = await import('crypto');
    const armBuffers = await Promise.all(armImageIds.map((id) => blob.get(id)));
    const hashes = armBuffers.map((b) =>
      createHash('sha256').update(b!.body).digest('hex'),
    );
    expect(new Set(hashes).size).toBe(3);

    // Lifecycle events. Exactly one fanout fired (on the scatter source).
    const fanoutEvents = events.filter((e) => e.type === 'pipeline:step:fanout');
    expect(fanoutEvents).toHaveLength(1);
    expect((fanoutEvents[0].payload as Record<string, unknown>).stepId).toBe('scatter');
    expect((fanoutEvents[0].payload as Record<string, unknown>).count).toBe(3);

    // One image:produced per crop arm (3) + one per source (image-source).
    // The exact count is N crops + 1 source. Both share the same event type.
    const imageEvents = events.filter((e) => e.type === 'pipeline:step:image:produced');
    expect(imageEvents).toHaveLength(4);
    const cropImageEvents = imageEvents.filter(
      (e) =>
        typeof (e.payload as Record<string, unknown>).stepId === 'string' &&
        ((e.payload as Record<string, unknown>).stepId as string).startsWith('crop:'),
    );
    expect(cropImageEvents).toHaveLength(3);
    // Each crop image event carries the bbox that drove the extraction.
    const bboxLabels = cropImageEvents
      .map((e) => {
        const bbox = (e.payload as Record<string, unknown>).bbox as Record<string, number>;
        return `${bbox.x},${bbox.y},${bbox.width},${bbox.height}`;
      })
      .sort();
    const expectedBboxLabels = regions
      .map((r) => `${r.x},${r.y},${r.width},${r.height}`)
      .sort();
    expect(bboxLabels).toEqual(expectedBboxLabels);
  });

  it('produces zero arms when vision returns an empty regions array', async () => {
    const blob = new InMemoryImageBlobStore();
    const { bytes, meta } = await makeSourceImage();
    await blob.put('src_e2e_2', bytes, meta);

    const customStepHandlers = new Map<string, StepHandler>();
    customStepHandlers.set('scatter', createScatterStepHandler());
    customStepHandlers.set('image:crop', createImageCropStepHandler({ imageBlobStore: blob }));
    customStepHandlers.set('image-source', createImageSourceStepHandler({ imageBlobStore: blob }));
    customStepHandlers.set(
      'image:vision-analyze',
      createVisionStepHandler({
        visionClient: makeStubVisionClient([]),
        imageBlobStore: blob,
        repository: { persistVisionResult: async () => {} },
        pipelineIdLookup: () => 'pl_e2e_2',
      }),
    );

    const bus = makeEventBus();
    await bus.start();

    const executor = new PipelineExecutor({
      definition: buildE2ePipeline(),
      ownerNodeId: 'test-node',
      llmClient: fakeLLMClient,
      eventBus: bus,
      speedMultiplier: 0.02,
      triggerPayload: { trigger: { imageRef: 'src_e2e_2' } },
      customStepHandlers,
    });

    const run = await executor.run();
    expect(run.status).toBe('completed');
    // No crop arms because scatter.items=[] → no fan-out dispatch.
    const cropArms = Object.keys(run.steps).filter((k) => k.startsWith('crop'));
    expect(cropArms).toEqual([]);
  });
});
