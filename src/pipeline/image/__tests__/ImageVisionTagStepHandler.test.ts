// platform-api/src/pipeline/image/__tests__/ImageVisionTagStepHandler.test.ts
//
// Lifecycle tests for ImageVisionTagStepHandler. Stubs VisionClientLike
// so tests are deterministic — no real model calls.

import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageVisionTagStepHandler } from '../ImageVisionTagStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { VisionClientCallResult, VisionClientLike } from '../VisionClient';
import type { ImageMeta, VisionResult } from '../types';

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

const IMAGE_BYTES = Buffer.from('jpeg-tag-bytes');
const IMAGE_META: ImageMeta = {
  mimeType: 'image/jpeg',
  sizeBytes: IMAGE_BYTES.byteLength,
  width: 400,
  height: 300,
  uploadedAt: '2026-06-06T00:00:00.000Z',
};

function makeNode(overData: Record<string, unknown> = {}): PipelineNode {
  return {
    id: 'step_tag',
    type: 'image:vision-tag' as never,
    position: { x: 0, y: 0 },
    data: {
      type: 'image:vision-tag',
      imageRefPath: '$.trigger.imageRef',
      ...overData,
    } as never,
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
    runId: 'run_tag',
    ownerNodeId: 'node_tag',
    stepId: 'step_tag',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

function fakeClient(result: VisionClientCallResult): VisionClientLike {
  return { analyze: jest.fn().mockResolvedValue(result) };
}

function successResult(over: Partial<VisionClientCallResult> = {}): VisionClientCallResult {
  return {
    output: {
      caption: 'A park scene.',
      extracted: {
        tags: [
          { tag: 'outdoors', confidence: 0.98 },
          { tag: 'park', confidence: 0.93 },
          { tag: 'trees', confidence: 0.87 },
        ],
        caption: 'A park scene.',
      },
    },
    usage: { input_tokens: 90, output_tokens: 60 },
    rawText: '{"tags":[{"tag":"outdoors","confidence":0.98}],"caption":"A park scene."}',
    latencyMs: 420,
    cacheHit: false,
    ...over,
  };
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

describe('ImageVisionTagStepHandler — happy path', () => {
  it('emits started then analyzed on success', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_tag', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionTagStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_tag',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_tag' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    expect(emitted.map((e) => e.event)).toEqual([
      'pipeline:image:vision:started',
      'pipeline:image:vision:analyzed',
    ]);
    const result = outcome.output as VisionResult;
    expect(result.imageId).toBe('img_tag');
  });

  it('synthesizes the multi-label-tags outputSchema with default maxItems 10', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_tag', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionTagStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_tag',
    });

    const { helpers } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_tag' } }, helpers);

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.outputSchema).toBeDefined();
    const schema = callArg.outputSchema as Record<string, unknown>;
    const props = schema['properties'] as Record<string, unknown>;
    const tags = props['tags'] as Record<string, unknown>;
    expect(tags['maxItems']).toBe(10);
  });

  it('respects per-node maxTags in the synthesized schema', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_tag', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionTagStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_tag',
    });

    const { helpers } = makeHelpers();
    await handler.execute(makeNode({ maxTags: 5 }), { trigger: { imageRef: 'img_tag' } }, helpers);

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    const schema = callArg.outputSchema as Record<string, unknown>;
    const props = schema['properties'] as Record<string, unknown>;
    const tags = props['tags'] as Record<string, unknown>;
    expect(tags['maxItems']).toBe(5);
  });

  it('includes candidateTags hint in the user prompt', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_tag', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionTagStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_tag',
    });

    const { helpers } = makeHelpers();
    await handler.execute(
      makeNode({ candidateTags: ['nature', 'wildlife'] }),
      { trigger: { imageRef: 'img_tag' } },
      helpers,
    );

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.userPrompt).toContain('nature, wildlife');
  });

  it('uses default model claude-haiku-4-5-20251001 when node omits model', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_tag', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionTagStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_tag',
    });

    const { helpers } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_tag' } }, helpers);

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.model).toBe('claude-haiku-4-5-20251001');
  });

  it('uses default maxOutputTokens 512 when node omits it', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_tag', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionTagStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_tag',
    });

    const { helpers } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_tag' } }, helpers);

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.maxOutputTokens).toBe(512);
  });

  it('started event carries hasSchema:true', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_tag', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionTagStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_tag',
    });

    const { helpers, emitted } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_tag' } }, helpers);

    expect(emitted[0].payload).toMatchObject({ hasSchema: true });
  });
});

// ---------------------------------------------------------------------------
// Failure modes
// ---------------------------------------------------------------------------

describe('ImageVisionTagStepHandler — failure modes', () => {
  it('returns failed when imageRefPath does not resolve', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = fakeClient(successResult());
    const handler = createImageVisionTagStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_tag',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(makeNode(), {}, helpers);

    expect(outcome.status).toBe('failed');
    expect(emitted[0].payload).toMatchObject({ reason: 'input-resolution' });
    expect(client.analyze).not.toHaveBeenCalled();
  });

  it('returns failed when imageId is not in blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = fakeClient(successResult());
    const handler = createImageVisionTagStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_tag',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_gone' } },
      helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(emitted[0].payload).toMatchObject({ reason: 'image-fetch' });
  });

  it('returns failed on malformed client output', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_tag', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient({
      output: null,
      usage: { input_tokens: 50, output_tokens: 8 },
      rawText: 'bad-json',
      latencyMs: 180,
      cacheHit: false,
      failureReason: 'malformed',
    });
    const handler = createImageVisionTagStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_tag',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_tag' } },
      helpers,
    );

    expect(outcome.status).toBe('failed');
    const failedEvt = emitted.find((e) => e.event === 'pipeline:image:vision:failed');
    expect(failedEvt?.payload).toMatchObject({ reason: 'malformed' });
  });
});
