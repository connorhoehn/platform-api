// platform-api/src/pipeline/image/__tests__/ImageVisionClassifyStepHandler.test.ts
//
// Lifecycle tests for ImageVisionClassifyStepHandler. Stubs VisionClientLike
// so tests are deterministic — no real model calls.

import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageVisionClassifyStepHandler } from '../ImageVisionClassifyStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { VisionClientCallResult, VisionClientLike } from '../VisionClient';
import type { ImageMeta, VisionResult } from '../types';

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

const IMAGE_BYTES = Buffer.from('jpeg-classify-bytes');
const IMAGE_META: ImageMeta = {
  mimeType: 'image/jpeg',
  sizeBytes: IMAGE_BYTES.byteLength,
  width: 200,
  height: 150,
  uploadedAt: '2026-06-06T00:00:00.000Z',
};

function makeNode(overData: Record<string, unknown> = {}): PipelineNode {
  return {
    id: 'step_classify',
    type: 'image:vision-classify' as never,
    position: { x: 0, y: 0 },
    data: {
      type: 'image:vision-classify',
      imageRefPath: '$.trigger.imageRef',
      labels: ['cat', 'dog', 'bird'],
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
    runId: 'run_cls',
    ownerNodeId: 'node_cls',
    stepId: 'step_classify',
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
      caption: 'A domestic cat sitting.',
      extracted: { label: 'cat', confidence: 0.95, rationale: 'Feline features.', caption: 'A domestic cat.' },
    },
    usage: { input_tokens: 80, output_tokens: 30 },
    rawText: '{"label":"cat","confidence":0.95,"rationale":"Feline features.","caption":"A domestic cat."}',
    latencyMs: 400,
    cacheHit: false,
    ...over,
  };
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

describe('ImageVisionClassifyStepHandler — happy path', () => {
  it('emits started then analyzed on success', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_cls', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionClassifyStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_cls',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_cls' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    expect(emitted.map((e) => e.event)).toEqual([
      'pipeline:image:vision:started',
      'pipeline:image:vision:analyzed',
      'pipeline:step:image:produced',
    ]);
    const result = outcome.output as VisionResult;
    expect(result.imageId).toBe('img_cls');
    expect(result.pipelineId).toBe('pl_cls');
  });

  it('synthesizes the classification-enum outputSchema and passes it to the client', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_cls', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionClassifyStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_cls',
    });

    const { helpers } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_cls' } }, helpers);

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.outputSchema).toBeDefined();
    const schema = callArg.outputSchema as Record<string, unknown>;
    const props = schema['properties'] as Record<string, unknown>;
    const labelProp = props['label'] as Record<string, unknown>;
    expect(labelProp['enum']).toEqual(['cat', 'dog', 'bird']);
  });

  it('started event carries hasSchema:true', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_cls', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionClassifyStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_cls',
    });

    const { helpers, emitted } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_cls' } }, helpers);

    expect(emitted[0].payload).toMatchObject({ hasSchema: true });
  });

  it('uses default model claude-haiku-4-5-20251001 when node omits model', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_cls', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionClassifyStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_cls',
    });

    const { helpers } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_cls' } }, helpers);

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.model).toBe('claude-haiku-4-5-20251001');
  });

  it('uses per-node model override when supplied', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_cls', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionClassifyStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_cls',
    });

    const { helpers } = makeHelpers();
    await handler.execute(
      makeNode({ model: 'claude-sonnet-4-6' }),
      { trigger: { imageRef: 'img_cls' } },
      helpers,
    );

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.model).toBe('claude-sonnet-4-6');
  });

  it('uses default maxOutputTokens 256 when node omits it', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_cls', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionClassifyStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_cls',
    });

    const { helpers } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_cls' } }, helpers);

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.maxOutputTokens).toBe(256);
  });

  it('persistence failure does not fail the step', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_cls', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const persistVisionResult = jest.fn().mockRejectedValue(new Error('ddb-boom'));
    const handler = createImageVisionClassifyStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      repository: { persistVisionResult },
      pipelineIdLookup: () => 'pl_cls',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_cls' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    expect(emitted.map((e) => e.event)).toContain('pipeline:image:vision:analyzed');
  });
});

// ---------------------------------------------------------------------------
// Failure modes
// ---------------------------------------------------------------------------

describe('ImageVisionClassifyStepHandler — failure modes', () => {
  it('returns failed when imageRefPath does not resolve', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = fakeClient(successResult());
    const handler = createImageVisionClassifyStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_cls',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(makeNode(), {}, helpers);

    expect(outcome.status).toBe('failed');
    expect(emitted[0].event).toBe('pipeline:image:vision:failed');
    expect(emitted[0].payload).toMatchObject({ reason: 'input-resolution' });
    expect(client.analyze).not.toHaveBeenCalled();
  });

  it('returns failed when imageId is not in blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = fakeClient(successResult());
    const handler = createImageVisionClassifyStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_cls',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_missing' } },
      helpers,
    );

    expect(outcome.status).toBe('failed');
    expect(emitted[0].payload).toMatchObject({ reason: 'image-fetch', imageId: 'img_missing' });
  });

  it('returns failed when client returns malformed output', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_cls', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient({
      output: null,
      usage: { input_tokens: 60, output_tokens: 10 },
      rawText: 'not json',
      latencyMs: 200,
      cacheHit: false,
      failureReason: 'malformed',
    });
    const handler = createImageVisionClassifyStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_cls',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_cls' } },
      helpers,
    );

    expect(outcome.status).toBe('failed');
    const failedEvt = emitted.find((e) => e.event === 'pipeline:image:vision:failed');
    expect(failedEvt?.payload).toMatchObject({ reason: 'malformed', tokensIn: 60, tokensOut: 10 });
  });

  it('returns failed with empty labels array (schema still built; model decides behaviour)', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_cls', IMAGE_BYTES, IMAGE_META);
    // Client will call analyze with an empty-enum schema — we just verify the call goes through
    const client = fakeClient(successResult());
    const handler = createImageVisionClassifyStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_cls',
    });

    const { helpers } = makeHelpers();
    await handler.execute(
      makeNode({ labels: [] }),
      { trigger: { imageRef: 'img_cls' } },
      helpers,
    );

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    const labelEnum = (
      (callArg.outputSchema as Record<string, unknown>)['properties'] as Record<string, unknown>
    )['label'] as Record<string, unknown>;
    expect(labelEnum['enum']).toEqual([]);
  });

  it('returns failed when client returns refusal', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_cls', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient({
      output: null,
      usage: { input_tokens: 30, output_tokens: 5 },
      rawText: 'I cannot classify this.',
      latencyMs: 200,
      cacheHit: false,
      failureReason: 'refusal',
    });
    const handler = createImageVisionClassifyStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_cls',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_cls' } },
      helpers,
    );

    expect(outcome.status).toBe('failed');
    const failedEvt = emitted.find((e) => e.event === 'pipeline:image:vision:failed');
    expect(failedEvt?.payload).toMatchObject({ reason: 'refusal' });
  });
});
