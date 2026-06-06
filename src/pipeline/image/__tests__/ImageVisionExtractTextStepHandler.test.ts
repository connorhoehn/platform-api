// platform-api/src/pipeline/image/__tests__/ImageVisionExtractTextStepHandler.test.ts
//
// Lifecycle tests for ImageVisionExtractTextStepHandler. Stubs VisionClientLike
// so tests are deterministic — no real model calls.

import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageVisionExtractTextStepHandler } from '../ImageVisionExtractTextStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { VisionClientCallResult, VisionClientLike } from '../VisionClient';
import type { ImageMeta, VisionResult } from '../types';

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

const IMAGE_BYTES = Buffer.from('jpeg-ocr-bytes');
const IMAGE_META: ImageMeta = {
  mimeType: 'image/jpeg',
  sizeBytes: IMAGE_BYTES.byteLength,
  width: 800,
  height: 600,
  uploadedAt: '2026-06-06T00:00:00.000Z',
};

function makeNode(overData: Record<string, unknown> = {}): PipelineNode {
  return {
    id: 'step_ocr',
    type: 'image:vision-extract-text' as never,
    position: { x: 0, y: 0 },
    data: {
      type: 'image:vision-extract-text',
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
    runId: 'run_ocr',
    ownerNodeId: 'node_ocr',
    stepId: 'step_ocr',
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
      caption: 'Document with invoice text.',
      extracted: {
        fullText: 'Invoice #12345\nTotal: $99.00',
        lines: [
          { text: 'Invoice #12345', confidence: 0.99 },
          { text: 'Total: $99.00', confidence: 0.97 },
        ],
        language: 'en',
        caption: 'Document with invoice text.',
      },
    },
    usage: { input_tokens: 200, output_tokens: 150 },
    rawText: '{"fullText":"Invoice #12345\\nTotal: $99.00","lines":[...],"language":"en","caption":"Document."}',
    latencyMs: 800,
    cacheHit: false,
    ...over,
  };
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

describe('ImageVisionExtractTextStepHandler — happy path', () => {
  it('emits started then analyzed on success', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_ocr', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_ocr',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_ocr' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    expect(emitted.map((e) => e.event)).toEqual([
      'pipeline:image:vision:started',
      'pipeline:image:vision:analyzed',
      'pipeline:step:image:produced',
    ]);
    const result = outcome.output as VisionResult;
    expect(result.imageId).toBe('img_ocr');
  });

  it('synthesizes the ocr-records outputSchema', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_ocr', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_ocr',
    });

    const { helpers } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_ocr' } }, helpers);

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.outputSchema).toBeDefined();
    const schema = callArg.outputSchema as Record<string, unknown>;
    expect(schema['required']).toEqual(
      expect.arrayContaining(['fullText', 'lines', 'caption']),
    );
  });

  it('started event carries hasSchema:true', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_ocr', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_ocr',
    });

    const { helpers, emitted } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_ocr' } }, helpers);

    expect(emitted[0].payload).toMatchObject({ hasSchema: true });
  });

  it('uses default model claude-sonnet-4-6 when node omits model', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_ocr', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_ocr',
    });

    const { helpers } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_ocr' } }, helpers);

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.model).toBe('claude-sonnet-4-6');
  });

  it('uses default maxOutputTokens 4096 when node omits it', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_ocr', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_ocr',
    });

    const { helpers } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_ocr' } }, helpers);

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.maxOutputTokens).toBe(4096);
  });

  it('uses the default OCR system prompt when node omits systemPrompt', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_ocr', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_ocr',
    });

    const { helpers } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_ocr' } }, helpers);

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.systemPrompt).toMatch(/OCR assistant/i);
    expect(callArg.systemPrompt).toMatch(/verbatim/i);
  });

  it('uses custom systemPrompt when node supplies one', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_ocr', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_ocr',
    });

    const { helpers } = makeHelpers();
    await handler.execute(
      makeNode({ systemPrompt: 'Custom OCR instructions here.' }),
      { trigger: { imageRef: 'img_ocr' } },
      helpers,
    );

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.systemPrompt).toBe('Custom OCR instructions here.');
  });

  it('uses the fixed OCR user prompt regardless of context', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_ocr', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_ocr',
    });

    const { helpers } = makeHelpers();
    await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_ocr', extra: 'ignored' } },
      helpers,
    );

    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.userPrompt).toBe('Transcribe all visible text in this image.');
  });

  it('persistence failure does not fail the step', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_ocr', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient(successResult());
    const persistVisionResult = jest.fn().mockRejectedValue(new Error('ddb-boom'));
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      repository: { persistVisionResult },
      pipelineIdLookup: () => 'pl_ocr',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_ocr' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    expect(emitted.map((e) => e.event)).toContain('pipeline:image:vision:analyzed');
  });
});

// ---------------------------------------------------------------------------
// Failure modes
// ---------------------------------------------------------------------------

describe('ImageVisionExtractTextStepHandler — failure modes', () => {
  it('returns failed when imageRefPath does not resolve', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = fakeClient(successResult());
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_ocr',
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
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_ocr',
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

  it('returns failed on malformed client output with usage propagated', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_ocr', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient({
      output: null,
      usage: { input_tokens: 300, output_tokens: 20 },
      rawText: 'not valid json',
      latencyMs: 500,
      cacheHit: false,
      failureReason: 'malformed',
    });
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_ocr',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_ocr' } },
      helpers,
    );

    expect(outcome.status).toBe('failed');
    const failedEvt = emitted.find((e) => e.event === 'pipeline:image:vision:failed');
    expect(failedEvt?.payload).toMatchObject({ reason: 'malformed', tokensIn: 300, tokensOut: 20 });
  });

  it('returns failed when client returns refusal', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_ocr', IMAGE_BYTES, IMAGE_META);
    const client = fakeClient({
      output: null,
      usage: { input_tokens: 100, output_tokens: 5 },
      rawText: 'I cannot process this document.',
      latencyMs: 300,
      cacheHit: false,
      failureReason: 'refusal',
    });
    const handler = createImageVisionExtractTextStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_ocr',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_ocr' } },
      helpers,
    );

    expect(outcome.status).toBe('failed');
    const failedEvt = emitted.find((e) => e.event === 'pipeline:image:vision:failed');
    expect(failedEvt?.payload).toMatchObject({ reason: 'refusal' });
  });
});
