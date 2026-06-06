// platform-api/src/pipeline/image/__tests__/VisionStepHandler.test.ts
//
// Lifecycle tests for VisionStepHandler. Drives the handler with a fake
// VisionClient + InMemoryImageBlobStore + stub repository and asserts:
//   - imageRefPath resolution shapes
//   - emit order for success / failure
//   - non-throwing soft-fail per audit rule #1
//   - persistence happens AFTER the analyzed event is composed
//   - VisionResult shape on the StepOutcome carries everything ui-components
//     needs to render.
//
// Code parity with JudgeStepHandler.test.ts.

import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createVisionStepHandler } from '../VisionStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import { VisionClient, type VisionClientCallResult } from '../VisionClient';
import type {
  ImageMeta,
  ImageVisionAnalyzeStepConfig,
  VisionResult,
} from '../types';

// ---------------------------------------------------------------------------
// Test fixtures + builders
// ---------------------------------------------------------------------------

const IMAGE_BYTES = Buffer.from('jpeg-bytes');
const IMAGE_META: ImageMeta = {
  mimeType: 'image/jpeg',
  sizeBytes: IMAGE_BYTES.byteLength,
  width: 100,
  height: 80,
  uploadedAt: '2026-06-04T10:00:00.000Z',
};

function makeNode(over: Partial<ImageVisionAnalyzeStepConfig> = {}): PipelineNode {
  return {
    id: 'step_vision',
    type: 'image:vision-analyze' as never,
    position: { x: 0, y: 0 },
    data: {
      type: 'image:vision-analyze',
      userPrompt: 'Describe this.',
      imageRefPath: '$.trigger.imageRef',
      ...over,
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
    runId: 'run_1',
    ownerNodeId: 'node_test',
    stepId: 'step_vision',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

function fakeClientReturning(result: VisionClientCallResult): VisionClient {
  return { analyze: jest.fn().mockResolvedValue(result) } as unknown as VisionClient;
}

/**
 * Variant that simulates the CLI streaming tokens — invokes the onToken
 * callback the step handler passes in, then resolves with `result`. Used
 * by the streaming test below to assert token events fire.
 */
function fakeStreamingClient(
  result: VisionClientCallResult,
  tokens: string[],
): VisionClient {
  return {
    analyze: jest.fn().mockImplementation(async (
      _input: unknown,
      onToken?: (delta: string) => void,
    ) => {
      for (const t of tokens) {
        onToken?.(t);
      }
      return result;
    }),
  } as unknown as VisionClient;
}

function successResult(over: Partial<VisionClientCallResult> = {}): VisionClientCallResult {
  return {
    output: { caption: 'A red apple.', extracted: null },
    usage: { input_tokens: 100, output_tokens: 20 },
    rawText: 'A red apple.',
    latencyMs: 500,
    cacheHit: false,
    ...over,
  };
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

describe('VisionStepHandler — happy path', () => {
  it('emits started, calls client, persists, then emits analyzed', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeClientReturning(successResult());
    const persistVisionResult = jest.fn().mockResolvedValue(undefined);

    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      repository: { persistVisionResult },
      pipelineIdLookup: () => 'pl_1',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_1' } },
      helpers,
    );

    expect(emitted.map((e) => e.event)).toEqual([
      'pipeline:image:vision:started',
      'pipeline:image:vision:analyzed',
    ]);
    expect(client.analyze).toHaveBeenCalledTimes(1);
    expect(persistVisionResult).toHaveBeenCalledTimes(1);
    expect(outcome.status).toBe('completed');
    expect((outcome.output as VisionResult).caption).toBe('A red apple.');
    expect((outcome.output as VisionResult).imageId).toBe('img_1');
    expect((outcome.output as VisionResult).pipelineId).toBe('pl_1');
  });

  it('passes the abortSignal from helpers through to the client', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeClientReturning(successResult());
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_1' } }, helpers);
    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.abortSignal).toBe(helpers.abortSignal);
  });

  it('renders {{context.foo}} into the user prompt', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeClientReturning(successResult());
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers } = makeHelpers();
    await handler.execute(
      makeNode({ userPrompt: 'Hello {{context.trigger.tone}}, look here.' }),
      { trigger: { imageRef: 'img_1', tone: 'friendly' } },
      helpers,
    );
    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.userPrompt).toBe('Hello friendly, look here.');
  });

  it('renderUserPrompt: leaves placeholder intact when context path is missing', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeClientReturning(successResult());
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers } = makeHelpers();
    await handler.execute(
      makeNode({ userPrompt: 'Hello {{context.trigger.unknown}}, look.' }),
      { trigger: { imageRef: 'img_1' } },
      helpers,
    );
    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    // Missing path means the placeholder text is preserved verbatim.
    expect(callArg.userPrompt).toBe('Hello {{context.trigger.unknown}}, look.');
  });

  it('renderUserPrompt: serializes non-string context values via JSON.stringify', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeClientReturning(successResult());
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers } = makeHelpers();
    await handler.execute(
      makeNode({ userPrompt: 'Count: {{context.trigger.count}}, list: {{context.trigger.items}}' }),
      { trigger: { imageRef: 'img_1', count: 42, items: ['a', 'b'] } },
      helpers,
    );
    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.userPrompt).toBe('Count: 42, list: ["a","b"]');
  });

  it('renderUserPrompt: resolves deeply nested context paths', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeClientReturning(successResult());
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers } = makeHelpers();
    await handler.execute(
      makeNode({ userPrompt: 'Label: {{context.trigger.meta.deep.label}}' }),
      { trigger: { imageRef: 'img_1', meta: { deep: { label: 'pinned' } } } },
      helpers,
    );
    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.userPrompt).toBe('Label: pinned');
  });

  it('resolves imageRefPath to an {imageId} object', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeClientReturning(successResult());
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ imageRefPath: '$.trigger.imageRef' }),
      { trigger: { imageRef: { imageId: 'img_1' } } },
      helpers,
    );
    expect(outcome.status).toBe('completed');
    expect(emitted[0].payload).toMatchObject({ imageId: 'img_1' });
  });

  it('emits pipeline:image:vision:token for each streamed delta with monotonic seq', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeStreamingClient(
      successResult(),
      ['Hello', ' world', '!'],
    );
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers, emitted } = makeHelpers();
    await handler.execute(makeNode(), { trigger: { imageRef: 'img_1' } }, helpers);

    const tokenEvents = emitted.filter((e) => e.event === 'pipeline:image:vision:token');
    expect(tokenEvents).toHaveLength(3);
    expect(tokenEvents.map((e) => (e.payload as { delta: string }).delta)).toEqual(['Hello', ' world', '!']);
    expect(tokenEvents.map((e) => (e.payload as { seq: number }).seq)).toEqual([1, 2, 3]);
    // Token events no longer carry `accumulated` — frontend accumulates locally.
    expect((tokenEvents[0]!.payload as Record<string, unknown>).accumulated).toBeUndefined();
    // Token events no longer carry `at` — drop per-event Date allocation on hot path.
    expect((tokenEvents[0]!.payload as Record<string, unknown>).at).toBeUndefined();
    // Event ordering: started → tokens → analyzed.
    const order = emitted.map((e) => e.event);
    expect(order[0]).toBe('pipeline:image:vision:started');
    expect(order.at(-1)).toBe('pipeline:image:vision:analyzed');
  });

  it('schema mode: passes outputSchema to the client + flags hasSchema:true on started', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeClientReturning(
      successResult({
        output: { caption: 'ok', extracted: { total: 12.5 } },
      }),
    );
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers, emitted } = makeHelpers();
    await handler.execute(
      makeNode({ outputSchema: { type: 'object' } }),
      { trigger: { imageRef: 'img_1' } },
      helpers,
    );
    expect(emitted[0].payload).toMatchObject({ hasSchema: true });
    const callArg = (client.analyze as jest.Mock).mock.calls[0][0];
    expect(callArg.outputSchema).toEqual({ type: 'object' });
  });
});

// ---------------------------------------------------------------------------
// Failure modes (all NON-THROWING — return StepOutcome failed + emit failed)
// ---------------------------------------------------------------------------

describe('VisionStepHandler — failure modes', () => {
  it('emits input-resolution failed when imageRefPath does not resolve', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = fakeClientReturning(successResult());
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(makeNode(), {}, helpers);
    expect(outcome.status).toBe('failed');
    expect(emitted[0].event).toBe('pipeline:image:vision:failed');
    expect(emitted[0].payload).toMatchObject({ reason: 'input-resolution' });
    expect(client.analyze).not.toHaveBeenCalled();
  });

  it('emits image-fetch failed when imageId is not in blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const client = fakeClientReturning(successResult());
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_missing' } },
      helpers,
    );
    expect(outcome.status).toBe('failed');
    expect(emitted[0].event).toBe('pipeline:image:vision:failed');
    expect(emitted[0].payload).toMatchObject({
      reason: 'image-fetch',
      imageId: 'img_missing',
    });
  });

  it('emits malformed failed when client returns failureReason=malformed', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeClientReturning({
      output: null,
      usage: { input_tokens: 100, output_tokens: 5 },
      rawText: 'gibberish',
      latencyMs: 200,
      cacheHit: false,
      failureReason: 'malformed',
    });
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ outputSchema: { type: 'object' } }),
      { trigger: { imageRef: 'img_1' } },
      helpers,
    );
    expect(outcome.status).toBe('failed');
    const failed = emitted.find((e) => e.event === 'pipeline:image:vision:failed');
    expect(failed?.payload).toMatchObject({ reason: 'malformed' });
    // usage propagates even on failure so cost telemetry stays honest
    expect(failed?.payload).toMatchObject({ tokensIn: 100, tokensOut: 5 });
  });

  it('emits aborted failed when client returns failureReason=aborted', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeClientReturning({
      output: null,
      usage: { input_tokens: 0, output_tokens: 0 },
      rawText: '',
      latencyMs: 5,
      cacheHit: false,
      failureReason: 'aborted',
    });
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_1' } },
      helpers,
    );
    expect(outcome.status).toBe('failed');
    const failed = emitted.find((e) => e.event === 'pipeline:image:vision:failed');
    expect(failed?.payload).toMatchObject({ reason: 'aborted' });
  });

  it('emits refusal failed when client returns failureReason=refusal', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeClientReturning({
      output: null,
      usage: { input_tokens: 50, output_tokens: 5 },
      rawText: 'Sorry, I cannot analyze this image.',
      latencyMs: 300,
      cacheHit: false,
      failureReason: 'refusal',
    });
    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_1' } },
      helpers,
    );
    expect(outcome.status).toBe('failed');
    const failed = emitted.find((e) => e.event === 'pipeline:image:vision:failed');
    expect(failed?.payload).toMatchObject({ reason: 'refusal' });
  });

  it('persistence failure does NOT fail the step (result already on bus)', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_1', IMAGE_BYTES, IMAGE_META);
    const client = fakeClientReturning(successResult());
    const persistVisionResult = jest.fn().mockRejectedValue(new Error('ddb boom'));

    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      repository: { persistVisionResult },
      pipelineIdLookup: () => 'pl_1',
    });
    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_1' } },
      helpers,
    );
    expect(outcome.status).toBe('completed');
    expect(emitted.map((e) => e.event)).toContain('pipeline:image:vision:analyzed');
  });
});
