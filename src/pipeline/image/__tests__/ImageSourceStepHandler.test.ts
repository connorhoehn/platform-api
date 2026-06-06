// platform-api/src/pipeline/image/__tests__/ImageSourceStepHandler.test.ts

import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { createImageSourceStepHandler } from '../ImageSourceStepHandler';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import type { ImageMeta } from '../types';

const TEST_BYTES = Buffer.from('fake-png-bytes');
const TEST_META: ImageMeta = {
  mimeType: 'image/png',
  sizeBytes: TEST_BYTES.length,
  width: 800,
  height: 600,
  uploadedAt: '2026-06-06T00:00:00.000Z',
};

function makeNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'step_source',
    type: 'image-source' as never,
    position: { x: 0, y: 0 },
    data: { type: 'image-source', ...data } as never,
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
    stepId: 'step_source',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

describe('ImageSourceStepHandler.execute', () => {
  it('default mode resolves from $.trigger.imageRef and emits image:produced', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('src_t', TEST_BYTES, TEST_META);
    const handler = createImageSourceStepHandler({ imageBlobStore: blob });
    const { helpers, emitted } = makeHelpers();

    const outcome = await handler.execute(
      makeNode({}),
      { trigger: { imageRef: 'src_t' } },
      helpers,
    );
    expect(outcome.status).toBe('completed');
    const out = outcome.output as Record<string, unknown>;
    expect(out.imageId).toBe('src_t');
    expect(out.width).toBe(800);
    expect(out.height).toBe(600);
    expect(out.mimeType).toBe('image/png');

    const produced = emitted.find((e) => e.event === 'pipeline:step:image:produced');
    expect(produced).toBeDefined();
    expect((produced!.payload as Record<string, unknown>).imageId).toBe('src_t');
    expect((produced!.payload as Record<string, unknown>).stepId).toBe('step_source');
  });

  it('mode=path resolves from a custom JSONPath', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('src_p', TEST_BYTES, TEST_META);
    const handler = createImageSourceStepHandler({ imageBlobStore: blob });

    const outcome = await handler.execute(
      makeNode({ mode: 'path', imagePath: '$.upload.id' }),
      { upload: { id: 'src_p' } },
      makeHelpers().helpers,
    );
    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).imageId).toBe('src_p');
  });

  it('mode=literal uses node.data.imageId verbatim', async () => {
    const blob = new InMemoryImageBlobStore();
    await blob.put('src_l', TEST_BYTES, TEST_META);
    const handler = createImageSourceStepHandler({ imageBlobStore: blob });

    const outcome = await handler.execute(
      makeNode({ mode: 'literal', imageId: 'src_l' }),
      {},
      makeHelpers().helpers,
    );
    expect(outcome.status).toBe('completed');
    expect((outcome.output as Record<string, unknown>).imageId).toBe('src_l');
  });

  it('fails when mode=path but imagePath is missing', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageSourceStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({ mode: 'path' }),
      {},
      makeHelpers().helpers,
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/imagePath/);
  });

  it('fails when the resolved imageId is not in the blob store', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageSourceStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      { trigger: { imageRef: 'missing' } },
      makeHelpers().helpers,
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/not found/);
  });

  it('fails when the resolved value is not a string', async () => {
    const blob = new InMemoryImageBlobStore();
    const handler = createImageSourceStepHandler({ imageBlobStore: blob });
    const outcome = await handler.execute(
      makeNode({}),
      { trigger: { imageRef: 42 } },
      makeHelpers().helpers,
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/failed to resolve/);
  });
});
