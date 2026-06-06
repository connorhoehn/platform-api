// platform-api/src/pipeline/image/__tests__/VisionIntegration.test.ts
//
// End-to-end wiring test: real VisionClient + InMemoryImageBlobStore +
// fake spawn through createVisionStepHandler.
//
// Mirrors the spawn-mock pattern from VisionClient.test.ts.
// The goal is to verify that tokens emitted by the CLI subprocess flow
// through VisionClient's onToken callback → VisionStepHandler's bus
// emit, and that the full StepOutcome carries the expected output.

import { describe, it, expect } from '@jest/globals';
import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';
import type {
  PipelineNode,
  StepHandlerHelpers,
} from 'distributed-core/applications/pipeline';
import { VisionClient } from '../VisionClient';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import { createVisionStepHandler } from '../VisionStepHandler';
import type { ImageMeta } from '../types';

// ─── Fake spawn helpers ────────────────────────────────────────────────

interface FakeChildOpts {
  stdoutLines?: string[];
  stderr?: string;
  exitCode?: number;
}

function makeFakeChild(opts: FakeChildOpts = {}) {
  const stdout = Readable.from(
    (async function* () {
      for (const line of opts.stdoutLines ?? []) {
        yield Buffer.from(line + '\n', 'utf8');
      }
    })(),
  );
  const stderr = new EventEmitter() as EventEmitter & { on: EventEmitter['on'] };
  setImmediate(() => {
    if (opts.stderr) stderr.emit('data', Buffer.from(opts.stderr, 'utf8'));
  });
  const proc = new EventEmitter() as EventEmitter & {
    stdout: Readable;
    stderr: EventEmitter;
    exitCode: number | null;
    signalCode: NodeJS.Signals | null;
    kill: (s?: NodeJS.Signals) => boolean;
  };
  proc.stdout = stdout;
  proc.stderr = stderr;
  proc.exitCode = null;
  proc.signalCode = null;
  proc.kill = () => true;
  setImmediate(() => {
    proc.exitCode = opts.exitCode ?? 0;
    proc.emit('exit', opts.exitCode ?? 0, null);
  });
  return proc;
}

function deltaLine(text: string): string {
  return JSON.stringify({
    type: 'stream_event',
    event: { type: 'content_block_delta', delta: { type: 'text_delta', text } },
  });
}

function resultLine(inputTokens: number, outputTokens: number): string {
  return JSON.stringify({
    type: 'result',
    usage: { input_tokens: inputTokens, output_tokens: outputTokens },
  });
}

// ─── Test fixtures ─────────────────────────────────────────────────────

const IMAGE_BYTES = Buffer.from([0xff, 0xd8, 0xff, 0xe0]); // fake JPEG magic
const IMAGE_META: ImageMeta = {
  mimeType: 'image/jpeg',
  sizeBytes: IMAGE_BYTES.byteLength,
  width: 200,
  height: 150,
  uploadedAt: '2026-06-04T10:00:00.000Z',
  uploadedBy: 'user-test',
};

function makeNode(over: Record<string, unknown> = {}): PipelineNode {
  return {
    id: 'step_vision',
    type: 'image:vision-analyze' as never,
    position: { x: 0, y: 0 },
    data: {
      type: 'image:vision-analyze',
      userPrompt: 'Describe this image.',
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
    runId: 'run_integ_1',
    ownerNodeId: 'node_integ',
    stepId: 'step_integ',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted };
}

// ─── Integration tests ─────────────────────────────────────────────────

describe('VisionIntegration — happy path', () => {
  it('emits started, multiple token, then analyzed; outcome.status === completed', async () => {
    const stdoutLines = [
      deltaLine('A red apple'),
      deltaLine(' on a wooden'),
      deltaLine(' table.'),
      resultLine(120, 25),
    ];
    const spawnImpl = (() =>
      makeFakeChild({ stdoutLines })) as unknown as typeof import('node:child_process').spawn;

    const client = new VisionClient({ spawnImpl });
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_integ_1', IMAGE_BYTES, IMAGE_META);

    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_integ',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_integ_1' } },
      helpers,
    );

    // Status
    expect(outcome.status).toBe('completed');

    // Event order: started → token(s) → analyzed
    const events = emitted.map((e) => e.event);
    expect(events[0]).toBe('pipeline:image:vision:started');
    expect(events.at(-1)).toBe('pipeline:image:vision:analyzed');

    // Token events present (3 deltas)
    const tokenEvents = emitted.filter((e) => e.event === 'pipeline:image:vision:token');
    expect(tokenEvents.length).toBeGreaterThanOrEqual(1);

    // Analyzed event present
    expect(emitted.some((e) => e.event === 'pipeline:image:vision:analyzed')).toBe(true);
  });
});

describe('VisionIntegration — failure path', () => {
  it('spawn returns non-zero exit → emits started then failed; outcome.status === failed', async () => {
    const spawnImpl = (() =>
      makeFakeChild({
        stdoutLines: [],
        stderr: 'internal error from CLI',
        exitCode: 1,
      })) as unknown as typeof import('node:child_process').spawn;

    const client = new VisionClient({ spawnImpl });
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_integ_fail', IMAGE_BYTES, IMAGE_META);

    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_integ',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ imageRefPath: '$.trigger.imageRef' }),
      { trigger: { imageRef: 'img_integ_fail' } },
      helpers,
    );

    expect(outcome.status).toBe('failed');

    const events = emitted.map((e) => e.event);
    expect(events[0]).toBe('pipeline:image:vision:started');
    expect(events).toContain('pipeline:image:vision:failed');
    expect(events).not.toContain('pipeline:image:vision:analyzed');
  });
});

describe('VisionIntegration — token callback end-to-end', () => {
  it('3 text deltas → 3 token events on the bus + step.output.caption matches joined deltas', async () => {
    const deltas = ['Hello', ' vision', ' world'];
    const stdoutLines = [
      ...deltas.map(deltaLine),
      resultLine(50, 10),
    ];
    const spawnImpl = (() =>
      makeFakeChild({ stdoutLines })) as unknown as typeof import('node:child_process').spawn;

    const client = new VisionClient({ spawnImpl });
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_integ_tok', IMAGE_BYTES, IMAGE_META);

    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_integ_tok',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ imageRefPath: '$.trigger.imageRef' }),
      { trigger: { imageRef: 'img_integ_tok' } },
      helpers,
    );

    // Three token bus events
    const tokenEvents = emitted.filter((e) => e.event === 'pipeline:image:vision:token');
    expect(tokenEvents).toHaveLength(3);

    // Delta values match
    expect(tokenEvents.map((e) => (e.payload as { delta: string }).delta)).toEqual(deltas);

    // Outcome caption matches joined deltas
    const joined = deltas.join('');
    expect(outcome.status).toBe('completed');
    const output = outcome.output as { caption: string };
    expect(output.caption).toBe(joined);
  });
});
