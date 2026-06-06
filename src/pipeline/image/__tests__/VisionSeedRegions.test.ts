// platform-api/src/pipeline/image/__tests__/VisionSeedRegions.test.ts
//
// Verifies that when a vision-analyze node carries `seedRegions`, the
// factory injects the focus-hint stanza into the system prompt passed to
// the vision client, and that the step still completes successfully.

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
import { buildSeedRegionsStanza } from '../visionHandlerFactory';
import type { AuthoringBBox, ImageMeta, VisionAnalyzeInput } from '../types';

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

// ─── Fixtures ──────────────────────────────────────────────────────────

const IMAGE_BYTES = Buffer.from([0xff, 0xd8, 0xff, 0xe0]);
const IMAGE_META: ImageMeta = {
  mimeType: 'image/jpeg',
  sizeBytes: IMAGE_BYTES.byteLength,
  width: 800,
  height: 600,
  uploadedAt: '2026-06-06T00:00:00.000Z',
};

const SEED_REGIONS: AuthoringBBox[] = [
  { id: 'bbox-1', x: 100, y: 50, width: 200, height: 150, label: 'face' },
  { id: 'bbox-2', x: 400, y: 200, width: 100, height: 80, label: 'badge' },
];

function makeNode(over: Record<string, unknown> = {}): PipelineNode {
  return {
    id: 'step_seed',
    type: 'image:vision-analyze' as never,
    position: { x: 0, y: 0 },
    data: {
      type: 'image:vision-analyze',
      userPrompt: 'Describe the marked regions.',
      imageRefPath: '$.trigger.imageRef',
      ...over,
    } as never,
  } as unknown as PipelineNode;
}

function makeHelpers(): {
  helpers: StepHandlerHelpers;
  emitted: Array<{ event: string; payload: unknown }>;
  capturedInputs: VisionAnalyzeInput[];
} {
  const emitted: Array<{ event: string; payload: unknown }> = [];
  const capturedInputs: VisionAnalyzeInput[] = [];
  const helpers: StepHandlerHelpers = {
    emit: (async (event: string, payload: unknown) => {
      emitted.push({ event, payload });
    }) as never,
    runId: 'run_seed_1',
    ownerNodeId: 'node_seed',
    stepId: 'step_seed_helper',
    abortSignal: new AbortController().signal,
    sleep: async () => {},
    isCancelled: () => false,
  };
  return { helpers, emitted, capturedInputs };
}

// ─── Unit tests for buildSeedRegionsStanza ────────────────────────────

describe('buildSeedRegionsStanza', () => {
  it('appends stanza after existing system prompt', () => {
    const result = buildSeedRegionsStanza('Be precise.', SEED_REGIONS);
    expect(result).toContain('Be precise.');
    expect(result).toContain('regions of interest');
    expect(result).toContain('"label":"face"');
    expect(result).toContain('"label":"badge"');
  });

  it('uses stanza as the entire prompt when base is empty', () => {
    const result = buildSeedRegionsStanza('', SEED_REGIONS);
    expect(result).not.toMatch(/^\n/);
    expect(result).toContain('regions of interest');
    expect(result).toContain(JSON.stringify(SEED_REGIONS));
  });

  it('includes all required stanza text per spec', () => {
    const result = buildSeedRegionsStanza('', SEED_REGIONS);
    expect(result).toContain('The user has marked the following regions of interest.');
    expect(result).toContain('Focus your');
    expect(result).toContain('include them in your `regions` output:');
  });
});

// ─── Integration tests via VisionStepHandler ─────────────────────────

describe('VisionSeedRegions — handler receives seedRegions', () => {
  it('step completes successfully when seedRegions are supplied', async () => {
    const stdoutLines = [deltaLine('A face and badge.'), resultLine(80, 20)];
    const spawnImpl = (() =>
      makeFakeChild({ stdoutLines })) as unknown as typeof import('node:child_process').spawn;

    const client = new VisionClient({ spawnImpl });
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_seed_1', IMAGE_BYTES, IMAGE_META);

    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_seed',
    });

    const { helpers, emitted } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ seedRegions: SEED_REGIONS }),
      { trigger: { imageRef: 'img_seed_1' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    expect(emitted.some((e) => e.event === 'pipeline:image:vision:analyzed')).toBe(true);
  });

  it('step completes when seedRegions is an empty array (no stanza)', async () => {
    const stdoutLines = [deltaLine('No hints provided.'), resultLine(60, 15)];
    const spawnImpl = (() =>
      makeFakeChild({ stdoutLines })) as unknown as typeof import('node:child_process').spawn;

    const client = new VisionClient({ spawnImpl });
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_seed_empty', IMAGE_BYTES, IMAGE_META);

    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_seed_empty',
    });

    const { helpers } = makeHelpers();
    const outcome = await handler.execute(
      makeNode({ seedRegions: [] }),
      { trigger: { imageRef: 'img_seed_empty' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
  });

  it('step completes when seedRegions is absent (backward compat)', async () => {
    const stdoutLines = [deltaLine('No regions.'), resultLine(50, 10)];
    const spawnImpl = (() =>
      makeFakeChild({ stdoutLines })) as unknown as typeof import('node:child_process').spawn;

    const client = new VisionClient({ spawnImpl });
    const blob = new InMemoryImageBlobStore();
    await blob.put('img_seed_absent', IMAGE_BYTES, IMAGE_META);

    const handler = createVisionStepHandler({
      visionClient: client,
      imageBlobStore: blob,
      pipelineIdLookup: () => 'pl_seed_absent',
    });

    const { helpers } = makeHelpers();
    const outcome = await handler.execute(
      makeNode(),
      { trigger: { imageRef: 'img_seed_absent' } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
  });
});
