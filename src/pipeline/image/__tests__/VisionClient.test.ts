// platform-api/src/pipeline/image/__tests__/VisionClient.test.ts
//
// Tests the CLI-subprocess VisionClient by injecting a fake spawn that
// emulates `claude -p --output-format stream-json` line emission. Mirrors
// JudgeClient.test.ts.
//
// Coverage:
//   - pure helpers: extractJsonObject, buildVisionSystemPrompt
//   - caption mode: stream text deltas → caption populated
//   - extract mode: JSON-only response → extracted parsed
//   - token-callback delivery: every text_delta fires onToken(delta, accum)
//   - markdown fences + prose preamble → defensive extractor still parses
//   - malformed extract → one corrective retry, then failureReason=malformed
//   - CLI non-zero exit → failureReason=unknown, stderr in rawText
//   - usage tokens propagate from `result` event
//   - tempfile cleanup happens on success AND failure paths

import { describe, it, expect } from '@jest/globals';
import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';
import { existsSync } from 'node:fs';
import {
  VisionClient,
  extractJsonObject,
  buildVisionSystemPrompt,
} from '../VisionClient';
import type { VisionAnalyzeInput } from '../types';

// ─── Fake child-process helpers ────────────────────────────────────────

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

function resultLine(inputTokens: number, outputTokens: number, extra: Record<string, unknown> = {}): string {
  return JSON.stringify({
    type: 'result',
    usage: { input_tokens: inputTokens, output_tokens: outputTokens },
    ...extra,
  });
}

const SAMPLE_IMAGE = Buffer.from([0xff, 0xd8, 0xff, 0xe0]);

function baseInput(over: Partial<VisionAnalyzeInput> = {}): VisionAnalyzeInput {
  return {
    model: 'sonnet',
    systemPrompt: '',
    userPrompt: 'Describe this image.',
    imageBytes: SAMPLE_IMAGE,
    imageMimeType: 'image/jpeg',
    maxOutputTokens: 1024,
    ...over,
  };
}

// ─── Pure helpers ──────────────────────────────────────────────────────

describe('extractJsonObject', () => {
  it('parses a plain JSON object', () => {
    expect(extractJsonObject('{"a":1}')).toEqual({ a: 1 });
  });

  it('strips prose around the object', () => {
    expect(extractJsonObject('Here is the result: {"caption":"a cat"} done.')).toEqual({ caption: 'a cat' });
  });

  it('strips markdown fences', () => {
    expect(extractJsonObject('```json\n{"caption":"a dog"}\n```')).toEqual({ caption: 'a dog' });
  });

  it('returns null for unparseable text', () => {
    expect(extractJsonObject('not json at all')).toBeNull();
  });

  it('returns null for empty string', () => {
    expect(extractJsonObject('')).toBeNull();
  });

  it('returns null when there is no closing brace', () => {
    expect(extractJsonObject('{"a":1')).toBeNull();
  });
});

describe('buildVisionSystemPrompt', () => {
  it('returns caller prompt verbatim when no schema', () => {
    expect(buildVisionSystemPrompt('CUSTOM', undefined)).toBe('CUSTOM');
  });

  it('falls back to the canned prompt when caller passes undefined', () => {
    const p = buildVisionSystemPrompt(undefined, undefined);
    expect(p).toMatch(/analyze images/i);
  });

  it('appends the schema instructions when a schema is supplied', () => {
    const p = buildVisionSystemPrompt('BASE', { type: 'object' });
    expect(p).toContain('BASE');
    expect(p).toContain('JSON Schema');
    expect(p).toContain('"type": "object"');
  });
});

// ─── CLI happy paths ───────────────────────────────────────────────────

describe('VisionClient.analyze — caption mode', () => {
  it('returns the full text as caption when no schema is set', async () => {
    const stdoutLines = [
      deltaLine('A red apple'),
      deltaLine(' on a wooden table.'),
      resultLine(100, 20),
    ];
    const spawnImpl = (() => makeFakeChild({ stdoutLines })) as unknown as typeof import('node:child_process').spawn;
    const client = new VisionClient({ spawnImpl });
    const r = await client.analyze(baseInput());
    expect(r.failureReason).toBeUndefined();
    expect(r.output?.caption).toBe('A red apple on a wooden table.');
    expect(r.output?.extracted).toBeNull();
    expect(r.usage.input_tokens).toBe(100);
    expect(r.usage.output_tokens).toBe(20);
  });

  it('invokes onToken once per text_delta with accumulated buffer', async () => {
    const stdoutLines = [
      deltaLine('Hello'),
      deltaLine(' world'),
      resultLine(50, 5),
    ];
    const spawnImpl = (() => makeFakeChild({ stdoutLines })) as unknown as typeof import('node:child_process').spawn;
    const client = new VisionClient({ spawnImpl });
    const tokens: Array<{ delta: string; accumulated: string }> = [];
    await client.analyze(baseInput(), (delta, accumulated) => {
      tokens.push({ delta, accumulated });
    });
    expect(tokens).toEqual([
      { delta: 'Hello', accumulated: 'Hello' },
      { delta: ' world', accumulated: 'Hello world' },
    ]);
  });

  it('swallows onToken exceptions so a bad subscriber cannot kill the call', async () => {
    const stdoutLines = [deltaLine('A'), resultLine(1, 1)];
    const spawnImpl = (() => makeFakeChild({ stdoutLines })) as unknown as typeof import('node:child_process').spawn;
    const client = new VisionClient({ spawnImpl });
    const r = await client.analyze(baseInput(), () => {
      throw new Error('subscriber boom');
    });
    expect(r.failureReason).toBeUndefined();
    expect(r.output?.caption).toBe('A');
  });
});

describe('VisionClient.analyze — extract mode', () => {
  it('parses a JSON-only response into output.extracted', async () => {
    const stdoutLines = [
      deltaLine('{"caption":"receipt","total":12.50}'),
      resultLine(200, 30),
    ];
    const spawnImpl = (() => makeFakeChild({ stdoutLines })) as unknown as typeof import('node:child_process').spawn;
    const client = new VisionClient({ spawnImpl });
    const r = await client.analyze(baseInput({ outputSchema: { type: 'object' } }));
    expect(r.failureReason).toBeUndefined();
    expect(r.output?.caption).toBe('receipt');
    expect((r.output?.extracted as { total: number }).total).toBe(12.5);
  });

  it('strips markdown fences from the response', async () => {
    const stdoutLines = [
      deltaLine('```json\n{"caption":"a dog","tags":["dog"]}\n```'),
      resultLine(100, 10),
    ];
    const spawnImpl = (() => makeFakeChild({ stdoutLines })) as unknown as typeof import('node:child_process').spawn;
    const client = new VisionClient({ spawnImpl });
    const r = await client.analyze(baseInput({ outputSchema: { type: 'object' } }));
    expect(r.failureReason).toBeUndefined();
    expect(r.output?.caption).toBe('a dog');
  });
});

// ─── Failure modes ─────────────────────────────────────────────────────

describe('VisionClient.analyze — failure modes', () => {
  it('CLI exit code non-zero → failureReason=unknown with stderr in rawText', async () => {
    const spawnImpl = (() =>
      makeFakeChild({ stdoutLines: [], stderr: 'boom', exitCode: 1 })) as unknown as typeof import('node:child_process').spawn;
    const client = new VisionClient({ spawnImpl });
    const r = await client.analyze(baseInput());
    expect(r.failureReason).toBe('unknown');
    expect(r.rawText).toContain('boom');
  });

  it('extract mode + unparseable response → corrective retry, usage summed', async () => {
    let callCount = 0;
    const spawnImpl = (() => {
      callCount += 1;
      return makeFakeChild({
        stdoutLines: [deltaLine('no json here'), resultLine(100, 5)],
      });
    }) as unknown as typeof import('node:child_process').spawn;
    const client = new VisionClient({ spawnImpl });
    const r = await client.analyze(baseInput({ outputSchema: { type: 'object' } }));
    expect(callCount).toBe(2);
    expect(r.failureReason).toBe('malformed');
    expect(r.usage.input_tokens).toBe(200);
    expect(r.usage.output_tokens).toBe(10);
  });

  it('extract mode + corrective retry succeeds → output returned, usage summed', async () => {
    let callCount = 0;
    const spawnImpl = (() => {
      callCount += 1;
      const stdoutLines = callCount === 1
        ? [deltaLine('no json'), resultLine(100, 5)]
        : [deltaLine('{"caption":"ok"}'), resultLine(110, 8)];
      return makeFakeChild({ stdoutLines });
    }) as unknown as typeof import('node:child_process').spawn;
    const client = new VisionClient({ spawnImpl });
    const r = await client.analyze(baseInput({ outputSchema: { type: 'object' } }));
    expect(callCount).toBe(2);
    expect(r.failureReason).toBeUndefined();
    expect(r.output?.caption).toBe('ok');
    expect(r.usage.input_tokens).toBe(210);
    expect(r.usage.output_tokens).toBe(13);
  });

  it('abort signal mid-stream → failureReason=aborted', async () => {
    const ctrl = new AbortController();
    const stdoutLines = [deltaLine('partial'), resultLine(1, 1)];
    const spawnImpl = (() => makeFakeChild({ stdoutLines })) as unknown as typeof import('node:child_process').spawn;
    const client = new VisionClient({ spawnImpl });
    // Abort before kicking off so the first stream iteration sees signal.aborted.
    ctrl.abort();
    const r = await client.analyze(baseInput({ abortSignal: ctrl.signal }));
    expect(r.failureReason).toBe('aborted');
  });
});

// ─── Tempfile + result eventing ────────────────────────────────────────

describe('VisionClient.analyze — telemetry', () => {
  it('captures cache_read_input_tokens from the result event', async () => {
    const stdoutLines = [
      deltaLine('ok'),
      resultLine(50, 10) + '', // base usage on result line
      // We replace the prior line with one that carries cache_read tokens:
    ];
    // Use a single richer result line instead.
    const enrichedLines = [
      deltaLine('cached.'),
      JSON.stringify({
        type: 'result',
        usage: { input_tokens: 50, output_tokens: 10, cache_read_input_tokens: 200 },
      }),
    ];
    const spawnImpl = (() => makeFakeChild({ stdoutLines: enrichedLines })) as unknown as typeof import('node:child_process').spawn;
    const client = new VisionClient({ spawnImpl });
    const r = await client.analyze(baseInput());
    expect(r.cacheHit).toBe(true);
    expect(r.usage.cache_read_input_tokens).toBe(200);
    // Ensure we don't accidentally reference stdoutLines we computed above.
    expect(stdoutLines.length).toBeGreaterThan(0);
  });

  it('cleans up the per-call tempdir on success', async () => {
    let createdDir: string | null = null;
    const spawnImpl = ((_bin: string, _args: string[], opts: { cwd?: string }) => {
      createdDir = opts.cwd ?? null;
      return makeFakeChild({ stdoutLines: [deltaLine('hi'), resultLine(1, 1)] });
    }) as unknown as typeof import('node:child_process').spawn;
    const client = new VisionClient({ spawnImpl });
    await client.analyze(baseInput());
    expect(createdDir).toBeTruthy();
    expect(existsSync(createdDir!)).toBe(false);
  });

  it('cleans up the per-call tempdir on failure', async () => {
    let createdDir: string | null = null;
    const spawnImpl = ((_bin: string, _args: string[], opts: { cwd?: string }) => {
      createdDir = opts.cwd ?? null;
      return makeFakeChild({ stdoutLines: [], stderr: 'crashed', exitCode: 137 });
    }) as unknown as typeof import('node:child_process').spawn;
    const client = new VisionClient({ spawnImpl });
    await client.analyze(baseInput());
    expect(createdDir).toBeTruthy();
    expect(existsSync(createdDir!)).toBe(false);
  });
});
