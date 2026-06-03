// platform-api/src/pipeline/judge/__tests__/JudgeClient.test.ts
//
// Tests the CLI-subprocess `JudgeClient` by injecting a fake `spawn` that
// emulates `claude -p --output-format stream-json` line emission. Exercises:
//   - happy path: JSON-only response → parsed RawJudgmentToolOutput
//   - markdown-fence wrapped response → defensive extractor still parses
//   - prose-preamble + JSON → extractor isolates the JSON
//   - malformed (no JSON) → one corrective retry, then failureReason='malformed'
//   - CLI exits non-zero → failureReason='unknown', stderr in rawText
//   - usage tokens propagate from `result` event
//
// Also covers the pure JSON-extraction helper.

import { describe, it, expect, jest } from '@jest/globals';
import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';
import { JudgeClient, extractJsonObject } from '../JudgeClient';
import type { JudgeInput } from '../types';

// ─── Fake child-process helpers ────────────────────────────────────────

interface FakeChildOpts {
  stdoutLines?: string[]; // each emitted with trailing \n
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
  // immediately resolve "no stderr" once consumers attach
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

function streamLineFor(textDelta: string): string {
  return JSON.stringify({
    type: 'stream_event',
    event: { type: 'content_block_delta', delta: { type: 'text_delta', text: textDelta } },
  });
}

function resultLine(inputTokens: number, outputTokens: number): string {
  return JSON.stringify({ type: 'result', usage: { input_tokens: inputTokens, output_tokens: outputTokens } });
}

const baseInput: JudgeInput = {
  model: 'sonnet',
  systemPrompt: 'sys',
  rubricPrompt: 'rubric',
  candidateText: 'candidate',
  maxOutputTokens: 1024,
};

// ─── Pure helper ───────────────────────────────────────────────────────

describe('extractJsonObject (defensive extractor)', () => {
  it('parses bare JSON', () => {
    expect(extractJsonObject('{"a":1}')).toEqual({ a: 1 });
  });
  it('strips prose preamble', () => {
    expect(extractJsonObject('Here is my judgment:\n{"a":1}')).toEqual({ a: 1 });
  });
  it('strips markdown fences', () => {
    expect(extractJsonObject('```json\n{"a":1}\n```')).toEqual({ a: 1 });
  });
  it('strips trailing prose', () => {
    expect(extractJsonObject('{"a":1}\n\nThat is my response.')).toEqual({ a: 1 });
  });
  it('returns null when no braces', () => {
    expect(extractJsonObject('no json here')).toBeNull();
  });
  it('returns null on syntax error', () => {
    expect(extractJsonObject('{not valid}')).toBeNull();
  });
});

// ─── Client tests ──────────────────────────────────────────────────────

describe('JudgeClient — happy path', () => {
  it('parses a clean JSON response into RawJudgmentToolOutput', async () => {
    const json = JSON.stringify({
      criteria: [
        { criterionId: 'a', score: 4, rationale: 'good' },
        { criterionId: 'b', score: 3, rationale: 'ok' },
      ],
      confidence: 0.85,
    });
    const lines = [streamLineFor(json), resultLine(120, 60)];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const spawnImpl = jest.fn(() => makeFakeChild({ stdoutLines: lines })) as any;
    const client = new JudgeClient({ spawnImpl });

    const result = await client.judge(baseInput);

    expect(result.output).not.toBeNull();
    expect(result.output!.criteria).toHaveLength(2);
    expect(result.output!.criteria[0].score).toBe(4);
    expect(result.output!.confidence).toBe(0.85);
    expect(result.usage.input_tokens).toBe(120);
    expect(result.usage.output_tokens).toBe(60);
    expect(result.failureReason).toBeUndefined();
  });

  it('parses through prose preamble', async () => {
    const json = JSON.stringify({ criteria: [{ criterionId: 'a', score: 3, rationale: 'mid' }] });
    const lines = [
      streamLineFor('Here is the judgment in JSON:\n'),
      streamLineFor(json),
      resultLine(100, 50),
    ];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const spawnImpl = jest.fn(() => makeFakeChild({ stdoutLines: lines })) as any;
    const client = new JudgeClient({ spawnImpl });
    const r = await client.judge(baseInput);
    expect(r.output).not.toBeNull();
    expect(r.output!.criteria[0].criterionId).toBe('a');
  });
});

describe('JudgeClient — malformed handling', () => {
  it('retries once on malformed output and surfaces failureReason on second fail', async () => {
    // First attempt: no JSON. Second attempt: also no JSON.
    let call = 0;
    const spawnImpl = jest.fn(() => {
      call++;
      const lines = call === 1
        ? [streamLineFor('I cannot do this.'), resultLine(50, 10)]
        : [streamLineFor('still cannot.'), resultLine(50, 10)];
      return makeFakeChild({ stdoutLines: lines });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    }) as any;
    const client = new JudgeClient({ spawnImpl });

    const r = await client.judge(baseInput);
    expect(r.output).toBeNull();
    expect(r.failureReason).toBe('malformed');
    // Two attempts → usage accumulates
    expect(r.usage.input_tokens).toBe(100);
    expect(r.usage.output_tokens).toBe(20);
    expect(spawnImpl).toHaveBeenCalledTimes(2);
  });

  it('recovers on the corrective retry when the second response is valid', async () => {
    let call = 0;
    const valid = JSON.stringify({ criteria: [{ criterionId: 'a', score: 4, rationale: 'good' }] });
    const spawnImpl = jest.fn(() => {
      call++;
      const lines = call === 1
        ? [streamLineFor('garbage'), resultLine(50, 10)]
        : [streamLineFor(valid), resultLine(80, 30)];
      return makeFakeChild({ stdoutLines: lines });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    }) as any;
    const client = new JudgeClient({ spawnImpl });

    const r = await client.judge(baseInput);
    expect(r.output).not.toBeNull();
    expect(r.output!.criteria[0].criterionId).toBe('a');
    // Usage covers both attempts
    expect(r.usage.input_tokens).toBe(130);
    expect(r.usage.output_tokens).toBe(40);
  });
});

describe('JudgeClient — CLI failure modes', () => {
  it('returns failureReason=unknown when CLI exits non-zero', async () => {
    const spawnImpl = jest.fn(() => makeFakeChild({
      stdoutLines: [],
      stderr: 'auth failed',
      exitCode: 1,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    })) as any;
    const client = new JudgeClient({ spawnImpl });
    const r = await client.judge(baseInput);
    expect(r.output).toBeNull();
    expect(r.failureReason).toBe('unknown');
  });

  it('reports tokens even on parse failure (rule #1: usage propagates)', async () => {
    const spawnImpl = jest.fn(() => makeFakeChild({
      stdoutLines: [streamLineFor('not json'), resultLine(75, 25)],
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    })) as any;
    const client = new JudgeClient({ spawnImpl });
    const r = await client.judge(baseInput);
    expect(r.output).toBeNull();
    // both attempts use the same tokens via the fake — sum = 150 / 50
    expect(r.usage.input_tokens).toBe(150);
    expect(r.usage.output_tokens).toBe(50);
  });
});
