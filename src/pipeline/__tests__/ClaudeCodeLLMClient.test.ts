// social-api/src/pipeline/__tests__/ClaudeCodeLLMClient.test.ts
//
// Tests for ClaudeCodeLLMClient.stream(): JSONL line parsing (stream_event and
// result types), assistant fallback, usage accounting, char/4 fallback,
// non-zero exit throws, spawn arg assembly, and pre-abort signal.

import { describe, it, expect, jest, beforeEach } from '@jest/globals';
import { EventEmitter } from 'node:events';

// ---------------------------------------------------------------------------
// Mock node:child_process before importing module under test
// ---------------------------------------------------------------------------

const mockSpawn = jest.fn<typeof import('node:child_process').spawn>();

jest.mock('node:child_process', () => ({
  spawn: mockSpawn,
}));

import { ClaudeCodeLLMClient } from '../ClaudeCodeLLMClient';

// ---------------------------------------------------------------------------
// Helpers — build a mock child process
// ---------------------------------------------------------------------------

function makeMockChild(lines: string[], exitCode = 0, stderrOutput = '') {
  const mainEmitter = new EventEmitter();
  const stderrEmitter = new EventEmitter();

  // Attach exitCode lazily (set in the setImmediate callback below)
  const child = {
    exitCode: null as number | null,
    signalCode: null as null,
    kill: jest.fn(),
    stderr: stderrEmitter,
    on: (ev: string, cb: (...args: unknown[]) => void) => mainEmitter.on(ev, cb),
    stdout: {
      [Symbol.asyncIterator]: async function* () {
        // Emit stderr before the data starts (order matches a real process)
        if (stderrOutput) stderrEmitter.emit('data', Buffer.from(stderrOutput));
        for (const line of lines) {
          yield Buffer.from(line + '\n');
        }
        // Schedule exit after stdout is fully drained
        setImmediate(() => {
          child.exitCode = exitCode;
          mainEmitter.emit('exit', exitCode, null);
        });
      },
    },
  };

  return child;
}

// JSONL helpers
function streamEventLine(text: string): string {
  return JSON.stringify({
    type: 'stream_event',
    event: {
      type: 'content_block_delta',
      delta: { type: 'text_delta', text },
    },
  });
}

function resultLine(usage: { input_tokens?: number; output_tokens?: number }, result?: string): string {
  return JSON.stringify({ type: 'result', usage, ...(result ? { result } : {}) });
}

function assistantLine(texts: string[]): string {
  return JSON.stringify({
    type: 'assistant',
    message: {
      content: texts.map((t) => ({ type: 'text', text: t })),
    },
  });
}

async function collectChunks(
  client: ClaudeCodeLLMClient,
  model = 'claude-sonnet-4-6',
  system = 'sys',
  user = 'hi',
  opts: { signal?: AbortSignal } = {},
) {
  const chunks = [];
  for await (const chunk of client.stream(model, system, user, opts)) {
    chunks.push(chunk);
  }
  return chunks;
}

beforeEach(() => {
  jest.resetAllMocks();
});

// ---------------------------------------------------------------------------
// Provider identity
// ---------------------------------------------------------------------------

describe('ClaudeCodeLLMClient — provider field', () => {
  it('has provider === "claude-code"', () => {
    expect(new ClaudeCodeLLMClient().provider).toBe('claude-code');
  });
});

// ---------------------------------------------------------------------------
// Pre-abort
// ---------------------------------------------------------------------------

describe('ClaudeCodeLLMClient.stream() — pre-abort', () => {
  it('throws AbortError when signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();

    const client = new ClaudeCodeLLMClient();
    await expect(async () => {
      for await (const _ of client.stream('model', 'sys', 'hi', { signal: controller.signal })) {
        // should not reach here
      }
    }).rejects.toThrow('Aborted');
  });

  it('does not call spawn when signal is pre-aborted', async () => {
    const controller = new AbortController();
    controller.abort();

    const client = new ClaudeCodeLLMClient();
    try {
      for await (const _ of client.stream('model', 'sys', 'hi', { signal: controller.signal })) {
        // ignore
      }
    } catch { /* expected */ }

    expect(mockSpawn).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// stream_event parsing
// ---------------------------------------------------------------------------

describe('ClaudeCodeLLMClient.stream() — stream_event token parsing', () => {
  it('yields a chunk for each stream_event content_block_delta', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      streamEventLine('Hello'),
      streamEventLine(' world'),
      resultLine({ input_tokens: 10, output_tokens: 5 }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    const chunks = await collectChunks(client);

    const tokens = chunks.filter((c) => !c.done);
    expect(tokens).toHaveLength(2);
    expect(tokens[0].token).toBe('Hello');
    expect(tokens[1].token).toBe(' world');
  });

  it('final chunk has done=true with full assembled response', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      streamEventLine('Hi'),
      resultLine({ input_tokens: 5, output_tokens: 2 }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    const chunks = await collectChunks(client);
    const last = chunks[chunks.length - 1];

    expect(last.done).toBe(true);
    expect((last as Extract<typeof last, { done: true }>).response).toBe('Hi');
  });

  it('skips blank lines without throwing', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      '  ',
      '',
      streamEventLine('ok'),
      resultLine({ input_tokens: 2, output_tokens: 1 }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    const chunks = await collectChunks(client);
    const tokens = chunks.filter((c) => !c.done);
    expect(tokens).toHaveLength(1);
    expect(tokens[0].token).toBe('ok');
  });

  it('skips malformed JSON lines without throwing', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      'not-json',
      streamEventLine('fine'),
      resultLine({ input_tokens: 2, output_tokens: 1 }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    const chunks = await collectChunks(client);
    const tokens = chunks.filter((c) => !c.done);
    expect(tokens).toHaveLength(1);
    expect(tokens[0].token).toBe('fine');
  });
});

// ---------------------------------------------------------------------------
// result event — usage and string result
// ---------------------------------------------------------------------------

describe('ClaudeCodeLLMClient.stream() — result event', () => {
  it('reads tokensIn from result usage.input_tokens', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      resultLine({ input_tokens: 99, output_tokens: 4 }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    const chunks = await collectChunks(client);
    expect((chunks[chunks.length - 1] as Extract<(typeof chunks)[number], { done: true }>).tokensIn).toBe(99);
  });

  it('reads tokensOut from result usage.output_tokens', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      resultLine({ input_tokens: 5, output_tokens: 77 }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    const chunks = await collectChunks(client);
    expect((chunks[chunks.length - 1] as Extract<(typeof chunks)[number], { done: true }>).tokensOut).toBe(77);
  });

  it('uses result string as fullResponse when no stream_events were seen', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      resultLine({ input_tokens: 3, output_tokens: 2 }, 'direct result text'),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    const chunks = await collectChunks(client);
    // One non-done chunk + one done chunk since fullResponse was '' before result
    const last = chunks[chunks.length - 1];
    expect(last.done).toBe(true);
    expect((last as Extract<typeof last, { done: true }>).response).toBe('direct result text');
  });
});

// ---------------------------------------------------------------------------
// assistant message fallback (non-partial mode)
// ---------------------------------------------------------------------------

describe('ClaudeCodeLLMClient.stream() — assistant message fallback', () => {
  it('yields content from assistant message when no partials seen', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      assistantLine(['Full reply text']),
      resultLine({ input_tokens: 5, output_tokens: 3 }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    const chunks = await collectChunks(client);
    const tokens = chunks.filter((c) => !c.done);
    expect(tokens).toHaveLength(1);
    expect(tokens[0].token).toBe('Full reply text');
  });

  it('ignores assistant message when stream_events already produced content', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      streamEventLine('partial'),
      assistantLine(['ignored']),
      resultLine({ input_tokens: 5, output_tokens: 3 }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    const chunks = await collectChunks(client);
    const tokens = chunks.filter((c) => !c.done);
    expect(tokens.map((c) => c.token)).toEqual(['partial']);
  });
});

// ---------------------------------------------------------------------------
// Fallback token counting
// ---------------------------------------------------------------------------

describe('ClaudeCodeLLMClient.stream() — fallback token counting', () => {
  it('falls back to round(prompt.length/4) for tokensIn when result has no usage', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      // result event with no usage fields
      JSON.stringify({ type: 'result' }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const userPrompt = 'twelve chars'; // 12 → 3
    const client = new ClaudeCodeLLMClient();
    const chunks: Array<{ done: boolean; tokensIn?: number }> = [];
    for await (const c of client.stream('model', 'sys', userPrompt)) chunks.push(c);
    const last = chunks[chunks.length - 1];
    expect(last.tokensIn).toBe(Math.max(1, Math.round('twelve chars'.length / 4)));
  });

  it('falls back to round(response.length/4) for tokensOut', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      streamEventLine('abcdefgh'),
      JSON.stringify({ type: 'result' }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    const chunks: Array<{ done: boolean; tokensOut?: number }> = [];
    for await (const c of client.stream('model', 'sys', 'hi')) chunks.push(c);
    expect(chunks[chunks.length - 1].tokensOut).toBe(2); // 8 chars / 4
  });
});

// ---------------------------------------------------------------------------
// Non-zero exit code
// ---------------------------------------------------------------------------

describe('ClaudeCodeLLMClient.stream() — exit code', () => {
  it('throws when claude exits with non-zero code', async () => {
    mockSpawn.mockReturnValue(makeMockChild([], 1, 'authentication failed') as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    await expect(collectChunks(client)).rejects.toThrow('claude exited 1');
  });
});

// ---------------------------------------------------------------------------
// Spawn args
// ---------------------------------------------------------------------------

describe('ClaudeCodeLLMClient.stream() — spawn args', () => {
  it('includes --append-system-prompt when systemPrompt is non-empty', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      resultLine({ input_tokens: 2, output_tokens: 1 }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    for await (const _ of client.stream('model', 'my-system-prompt', 'hi')) { /* drain */ }

    const args = (mockSpawn.mock.calls[0] as unknown as [string, string[]])[1];
    expect(args).toContain('--append-system-prompt');
    const idx = args.indexOf('--append-system-prompt');
    expect(args[idx + 1]).toBe('my-system-prompt');
  });

  // The Bedrock-alias resolution path is gated on CLAUDE_CODE_USE_BEDROCK=1
  // (see ClaudeCodeLLMClient.bedrockMode). These tests exercise that path,
  // so they enable the env var locally and restore it afterwards.
  describe('Bedrock alias resolution (CLAUDE_CODE_USE_BEDROCK=1)', () => {
    let prevBedrock: string | undefined;
    beforeEach(() => {
      prevBedrock = process.env.CLAUDE_CODE_USE_BEDROCK;
      process.env.CLAUDE_CODE_USE_BEDROCK = '1';
    });
    afterEach(() => {
      if (prevBedrock === undefined) delete process.env.CLAUDE_CODE_USE_BEDROCK;
      else process.env.CLAUDE_CODE_USE_BEDROCK = prevBedrock;
    });

    it('includes --model when model is non-empty', async () => {
      mockSpawn.mockReturnValue(makeMockChild([
        resultLine({ input_tokens: 2, output_tokens: 1 }),
      ]) as unknown as ReturnType<typeof mockSpawn>);

      const client = new ClaudeCodeLLMClient();
      for await (const _ of client.stream('claude-haiku-4-5-20251001', 'sys', 'hi')) { /* drain */ }

      const args = (mockSpawn.mock.calls[0] as unknown as [string, string[]])[1];
      expect(args).toContain('--model');
      const idx = args.indexOf('--model');
      expect(args[idx + 1]).toBe('us.anthropic.claude-haiku-4-5-20251001-v1:0');
    });

    it('resolves short alias "sonnet" to full model ID', async () => {
      mockSpawn.mockReturnValue(makeMockChild([
        resultLine({ input_tokens: 2, output_tokens: 1 }),
      ]) as unknown as ReturnType<typeof mockSpawn>);

      const client = new ClaudeCodeLLMClient();
      for await (const _ of client.stream('sonnet', 'sys', 'hi')) { /* drain */ }

      const args = (mockSpawn.mock.calls[0] as unknown as [string, string[]])[1];
      const idx = args.indexOf('--model');
      expect(args[idx + 1]).toBe('us.anthropic.claude-sonnet-4-20250514-v1:0');
    });

    it('resolves short alias "opus" to full model ID', async () => {
      mockSpawn.mockReturnValue(makeMockChild([
        resultLine({ input_tokens: 2, output_tokens: 1 }),
      ]) as unknown as ReturnType<typeof mockSpawn>);

      const client = new ClaudeCodeLLMClient();
      for await (const _ of client.stream('opus', 'sys', 'hi')) { /* drain */ }

      const args = (mockSpawn.mock.calls[0] as unknown as [string, string[]])[1];
      const idx = args.indexOf('--model');
      expect(args[idx + 1]).toBe('us.anthropic.claude-opus-4-20250514-v1:0');
    });
  });

  it('includes --output-format stream-json and --include-partial-messages', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      resultLine({ input_tokens: 2, output_tokens: 1 }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    for await (const _ of client.stream('model', 'sys', 'hi')) { /* drain */ }

    const args = (mockSpawn.mock.calls[0] as unknown as [string, string[]])[1];
    expect(args).toContain('--output-format');
    expect(args).toContain('stream-json');
    expect(args).toContain('--include-partial-messages');
  });

  it('appends the user prompt as the last argument', async () => {
    mockSpawn.mockReturnValue(makeMockChild([
      resultLine({ input_tokens: 2, output_tokens: 1 }),
    ]) as unknown as ReturnType<typeof mockSpawn>);

    const client = new ClaudeCodeLLMClient();
    for await (const _ of client.stream('model', 'sys', 'the user query')) { /* drain */ }

    const args = (mockSpawn.mock.calls[0] as unknown as [string, string[]])[1];
    expect(args[args.length - 1]).toBe('the user query');
  });
});
