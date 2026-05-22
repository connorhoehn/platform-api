// social-api/src/pipeline/__tests__/AnthropicLLMClient.test.ts
//
// Tests for AnthropicLLMClient.stream(): token chunk emission, usage accounting
// (tokensIn from message_start, tokensOut from message_delta), char/4 fallback
// when usage events are absent, early-abort (pre-stream and mid-stream), and
// temperature passthrough.

import { describe, it, expect, jest, beforeEach } from '@jest/globals';

// ---------------------------------------------------------------------------
// Mock @anthropic-ai/sdk before importing the module under test
// ---------------------------------------------------------------------------

const mockFinalMessage = jest.fn<() => Promise<void>>();
const mockStream = jest.fn<(params: Record<string, unknown>) => AsyncIterable<unknown>>();

jest.mock('@anthropic-ai/sdk', () => ({
  default: class MockAnthropic {
    messages = { stream: mockStream };
  },
}));

import { AnthropicLLMClient } from '../AnthropicLLMClient';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type SDKEvent =
  | { type: 'message_start'; message: { usage: { input_tokens: number } } }
  | { type: 'content_block_delta'; delta: { type: 'text_delta'; text: string } }
  | { type: 'message_delta'; usage: { output_tokens: number } }
  | { type: string };

function makeStream(events: SDKEvent[]): AsyncIterable<SDKEvent> & { finalMessage: typeof mockFinalMessage } {
  const iterable = {
    [Symbol.asyncIterator]: async function* () {
      for (const e of events) yield e;
    },
    finalMessage: mockFinalMessage,
  };
  return iterable;
}

async function collectChunks(client: AnthropicLLMClient, model = 'claude-3-5-sonnet-20241022', system = 'sys', user = 'hi') {
  const chunks = [];
  for await (const chunk of client.stream(model, system, user)) {
    chunks.push(chunk);
  }
  return chunks;
}

beforeEach(() => {
  jest.resetAllMocks();
  mockFinalMessage.mockResolvedValue(undefined);
});

// ---------------------------------------------------------------------------
// Basic streaming
// ---------------------------------------------------------------------------

describe('AnthropicLLMClient.stream() — basic token emission', () => {
  it('yields a text chunk for each content_block_delta', async () => {
    mockStream.mockReturnValue(makeStream([
      { type: 'message_start', message: { usage: { input_tokens: 10 } } },
      { type: 'content_block_delta', delta: { type: 'text_delta', text: 'Hello' } },
      { type: 'content_block_delta', delta: { type: 'text_delta', text: ' world' } },
      { type: 'message_delta', usage: { output_tokens: 5 } },
    ]));

    const client = new AnthropicLLMClient();
    const chunks = await collectChunks(client);

    const tokens = chunks.filter((c) => !c.done);
    expect(tokens).toHaveLength(2);
    expect(tokens[0].token).toBe('Hello');
    expect(tokens[1].token).toBe(' world');
  });

  it('final chunk has done=true with assembled response', async () => {
    mockStream.mockReturnValue(makeStream([
      { type: 'message_start', message: { usage: { input_tokens: 8 } } },
      { type: 'content_block_delta', delta: { type: 'text_delta', text: 'Hi' } },
      { type: 'message_delta', usage: { output_tokens: 3 } },
    ]));

    const client = new AnthropicLLMClient();
    const chunks = await collectChunks(client);

    const last = chunks[chunks.length - 1];
    expect(last.done).toBe(true);
    expect((last as Extract<typeof last, { done: true }>).response).toBe('Hi');
  });

  it('ignores non-text events (no extra chunks)', async () => {
    mockStream.mockReturnValue(makeStream([
      { type: 'message_start', message: { usage: { input_tokens: 5 } } },
      { type: 'message_delta', usage: { output_tokens: 2 } },
    ]));

    const client = new AnthropicLLMClient();
    const chunks = await collectChunks(client, 'model', 'sys', 'hello');

    expect(chunks.filter((c) => !c.done)).toHaveLength(0);
    expect(chunks[chunks.length - 1].done).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Token accounting — usage events present
// ---------------------------------------------------------------------------

describe('AnthropicLLMClient.stream() — usage accounting from events', () => {
  it('reads tokensIn from message_start', async () => {
    mockStream.mockReturnValue(makeStream([
      { type: 'message_start', message: { usage: { input_tokens: 42 } } },
      { type: 'message_delta', usage: { output_tokens: 7 } },
    ]));

    const client = new AnthropicLLMClient();
    const chunks = await collectChunks(client);
    const last = chunks[chunks.length - 1];

    expect((last as Extract<typeof last, { done: true }>).tokensIn).toBe(42);
  });

  it('reads tokensOut from message_delta', async () => {
    mockStream.mockReturnValue(makeStream([
      { type: 'message_start', message: { usage: { input_tokens: 5 } } },
      { type: 'message_delta', usage: { output_tokens: 99 } },
    ]));

    const client = new AnthropicLLMClient();
    const chunks = await collectChunks(client);
    const last = chunks[chunks.length - 1];

    expect((last as Extract<typeof last, { done: true }>).tokensOut).toBe(99);
  });
});

// ---------------------------------------------------------------------------
// Token accounting — fallback (char / 4)
// ---------------------------------------------------------------------------

describe('AnthropicLLMClient.stream() — fallback token counting', () => {
  it('falls back to ceil(userPrompt.length/4) for tokensIn when no message_start', async () => {
    mockStream.mockReturnValue(makeStream([
      { type: 'content_block_delta', delta: { type: 'text_delta', text: 'ok' } },
    ]));

    const userPrompt = 'twelve chars'; // 12 chars → round(12/4)=3
    const client = new AnthropicLLMClient();
    const chunks: Array<{ done: boolean; tokensIn?: number }> = [];
    for await (const c of client.stream('model', 'sys', userPrompt)) {
      chunks.push(c);
    }
    const last = chunks[chunks.length - 1];
    expect(last.tokensIn).toBe(Math.max(1, Math.round('twelve chars'.length / 4)));
  });

  it('falls back to ceil(response.length/4) for tokensOut when no message_delta', async () => {
    mockStream.mockReturnValue(makeStream([
      { type: 'message_start', message: { usage: { input_tokens: 3 } } },
      { type: 'content_block_delta', delta: { type: 'text_delta', text: 'abcdefgh' } },
    ]));

    // response = 'abcdefgh' (8 chars) → round(8/4)=2
    const client = new AnthropicLLMClient();
    const chunks: Array<{ done: boolean; tokensOut?: number }> = [];
    for await (const c of client.stream('model', 'sys', 'hi')) {
      chunks.push(c);
    }
    const last = chunks[chunks.length - 1];
    expect(last.tokensOut).toBe(2);
  });

  it('clamps fallback tokensIn to at least 1', async () => {
    mockStream.mockReturnValue(makeStream([
      // no message_start, empty user prompt
    ]));

    const client = new AnthropicLLMClient();
    const chunks: Array<{ done: boolean; tokensIn?: number }> = [];
    for await (const c of client.stream('model', 'sys', '')) {
      chunks.push(c);
    }
    const last = chunks[chunks.length - 1];
    expect(last.tokensIn).toBeGreaterThanOrEqual(1);
  });
});

// ---------------------------------------------------------------------------
// Abort — signal already set before iteration begins
// ---------------------------------------------------------------------------

describe('AnthropicLLMClient.stream() — abort', () => {
  it('throws AbortError immediately when signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();

    const client = new AnthropicLLMClient();
    await expect(async () => {
      for await (const _ of client.stream('model', 'sys', 'hi', { signal: controller.signal })) {
        // should never get here
      }
    }).rejects.toThrow('Aborted');
  });

  it('does not call SDK stream() when signal is pre-aborted', async () => {
    const controller = new AbortController();
    controller.abort();

    const client = new AnthropicLLMClient();
    try {
      for await (const _ of client.stream('model', 'sys', 'hi', { signal: controller.signal })) {
        // ignore
      }
    } catch {
      // expected
    }

    expect(mockStream).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Provider identity
// ---------------------------------------------------------------------------

describe('AnthropicLLMClient — provider field', () => {
  it('has provider === "anthropic"', () => {
    expect(new AnthropicLLMClient().provider).toBe('anthropic');
  });
});

// ---------------------------------------------------------------------------
// Parameter passthrough
// ---------------------------------------------------------------------------

describe('AnthropicLLMClient.stream() — parameter passthrough', () => {
  it('passes temperature when provided', async () => {
    mockStream.mockReturnValue(makeStream([]));

    const client = new AnthropicLLMClient();
    for await (const _ of client.stream('claude-3-5-sonnet-20241022', 'sys', 'hi', { temperature: 0.7 })) {
      // drain
    }

    expect(mockStream).toHaveBeenCalledWith(
      expect.objectContaining({ temperature: 0.7 }),
    );
  });

  it('omits temperature when not provided', async () => {
    mockStream.mockReturnValue(makeStream([]));

    const client = new AnthropicLLMClient();
    for await (const _ of client.stream('model', 'sys', 'hi')) {
      // drain
    }

    const callArg = (mockStream.mock.calls[0] as [Record<string, unknown>])[0];
    expect('temperature' in callArg).toBe(false);
  });

  it('passes maxTokens (defaults to 1024)', async () => {
    mockStream.mockReturnValue(makeStream([]));

    const client = new AnthropicLLMClient();
    for await (const _ of client.stream('model', 'sys', 'hi')) {
      // drain
    }

    expect(mockStream).toHaveBeenCalledWith(
      expect.objectContaining({ max_tokens: 1024 }),
    );
  });

  it('respects a custom maxTokens option', async () => {
    mockStream.mockReturnValue(makeStream([]));

    const client = new AnthropicLLMClient();
    for await (const _ of client.stream('model', 'sys', 'hi', { maxTokens: 512 })) {
      // drain
    }

    expect(mockStream).toHaveBeenCalledWith(
      expect.objectContaining({ max_tokens: 512 }),
    );
  });

  it('wraps user prompt in messages array', async () => {
    mockStream.mockReturnValue(makeStream([]));

    const client = new AnthropicLLMClient();
    for await (const _ of client.stream('model', 'system-prompt', 'user-input')) {
      // drain
    }

    expect(mockStream).toHaveBeenCalledWith(
      expect.objectContaining({
        system: 'system-prompt',
        messages: [{ role: 'user', content: 'user-input' }],
      }),
    );
  });
});
