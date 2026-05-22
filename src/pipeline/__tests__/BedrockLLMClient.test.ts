// social-api/src/pipeline/__tests__/BedrockLLMClient.test.ts
//
// Tests for BedrockLLMClient.stream(): Bedrock chunk decoding, token accounting
// (from decoded message_start / message_delta), char/4 fallback, model name
// mapping, temperature passthrough, pre-abort signal, and null response.body.

import { describe, it, expect, jest, beforeEach } from '@jest/globals';

// ---------------------------------------------------------------------------
// Mock @aws-sdk/client-bedrock-runtime before importing module under test
// ---------------------------------------------------------------------------

const mockSend = jest.fn<(cmd: unknown) => Promise<unknown>>();

const MockBedrockRuntimeClient = jest.fn().mockImplementation(() => ({
  send: mockSend,
}));

const MockInvokeModelWithResponseStreamCommand = jest.fn().mockImplementation((params: unknown) => params);

jest.mock('@aws-sdk/client-bedrock-runtime', () => ({
  BedrockRuntimeClient: MockBedrockRuntimeClient,
  InvokeModelWithResponseStreamCommand: MockInvokeModelWithResponseStreamCommand,
}));

import { BedrockLLMClient } from '../BedrockLLMClient';

// ---------------------------------------------------------------------------
// Helpers — build Bedrock-shaped chunks
// ---------------------------------------------------------------------------

type BedrockEvent =
  | { type: 'message_start'; message: { usage: { input_tokens: number } } }
  | { type: 'content_block_delta'; delta: { type: 'text_delta'; text: string } }
  | { type: 'message_delta'; usage: { output_tokens: number } }
  | { type: string };

function encodeChunk(event: BedrockEvent): { chunk: { bytes: Buffer } } {
  return { chunk: { bytes: Buffer.from(JSON.stringify(event), 'utf8') } };
}

async function* makeBodyIterable(events: BedrockEvent[]) {
  for (const e of events) {
    yield encodeChunk(e);
  }
}

function mockResponse(events: BedrockEvent[]) {
  mockSend.mockResolvedValue({ body: makeBodyIterable(events) });
}

async function collectChunks(
  client: BedrockLLMClient,
  model = 'claude-sonnet-4-6',
  system = 'sys',
  user = 'hi',
  opts: { temperature?: number; maxTokens?: number; signal?: AbortSignal } = {},
) {
  const chunks = [];
  for await (const chunk of client.stream(model, system, user, opts)) {
    chunks.push(chunk);
  }
  return chunks;
}

beforeEach(() => {
  jest.resetAllMocks();
  MockBedrockRuntimeClient.mockImplementation(() => ({ send: mockSend }));
  MockInvokeModelWithResponseStreamCommand.mockImplementation((p: unknown) => p);
});

// ---------------------------------------------------------------------------
// Provider identity
// ---------------------------------------------------------------------------

describe('BedrockLLMClient — provider field', () => {
  it('has provider === "bedrock"', () => {
    expect(new BedrockLLMClient().provider).toBe('bedrock');
  });
});

// ---------------------------------------------------------------------------
// Token emission
// ---------------------------------------------------------------------------

describe('BedrockLLMClient.stream() — token emission', () => {
  it('yields a text chunk for each content_block_delta', async () => {
    mockResponse([
      { type: 'message_start', message: { usage: { input_tokens: 10 } } },
      { type: 'content_block_delta', delta: { type: 'text_delta', text: 'Hello' } },
      { type: 'content_block_delta', delta: { type: 'text_delta', text: ' world' } },
      { type: 'message_delta', usage: { output_tokens: 5 } },
    ]);

    const client = new BedrockLLMClient();
    const chunks = await collectChunks(client);

    const tokens = chunks.filter((c) => !c.done);
    expect(tokens).toHaveLength(2);
    expect(tokens[0].token).toBe('Hello');
    expect(tokens[1].token).toBe(' world');
  });

  it('final chunk assembles the full response', async () => {
    mockResponse([
      { type: 'message_start', message: { usage: { input_tokens: 5 } } },
      { type: 'content_block_delta', delta: { type: 'text_delta', text: 'Hi' } },
      { type: 'message_delta', usage: { output_tokens: 3 } },
    ]);

    const client = new BedrockLLMClient();
    const chunks = await collectChunks(client);

    const last = chunks[chunks.length - 1];
    expect(last.done).toBe(true);
    expect((last as Extract<typeof last, { done: true }>).response).toBe('Hi');
  });

  it('handles null response.body gracefully', async () => {
    mockSend.mockResolvedValue({ body: null });

    const client = new BedrockLLMClient();
    const chunks = await collectChunks(client, 'claude-sonnet-4-6', 'sys', 'hello');

    const last = chunks[chunks.length - 1];
    expect(last.done).toBe(true);
    // fallback: input prompt 'hello' = 5 chars → round(5/4)=1; response ''→1
    expect((last as Extract<typeof last, { done: true }>).tokensIn).toBeGreaterThanOrEqual(1);
    expect((last as Extract<typeof last, { done: true }>).tokensOut).toBeGreaterThanOrEqual(1);
  });
});

// ---------------------------------------------------------------------------
// Usage accounting
// ---------------------------------------------------------------------------

describe('BedrockLLMClient.stream() — usage accounting', () => {
  it('reads tokensIn from decoded message_start', async () => {
    mockResponse([
      { type: 'message_start', message: { usage: { input_tokens: 77 } } },
      { type: 'message_delta', usage: { output_tokens: 4 } },
    ]);

    const client = new BedrockLLMClient();
    const chunks = await collectChunks(client);
    const last = chunks[chunks.length - 1];

    expect((last as Extract<typeof last, { done: true }>).tokensIn).toBe(77);
  });

  it('reads tokensOut from decoded message_delta', async () => {
    mockResponse([
      { type: 'message_start', message: { usage: { input_tokens: 5 } } },
      { type: 'message_delta', usage: { output_tokens: 88 } },
    ]);

    const client = new BedrockLLMClient();
    const chunks = await collectChunks(client);
    const last = chunks[chunks.length - 1];

    expect((last as Extract<typeof last, { done: true }>).tokensOut).toBe(88);
  });
});

// ---------------------------------------------------------------------------
// Fallback token counting
// ---------------------------------------------------------------------------

describe('BedrockLLMClient.stream() — fallback token counting', () => {
  it('falls back to round(prompt.length/4) for tokensIn when no message_start', async () => {
    mockResponse([
      { type: 'content_block_delta', delta: { type: 'text_delta', text: 'ok' } },
    ]);

    const userPrompt = 'twelve chars'; // 12 → round(12/4)=3
    const client = new BedrockLLMClient();
    const chunks: Array<{ done: boolean; tokensIn?: number }> = [];
    for await (const c of client.stream('claude-sonnet-4-6', 'sys', userPrompt)) {
      chunks.push(c);
    }
    const last = chunks[chunks.length - 1];
    expect(last.tokensIn).toBe(Math.max(1, Math.round('twelve chars'.length / 4)));
  });

  it('falls back to round(response.length/4) for tokensOut when no message_delta', async () => {
    mockResponse([
      { type: 'message_start', message: { usage: { input_tokens: 3 } } },
      { type: 'content_block_delta', delta: { type: 'text_delta', text: 'abcdefgh' } },
    ]);

    const client = new BedrockLLMClient();
    const chunks: Array<{ done: boolean; tokensOut?: number }> = [];
    for await (const c of client.stream('claude-sonnet-4-6', 'sys', 'hi')) {
      chunks.push(c);
    }
    const last = chunks[chunks.length - 1];
    expect(last.tokensOut).toBe(2); // 8 chars / 4
  });

  it('clamps fallback tokensIn to at least 1 for empty prompt', async () => {
    mockResponse([]);

    const client = new BedrockLLMClient();
    const chunks: Array<{ done: boolean; tokensIn?: number }> = [];
    for await (const c of client.stream('claude-sonnet-4-6', 'sys', '')) {
      chunks.push(c);
    }
    const last = chunks[chunks.length - 1];
    expect(last.tokensIn).toBeGreaterThanOrEqual(1);
  });
});

// ---------------------------------------------------------------------------
// Model name mapping
// ---------------------------------------------------------------------------

describe('BedrockLLMClient.stream() — model name mapping', () => {
  it('maps claude-sonnet-4-6 to Bedrock model ID', async () => {
    mockResponse([]);

    const client = new BedrockLLMClient();
    for await (const _ of client.stream('claude-sonnet-4-6', 'sys', 'hi')) { /* drain */ }

    const callArg = (MockInvokeModelWithResponseStreamCommand.mock.calls[0] as [{ modelId: string }])[0];
    expect(callArg.modelId).toBe('us.anthropic.claude-sonnet-4-20250514-v1:0');
  });

  it('maps claude-opus-4-7 to Bedrock model ID', async () => {
    mockResponse([]);

    const client = new BedrockLLMClient();
    for await (const _ of client.stream('claude-opus-4-7', 'sys', 'hi')) { /* drain */ }

    const callArg = (MockInvokeModelWithResponseStreamCommand.mock.calls[0] as [{ modelId: string }])[0];
    expect(callArg.modelId).toBe('us.anthropic.claude-opus-4-20250514-v1:0');
  });

  it('passes unknown model names through unchanged', async () => {
    mockResponse([]);

    const client = new BedrockLLMClient();
    for await (const _ of client.stream('my-custom-model-id', 'sys', 'hi')) { /* drain */ }

    const callArg = (MockInvokeModelWithResponseStreamCommand.mock.calls[0] as [{ modelId: string }])[0];
    expect(callArg.modelId).toBe('my-custom-model-id');
  });
});

// ---------------------------------------------------------------------------
// Parameter passthrough
// ---------------------------------------------------------------------------

describe('BedrockLLMClient.stream() — parameter passthrough', () => {
  it('passes temperature when provided', async () => {
    mockResponse([]);

    const client = new BedrockLLMClient();
    for await (const _ of client.stream('claude-sonnet-4-6', 'sys', 'hi', { temperature: 0.5 })) { /* drain */ }

    const callArg = (MockInvokeModelWithResponseStreamCommand.mock.calls[0] as [{ body: string }])[0];
    const body = JSON.parse(callArg.body);
    expect(body.temperature).toBe(0.5);
  });

  it('omits temperature when not provided', async () => {
    mockResponse([]);

    const client = new BedrockLLMClient();
    for await (const _ of client.stream('claude-sonnet-4-6', 'sys', 'hi')) { /* drain */ }

    const callArg = (MockInvokeModelWithResponseStreamCommand.mock.calls[0] as [{ body: string }])[0];
    const body = JSON.parse(callArg.body);
    expect('temperature' in body).toBe(false);
  });

  it('defaults max_tokens to 1024', async () => {
    mockResponse([]);

    const client = new BedrockLLMClient();
    for await (const _ of client.stream('claude-sonnet-4-6', 'sys', 'hi')) { /* drain */ }

    const callArg = (MockInvokeModelWithResponseStreamCommand.mock.calls[0] as [{ body: string }])[0];
    const body = JSON.parse(callArg.body);
    expect(body.max_tokens).toBe(1024);
  });

  it('uses anthropic_version bedrock-2023-05-31', async () => {
    mockResponse([]);

    const client = new BedrockLLMClient();
    for await (const _ of client.stream('claude-sonnet-4-6', 'sys', 'hi')) { /* drain */ }

    const callArg = (MockInvokeModelWithResponseStreamCommand.mock.calls[0] as [{ body: string }])[0];
    const body = JSON.parse(callArg.body);
    expect(body.anthropic_version).toBe('bedrock-2023-05-31');
  });
});

// ---------------------------------------------------------------------------
// Abort — pre-stream
// ---------------------------------------------------------------------------

describe('BedrockLLMClient.stream() — abort', () => {
  it('throws AbortError when signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();

    const client = new BedrockLLMClient();
    await expect(async () => {
      for await (const _ of client.stream('claude-sonnet-4-6', 'sys', 'hi', { signal: controller.signal })) {
        // should not reach here
      }
    }).rejects.toThrow('Aborted');
  });

  it('does not call SDK send() when signal is pre-aborted', async () => {
    const controller = new AbortController();
    controller.abort();

    const client = new BedrockLLMClient();
    try {
      for await (const _ of client.stream('claude-sonnet-4-6', 'sys', 'hi', { signal: controller.signal })) {
        // ignore
      }
    } catch {
      // expected
    }

    expect(mockSend).not.toHaveBeenCalled();
  });
});
