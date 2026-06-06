// platform-api/src/pipeline/image/__tests__/AnthropicVisionClient.test.ts
//
// Behavioral tests for the SDK-based vision client. Mocks
// `Anthropic.messages.stream` so we can assert:
//   - base64 image content block is constructed correctly
//   - onToken fires per text delta
//   - usage is collected from finalMessage()
//   - cache fields are read defensively from the wire shape
//   - corrective retry on malformed JSON
//   - refusal detection
//   - abort propagation via the abortSignal
//
// We do NOT exercise the real network — every test injects a fake
// `Anthropic`-shaped client via the constructor.

import { AnthropicVisionClient } from '../AnthropicVisionClient';
import type { VisionAnalyzeInput } from '../types';

interface MockMessage {
  usage: Record<string, unknown>;
  stop_reason: string | null;
  content: Array<{ type: 'text'; text: string }>;
}

interface MockStream {
  on(event: 'text', handler: (delta: string) => void): MockStream;
  finalMessage(): Promise<MockMessage>;
}

/** Build a fake `Anthropic`-shaped client for one or more queued responses. */
function makeFakeClient(responses: MockMessage[], deltasPerCall: string[][] = []) {
  const calls: unknown[] = [];
  let callIdx = 0;
  return {
    calls,
    client: {
      messages: {
        stream(params: unknown, _opts?: { signal?: AbortSignal }) {
          calls.push(params);
          const idx = callIdx++;
          const deltas = deltasPerCall[idx] ?? [];
          const response = responses[idx];
          let textHandler: ((d: string) => void) | undefined;
          const stream: MockStream = {
            on(event, handler) {
              if (event === 'text') textHandler = handler;
              return stream;
            },
            async finalMessage() {
              // Replay deltas synchronously before finalMessage resolves so
              // the test sees onToken fire deterministically.
              for (const d of deltas) textHandler?.(d);
              return response;
            },
          };
          return stream;
        },
      },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any,
  };
}

function buildInput(over: Partial<VisionAnalyzeInput> = {}): VisionAnalyzeInput {
  return {
    model: 'claude-sonnet-4-6',
    systemPrompt: 'You are precise.',
    userPrompt: 'Describe this.',
    imageBytes: Buffer.from('img-bytes'),
    imageMimeType: 'image/png',
    maxOutputTokens: 256,
    ...over,
  };
}

describe('AnthropicVisionClient.analyze', () => {
  it('streams text deltas to onToken and resolves with the caption', async () => {
    const { client, calls } = makeFakeClient(
      [
        {
          usage: { input_tokens: 12, output_tokens: 5 },
          stop_reason: 'end_turn',
          content: [{ type: 'text', text: 'A red apple.' }],
        },
      ],
      [['A red', ' apple.']],
    );
    const sdk = new AnthropicVisionClient({ client });
    const tokens: string[] = [];
    const result = await sdk.analyze(buildInput(), (delta) => tokens.push(delta));

    expect(tokens).toEqual(['A red', ' apple.']);
    expect(result.output).toEqual({ caption: 'A red apple.', extracted: null });
    expect(result.usage).toEqual({ input_tokens: 12, output_tokens: 5 });
    expect(result.cacheHit).toBe(false);

    // Verify the message params include the base64 image content block.
    expect(calls).toHaveLength(1);
    const params = calls[0] as { messages: Array<{ content: Array<Record<string, unknown>> }> };
    const content = params.messages[0].content;
    expect(content[0]).toMatchObject({ type: 'image' });
    const source = content[0] as { source: { type: string; data: string; media_type: string } };
    expect(source.source.type).toBe('base64');
    expect(source.source.media_type).toBe('image/png');
    expect(source.source.data).toBe(Buffer.from('img-bytes').toString('base64'));
    expect(content[1]).toMatchObject({ type: 'text', text: 'Describe this.' });
  });

  it('parses structured JSON when outputSchema is supplied', async () => {
    const json = JSON.stringify({ caption: 'two cats', regions: [{ tag: 'cat-1' }] });
    const { client } = makeFakeClient(
      [
        {
          usage: { input_tokens: 20, output_tokens: 30 },
          stop_reason: 'end_turn',
          content: [{ type: 'text', text: json }],
        },
      ],
      [[json]],
    );
    const sdk = new AnthropicVisionClient({ client });
    const result = await sdk.analyze(
      buildInput({ outputSchema: { type: 'object' } }),
    );
    expect(result.output).not.toBeNull();
    expect(result.output!.caption).toBe('two cats');
    expect((result.output!.extracted as Record<string, unknown>).regions).toEqual([
      { tag: 'cat-1' },
    ]);
  });

  it('retries ONCE on malformed JSON and merges usage across attempts', async () => {
    const goodJson = '{"caption":"ok"}';
    const { client, calls } = makeFakeClient(
      [
        {
          usage: { input_tokens: 10, output_tokens: 4 },
          stop_reason: 'end_turn',
          content: [{ type: 'text', text: 'not json' }],
        },
        {
          usage: { input_tokens: 20, output_tokens: 6 },
          stop_reason: 'end_turn',
          content: [{ type: 'text', text: goodJson }],
        },
      ],
      [['not json'], [goodJson]],
    );
    const sdk = new AnthropicVisionClient({ client });
    const result = await sdk.analyze(
      buildInput({ outputSchema: { type: 'object' } }),
    );

    expect(calls).toHaveLength(2);
    expect(result.output).not.toBeNull();
    expect(result.output!.caption).toBe('ok');
    // Merged usage = first attempt + retry attempt.
    expect(result.usage.input_tokens).toBe(30);
    expect(result.usage.output_tokens).toBe(10);
  });

  it('surfaces refusal stop_reason as failureReason="refusal" with no retry', async () => {
    const { client, calls } = makeFakeClient(
      [
        {
          usage: { input_tokens: 50, output_tokens: 0 },
          stop_reason: 'refusal',
          content: [{ type: 'text', text: '' }],
        },
      ],
      [[]],
    );
    const sdk = new AnthropicVisionClient({ client });
    const result = await sdk.analyze(buildInput({ outputSchema: { type: 'object' } }));
    expect(calls).toHaveLength(1);
    expect(result.output).toBeNull();
    expect(result.failureReason).toBe('refusal');
  });

  it('picks up cache_read_input_tokens defensively from the wire shape', async () => {
    const { client } = makeFakeClient(
      [
        {
          usage: {
            input_tokens: 100,
            output_tokens: 20,
            cache_read_input_tokens: 80,
            cache_creation_input_tokens: 5,
          },
          stop_reason: 'end_turn',
          content: [{ type: 'text', text: 'ok' }],
        },
      ],
      [['ok']],
    );
    const sdk = new AnthropicVisionClient({ client });
    const result = await sdk.analyze(buildInput());
    expect(result.usage.cache_read_input_tokens).toBe(80);
    expect(result.usage.cache_creation_input_tokens).toBe(5);
    expect(result.cacheHit).toBe(true);
  });

  it('rejects an unsupported mime type without calling the SDK', async () => {
    const { client, calls } = makeFakeClient([], []);
    const sdk = new AnthropicVisionClient({ client });
    const result = await sdk.analyze(
      buildInput({ imageMimeType: 'image/tiff' }),
    );
    expect(calls).toHaveLength(0);
    expect(result.output).toBeNull();
    expect(result.failureReason).toBe('unknown');
  });

  it('treats an aborted stream as failureReason="aborted"', async () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const client: any = {
      messages: {
        stream() {
          const stream = {
            on() { return stream; },
            finalMessage() {
              const err = new Error('Request was aborted');
              err.name = 'AbortError';
              return Promise.reject(err);
            },
          };
          return stream;
        },
      },
    };
    const sdk = new AnthropicVisionClient({ client });
    const ac = new AbortController();
    ac.abort();
    const result = await sdk.analyze(buildInput({ abortSignal: ac.signal }));
    expect(result.output).toBeNull();
    expect(result.failureReason).toBe('aborted');
  });
});
