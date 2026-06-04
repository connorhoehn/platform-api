// platform-api/src/pipeline/image/__tests__/VisionClient.test.ts
//
// Unit tests for VisionClient. We mock fetch — no real HTTP. Code parity
// with JudgeClient.test.ts, except the transport is HTTP not subprocess.

import { VisionClient, extractJsonObject, buildVisionSystemPrompt } from '../VisionClient';
import type { VisionAnalyzeInput } from '../types';

const SAMPLE_IMAGE = Buffer.from([0xff, 0xd8, 0xff, 0xe0]); // JPEG magic header
const API_KEY = 'sk-ant-fake-key';

function baseInput(over: Partial<VisionAnalyzeInput> = {}): VisionAnalyzeInput {
  return {
    model: 'claude-sonnet-4-6',
    systemPrompt: '',
    userPrompt: 'Describe this image.',
    imageBytes: SAMPLE_IMAGE,
    imageMimeType: 'image/jpeg',
    maxOutputTokens: 1024,
    ...over,
  };
}

function makeOkResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  }) as unknown as Response;
}

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

describe('VisionClient.analyze — happy paths', () => {
  it('caption mode: returns full text as caption when no schema', async () => {
    const fetchImpl = jest.fn().mockResolvedValue(
      makeOkResponse({
        content: [{ type: 'text', text: 'A red apple on a wooden table.' }],
        usage: { input_tokens: 100, output_tokens: 20 },
        stop_reason: 'end_turn',
      }),
    );
    const client = new VisionClient({ apiKey: API_KEY, fetchImpl });
    const r = await client.analyze(baseInput());

    expect(r.failureReason).toBeUndefined();
    expect(r.output?.caption).toBe('A red apple on a wooden table.');
    expect(r.output?.extracted).toBeNull();
    expect(r.usage.input_tokens).toBe(100);
    expect(r.usage.output_tokens).toBe(20);
    expect(r.cacheHit).toBe(false);
    expect(r.rawText).toBe('A red apple on a wooden table.');
  });

  it('extract mode: parses the structured JSON output', async () => {
    const fetchImpl = jest.fn().mockResolvedValue(
      makeOkResponse({
        content: [{ type: 'text', text: '{"caption":"receipt","total":12.50}' }],
        usage: { input_tokens: 200, output_tokens: 30 },
        stop_reason: 'end_turn',
      }),
    );
    const client = new VisionClient({ apiKey: API_KEY, fetchImpl });
    const r = await client.analyze(
      baseInput({ outputSchema: { type: 'object' } }),
    );
    expect(r.output?.caption).toBe('receipt');
    expect((r.output?.extracted as { total: number })?.total).toBe(12.5);
  });

  it('propagates cache_read_input_tokens as cacheHit=true + cacheReadTokens', async () => {
    const fetchImpl = jest.fn().mockResolvedValue(
      makeOkResponse({
        content: [{ type: 'text', text: 'cached.' }],
        usage: { input_tokens: 50, output_tokens: 10, cache_read_input_tokens: 200 },
        stop_reason: 'end_turn',
      }),
    );
    const client = new VisionClient({ apiKey: API_KEY, fetchImpl });
    const r = await client.analyze(baseInput());
    expect(r.cacheHit).toBe(true);
    expect(r.usage.cache_read_input_tokens).toBe(200);
  });

  it('sends image+text content blocks with base64 inline data', async () => {
    const fetchImpl = jest.fn().mockResolvedValue(
      makeOkResponse({ content: [{ type: 'text', text: 'ok' }], usage: { input_tokens: 1, output_tokens: 1 }, stop_reason: 'end_turn' }),
    );
    const client = new VisionClient({ apiKey: API_KEY, fetchImpl });
    await client.analyze(baseInput());

    const [, opts] = fetchImpl.mock.calls[0];
    const body = JSON.parse((opts as RequestInit).body as string);
    expect(body.messages[0].role).toBe('user');
    const content = body.messages[0].content;
    expect(content[0].type).toBe('image');
    expect(content[0].source.type).toBe('base64');
    expect(content[0].source.media_type).toBe('image/jpeg');
    expect(content[0].source.data).toBe(SAMPLE_IMAGE.toString('base64'));
    expect(content[1].type).toBe('text');
    expect(content[1].text).toBe('Describe this image.');
  });

  it('sets x-api-key + anthropic-version + content-type headers', async () => {
    const fetchImpl = jest.fn().mockResolvedValue(
      makeOkResponse({ content: [{ type: 'text', text: 'ok' }], usage: { input_tokens: 1, output_tokens: 1 }, stop_reason: 'end_turn' }),
    );
    const client = new VisionClient({ apiKey: API_KEY, fetchImpl });
    await client.analyze(baseInput());

    const [, opts] = fetchImpl.mock.calls[0];
    const headers = (opts as RequestInit).headers as Record<string, string>;
    expect(headers['x-api-key']).toBe(API_KEY);
    expect(headers['anthropic-version']).toBe('2023-06-01');
    expect(headers['content-type']).toBe('application/json');
  });
});

describe('VisionClient.analyze — failure modes (soft-fail)', () => {
  it('returns failureReason=http-error when no API key is configured', async () => {
    const fetchImpl = jest.fn();
    const client = new VisionClient({ apiKey: '', fetchImpl });
    const r = await client.analyze(baseInput());
    expect(r.failureReason).toBe('http-error');
    expect(r.output).toBeNull();
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it('returns failureReason=http-error on non-2xx response', async () => {
    const fetchImpl = jest.fn().mockResolvedValue(
      new Response('{"type":"error"}', { status: 500 }) as unknown as Response,
    );
    const client = new VisionClient({ apiKey: API_KEY, fetchImpl });
    const r = await client.analyze(baseInput());
    expect(r.failureReason).toBe('http-error');
    expect(r.output).toBeNull();
  });

  it('returns failureReason=overloaded on 529', async () => {
    const fetchImpl = jest.fn().mockResolvedValue(
      new Response('{"type":"overloaded"}', { status: 529 }) as unknown as Response,
    );
    const client = new VisionClient({ apiKey: API_KEY, fetchImpl });
    const r = await client.analyze(baseInput());
    expect(r.failureReason).toBe('overloaded');
  });

  it('returns failureReason=refusal when stop_reason is refusal', async () => {
    const fetchImpl = jest.fn().mockResolvedValue(
      makeOkResponse({
        content: [{ type: 'text', text: 'refused' }],
        usage: { input_tokens: 10, output_tokens: 0 },
        stop_reason: 'refusal',
      }),
    );
    const client = new VisionClient({ apiKey: API_KEY, fetchImpl });
    const r = await client.analyze(baseInput());
    expect(r.failureReason).toBe('refusal');
    expect(r.usage.input_tokens).toBe(10);
  });

  it('returns failureReason=aborted when fetch throws AbortError', async () => {
    const fetchImpl = jest.fn().mockImplementation(() => {
      const e = new Error('aborted');
      e.name = 'AbortError';
      return Promise.reject(e);
    });
    const client = new VisionClient({ apiKey: API_KEY, fetchImpl });
    const r = await client.analyze(baseInput());
    expect(r.failureReason).toBe('aborted');
  });

  it('returns failureReason=http-error on network error', async () => {
    const fetchImpl = jest.fn().mockRejectedValue(new Error('ECONNREFUSED'));
    const client = new VisionClient({ apiKey: API_KEY, fetchImpl });
    const r = await client.analyze(baseInput());
    expect(r.failureReason).toBe('http-error');
  });

  it('schema mode + unparseable text → one corrective retry, then failureReason=malformed', async () => {
    const fetchImpl = jest
      .fn()
      .mockResolvedValueOnce(
        makeOkResponse({
          content: [{ type: 'text', text: 'sorry no json here' }],
          usage: { input_tokens: 100, output_tokens: 5 },
          stop_reason: 'end_turn',
        }),
      )
      .mockResolvedValueOnce(
        makeOkResponse({
          content: [{ type: 'text', text: 'still no json' }],
          usage: { input_tokens: 120, output_tokens: 6 },
          stop_reason: 'end_turn',
        }),
      );
    const client = new VisionClient({ apiKey: API_KEY, fetchImpl });
    const r = await client.analyze(baseInput({ outputSchema: { type: 'object' } }));

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(r.failureReason).toBe('malformed');
    // Usage from BOTH attempts must be summed so cost telemetry is honest.
    expect(r.usage.input_tokens).toBe(220);
    expect(r.usage.output_tokens).toBe(11);
  });

  it('schema mode + corrective retry succeeds → returns output, usage summed', async () => {
    const fetchImpl = jest
      .fn()
      .mockResolvedValueOnce(
        makeOkResponse({
          content: [{ type: 'text', text: 'no json' }],
          usage: { input_tokens: 100, output_tokens: 5 },
          stop_reason: 'end_turn',
        }),
      )
      .mockResolvedValueOnce(
        makeOkResponse({
          content: [{ type: 'text', text: '{"caption":"ok"}' }],
          usage: { input_tokens: 110, output_tokens: 8 },
          stop_reason: 'end_turn',
        }),
      );
    const client = new VisionClient({ apiKey: API_KEY, fetchImpl });
    const r = await client.analyze(baseInput({ outputSchema: { type: 'object' } }));

    expect(r.failureReason).toBeUndefined();
    expect(r.output?.caption).toBe('ok');
    expect(r.usage.input_tokens).toBe(210);
    expect(r.usage.output_tokens).toBe(13);
  });
});
