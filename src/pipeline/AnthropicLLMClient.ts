// social-api/src/pipeline/AnthropicLLMClient.ts
//
// Anthropic provider implementation of LLMClient. Lazy-requires
// @anthropic-ai/sdk so the module is only loaded when stream() is called —
// allows the file to be imported before the SDK is installed.
//
// Token accounting:
//   - tokensIn  ← message_start.message.usage.input_tokens
//   - tokensOut ← message_delta.usage.output_tokens
//   - Both fall back to a length/4 char approximation if the SDK omits usage
//     (defensive; shouldn't happen on normal responses).
//
// Cancellation: await stream.finalMessage().catch(() => {}) on abort cleanly
// terminates the underlying SSE connection instead of leaking it.

import type { LLMClient, LLMChunk, LLMStreamOptions } from './LLMClient';

export class AnthropicLLMClient implements LLMClient {
  readonly provider = 'anthropic' as const;

  stream(
    model: string,
    systemPrompt: string,
    userPrompt: string,
    opts: LLMStreamOptions = {},
  ): AsyncIterable<LLMChunk> {
    const { temperature, maxTokens = 1024, signal } = opts;

    return {
      [Symbol.asyncIterator]: async function* () {
        if (signal?.aborted) {
          throw new DOMException('Aborted', 'AbortError');
        }

        // Lazy import so the SDK is only loaded when actually needed.
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const Anthropic = require('@anthropic-ai/sdk').default ?? require('@anthropic-ai/sdk');
        const client = new Anthropic();

        const params: Record<string, unknown> = {
          model,
          max_tokens: maxTokens,
          system: systemPrompt,
          messages: [{ role: 'user', content: userPrompt }],
        };
        if (temperature !== undefined) params.temperature = temperature;

        const stream = client.messages.stream(params);

        let fullResponse = '';
        let tokensIn = 0;
        let tokensOut = 0;

        for await (const event of stream) {
          if (signal?.aborted) {
            await stream.finalMessage().catch(() => { /* best-effort cleanup */ });
            throw new DOMException('Aborted', 'AbortError');
          }

          if (event.type === 'content_block_delta' && event.delta.type === 'text_delta') {
            const token = event.delta.text;
            fullResponse += token;
            yield { done: false, token };
          }

          if (event.type === 'message_delta' && event.usage) {
            tokensOut = event.usage.output_tokens ?? 0;
          }

          if (event.type === 'message_start' && event.message.usage) {
            tokensIn = event.message.usage.input_tokens ?? 0;
          }
        }

        // Fallback token counting if the SDK didn't emit usage events.
        if (tokensIn === 0) tokensIn = Math.max(1, Math.round(userPrompt.length / 4));
        if (tokensOut === 0) tokensOut = Math.max(1, Math.round(fullResponse.length / 4));

        yield { done: true, response: fullResponse, tokensIn, tokensOut };
      },
    };
  }
}
