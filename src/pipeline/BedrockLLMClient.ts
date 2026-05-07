// social-api/src/pipeline/BedrockLLMClient.ts
//
// AWS Bedrock provider implementation of LLMClient. Lazy-requires
// @aws-sdk/client-bedrock-runtime so module load doesn't fail when the SDK
// isn't installed.
//
// Payload format: `anthropic_version: 'bedrock-2023-05-31'` — Bedrock's
// Anthropic-on-Bedrock shape. AWS has historically added/removed fields; if
// token accounting ever comes back as 0 in production, that's why, and the
// length/4 char fallback below covers it.

import type { LLMClient, LLMChunk, LLMStreamOptions } from './LLMClient';

const MODEL_ALIAS: Record<string, string> = {
  'opus':   'claude-opus-4-7',
  'sonnet': 'claude-sonnet-4-6',
  'haiku':  'claude-haiku-4-5-20251001',
};

const MODEL_MAP: Record<string, string> = {
  'claude-opus-4-7':           'us.anthropic.claude-opus-4-20250514-v1:0',
  'claude-sonnet-4-6':         'us.anthropic.claude-sonnet-4-20250514-v1:0',
  'claude-haiku-4-5-20251001': 'us.anthropic.claude-haiku-4-5-20251001-v1:0',
};

export class BedrockLLMClient implements LLMClient {
  readonly provider = 'bedrock' as const;

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

        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const { BedrockRuntimeClient, InvokeModelWithResponseStreamCommand } =
          require('@aws-sdk/client-bedrock-runtime');

        const llmRegion = process.env['PIPELINE_LLM_AWS_REGION'] ?? process.env.AWS_REGION ?? 'us-east-1';
        const client = new BedrockRuntimeClient({
          region: llmRegion,
        });

        const body: Record<string, unknown> = {
          anthropic_version: 'bedrock-2023-05-31',
          max_tokens: maxTokens,
          system: systemPrompt,
          messages: [{ role: 'user', content: userPrompt }],
        };
        if (temperature !== undefined) body.temperature = temperature;

        const canonical = MODEL_ALIAS[model] ?? model;
        const resolvedModel = MODEL_MAP[canonical] ?? canonical;
        const command = new InvokeModelWithResponseStreamCommand({
          modelId: resolvedModel,
          body: JSON.stringify(body),
          contentType: 'application/json',
          accept: 'application/json',
        });

        const response = await client.send(command);

        let fullResponse = '';
        let tokensIn = 0;
        let tokensOut = 0;

        if (response.body) {
          for await (const chunk of response.body) {
            if (signal?.aborted) {
              throw new DOMException('Aborted', 'AbortError');
            }

            if (chunk.chunk?.bytes) {
              const decoded = JSON.parse(
                Buffer.from(chunk.chunk.bytes).toString('utf8'),
              ) as Record<string, unknown>;

              if (decoded.type === 'content_block_delta') {
                const delta = decoded.delta as Record<string, unknown> | undefined;
                if (delta?.type === 'text_delta' && typeof delta.text === 'string') {
                  const token = delta.text;
                  fullResponse += token;
                  yield { done: false, token };
                }
              }

              if (decoded.type === 'message_delta') {
                const usage = decoded.usage as Record<string, unknown> | undefined;
                if (typeof usage?.output_tokens === 'number') tokensOut = usage.output_tokens;
              }

              if (decoded.type === 'message_start') {
                const message = decoded.message as Record<string, unknown> | undefined;
                const usage = message?.usage as Record<string, unknown> | undefined;
                if (typeof usage?.input_tokens === 'number') tokensIn = usage.input_tokens;
              }
            }
          }
        }

        if (tokensIn === 0) tokensIn = Math.max(1, Math.round(userPrompt.length / 4));
        if (tokensOut === 0) tokensOut = Math.max(1, Math.round(fullResponse.length / 4));

        yield { done: true, response: fullResponse, tokensIn, tokensOut };
      },
    };
  }
}
