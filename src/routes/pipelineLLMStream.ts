// platform-api/src/routes/pipelineLLMStream.ts
//
// POST /api/pipelines/llm/stream — server-side LLM streaming proxy.
//
// Accepts a prompt payload, streams token chunks back as SSE events using the
// configured LLM provider (PIPELINE_LLM_PROVIDER env, default 'anthropic').
// The API key lives server-side only — the frontend never sees it.
//
// SSE event format (AG-UI shapes):
//   data: {"type":"RUN_STARTED","runId":"<uuid>","threadId":"<uuid>"}
//   data: {"type":"TEXT_MESSAGE_CONTENT","messageId":"<uuid>","delta":"Hello"}
//   data: {"type":"RUN_FINISHED","runId":"<uuid>"}
//   data: {"type":"RUN_ERROR","message":"...","code":"LLM_STREAM_ERROR"}
//
// The stream terminates after RUN_FINISHED / RUN_ERROR followed by closing
// the connection. On client disconnect (AbortController), the LLM stream is
// cancelled via AbortSignal to avoid wasting tokens.
//
// Wire format is unchanged: data: <json>\n\n (standard SSE).

import { randomUUID } from 'crypto';
import { Router } from 'express';
import { createLLMClient } from '../pipeline/createLLMClient';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import { createRateLimiter } from '../middleware/rateLimit';
import { withContext } from '../lib/logger';
import { gatedLLMClient } from '../lib/modelGate';

// ---------------------------------------------------------------------------
// AG-UI SSE event shapes
// ---------------------------------------------------------------------------

interface AgUiRunStarted {
  type: 'RUN_STARTED';
  runId: string;
  threadId?: string;
}

interface AgUiTextMessageContent {
  type: 'TEXT_MESSAGE_CONTENT';
  messageId: string;
  delta: string;
}

interface AgUiRunFinished {
  type: 'RUN_FINISHED';
  runId: string;
}

interface AgUiRunError {
  type: 'RUN_ERROR';
  message: string;
  code?: string;
}

type AgUiSseEvent = AgUiRunStarted | AgUiTextMessageContent | AgUiRunFinished | AgUiRunError;

const log = withContext({ route: 'pipelineLLMStream' });

const llmStreamRateLimiter = createRateLimiter({
  scope: 'pipeline-llm-stream',
  capacity: 20,
  refillRate: 20,
  refillIntervalMs: 60_000,
  key: (req) => req.user?.sub ?? req.ip ?? 'anon',
});

interface LLMStreamRequest {
  model: string;
  systemPrompt: string;
  userPrompt: string;
  temperature?: number;
  maxTokens?: number;
}

const MODEL_ALIAS: Record<string, string> = {
  'opus':   'claude-opus-4-7',
  'sonnet': 'claude-sonnet-4-6',
  'haiku':  'claude-haiku-4-5-20251001',
};

const BEDROCK_MODEL_MAP: Record<string, string> = {
  'claude-opus-4-7':           'us.anthropic.claude-opus-4-20250514-v1:0',
  'claude-sonnet-4-6':         'us.anthropic.claude-sonnet-4-20250514-v1:0',
  'claude-haiku-4-5-20251001': 'us.anthropic.claude-haiku-4-5-20251001-v1:0',
};

function resolveModelId(model: string): string {
  const resolved = MODEL_ALIAS[model] ?? model;
  const provider = process.env['PIPELINE_LLM_PROVIDER'] ?? 'anthropic';
  if (provider === 'bedrock' && BEDROCK_MODEL_MAP[resolved]) {
    return BEDROCK_MODEL_MAP[resolved]!;
  }
  return resolved;
}

export const pipelineLLMStreamRouter = Router();

pipelineLLMStreamRouter.post(
  '/',
  llmStreamRateLimiter,
  asyncHandler<unknown, unknown, LLMStreamRequest>(async (req, res) => {
    const userId = req.user?.sub ?? 'anon';
    const body = req.body as LLMStreamRequest;

    if (!body.model || typeof body.model !== 'string') {
      throw new ValidationError('model is required');
    }
    if (!body.systemPrompt || typeof body.systemPrompt !== 'string') {
      throw new ValidationError('systemPrompt is required');
    }
    if (!body.userPrompt || typeof body.userPrompt !== 'string') {
      throw new ValidationError('userPrompt is required');
    }
    if (body.temperature !== undefined && (typeof body.temperature !== 'number' || body.temperature < 0 || body.temperature > 2)) {
      throw new ValidationError('temperature must be a number between 0 and 2');
    }
    if (body.maxTokens !== undefined && (typeof body.maxTokens !== 'number' || body.maxTokens < 1 || body.maxTokens > 8192)) {
      throw new ValidationError('maxTokens must be a number between 1 and 8192');
    }

    const runId = randomUUID();
    const messageId = randomUUID();
    const threadId = randomUUID();

    log.info(
      { userId, model: body.model, maxTokens: body.maxTokens, runId },
      'llm stream started',
    );

    const abortController = new AbortController();

    req.on('close', () => {
      abortController.abort();
    });

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    });

    function writeEvent(event: AgUiSseEvent): void {
      if (!res.writableEnded) {
        res.write(`data: ${JSON.stringify(event)}\n\n`);
      }
    }

    try {
      const client = gatedLLMClient(createLLMClient(), 'pipeline');
      const modelId = resolveModelId(body.model);
      const stream = client.stream(modelId, body.systemPrompt, body.userPrompt, {
        temperature: body.temperature,
        maxTokens: body.maxTokens ?? 1024,
        signal: abortController.signal,
      });

      writeEvent({ type: 'RUN_STARTED', runId, threadId });

      for await (const chunk of stream) {
        if (res.writableEnded) break;
        if (!chunk.done) {
          writeEvent({ type: 'TEXT_MESSAGE_CONTENT', messageId, delta: chunk.token });
        }
        // done:true chunk (with response/tokensIn/tokensOut) is consumed silently;
        // RUN_FINISHED is emitted after the loop to signal stream completion.
      }

      writeEvent({ type: 'RUN_FINISHED', runId });
    } catch (err) {
      if ((err as Error)?.name === 'AbortError') {
        log.info({ userId, model: body.model, runId }, 'llm stream aborted by client');
      } else {
        log.error(
          { userId, model: body.model, runId, err: err instanceof Error ? err.message : String(err) },
          'llm stream error',
        );
        writeEvent({
          type: 'RUN_ERROR',
          message: err instanceof Error ? err.message : 'LLM stream failed',
          code: 'LLM_STREAM_ERROR',
        });
      }
    } finally {
      if (!res.writableEnded) {
        res.end();
      }
    }
  }),
);
