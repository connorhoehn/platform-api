// social-api/src/routes/__tests__/pipelineLLMStream.test.ts
//
// Unit tests for pipelineLLMStreamRouter: POST /api/pipelines/llm/stream.
// Covers input validation (400s) and the happy-path SSE streaming flow
// with a mocked LLM client.

import express, { type NextFunction, type Request, type Response } from 'express';

jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
}));

jest.mock('../../lib/logger', () => ({
  withContext: () => ({
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    child: () => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() }),
  }),
}));

// Mock LLM client factory — default: never called (validation tests).
const mockStream = jest.fn();
jest.mock('../../pipeline/createLLMClient', () => ({
  createLLMClient: () => ({ stream: mockStream }),
}));

import { errorHandler } from '../../middleware/error-handler';
import { pipelineLLMStreamRouter } from '../pipelineLLMStream';

// ---------------------------------------------------------------------------
// Test rig (mirrors pipelineTriggers.test.ts)
// ---------------------------------------------------------------------------

interface MockRes {
  statusCode: number;
  headers: Record<string, string | number | readonly string[]>;
  body: unknown;
  ended: boolean;
  chunks: string[];
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  setHeader(name: string, value: number | string | readonly string[]): MockRes;
  getHeader(name: string): unknown;
  writeHead(code: number, hdrs?: Record<string, string>): MockRes;
  write(chunk: string): boolean;
  end(...args: unknown[]): MockRes;
  writableEnded: boolean;
}

function mockRes(): MockRes {
  let resolveFinished!: () => void;
  const finished = new Promise<void>((r) => { resolveFinished = r; });
  const r: MockRes = {
    statusCode: 200,
    headers: {},
    body: undefined,
    ended: false,
    writableEnded: false,
    chunks: [],
    finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; this.writableEnded = true; resolveFinished(); return this; },
    setHeader(name, value) { this.headers[String(name)] = value; return this; },
    getHeader(name) { return this.headers[String(name)]; },
    writeHead(code, hdrs) {
      this.statusCode = code;
      for (const [k, v] of Object.entries(hdrs ?? {})) this.headers[k] = v;
      return this;
    },
    write(chunk) { this.chunks.push(chunk); return true; },
    end(..._args) {
      this.ended = true;
      this.writableEnded = true;
      resolveFinished();
      return this;
    },
  };
  return r;
}

async function runPost(
  app: express.Express,
  body: unknown,
): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: 'POST',
    url: '/',
    originalUrl: '/',
    path: '/',
    headers: {},
    query: {},
    body,
    user: { sub: 'user-1' },
    ip: '127.0.0.1',
    on: jest.fn(), // for req.on('close', ...)
  } as unknown as Request;

  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) { reject(err); return; }
      if (!res.ended) res.status(404).json({ error: 'not found' });
      resolve();
    };
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
      req,
      res as unknown as Response,
      finalHandler,
    );
    res.finished.then(() => resolve()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    if (!(req as Request & { user?: unknown }).user) {
      (req as Request & { user?: { sub: string } }).user = { sub: 'user-1' };
    }
    next();
  });
  app.use('/', pipelineLLMStreamRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  mockStream.mockReset();
});

// ---------------------------------------------------------------------------
// Input validation
// ---------------------------------------------------------------------------

describe('POST /pipelines/llm/stream — input validation', () => {
  test('400 when model is missing', async () => {
    const res = await runPost(buildApp(), {
      systemPrompt: 'You are helpful.',
      userPrompt: 'Hello',
    });
    expect(res.statusCode).toBe(400);
    expect(mockStream).not.toHaveBeenCalled();
  });

  test('400 when systemPrompt is missing', async () => {
    const res = await runPost(buildApp(), {
      model: 'claude-sonnet-4-6',
      userPrompt: 'Hello',
    });
    expect(res.statusCode).toBe(400);
  });

  test('400 when userPrompt is missing', async () => {
    const res = await runPost(buildApp(), {
      model: 'claude-sonnet-4-6',
      systemPrompt: 'You are helpful.',
    });
    expect(res.statusCode).toBe(400);
  });

  test('400 when temperature is negative', async () => {
    const res = await runPost(buildApp(), {
      model: 'claude-sonnet-4-6',
      systemPrompt: 'sys',
      userPrompt: 'hi',
      temperature: -0.1,
    });
    expect(res.statusCode).toBe(400);
  });

  test('400 when temperature exceeds 2', async () => {
    const res = await runPost(buildApp(), {
      model: 'claude-sonnet-4-6',
      systemPrompt: 'sys',
      userPrompt: 'hi',
      temperature: 2.1,
    });
    expect(res.statusCode).toBe(400);
  });

  test('400 when maxTokens is 0', async () => {
    const res = await runPost(buildApp(), {
      model: 'claude-sonnet-4-6',
      systemPrompt: 'sys',
      userPrompt: 'hi',
      maxTokens: 0,
    });
    expect(res.statusCode).toBe(400);
  });

  test('400 when maxTokens exceeds 8192', async () => {
    const res = await runPost(buildApp(), {
      model: 'claude-sonnet-4-6',
      systemPrompt: 'sys',
      userPrompt: 'hi',
      maxTokens: 9000,
    });
    expect(res.statusCode).toBe(400);
  });
});

// ---------------------------------------------------------------------------
// SSE streaming — happy path with mocked LLM
// ---------------------------------------------------------------------------

function parseSseChunk(chunk: string): Record<string, unknown> {
  // SSE wire format: "data: <json>\n\n"
  const jsonStr = chunk.replace(/^data: /, '').trimEnd();
  return JSON.parse(jsonStr) as Record<string, unknown>;
}

describe('POST /pipelines/llm/stream — SSE happy path', () => {
  test('emits RUN_STARTED, TEXT_MESSAGE_CONTENT per token, RUN_FINISHED (AG-UI shapes)', async () => {
    async function* fakeStream() {
      yield { done: false, token: 'Hello' };
      yield { done: false, token: ' world' };
      yield { done: true, response: 'Hello world', tokensIn: 5, tokensOut: 2 };
    }
    mockStream.mockReturnValue(fakeStream());

    const res = await runPost(buildApp(), {
      model: 'claude-sonnet-4-6',
      systemPrompt: 'You are helpful.',
      userPrompt: 'Hello',
    });

    expect(res.statusCode).toBe(200);
    expect(res.headers['Content-Type']).toBe('text/event-stream');

    // 1 RUN_STARTED + 2 TEXT_MESSAGE_CONTENT + 1 RUN_FINISHED = 4 chunks
    expect(res.chunks).toHaveLength(4);

    const events = res.chunks.map(parseSseChunk);

    // RUN_STARTED
    expect(events[0]).toMatchObject({ type: 'RUN_STARTED' });
    expect(typeof (events[0] as { runId: string }).runId).toBe('string');
    expect(typeof (events[0] as { threadId: string }).threadId).toBe('string');

    // TEXT_MESSAGE_CONTENT for each token
    expect(events[1]).toMatchObject({ type: 'TEXT_MESSAGE_CONTENT', delta: 'Hello' });
    expect(events[2]).toMatchObject({ type: 'TEXT_MESSAGE_CONTENT', delta: ' world' });

    // Both token events share the same stable messageId
    expect((events[1] as { messageId: string }).messageId).toBe((events[2] as { messageId: string }).messageId);

    // RUN_FINISHED
    expect(events[3]).toMatchObject({ type: 'RUN_FINISHED' });
    expect((events[3] as { runId: string }).runId).toBe((events[0] as { runId: string }).runId);
  });

  test('done:true LLM chunk does not emit an extra SSE event', async () => {
    async function* fakeStream() {
      yield { done: true, response: 'empty', tokensIn: 1, tokensOut: 1 };
    }
    mockStream.mockReturnValue(fakeStream());

    const res = await runPost(buildApp(), {
      model: 'claude-sonnet-4-6',
      systemPrompt: 'sys',
      userPrompt: 'user',
    });

    // RUN_STARTED + RUN_FINISHED only — no TEXT_MESSAGE_CONTENT for done:true
    expect(res.chunks).toHaveLength(2);
    const events = res.chunks.map(parseSseChunk);
    expect(events[0]).toMatchObject({ type: 'RUN_STARTED' });
    expect(events[1]).toMatchObject({ type: 'RUN_FINISHED' });
  });

  test('calls stream with correct model, prompts and defaults', async () => {
    async function* empty() { /* no chunks */ }
    mockStream.mockReturnValue(empty());

    await runPost(buildApp(), {
      model: 'claude-sonnet-4-6',
      systemPrompt: 'sys',
      userPrompt: 'user',
    });

    expect(mockStream).toHaveBeenCalledWith(
      'claude-sonnet-4-6',
      'sys',
      'user',
      expect.objectContaining({ maxTokens: 1024 }),
    );
  });

  test('writes a RUN_ERROR SSE event when LLM throws a non-abort error', async () => {
    async function* failing(): AsyncGenerator<never> {
      throw new Error('LLM overloaded');
      // unreachable, needed to satisfy TS generator return type
      yield undefined as never;
    }
    mockStream.mockReturnValue(failing());

    const res = await runPost(buildApp(), {
      model: 'claude-sonnet-4-6',
      systemPrompt: 'sys',
      userPrompt: 'user',
    });

    expect(res.statusCode).toBe(200); // SSE headers already sent
    const errorChunk = res.chunks.find((c) => c.includes('"RUN_ERROR"'));
    expect(errorChunk).toBeDefined();
    const event = parseSseChunk(errorChunk!);
    expect(event).toMatchObject({ type: 'RUN_ERROR', message: 'LLM overloaded', code: 'LLM_STREAM_ERROR' });
  });

  test('logs AbortError as info and does not write RUN_ERROR chunk', async () => {
    async function* abortStream(): AsyncGenerator<never> {
      const err = new Error('Request aborted');
      err.name = 'AbortError';
      throw err;
      yield undefined as never;
    }
    mockStream.mockReturnValue(abortStream());

    const res = await runPost(buildApp(), {
      model: 'claude-sonnet-4-6',
      systemPrompt: 'sys',
      userPrompt: 'user',
    });

    // AbortError does not write a RUN_ERROR SSE chunk to the client
    expect(res.chunks.every((c) => !c.includes('"RUN_ERROR"'))).toBe(true);
  });

  test('resolves Bedrock model ID when PIPELINE_LLM_PROVIDER=bedrock', async () => {
    process.env['PIPELINE_LLM_PROVIDER'] = 'bedrock';
    async function* empty() { /* no chunks */ }
    mockStream.mockReturnValue(empty());

    await runPost(buildApp(), {
      model: 'claude-sonnet-4-6',
      systemPrompt: 'sys',
      userPrompt: 'user',
    });

    const [modelArg] = mockStream.mock.calls[0] as [string];
    expect(modelArg).toBe('us.anthropic.claude-sonnet-4-20250514-v1:0');

    delete process.env['PIPELINE_LLM_PROVIDER'];
  });

  test('passes through model unchanged for unknown model in bedrock mode', async () => {
    process.env['PIPELINE_LLM_PROVIDER'] = 'bedrock';
    async function* empty() { /* no chunks */ }
    mockStream.mockReturnValue(empty());

    await runPost(buildApp(), {
      model: 'custom-model-xyz',
      systemPrompt: 'sys',
      userPrompt: 'user',
    });

    const [modelArg] = mockStream.mock.calls[0] as [string];
    expect(modelArg).toBe('custom-model-xyz');

    delete process.env['PIPELINE_LLM_PROVIDER'];
  });
});
