// The devbox loop, against a REAL http server speaking the re-framed
// stream-json SSE the middleware sends. A stubbed fetch would prove the
// parser and nothing about chunk boundaries, headers or the session dance —
// and chunk boundaries are exactly where an SSE reader goes wrong.
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { SERVICE_AUTH_HEADER } from '@connorhoehn/service-runtime';
import {
  DevboxAgentClient, DevboxUnavailableError, DEVBOX_CONTINUE_PROMPT,
  clearRunLabs, contextTokensOf, recallRunLab, rememberRunLab, saysDone, takeRunLab,
} from '../DevboxAgentClient';
import type { AgentEvent } from '../types';

const SECRET = 'test-service-auth-secret';

interface TaskCall { prompt: string; model: string; resume: boolean }

/** A middleware stand-in: one session, and a scripted answer per task call. */
function fakeMiddleware(script: (call: TaskCall, index: number) => string[]): {
  server: Server; url: Promise<string>; calls: TaskCall[]; deleted: string[]; signed: string[];
} {
  const calls: TaskCall[] = [];
  const deleted: string[] = [];
  const signed: string[] = [];

  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      const body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) as Record<string, unknown> : {};
      signed.push(String(req.headers[SERVICE_AUTH_HEADER.toLowerCase()] ?? ''));
      const path = req.url ?? '';

      if (req.method === 'POST' && path.endsWith('/internal/devbox/sessions')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ sessionId: 'sess-1', boxId: 'box-1' }));
        return;
      }
      if (req.method === 'DELETE') {
        deleted.push(path);
        res.writeHead(204).end();
        return;
      }
      if (req.method === 'POST' && path.endsWith('/shell-url')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ url: 'https://shell.example/abc', expiresAt: '2026-09-21T00:00:00.000Z' }));
        return;
      }
      if (req.method === 'POST' && path.endsWith('/task')) {
        const index = calls.length;
        calls.push({ prompt: String(body.prompt ?? ''), model: String(body.model ?? ''), resume: body.resume === true });
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        for (const frame of script(calls[index], index)) res.write(frame);
        res.end();
        return;
      }
      res.writeHead(404).end();
    });
  });

  const url = new Promise<string>((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve(`http://127.0.0.1:${(server.address() as AddressInfo).port}`));
  });
  return { server, url, calls, deleted, signed };
}

const sse = (frame: Record<string, unknown>): string => `data: ${JSON.stringify(frame)}\n\n`;

const usage = (input: number, out = 50): Record<string, unknown> => ({
  input_tokens: input, output_tokens: out, cache_read_input_tokens: 0, cache_creation_input_tokens: 0,
});

/** A plausible one-task stream: some text, a tool round-trip, then the result. */
function turn(text: string, input: number): string[] {
  return [
    sse({ type: 'task_started', ts: '2026-09-21T00:00:00.000Z' }),
    sse({ type: 'assistant', text: 'working…' }),
    sse({ type: 'tool_use', id: 't1', name: 'Bash', input: { command: 'ls' }, ts: '2026-09-21T00:00:01.000Z' }),
    sse({ type: 'tool_result', tool_use_id: 't1', content: 'notes.md', ts: '2026-09-21T00:00:02.000Z' }),
    sse({ type: 'result', result: text, usage: usage(input) }),
  ];
}

async function collect(stream: AsyncIterable<AgentEvent>): Promise<AgentEvent[]> {
  const events: AgentEvent[] = [];
  for await (const ev of stream) events.push(ev);
  return events;
}

const doneEvent = (events: AgentEvent[]) => events.find((e) => e.type === 'done') as Extract<AgentEvent, { type: 'done' }>;

let harness: ReturnType<typeof fakeMiddleware> | null = null;

afterEach(async () => {
  clearRunLabs();
  if (harness) await new Promise<void>((r) => harness!.server.close(() => r()));
  harness = null;
});

async function client(script: (call: TaskCall, index: number) => string[]): Promise<DevboxAgentClient> {
  harness = fakeMiddleware(script);
  return new DevboxAgentClient({ baseUrl: await harness.url, secret: SECRET });
}

describe('DevboxAgentClient', () => {
  it('opens a session, runs until DONE, and reports complete', async () => {
    const c = await client((_call, i) => turn(i === 1 ? 'here is the deck\nDONE' : 'still going', 1000 * (i + 1)));
    const events = await collect(c.run('build the deck', { model: 'opus', maxTurns: 5, contextBudgetTokens: 200_000 }));

    expect(events[0]).toEqual({ type: 'session', sessionId: 'sess-1', boxId: 'box-1', created: true });
    const done = doneEvent(events);
    expect(done.stopReason).toBe('complete');
    expect(done.iterations).toBe(2);
    expect(done.sessionId).toBe('sess-1');
    expect(done.text).toContain('DONE');

    // First task carries the real prompt and does NOT resume; the second resumes.
    expect(harness!.calls).toEqual([
      { prompt: 'build the deck', model: 'opus', resume: false },
      { prompt: DEVBOX_CONTINUE_PROMPT, model: 'opus', resume: true },
    ]);
    expect(harness!.signed.every((h) => h.length > 0)).toBe(true);
  });

  it('emits an iteration event per pass, carrying the context against the budget', async () => {
    const c = await client((_call, i) => turn(i === 1 ? 'DONE' : 'more', 40_000 * (i + 1)));
    const events = await collect(c.run('x', { maxTurns: 5, contextBudgetTokens: 200_000 }));
    expect(events.filter((e) => e.type === 'iteration')).toEqual([
      { type: 'iteration', iteration: 1, contextTokens: 40_000, contextBudgetTokens: 200_000, model: 'sonnet' },
      { type: 'iteration', iteration: 2, contextTokens: 80_000, contextBudgetTokens: 200_000, model: 'sonnet' },
    ]);
  });

  it('stops with budget when the session\'s context grows past the ceiling', async () => {
    // Never says DONE; context grows 30k per resumed task.
    const c = await client((_call, i) => turn('still going', 30_000 * (i + 1)));
    const done = doneEvent(await collect(c.run('x', { maxTurns: 20, contextBudgetTokens: 100_000 })));
    expect(done.stopReason).toBe('budget');
    expect(done.contextTokens).toBeGreaterThanOrEqual(100_000);
    expect(done.iterations).toBe(4);
  });

  it('stops with max_turns when the agent never finishes and the budget is generous', async () => {
    const c = await client(() => turn('still going', 100));
    const done = doneEvent(await collect(c.run('x', { maxTurns: 3, contextBudgetTokens: 900_000 })));
    expect(done.stopReason).toBe('max_turns');
    expect(done.iterations).toBe(3);
    expect(harness!.calls).toHaveLength(3);
  });

  it('prefers DONE over budget — finishing ON the boundary is finishing', async () => {
    const c = await client(() => turn('delivered\nDONE', 500_000));
    const done = doneEvent(await collect(c.run('x', { maxTurns: 5, contextBudgetTokens: 100_000 })));
    expect(done.stopReason).toBe('complete');
  });

  it('stops with error when the stream says so', async () => {
    const c = await client(() => [sse({ type: 'devbox_error', error: 'the box went away' })]);
    const done = doneEvent(await collect(c.run('x', { maxTurns: 3 })));
    expect(done.stopReason).toBe('error');
    expect(done.text).toContain('the box went away');
  });

  it('stops with timeout once the wall clock is past, without another task call', async () => {
    let clock = 0;
    harness = fakeMiddleware(() => turn('still going', 100));
    const c = new DevboxAgentClient({ baseUrl: await harness.url, secret: SECRET, now: () => (clock += 400_000) });
    const done = doneEvent(await collect(c.run('x', { maxTurns: 10, timeoutMs: 600_000 })));
    expect(done.stopReason).toBe('timeout');
  });

  it('builds the tool trace from tool_use/tool_result pairs and caps each payload', async () => {
    const big = 'x'.repeat(50_000);
    harness = fakeMiddleware(() => [
      sse({ type: 'tool_use', id: 't1', name: 'Read', input: { file: 'a.md' } }),
      sse({ type: 'tool_result', tool_use_id: 't1', content: big }),
      sse({ type: 'result', result: 'DONE', usage: usage(10) }),
    ]);
    const c = new DevboxAgentClient({ baseUrl: await harness.url, secret: SECRET, maxToolPayloadChars: 100 });
    const done = doneEvent(await collect(c.run('x', { maxTurns: 2 })));
    expect(done.toolCalls).toHaveLength(1);
    expect(done.toolCalls[0]).toMatchObject({ name: 'Read', ok: true });
    expect(String(done.toolCalls[0].output)).toHaveLength(100 + '…[truncated 49900 chars]'.length);
  });

  it('marks a failed tool as failed rather than silently ok', async () => {
    harness = fakeMiddleware(() => [
      sse({ type: 'tool_use', id: 't1', name: 'Bash' }),
      sse({ type: 'tool_result', tool_use_id: 't1', content: 'permission denied', is_error: true }),
      sse({ type: 'result', result: 'DONE', usage: usage(10) }),
    ]);
    const c = new DevboxAgentClient({ baseUrl: await harness.url, secret: SECRET });
    const done = doneEvent(await collect(c.run('x', { maxTurns: 2 })));
    expect(done.toolCalls[0]).toMatchObject({ ok: false, error: 'permission denied' });
  });

  it('reuses the session an earlier step opened, and resumes from the first task', async () => {
    const c = await client(() => turn('DONE', 100));
    const events = await collect(c.run('second step', { maxTurns: 2, lab: { sessionId: 'sess-earlier', boxId: 'box-earlier' } }));
    expect(events[0]).toEqual({ type: 'session', sessionId: 'sess-earlier', boxId: 'box-earlier', created: false });
    expect(harness!.calls[0].resume).toBe(true);
  });

  it('says devbox runtime unavailable against an older middleware that 404s the internal router', async () => {
    // An aws-agentcore build from before /internal/devbox existed answers 404
    // for every path under it. That is a deployment fact, not a task failure.
    const old = createServer((_req, res) => { res.writeHead(404).end(); });
    const url = await new Promise<string>((resolve) => {
      old.listen(0, '127.0.0.1', () => resolve(`http://127.0.0.1:${(old.address() as AddressInfo).port}`));
    });
    try {
      const c = new DevboxAgentClient({ baseUrl: url, secret: SECRET });
      await expect(collect(c.run('x'))).rejects.toThrow(/devbox runtime unavailable: middleware has no \/internal\/devbox\/sessions/);
    } finally {
      await new Promise<void>((r) => old.close(() => r()));
    }
  });

  it('says devbox runtime unavailable when no URL is configured at all', async () => {
    const previousUrl = process.env.AGENTCORE_DEVBOX_URL;
    const previousSource = process.env.WORK_GRAPH_COMPUTE_SOURCE_URL;
    delete process.env.AGENTCORE_DEVBOX_URL;
    delete process.env.WORK_GRAPH_COMPUTE_SOURCE_URL;
    try {
      await expect(collect(new DevboxAgentClient({ secret: SECRET }).run('x'))).rejects.toThrow(DevboxUnavailableError);
    } finally {
      if (previousUrl !== undefined) process.env.AGENTCORE_DEVBOX_URL = previousUrl;
      if (previousSource !== undefined) process.env.WORK_GRAPH_COMPUTE_SOURCE_URL = previousSource;
    }
  });

  it('deletes a session and hands back a shell url', async () => {
    const c = await client(() => []);
    expect(await c.deleteSession('sess-1')).toBe(true);
    expect(harness!.deleted).toEqual(['/internal/devbox/sess-1']);
    expect(await c.shellUrl('sess-1', 'dev-frank')).toEqual({ url: 'https://shell.example/abc', expiresAt: '2026-09-21T00:00:00.000Z' });
  });
});

describe('saysDone', () => {
  it('needs DONE alone on the last non-empty line', () => {
    expect(saysDone('all finished\nDONE')).toBe(true);
    expect(saysDone('all finished\ndone.\n\n')).toBe(true);
    expect(saysDone('I am not DONE yet')).toBe(false);
    expect(saysDone('DONE\nbut actually one more thing')).toBe(false);
    expect(saysDone('')).toBe(false);
  });
});

describe('contextTokensOf', () => {
  it('is input plus both cache figures — what the model is carrying', () => {
    expect(contextTokensOf({ input_tokens: 10, cache_read_input_tokens: 5, cache_creation_input_tokens: 2, output_tokens: 99 })).toBe(17);
    expect(contextTokensOf(undefined)).toBe(0);
  });
});

describe('the run → session map', () => {
  it('remembers, recalls, and gives a session up exactly once', () => {
    rememberRunLab('run-1', { sessionId: 's', boxId: 'b' });
    expect(recallRunLab('run-1')).toEqual({ sessionId: 's', boxId: 'b' });
    expect(takeRunLab('run-1')).toEqual({ sessionId: 's', boxId: 'b' });
    expect(takeRunLab('run-1')).toBeUndefined();
  });

  it('stays bounded, so a long-lived process does not accumulate runs forever', () => {
    for (let i = 0; i < 700; i += 1) rememberRunLab(`run-${i}`, { sessionId: `s${i}`, boxId: 'b' });
    expect(recallRunLab('run-0')).toBeUndefined();
    expect(recallRunLab('run-699')).toBeDefined();
  });
});
