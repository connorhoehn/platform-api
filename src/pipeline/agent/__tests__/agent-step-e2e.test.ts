// platform-api/src/pipeline/agent/__tests__/agent-step-e2e.test.ts
//
// THE GATE for the `agent` step.
//
// What makes this bite rather than decorate: it runs the REAL
// ClaudeCodeAgentClient against a REAL child process, and that child
// replays a byte-for-byte capture of `claude -p --output-format
// stream-json` taken from the running pod (fixtures/read-tool-run.jsonl
// — an actual Read tool call against /tmp/agentprobe/secret.txt). The
// contract under test — "an agent step used a tool and the trace
// persisted" — is asserted against the tool-call RECORD, never against
// "some text came back". Nothing mocks the stream parser, the argv
// construction, the spawn, or the persistence write.
//
// The stub also records the argv it was invoked with, so the safety
// posture (default-deny via `--tools`, MCP stripped, no bypass flag) is
// verified as it actually reached the process, not as intent.

import { describe, it, expect, beforeAll, afterAll, jest } from '@jest/globals';
import { mkdtempSync, writeFileSync, readFileSync, chmodSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { EventBus } from 'distributed-core';
import type { PipelineEventMap } from 'distributed-core/applications/pipeline';
import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';

import { createAgentStepHandler } from '../AgentStepHandler';
import { ClaudeCodeAgentClient, buildAgentArgs, AGENT_ENV_ALLOWLIST } from '../ClaudeCodeAgentClient';
import type { AgentClient } from '../types';
import type { AgentStepOutput } from '../types';
import { RunPersistenceService } from '../../runPersistence';
import type { PipelineRunRepository, PipelineRunItem } from '../../../repositories/PipelineRunRepository';
import { runItemToSnapshot } from '../../createBridge';

jest.setTimeout(60_000);

const FIXTURE = join(__dirname, 'fixtures', 'read-tool-run.jsonl');

let workDir: string;
let stubPath: string;

let stubSeq = 0;

/**
 * Writes a stand-in for the `claude` binary and points CLAUDE_CLI_PATH
 * at it. Paths are baked into the generated source rather than passed
 * through the environment — under jest, env mutations in the test file
 * do not reach a spawned child, and silently reading `undefined` there
 * would make these assertions meaningless.
 *
 * mode 'replay' streams a fixture one JSONL line at a time; mode 'hang'
 * emits an init line and then never exits (timeout coverage).
 */
function useStub(opts: {
  mode?: 'replay' | 'hang';
  fixture?: string;
  argvFile?: string;
  pidFile?: string;
  envFile?: string;
}): void {
  const lit = (v: string | undefined): string => (v === undefined ? 'null' : JSON.stringify(v));
  const src = `#!/usr/bin/env node
const fs = require('fs');
const argvOut = ${lit(opts.argvFile)};
if (argvOut) fs.writeFileSync(argvOut, JSON.stringify(process.argv.slice(2)));
const envOut = ${lit(opts.envFile)};
if (envOut) fs.writeFileSync(envOut, JSON.stringify(Object.keys(process.env)));
const pidOut = ${lit(opts.pidFile)};
if (pidOut) fs.writeFileSync(pidOut, String(process.pid));

if (${JSON.stringify(opts.mode ?? 'replay')} === 'hang') {
  process.stdout.write(JSON.stringify({ type: 'system', subtype: 'init', tools: [] }) + '\\n');
  setInterval(() => {}, 1000);
} else {
  const lines = fs.readFileSync(${lit(opts.fixture)}, 'utf8').split('\\n').filter(Boolean);
  let i = 0;
  const tick = () => {
    if (i >= lines.length) { process.exit(0); return; }
    process.stdout.write(lines[i++] + '\\n');
    setTimeout(tick, 2);
  };
  tick();
}
`;
  stubPath = join(workDir, `claude-stub-${++stubSeq}.js`);
  writeFileSync(stubPath, src, 'utf8');
  chmodSync(stubPath, 0o755);
  process.env['CLAUDE_CLI_PATH'] = stubPath;
}

beforeAll(() => {
  workDir = mkdtempSync(join(tmpdir(), 'agent-step-gate-'));
});

afterAll(() => {
  delete process.env['CLAUDE_CLI_PATH'];
});

interface Emitted {
  event: string;
  payload: Record<string, unknown>;
}

function makeHelpers(emitted: Emitted[], signal?: AbortSignal): StepHandlerHelpers {
  const controller = new AbortController();
  return {
    emit: (async (event: string, payload: Record<string, unknown>) => {
      emitted.push({ event, payload });
    }) as unknown as StepHandlerHelpers['emit'],
    runId: 'run-gate-1',
    ownerNodeId: 'node-test',
    stepId: 'agent-1',
    abortSignal: signal ?? controller.signal,
    sleep: async () => undefined,
    isCancelled: () => (signal ?? controller.signal).aborted,
  };
}

/**
 * Tool-using agent steps run only on the devbox runtime (AgentStepHandler).
 * The tool-trace tests below still exercise the REAL CLI client and parser,
 * so they hand it to the handler as the devbox client and mark the node
 * `runtime: 'devbox'`; the local slot is a client that must never be called.
 */
const localNeverCalled: AgentClient = {
  provider: 'claude-code',
  run: () => { throw new Error('the local runtime must not run a tool-using step'); },
} as unknown as AgentClient;

function toolTraceHandler(): ReturnType<typeof createAgentStepHandler> {
  return createAgentStepHandler({ agentClient: localNeverCalled, devboxAgentClient: new ClaudeCodeAgentClient() });
}

function agentNode(data: Record<string, unknown>): PipelineNode {
  return {
    id: 'agent-1',
    type: 'action',
    data: { type: 'agent', ...data },
  } as unknown as PipelineNode;
}

// ---------------------------------------------------------------------------
// 1. The agent used a tool, and the trace is in the step output.
// ---------------------------------------------------------------------------

describe('agent step — tool use trace', () => {
  it('records the real Read tool call from a captured claude stream', async () => {
    const argvFile = join(workDir, 'argv-1.json');
    useStub({ fixture: FIXTURE, argvFile });

    const handler = toolTraceHandler();
    const emitted: Emitted[] = [];
    const outcome = await handler.execute(
      agentNode({
        prompt: 'Read {{ context.target }} and report the secret word.',
        allowedTools: ['Read'],
        runtime: 'devbox',
      }),
      { target: '/tmp/agentprobe/secret.txt' },
      makeHelpers(emitted),
    );

    expect(outcome.error).toBeUndefined();
    expect(outcome.status).toBe('completed');
    const out = outcome.output as AgentStepOutput;

    // ── The load-bearing assertion: a TOOL CALL RECORD, not just text ──
    expect(out.toolCalls).toHaveLength(1);
    const call = out.toolCalls[0]!;
    expect(call.name).toBe('Read');
    expect(call.ok).toBe(true);
    expect(call.input).toEqual({ file_path: '/tmp/agentprobe/secret.txt' });
    expect(String(call.output)).toContain('BANANA-77');
    expect(Date.parse(call.startedAt)).not.toBeNaN();
    expect(Date.parse(call.endedAt!)).not.toBeNaN();
    expect(call.error).toBeUndefined();

    // Honest output: a step that really had tools is not marked toolless.
    expect(out.toolless).toBe(false);
    expect(out.stopReason).toBe('complete');
    expect(out.turns).toBe(2);
    expect(out.model).toBe('claude-sonnet-5');
    expect(out.tokensIn).toBeGreaterThan(0);
    expect(out.tokensOut).toBeGreaterThan(0);
    expect(out.text).toContain('BANANA-77');

    // ── The contract's one new event, start and end ────────────────────
    const toolEvents = emitted.filter((e) => e.event === 'pipeline:step:agent:tool');
    expect(toolEvents).toHaveLength(2);
    expect(toolEvents[0]!.payload).toMatchObject({
      runId: 'run-gate-1',
      stepId: 'agent-1',
      callIndex: 0,
      name: 'Read',
      phase: 'start',
    });
    expect(toolEvents[1]!.payload).toMatchObject({
      callIndex: 0,
      name: 'Read',
      phase: 'end',
      ok: true,
    });
    // Tool payloads stay out of the event — they live in the step output.
    expect(toolEvents[0]!.payload['input']).toBeUndefined();
    expect(toolEvents[1]!.payload['output']).toBeUndefined();

    // ── Prompt interpolation actually reached the process ──────────────
    const argv = JSON.parse(readFileSync(argvFile, 'utf8')) as string[];
    expect(argv[argv.length - 1]).toBe(
      'Read /tmp/agentprobe/secret.txt and report the secret word.',
    );
  });
});

// ---------------------------------------------------------------------------
// 2. The trace survives persistence and comes back on the read path.
// ---------------------------------------------------------------------------

describe('agent step — trace persistence round-trip', () => {
  it('persists toolCalls through pipeline:step:completed and returns them from the run read path', async () => {
    useStub({ fixture: FIXTURE });

    const handler = toolTraceHandler();
    const emitted: Emitted[] = [];
    const outcome = await handler.execute(
      agentNode({ prompt: 'read the file', allowedTools: ['Read'], runtime: 'devbox' }),
      {},
      makeHelpers(emitted),
    );
    expect(outcome.status).toBe('completed');

    // Real persistence service on a real EventBus, mock repo as the sink.
    const puts: PipelineRunItem[] = [];
    const repository = {
      put: async (item: PipelineRunItem) => {
        puts.push(JSON.parse(JSON.stringify(item)) as PipelineRunItem);
      },
    } as unknown as PipelineRunRepository;

    const bus = new EventBus<PipelineEventMap>(makeLocalPubSub(), 'test-node', {
      topic: 'pipeline.agent.gate',
    });
    await bus.start();
    const persistence = new RunPersistenceService({ eventBus: bus, repository });
    persistence.start();

    const now = new Date().toISOString();
    await bus.publish('pipeline:run:started', {
      runId: 'run-gate-1',
      pipelineId: 'pipe-gate',
      triggeredBy: { userId: 'u1', triggerType: 'manual' },
      at: now,
    } as PipelineEventMap['pipeline:run:started']);
    await bus.publish('pipeline:step:completed', {
      runId: 'run-gate-1',
      stepId: 'agent-1',
      durationMs: 3152,
      output: outcome.output,
      at: now,
    } as PipelineEventMap['pipeline:step:completed']);
    await bus.publish('pipeline:run:completed', {
      runId: 'run-gate-1',
      durationMs: 3200,
      at: now,
    } as PipelineEventMap['pipeline:run:completed']);

    // Terminal event flushes; give the async subscriber a tick to land.
    await new Promise((r) => setTimeout(r, 50));

    expect(puts.length).toBeGreaterThan(0);
    const persisted = puts[puts.length - 1]!;
    const stored = persisted.stepsSummary?.['agent-1'];
    expect(stored).toBeDefined();

    // Not the {truncated:true} sentinel — the whole trace made it through
    // runPersistence's 100 KB output budget.
    const storedOut = stored!.output as AgentStepOutput;
    expect(storedOut).not.toMatchObject({ truncated: true });
    expect(storedOut.toolCalls).toHaveLength(1);
    expect(storedOut.toolCalls[0]!.name).toBe('Read');
    expect(String(storedOut.toolCalls[0]!.output)).toContain('BANANA-77');

    // And it survives the exact mapping GET /runs/:runId uses.
    const snapshot = runItemToSnapshot(persisted);
    const step = (snapshot.steps as Record<string, { output?: AgentStepOutput }>)['agent-1'];
    expect(step?.output?.toolCalls?.[0]).toMatchObject({ name: 'Read', ok: true });
    expect(step?.output?.toolless).toBe(false);

    await persistence.stop();
    await bus.stop();
  });
});

// ---------------------------------------------------------------------------
// 3. Safety posture, as it reaches the process.
// ---------------------------------------------------------------------------

describe('agent step — safety', () => {
  it('default-denies: no allowlist yields --tools "" and toolless: true', async () => {
    const argvFile = join(workDir, 'argv-toolless.json');
    // Replay a stream with no tool_use at all; what matters is the argv.
    const tooLessFixture = join(workDir, 'toolless.jsonl');
    writeFileSync(
      tooLessFixture,
      [
        JSON.stringify({ type: 'system', subtype: 'init', tools: [] }),
        JSON.stringify({
          type: 'assistant',
          message: {
            id: 'msg_1',
            model: 'claude-sonnet-5',
            content: [{ type: 'text', text: 'I have no tools.' }],
          },
        }),
        JSON.stringify({
          type: 'result',
          subtype: 'success',
          is_error: false,
          num_turns: 1,
          result: 'I have no tools.',
          usage: { input_tokens: 10, output_tokens: 5 },
        }),
      ].join('\n'),
      'utf8',
    );
    const envFile = join(workDir, 'env-toolless.json');
    useStub({ fixture: tooLessFixture, argvFile, envFile });

    const planted = ['REALTIME_JWT_SECRET', 'ADMIN_TOKEN', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'];
    for (const key of planted) process.env[key] = 'must-not-reach-the-child';
    const handler = createAgentStepHandler({ agentClient: new ClaudeCodeAgentClient() });
    let outcome: Awaited<ReturnType<typeof handler.execute>>;
    try {
      outcome = await handler.execute(
        agentNode({ prompt: 'do a thing' }), // NO allowedTools
        {},
        makeHelpers([]),
      );
    } finally {
      for (const key of planted) delete process.env[key];
    }

    // The child's environment is the allowlist, not the pod's.
    const childEnv = JSON.parse(readFileSync(envFile, 'utf8')) as string[];
    for (const key of planted) expect(childEnv).not.toContain(key);
    // macOS adds __CF_USER_TEXT_ENCODING to every process it starts.
    const osAdded = new Set(['__CF_USER_TEXT_ENCODING']);
    for (const key of childEnv.filter((k) => !osAdded.has(k))) {
      expect(AGENT_ENV_ALLOWLIST as readonly string[]).toContain(key);
    }

    const out = outcome.output as AgentStepOutput;
    expect(out.toolless).toBe(true);
    expect(out.toolCalls).toHaveLength(0);

    const argv = JSON.parse(readFileSync(argvFile, 'utf8')) as string[];
    const toolsIdx = argv.indexOf('--tools');
    expect(toolsIdx).toBeGreaterThanOrEqual(0);
    expect(argv[toolsIdx + 1]).toBe(''); // availability gate closed
    expect(argv).not.toContain('--allowedTools'); // nothing to permit
    expect(argv).toContain('--strict-mcp-config'); // ambient MCP servers stripped
  });

  it('never emits a permission-bypass flag, for any node data', () => {
    for (const allowedTools of [undefined, [], ['Read'], ['Bash(cat:*)', 'Read', 'Write']]) {
      const argv = buildAgentArgs('task', { allowedTools: allowedTools as string[] | undefined });
      expect(argv).not.toContain('--dangerously-skip-permissions');
      expect(argv).not.toContain('--allow-dangerously-skip-permissions');
      expect(argv).not.toContain('--permission-mode');
      expect(argv).not.toContain('bypassPermissions');
      expect(argv).toContain('--strict-mcp-config');
    }
  });

  it('gates availability by base tool name and narrows permission by specifier', () => {
    const argv = buildAgentArgs('task', { allowedTools: ['Bash(cat:*)', 'Read'] });
    expect(argv[argv.indexOf('--tools') + 1]).toBe('Bash,Read');
    expect(argv[argv.indexOf('--allowedTools') + 1]).toBe('Bash(cat:*),Read');
  });

  it('refuses to build argv for an empty prompt rather than spawn a doomed process', () => {
    expect(() => buildAgentArgs('')).toThrow(/no prompt to send/i);
    expect(() => buildAgentArgs('   \n  ')).toThrow(/no prompt to send/i);
  });

  it('enforces timeoutMs with a real process kill, not a promise race', async () => {
    const pidFile = join(workDir, 'hang.pid');
    useStub({ mode: 'hang', pidFile });

    const handler = createAgentStepHandler({ agentClient: new ClaudeCodeAgentClient() });
    const started = Date.now();
    const outcome = await handler.execute(
      agentNode({ prompt: 'stall', timeoutMs: 1500 }),
      {},
      makeHelpers([]),
    );
    const elapsed = Date.now() - started;

    expect(outcome.status).toBe('failed');
    expect((outcome.output as AgentStepOutput).stopReason).toBe('timeout');
    expect(elapsed).toBeLessThan(10_000);

    // The child is genuinely gone — not orphaned behind a resolved promise.
    const pid = Number(readFileSync(pidFile, 'utf8'));
    expect(Number.isFinite(pid)).toBe(true);
    await new Promise((r) => setTimeout(r, 800));
    let alive = true;
    try {
      process.kill(pid, 0);
    } catch {
      alive = false;
    }
    expect(alive).toBe(false);
  });

  it('enforces maxTurns by killing the run and failing with stopReason max_turns', async () => {
    // Synthetic but in the real observed shape: distinct message ids per
    // turn, which is how the CLI actually delimits them.
    const manyTurns = join(workDir, 'many-turns.jsonl');
    const lines: string[] = [
      JSON.stringify({ type: 'system', subtype: 'init', tools: ['Read'] }),
    ];
    for (let i = 0; i < 20; i++) {
      lines.push(
        JSON.stringify({
          type: 'assistant',
          message: {
            id: `msg_${i}`,
            model: 'claude-sonnet-5',
            content: [{ type: 'text', text: `turn ${i}` }],
          },
        }),
      );
    }
    writeFileSync(manyTurns, lines.join('\n'), 'utf8');
    useStub({ fixture: manyTurns });

    const handler = createAgentStepHandler({ agentClient: new ClaudeCodeAgentClient() });
    const outcome = await handler.execute(
      agentNode({ prompt: 'loop', maxTurns: 3 }),
      {},
      makeHelpers([]),
    );

    expect(outcome.status).toBe('failed');
    const out = outcome.output as AgentStepOutput;
    expect(out.stopReason).toBe('max_turns');
    expect(out.turns).toBe(4); // stopped on the turn that breached the ceiling
  });
});

// ---------------------------------------------------------------------------

function makeLocalPubSub(): ConstructorParameters<typeof EventBus>[0] {
  const handlers = new Map<
    string,
    (topic: string, payload: unknown, meta: Record<string, unknown>) => Promise<void>
  >();
  let n = 0;
  return {
    subscribe(
      _topic: string,
      h: (topic: string, payload: unknown, meta: Record<string, unknown>) => Promise<void>,
    ) {
      const id = `sub-${++n}`;
      handlers.set(id, h);
      return Promise.resolve(id);
    },
    unsubscribe(id: string) {
      handlers.delete(id);
      return Promise.resolve();
    },
    async publish(topic: string, payload: unknown, meta: Record<string, unknown> = {}) {
      for (const h of handlers.values()) await h(topic, payload, meta);
    },
    subscribeAll(
      h: (topic: string, payload: unknown, meta: Record<string, unknown>) => Promise<void>,
    ) {
      const id = `sub-all-${++n}`;
      handlers.set(id, h);
      return Promise.resolve(id);
    },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;
}
