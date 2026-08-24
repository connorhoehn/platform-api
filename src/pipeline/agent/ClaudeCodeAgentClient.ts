// platform-api/src/pipeline/agent/ClaudeCodeAgentClient.ts
//
// Agentic provider: shells out to `claude -p` and runs a real agent
// loop (multiple turns, tool use), yielding STRUCTURED events rather
// than bare text tokens. Sits beside ClaudeCodeLLMClient (which stays
// the single-shot completion path) and shares its auth story: the
// mounted Claude Code session at /root/.claude, HOME=/root.
//
// ─── WHAT THE INSTALLED CLI ACTUALLY SUPPORTS (v2.1.197, verified in
//     the running pod, not from memory) ────────────────────────────────
//
//   --tools <list>          Availability gate. `--tools ""` yields an
//                           init event with `tools: []` — the agent
//                           genuinely has no tools. `--tools Read`
//                           yields `tools: ["Read"]` and the model
//                           cannot reach Bash at all.
//   --allowedTools <list>   PERMISSION gate, accepts specifiers like
//                           `Bash(git *)`. It does NOT restrict
//                           availability.
//   --strict-mcp-config     With no --mcp-config, strips every MCP
//                           server (`mcp_servers: []`). The pod has
//                           MCP servers configured in .claude.json, so
//                           without this an "allowlisted" agent still
//                           reaches Google Drive et al.
//   --permission-mode       Exists, with a `bypassPermissions` choice.
//                           WE NEVER PASS IT. Same for
//                           --dangerously-skip-permissions.
//   --max-turns             DOES NOT EXIST in this version. Turn
//                           bounding is enforced here, by killing the
//                           process group.
//
// ─── THE SAFETY FINDING THAT DROVE THE DESIGN ──────────────────────────
//
// `--allowedTools` alone is NOT default-deny. Probed live: with NO
// allowlist flag at all, `claude -p` happily invoked Read and returned
// the file contents; with `--tools Bash` and no allowlist it invoked
// Bash too. In non-interactive print mode there is no human to answer a
// prompt, and the CLI resolves that by permitting rather than denying.
//
// So availability (`--tools`) is the gate we rely on, and it is applied
// unconditionally: no allowlist ⇒ `--tools ""` ⇒ zero tools. The
// permission layer (`--allowedTools`) is passed too, but only ever to
// NARROW an already-available tool. Never to grant one.
//
// ─── BOUNDING ──────────────────────────────────────────────────────────
//
// maxTurns and timeoutMs are enforced by killing the child's PROCESS
// GROUP (spawn detached ⇒ pgid === child.pid ⇒ `process.kill(-pid)`),
// not by racing a promise. An agent's Bash tool spawns grandchildren;
// signalling only the direct child would leak them.

import { spawn } from 'node:child_process';
import type { Readable } from 'node:stream';
import type { ChildProcessByStdio } from 'node:child_process';
import { withAbort } from 'distributed-core/common';
import type {
  AgentClient,
  AgentEvent,
  AgentRunOptions,
  AgentStopReason,
  AgentToolCall,
} from './types';
import { DEFAULT_MAX_TURNS, DEFAULT_TIMEOUT_MS } from './types';

/**
 * Resolved per call, not captured at module load: the binary path is
 * deployment config, and reading it once at import makes it impossible
 * to point at a different binary after the module is first required.
 */
function claudeBin(): string {
  return process.env['CLAUDE_CLI_PATH'] ?? 'claude';
}

const KILL_GRACE_MS = 500;

/**
 * Flags that hand an agent unrestricted authority. Building an argv
 * containing any of these is a bug, and `buildAgentArgs` throws rather
 * than emitting one. Listed explicitly so the intent survives a future
 * refactor that "just needs the demo to work".
 */
export const FORBIDDEN_FLAGS = [
  '--dangerously-skip-permissions',
  '--allow-dangerously-skip-permissions',
  'bypassPermissions',
] as const;

// Bedrock alias translation, same rules as ClaudeCodeLLMClient: only
// applies when CLAUDE_CODE_USE_BEDROCK is on, because in subscription
// mode the CLI wants the friendly alias and translating breaks it.
const BEDROCK_MODEL_ALIAS: Record<string, string> = {
  opus: 'us.anthropic.claude-opus-4-20250514-v1:0',
  sonnet: 'us.anthropic.claude-sonnet-4-20250514-v1:0',
  haiku: 'us.anthropic.claude-haiku-4-5-20251001-v1:0',
};

function bedrockMode(): boolean {
  const v = process.env['CLAUDE_CODE_USE_BEDROCK'];
  return v === '1' || v?.toLowerCase() === 'true';
}

function resolveModel(model: string): string {
  if (!model?.trim()) return model;
  if (bedrockMode()) return BEDROCK_MODEL_ALIAS[model] ?? model;
  return model;
}

/**
 * `Bash(git status:*)` -> `Bash`. The availability gate takes bare
 * built-in tool names; the permission gate takes the full specifier.
 */
export function baseToolName(spec: string): string {
  const i = spec.indexOf('(');
  return (i === -1 ? spec : spec.slice(0, i)).trim();
}

/** True when the step degenerates to a plain completion (no tools). */
export function isToolless(allowedTools?: string[]): boolean {
  return normalizeTools(allowedTools).length === 0;
}

function normalizeTools(allowedTools?: string[]): string[] {
  if (!Array.isArray(allowedTools)) return [];
  return allowedTools.map((t) => String(t).trim()).filter((t) => t.length > 0);
}

/**
 * Pure argv construction — exported so the safety posture is testable
 * without spawning a process.
 */
export function buildAgentArgs(prompt: string, opts: AgentRunOptions = {}): string[] {
  const tools = normalizeTools(opts.allowedTools);

  const args = [
    '-p',
    '--output-format',
    'stream-json',
    '--verbose',
    '--no-session-persistence',
    // No --mcp-config alongside this ⇒ every configured MCP server is
    // dropped. Without it the pod's ambient MCP servers are reachable
    // regardless of allowedTools.
    '--strict-mcp-config',
  ];

  if (tools.length === 0) {
    // DEFAULT-DENY. Empty string is the CLI's documented "disable all
    // tools" value and is verified to produce `tools: []`.
    args.push('--tools', '');
  } else {
    const available = [...new Set(tools.map(baseToolName))].filter((t) => t.length > 0);
    args.push('--tools', available.join(','));
    // Narrows permission within the available set (e.g. Bash(cat:*)).
    args.push('--allowedTools', tools.join(','));
  }

  if (opts.systemPrompt?.trim()) {
    args.push('--append-system-prompt', opts.systemPrompt);
  }
  if (opts.model?.trim()) {
    args.push('--model', resolveModel(opts.model));
  }
  args.push(prompt);

  for (const arg of args) {
    for (const forbidden of FORBIDDEN_FLAGS) {
      if (arg === forbidden) {
        throw new Error(`refusing to build agent argv containing ${forbidden}`);
      }
    }
  }
  return args;
}

/** SIGTERM then SIGKILL the whole process group; never throws. */
function killGroup(child: { pid?: number; kill: (s: NodeJS.Signals) => boolean }): void {
  const pid = child.pid;
  const signalGroup = (sig: NodeJS.Signals): void => {
    if (typeof pid === 'number') {
      try {
        process.kill(-pid, sig);
        return;
      } catch {
        /* group already gone, or not a group leader — fall through */
      }
    }
    try {
      child.kill(sig);
    } catch {
      /* already exited */
    }
  };
  signalGroup('SIGTERM');
  const t = setTimeout(() => signalGroup('SIGKILL'), KILL_GRACE_MS);
  // Never hold the event loop open on the grace timer.
  if (typeof t.unref === 'function') t.unref();
}

/** Flatten a CLI tool_result `content` into something persistable. */
function normalizeToolOutput(content: unknown): unknown {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    const texts = content
      .filter((b): b is Record<string, unknown> => !!b && typeof b === 'object')
      .map((b) => (b['type'] === 'text' && typeof b['text'] === 'string' ? b['text'] : null))
      .filter((t): t is string => t !== null);
    if (texts.length > 0) return texts.join('\n');
  }
  return content;
}

export class ClaudeCodeAgentClient implements AgentClient {
  readonly provider = 'claude-code' as const;

  run(prompt: string, opts: AgentRunOptions = {}): AsyncIterable<AgentEvent> {
    const maxTurns = opts.maxTurns ?? DEFAULT_MAX_TURNS;
    const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    const { signal } = opts;
    const args = buildAgentArgs(prompt, opts);
    const requestedModel = opts.model?.trim() || '';
    const bin = claudeBin();

    return {
      [Symbol.asyncIterator]: async function* (): AsyncGenerator<AgentEvent> {
        if (signal?.aborted) {
          throw new DOMException('Aborted', 'AbortError');
        }

        const child: ChildProcessByStdio<null, Readable, Readable> = spawn(bin, args, {
          stdio: ['ignore', 'pipe', 'pipe'],
          // New process group so a Bash tool's grandchildren die with us.
          detached: true,
          ...(opts.cwd ? { cwd: opts.cwd } : {}),
        });

        // ── Bounding: one kill path, first reason wins ────────────────
        let killReason: AgentStopReason | null = null;
        const killWith = (reason: AgentStopReason): void => {
          if (killReason !== null) return;
          killReason = reason;
          killGroup(child);
        };

        const timeoutTimer = setTimeout(() => killWith('timeout'), timeoutMs);
        if (typeof timeoutTimer.unref === 'function') timeoutTimer.unref();
        const abortReg = signal ? withAbort(signal, () => killWith('cancelled')) : null;

        const stderrChunks: Buffer[] = [];
        child.stderr.on('data', (b: Buffer) => stderrChunks.push(b));
        const exitPromise = new Promise<{ code: number | null; sig: NodeJS.Signals | null }>(
          (resolve) => child.on('exit', (code, sig) => resolve({ code, sig })),
        );

        // ── Accumulators ──────────────────────────────────────────────
        const toolCalls: AgentToolCall[] = [];
        const pending = new Map<string, number>(); // tool_use.id -> callIndex
        const seenTurnIds = new Set<string>();
        let turns = 0;
        // The CLI emits one `assistant` event per CONTENT BLOCK, and the
        // blocks of a single turn share `message.id`. Counting events
        // would triple-count a turn that thought, called a tool, then
        // spoke — so turns are counted by distinct message id, which
        // reconciles with the final event's `num_turns`.
        let currentTurnId = '';
        let currentTurnText = '';
        let lastCompleteTurnText = '';
        let resultText: string | null = null;
        let tokensIn = 0;
        let tokensOut = 0;
        let reportedModel = '';
        let sawResultError = false;
        let buffer = '';

        try {
          for await (const chunk of child.stdout as AsyncIterable<Buffer>) {
            buffer += chunk.toString('utf8');
            let nl = buffer.indexOf('\n');
            while (nl !== -1) {
              const line = buffer.slice(0, nl);
              buffer = buffer.slice(nl + 1);
              nl = buffer.indexOf('\n');
              if (!line.trim()) continue;

              let evt: Record<string, unknown>;
              try {
                evt = JSON.parse(line) as Record<string, unknown>;
              } catch {
                continue;
              }

              const type = evt['type'] as string | undefined;

              if (type === 'assistant') {
                const message = evt['message'] as Record<string, unknown> | undefined;
                if (!message) continue;
                if (typeof message['model'] === 'string') reportedModel = message['model'];

                const msgId = String(message['id'] ?? '');
                if (msgId && !seenTurnIds.has(msgId)) {
                  seenTurnIds.add(msgId);
                  turns += 1;
                  if (currentTurnText.trim()) lastCompleteTurnText = currentTurnText;
                  currentTurnId = msgId;
                  currentTurnText = '';
                  yield { type: 'turn', index: turns };

                  if (turns > maxTurns) {
                    // Real enforcement: the CLI has no --max-turns, so
                    // the ceiling is a kill of the process group.
                    killWith('max_turns');
                    break;
                  }
                } else if (msgId && msgId !== currentTurnId) {
                  currentTurnId = msgId;
                }

                const content = message['content'] as Array<Record<string, unknown>> | undefined;
                if (!Array.isArray(content)) continue;
                for (const block of content) {
                  if (block['type'] === 'text' && typeof block['text'] === 'string') {
                    currentTurnText += block['text'];
                    yield { type: 'text', text: block['text'] };
                  } else if (block['type'] === 'tool_use') {
                    const id = String(block['id'] ?? `call-${toolCalls.length}`);
                    const name = String(block['name'] ?? 'unknown');
                    const at = new Date().toISOString();
                    const callIndex = toolCalls.length;
                    toolCalls.push({
                      name,
                      input: block['input'],
                      ok: false,
                      startedAt: at,
                    });
                    pending.set(id, callIndex);
                    yield {
                      type: 'tool_start',
                      id,
                      callIndex,
                      name,
                      input: block['input'],
                      at,
                    };
                  }
                }
              } else if (type === 'user') {
                const message = evt['message'] as Record<string, unknown> | undefined;
                const content = message?.['content'];
                if (!Array.isArray(content)) continue;
                for (const raw of content as Array<Record<string, unknown>>) {
                  if (!raw || raw['type'] !== 'tool_result') continue;
                  const id = String(raw['tool_use_id'] ?? '');
                  const callIndex = pending.get(id);
                  if (callIndex === undefined) continue;
                  pending.delete(id);
                  const isError = raw['is_error'] === true;
                  const output = normalizeToolOutput(raw['content']);
                  const at = new Date().toISOString();
                  const call = toolCalls[callIndex];
                  if (!call) continue;
                  call.ok = !isError;
                  call.output = output;
                  call.endedAt = at;
                  if (isError) {
                    call.error = typeof output === 'string' ? output : 'tool reported an error';
                  }
                  yield {
                    type: 'tool_end',
                    id,
                    callIndex,
                    name: call.name,
                    ok: call.ok,
                    output,
                    ...(call.error ? { error: call.error } : {}),
                    at,
                  };
                }
              } else if (type === 'result') {
                const usage = evt['usage'] as Record<string, unknown> | undefined;
                if (typeof usage?.['input_tokens'] === 'number') tokensIn = usage['input_tokens'];
                if (typeof usage?.['output_tokens'] === 'number') tokensOut = usage['output_tokens'];
                if (typeof evt['num_turns'] === 'number') turns = evt['num_turns'];
                if (typeof evt['result'] === 'string') resultText = evt['result'];
                if (evt['is_error'] === true) sawResultError = true;
              }
            }
            if (killReason !== null) break;
          }

          const { code } = await exitPromise;
          if (killReason === null && code !== 0) {
            const stderr = Buffer.concat(stderrChunks).toString('utf8').trim();
            throw new Error(`claude exited ${code}: ${stderr || '(no stderr)'}`);
          }
        } finally {
          clearTimeout(timeoutTimer);
          abortReg?.dispose();
          if (child.exitCode === null && child.signalCode === null) {
            killGroup(child);
          }
        }

        if (killReason === 'cancelled') {
          throw new DOMException('Aborted', 'AbortError');
        }

        // Any tool still in flight when we stopped never reported back.
        const stoppedAt = new Date().toISOString();
        for (const callIndex of pending.values()) {
          const call = toolCalls[callIndex];
          if (!call) continue;
          call.ok = false;
          call.endedAt = stoppedAt;
          call.error = `tool did not complete (${killReason ?? 'stream ended'})`;
        }

        if (currentTurnText.trim()) lastCompleteTurnText = currentTurnText;
        const text = resultText ?? lastCompleteTurnText;
        const stopReason: AgentStopReason =
          killReason ?? (sawResultError ? 'error' : 'complete');

        yield {
          type: 'done',
          text,
          turns,
          tokensIn,
          tokensOut,
          model: reportedModel || requestedModel,
          stopReason,
          toolCalls,
        };
      },
    };
  }
}
