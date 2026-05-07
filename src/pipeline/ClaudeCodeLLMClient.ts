// social-api/src/pipeline/ClaudeCodeLLMClient.ts
//
// Claude Code CLI subprocess provider. Shells out to `claude -p` so an
// operator running on a machine with only a Claude Code subscription
// (no Anthropic API key, no Bedrock) can still execute pipeline LLM
// steps. Auth is the host's existing CLI session — for macOS hosts
// the OAuth token is in Keychain; the Tilt local_resource extracts it
// to .tilt-claude/.credentials.json which is mounted into this pod at
// /root/.claude (HOME=/root).
//
// Streaming: --output-format stream-json --include-partial-messages
// emits per-chunk text deltas as JSONL events. Each `stream_event`
// with content_block_delta yields a token; the final `result` event
// carries token usage.
//
// Cancellation: AbortSignal -> kill the child process group with
// SIGTERM, then SIGKILL after a short grace.

import { spawn } from 'node:child_process';
import type { Readable } from 'node:stream';
import type { ChildProcessByStdio } from 'node:child_process';
import type { LLMClient, LLMChunk, LLMStreamOptions } from './LLMClient';

const CLAUDE_BIN = process.env['CLAUDE_CLI_PATH'] ?? 'claude';
const KILL_GRACE_MS = 500;

// Bedrock-mode model translation. When CLAUDE_CODE_USE_BEDROCK=1, claude CLI
// routes through AWS Bedrock and expects the inference-profile ARN form,
// not the friendly aliases. When the env var is unset (subscription mode,
// OAuth creds at ~/.claude/.credentials.json), the CLI accepts the friendly
// aliases natively and the translation here would actively break it.
//
// `bedrockMode()` reads the env at call time so a single deployment can
// flip via `CLAUDE_CODE_USE_BEDROCK=1` (see k8s/helm/websocket-gateway/
// values.yaml: socialApi.env.CLAUDE_CODE_USE_BEDROCK and the
// values-bedrock-local.yaml overlay).
const BEDROCK_MODEL_ALIAS: Record<string, string> = {
  'opus':   'us.anthropic.claude-opus-4-20250514-v1:0',
  'sonnet': 'us.anthropic.claude-sonnet-4-20250514-v1:0',
  'haiku':  'us.anthropic.claude-haiku-4-5-20251001-v1:0',
  'claude-opus-4-7':           'us.anthropic.claude-opus-4-20250514-v1:0',
  'claude-sonnet-4-6':         'us.anthropic.claude-sonnet-4-20250514-v1:0',
  'claude-haiku-4-5-20251001': 'us.anthropic.claude-haiku-4-5-20251001-v1:0',
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

export class ClaudeCodeLLMClient implements LLMClient {
  readonly provider = 'claude-code' as const;

  stream(
    model: string,
    systemPrompt: string,
    userPrompt: string,
    opts: LLMStreamOptions = {},
  ): AsyncIterable<LLMChunk> {
    const { signal } = opts;

    return {
      [Symbol.asyncIterator]: async function* () {
        if (signal?.aborted) {
          throw new DOMException('Aborted', 'AbortError');
        }

        // Drop --bare: it forces ANTHROPIC_API_KEY auth and skips the
        // OAuth keychain reads that the subscription path depends on.
        // The Tilt mount at /root/.claude/.credentials.json IS where
        // the OAuth tokens live, so leaving --bare off lets claude CLI
        // pick them up. --no-session-persistence keeps each run clean.
        const args = [
          '-p',
          '--output-format', 'stream-json',
          '--include-partial-messages',
          '--no-session-persistence',
          '--verbose',
        ];
        if (systemPrompt?.trim()) {
          args.push('--append-system-prompt', systemPrompt);
        }
        if (model?.trim()) {
          args.push('--model', resolveModel(model));
        }
        args.push(userPrompt);

        const child: ChildProcessByStdio<null, Readable, Readable> = spawn(CLAUDE_BIN, args, {
          stdio: ['ignore', 'pipe', 'pipe'],
        });

        const onAbort = () => {
          child.kill('SIGTERM');
          setTimeout(() => { try { child.kill('SIGKILL'); } catch { /* already gone */ } }, KILL_GRACE_MS);
        };
        signal?.addEventListener('abort', onAbort);

        const stderrChunks: Buffer[] = [];
        child.stderr.on('data', (b: Buffer) => stderrChunks.push(b));

        const exitPromise = new Promise<{ code: number | null; sig: NodeJS.Signals | null }>((resolve) => {
          child.on('exit', (code, sig) => resolve({ code, sig }));
        });

        let fullResponse = '';
        let tokensIn = 0;
        let tokensOut = 0;
        let buffer = '';

        try {
          for await (const chunk of child.stdout as AsyncIterable<Buffer>) {
            if (signal?.aborted) {
              throw new DOMException('Aborted', 'AbortError');
            }
            buffer += chunk.toString('utf8');
            let nl = buffer.indexOf('\n');
            while (nl !== -1) {
              const line = buffer.slice(0, nl);
              buffer = buffer.slice(nl + 1);
              nl = buffer.indexOf('\n');
              if (!line.trim()) continue;

              let evt: Record<string, unknown>;
              try {
                evt = JSON.parse(line);
              } catch {
                continue;
              }

              const type = evt['type'] as string | undefined;

              if (type === 'stream_event') {
                const event = evt['event'] as Record<string, unknown> | undefined;
                if (event?.['type'] === 'content_block_delta') {
                  const delta = event['delta'] as Record<string, unknown> | undefined;
                  if (delta?.['type'] === 'text_delta' && typeof delta['text'] === 'string') {
                    const token = delta['text'];
                    fullResponse += token;
                    yield { done: false, token };
                  }
                }
              } else if (type === 'assistant') {
                // Non-partial mode fallback: assistant message arrives in one piece.
                // Only count it if we haven't been streaming partials.
                if (fullResponse === '') {
                  const message = evt['message'] as Record<string, unknown> | undefined;
                  const content = message?.['content'] as Array<Record<string, unknown>> | undefined;
                  if (Array.isArray(content)) {
                    for (const block of content) {
                      if (block['type'] === 'text' && typeof block['text'] === 'string') {
                        const token = block['text'];
                        fullResponse += token;
                        yield { done: false, token };
                      }
                    }
                  }
                }
              } else if (type === 'result') {
                const usage = evt['usage'] as Record<string, unknown> | undefined;
                if (typeof usage?.['input_tokens'] === 'number') tokensIn = usage['input_tokens'];
                if (typeof usage?.['output_tokens'] === 'number') tokensOut = usage['output_tokens'];
                const result = evt['result'];
                if (typeof result === 'string' && fullResponse === '') {
                  fullResponse = result;
                }
              }
            }
          }

          const { code } = await exitPromise;
          if (code !== 0 && !signal?.aborted) {
            const stderr = Buffer.concat(stderrChunks).toString('utf8').trim();
            throw new Error(`claude exited ${code}: ${stderr || '(no stderr)'}`);
          }
        } finally {
          signal?.removeEventListener('abort', onAbort);
          if (child.exitCode === null && child.signalCode === null) {
            try { child.kill('SIGTERM'); } catch { /* already exited */ }
          }
        }

        if (tokensIn === 0) tokensIn = Math.max(1, Math.round(userPrompt.length / 4));
        if (tokensOut === 0) tokensOut = Math.max(1, Math.round(fullResponse.length / 4));

        yield { done: true, response: fullResponse, tokensIn, tokensOut };
      },
    };
  }
}
