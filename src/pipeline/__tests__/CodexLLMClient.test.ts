import { createServer, type Server, type RequestListener } from 'node:http';
import { mkdtemp, writeFile, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CodexCompletion, codexSummaryArgs, codexBridgeUrl, CodexLLMClient } from '../CodexLLMClient';

const message = JSON.stringify({ type: 'item.completed', item: { type: 'agent_message', text: '## Summary\nValidated the dependency map.' } });
const done = JSON.stringify({ type: 'turn.completed', usage: { input_tokens: 100, output_tokens: 12 } });
describe('Codex subscription summaries', () => {
  it('requires a completed nonempty turn and retains actual usage', () => {
    const result = new CodexCompletion(); result.accept(message); result.accept(done);
    expect(result.result(0)).toEqual({ done: true, response: '## Summary\nValidated the dependency map.', tokensIn: 100, tokensOut: 12 });
  });
  it('rejects tools, failed turns, empty output and nonzero process exit', () => {
    for (const type of ['command_execution', 'mcp_tool_call', 'file_change', 'web_search']) {
      expect(() => new CodexCompletion().accept(JSON.stringify({ type: 'item.started', item: { type } }))).toThrow('tool action');
    }
    expect(() => new CodexCompletion().accept('{"type":"turn.failed"}')).toThrow('could not complete');
    for (const raw of ['sensitive raw diagnostics', 'null', '[]']) expect(() => new CodexCompletion().accept(raw)).toThrow('invalid event data');
    const empty = new CodexCompletion(); empty.accept(done); expect(() => empty.result(0)).toThrow('nonempty');
    const partial = new CodexCompletion(); partial.accept(message); expect(() => partial.result(0)).toThrow('completed');
    partial.accept(done); expect(() => partial.result(1)).toThrow('completed');
  });
  it('does not inherit workspace config and disables tool-bearing surfaces', () => {
    const args = codexSummaryArgs('/tmp/owned', '');
    expect(args).toEqual(expect.arrayContaining(['--ignore-user-config', '--ephemeral', '--sandbox', 'read-only', 'web_search="disabled"', 'project_doc_max_bytes=0']));
    for (const feature of ['shell_tool', 'apps', 'plugins', 'browser_use', 'computer_use', 'multi_agent', 'image_generation', 'view_image']) {
      expect(args[args.indexOf(feature) - 1]).toBe('--disable');
    }
    expect(args).not.toContain('--model');
    expect(args.at(-1)).toBe('-');
  });
  it('honors cancellation before creating a subprocess', async () => {
    const controller = new AbortController(); controller.abort();
    const consume = async () => { for await (const _ of new CodexLLMClient().stream('', '', 'speech', { signal: controller.signal })) { /* collect */ } };
    await expect(consume()).rejects.toMatchObject({ name: 'AbortError' });
  });
  it('only sends bridge credentials to configured local origins', () => {
    expect(codexBridgeUrl('http://host.docker.internal:14042')).toBe('http://host.docker.internal:14042/complete');
    for (const url of ['https://example.com', 'http://127.0.0.1@evil.test', 'http://localhost/a', 'http://localhost?token=oops']) {
      expect(() => codexBridgeUrl(url)).toThrow('local HTTP origin');
    }
  });
});


describe('Codex subprocess lifecycle', () => {
  const env = { ...process.env }; let directory: string;
  const collect = async (signal?: AbortSignal) => { const out = []; for await (const value of new CodexLLMClient({ local: true }).stream('', '', 'test transcript', { signal })) out.push(value); return out; };
  beforeEach(async () => {
    directory = await mkdtemp(join(tmpdir(), 'codex-provider-test-'));
    process.env.CODEX_CLI_PATH = join(directory, 'fake-codex');
    process.env.CODEX_SUMMARY_TIMEOUT_MS = '5000';
  });
  afterEach(async () => { process.env = { ...env }; await rm(directory, { recursive: true, force: true }); });
  const cli = async (body: string) => writeFile(process.env.CODEX_CLI_PATH!, '#!/usr/bin/env node\n' + body, { mode: 0o700 });
  it('collects successful JSONL across chunks only after completion', async () => {
    await cli(`process.stdin.resume(); process.stdin.on('end', () => { process.stdout.write(${JSON.stringify(message + '\n')}); setTimeout(() => process.stdout.write(${JSON.stringify(done + '\n')}), 10); });`);
    expect(await collect()).toEqual([{ done: true, response: '## Summary\nValidated the dependency map.', tokensIn: 100, tokensOut: 12 }]);
  });
  it('rejects a model tool attempt and terminates the owned child', async () => {
    await cli(`console.log(JSON.stringify({type:'item.started',item:{type:'command_execution'}})); setInterval(()=>{},1000);`);
    await expect(collect()).rejects.toThrow('tool action');
  });
  it('cancels an in-flight child without returning partial text', async () => {
    await cli(`console.log(${JSON.stringify(message)}); setInterval(()=>{},1000);`);
    const controller = new AbortController(); const pending = collect(controller.signal);
    setTimeout(() => controller.abort(), 100);
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
  });
  it('runs codex with the allowlisted environment, not the pod secrets', async () => {
    const envFile = join(directory, 'env.json');
    await cli(`require('fs').writeFileSync(${JSON.stringify(envFile)}, JSON.stringify(Object.keys(process.env))); process.stdin.resume(); process.stdin.on('end', () => { process.stdout.write(${JSON.stringify(message + '\n')}); process.stdout.write(${JSON.stringify(done + '\n')}); });`);
    for (const k of ['REALTIME_JWT_SECRET', 'ADMIN_TOKEN', 'AWS_SECRET_ACCESS_KEY']) process.env[k] = 'must-not-reach-the-child';
    await collect();
    const keys = JSON.parse(await readFile(envFile, 'utf8')) as string[];
    const { CLI_CHILD_ENV_ALLOWLIST } = await import('../cliChildEnv.js');
    // macOS adds __CF_USER_TEXT_ENCODING to every process it starts.
    for (const k of keys.filter((key) => key !== '__CF_USER_TEXT_ENCODING')) expect(CLI_CHILD_ENV_ALLOWLIST as readonly string[]).toContain(k);
    expect(keys).not.toContain('REALTIME_JWT_SECRET');
  });
  it('reports timeout and missing executable without hanging', async () => {
    await cli('setInterval(()=>{},1000);'); process.env.CODEX_SUMMARY_TIMEOUT_MS = '100';
    await expect(collect()).rejects.toThrow('timed out');
    process.env.CODEX_CLI_PATH = join(directory, 'missing');
    await expect(collect()).rejects.toThrow('could not start');
  });
});


describe('Codex local bridge client', () => {
  const env = { ...process.env }; let server: Server | undefined;
  const collect = async () => { const out = []; for await (const value of new CodexLLMClient().stream('', 'Summary', 'Owned speech')) out.push(value); return out; };
  afterEach(async () => {
    process.env = { ...env };
    if (server) { const current = server; current.closeAllConnections(); await new Promise<void>(resolve => current.close(() => resolve())); server = undefined; }
  });
  async function serve(handler: RequestListener) {
    server = createServer(handler);
    await new Promise<void>(resolve => server!.listen(0, '127.0.0.1', resolve));
    process.env.CODEX_EXEC_URL = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
    process.env.CODEX_EXEC_TOKEN = 'owned-test-token-00000000000000000';
  }
  it('passes only bridge credentials and returns the native completed response', async () => {
    await serve((req, res) => {
      expect(req.headers.authorization).toBe(`Bearer ${process.env.CODEX_EXEC_TOKEN}`);
      let body = ''; req.on('data', chunk => { body += chunk; });
      req.on('end', () => { expect(JSON.parse(body)).toEqual({ model: '', systemPrompt: 'Summary', userPrompt: 'Owned speech' }); res.end(JSON.stringify({ done: true, response: 'Summary', tokensIn: 5, tokensOut: 1 })); });
    });
    expect(await collect()).toEqual([{ done: true, response: 'Summary', tokensIn: 5, tokensOut: 1 }]);
  });
  it('rejects redirects without forwarding the transcript and bounds chunked responses', async () => {
    let calls = 0;
    await serve((_req, res) => { calls++; if (calls === 1) { res.writeHead(307, { location: '/redirected' }); res.end(); } else { res.end('must not follow'); } });
    await expect(collect()).rejects.toThrow(); expect(calls).toBe(1);
    await new Promise<void>(resolve => server!.close(() => resolve())); server = undefined;
    await serve((_req, res) => { res.writeHead(200); res.end('x'.repeat(2 * 1024 * 1024 + 1)); });
    await expect(collect()).rejects.toThrow('too large');
  });
});
