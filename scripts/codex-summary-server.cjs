#!/usr/bin/env node
// Optional native host bridge for a containerized platform-api. Build first.
// CODEX_EXEC_TOKEN is a bridge credential, never the user's subscription token.
const http = require('node:http');
const { timingSafeEqual } = require('node:crypto');
const { CodexLLMClient } = require('../dist/pipeline/CodexLLMClient');

function createSummaryServer({ token, client = new CodexLLMClient({ local: true }), timeoutMs = 120_000 }) {
  if (typeof token !== 'string' || token.length < 32) throw new Error('A CODEX_EXEC_TOKEN of at least 32 characters is required.');
  let active = false;
  const send = (res, code, body) => {
    if (res.destroyed || res.headersSent) return;
    res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify(body));
  };
  return http.createServer(async (req, res) => {
    if (req.method === 'GET' && req.url === '/health') return send(res, 200, { ok: true, provider: 'codex', busy: active });
    const supplied = Buffer.from(req.headers.authorization || '');
    const expected = Buffer.from(`Bearer ${token}`);
    if (supplied.length !== expected.length || !timingSafeEqual(supplied, expected)) return send(res, 401, { error: 'Unauthorized' });
    if (req.method !== 'POST' || req.url !== '/complete') return send(res, 404, { error: 'Not found' });
    if (active) { res.setHeader('retry-after', '5'); return send(res, 429, { error: 'Summary service is busy' }); }
    active = true;
    const abort = new AbortController();
    const timeout = setTimeout(() => {
      abort.abort();
      send(res, 504, { error: 'Summary request timed out.' });
      if (!req.complete) req.destroy();
    }, timeoutMs);
    res.once('close', () => { if (!res.writableEnded) abort.abort(); });
    try {
      let body = '', bytes = 0;
      req.setEncoding('utf8');
      for await (const chunk of req) {
        bytes += Buffer.byteLength(chunk);
        if (bytes > 1024 * 1024) { send(res, 413, { error: 'Input is too large' }); return; }
        body += chunk;
      }
      let input;
      try { input = JSON.parse(body); } catch { return send(res, 400, { error: 'Invalid JSON' }); }
      if (!input || typeof input.userPrompt !== 'string' || !input.userPrompt.trim()
        || typeof input.systemPrompt !== 'string' || typeof input.model !== 'string'
        || input.model.length > 200) return send(res, 400, { error: 'Invalid summary request' });
      let result;
      for await (const chunk of client.stream(input.model, input.systemPrompt, input.userPrompt, { signal: abort.signal })) {
        if (chunk.done) result = chunk;
      }
      if (!result?.response?.trim()) throw new Error('Empty summary');
      send(res, 200, result);
    } catch {
      send(res, abort.signal.aborted ? 504 : 502, { error: 'Codex could not complete this summary.' });
    } finally { clearTimeout(timeout); active = false; }
  });
}

module.exports = { createSummaryServer };
if (require.main === module) {
  const port = Number(process.env.CODEX_EXEC_PORT || 14042);
  const host = process.env.CODEX_EXEC_HOST || '127.0.0.1';
  createSummaryServer({ token: process.env.CODEX_EXEC_TOKEN }).listen(port, host, () => {
    console.log(`Native Codex summary bridge listening on ${host}:${port}`);
  });
}
