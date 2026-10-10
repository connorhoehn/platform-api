#!/usr/bin/env node
// The speech engine behind HTTP, for a platform-api that runs where the engine
// cannot: the dev pod is Alpine and the engine (OpenSuperWhisper) is a macOS
// app, so the call-transcription pipeline's transcribe step reaches this
// process on the host instead of exec'ing the CLI itself (`SPEECH_URL`).
//
//   POST /        body: the media file (audio or mp4), `x-filename` names it
//                 → 200, the CLI's whisper JSON `{ "text": "…" }`
//                 → 502 `{ "error": "…" }` when the engine fails
//   GET  /health  → 200 `{ "ok": true, "bin": "…" }`
//
//   PORT              default 14040
//   SPEECH_CLI        default /Applications/OpenSuperWhisper.app/Contents/MacOS/opensuperwhisper
//   SPEECH_TIMEOUT_MS default 600000
//
// One engine run at a time: the Metal engine is one process, and two of them
// only slow each other down. Requests queue.
import http from 'node:http';
import { execFile } from 'node:child_process';
import { promises as fs } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, basename } from 'node:path';

const PORT = process.env.PORT === '0' ? 0 : Number(process.env.PORT) || 14040;
const BIN = process.env.SPEECH_CLI ?? '/Applications/OpenSuperWhisper.app/Contents/MacOS/opensuperwhisper';
const TIMEOUT_MS = Number(process.env.SPEECH_TIMEOUT_MS) || 600_000;
const MAX_BODY = 1024 * 1024 * 1024;

let queue = Promise.resolve();

function runEngine(file) {
  return new Promise((resolve, reject) => {
    execFile(BIN, ['transcribe', file, '--json'], { maxBuffer: 16 * 1024 * 1024, timeout: TIMEOUT_MS }, (err, stdout, stderr) => {
      if (err) reject(new Error(`${err.message}${stderr ? ` — ${String(stderr).slice(0, 300)}` : ''}`));
      else resolve(String(stdout));
    });
  });
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > MAX_BODY) { reject(new Error('body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function send(res, status, body) {
  const json = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(json) });
  res.end(json);
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET' && (req.url === '/health' || req.url === '/')) { send(res, 200, { ok: true, bin: BIN }); return; }
  if (req.method !== 'POST') { send(res, 405, { error: 'POST the media file' }); return; }
  const started = Date.now();
  let dir = null;
  try {
    const body = await readBody(req);
    if (body.length === 0) { send(res, 400, { error: 'empty body' }); return; }
    dir = await fs.mkdtemp(join(tmpdir(), 'speech-'));
    const name = basename(String(req.headers['x-filename'] ?? 'audio.mp4')).replace(/[^A-Za-z0-9._-]/g, '_') || 'audio.mp4';
    const file = join(dir, name);
    await fs.writeFile(file, body);
    const out = await new Promise((resolve, reject) => {
      queue = queue.then(() => runEngine(file)).then(resolve, reject);
    });
    const start = out.indexOf('{'); const end = out.lastIndexOf('}');
    const text = start >= 0 && end >= start ? out.slice(start, end + 1) : JSON.stringify({ text: out });
    res.writeHead(200, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) });
    res.end(text);
    console.log(`[speech] ${name} ${body.length}B → ${Date.now() - started}ms`);
  } catch (err) {
    console.error(`[speech] failed after ${Date.now() - started}ms: ${err instanceof Error ? err.message : String(err)}`);
    if (!res.headersSent) send(res, 502, { error: err instanceof Error ? err.message : String(err) });
  } finally {
    if (dir) await fs.rm(dir, { recursive: true, force: true }).catch(() => undefined);
  }
});

server.listen(PORT, process.env.SPEECH_HOST, () => {
  console.log(`[speech] listening on :${server.address().port}, engine ${BIN}`);
});
