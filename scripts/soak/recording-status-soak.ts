// Local heap soak of the recording-status long-poll path (lane P1).
//
// The 29-hour OOM on the media stack was the long-poll leaking bus
// subscriptions and timers (fixed by 6ec1c81). This runs the REAL route
// (`installRecordingStatusRoute` + `requireAuth` + `errorHandler`) in-process
// over a real distributed-core EventBus on an in-memory transport, with an
// in-process fake BFF answering lobby authority, and drives it hard:
//
//   woken    long-poll on the current revision, woken by a bus publication
//   deadline long-poll that nothing wakes; ends at the resync deadline
//   changed  `after` names an old revision; answers at once
//   refused  participant refused (404 before subscribe) or host refused (BFF 403)
//   expired  expired bearer (401)
//   aborted  client disconnects mid-wait, or during the authority read
//
// Every minute it forces GC and samples heap, RSS, live bus subscriptions,
// active handles and cycle counts, then rewrites the record in docs/soak/.
//
// Flat (the verdict): after SOAK_WARMUP_MIN minutes, the least-squares slope
// of post-GC heapUsed is at most SOAK_SLOPE_MB_PER_H (default 1.0 MB/hour,
// i.e. under 30 MB over the 29 hours the OOM took), live bus subscriptions
// never exceed requests in flight, active handles do not trend up (slope at
// most 1 per hour), and every post-warm-up minute completed at least 1,000
// cycles. `verdict: pass` is written only when the run reached
// SOAK_MINUTES and all of that holds; before then the record says pending.
//
//   node --expose-gc -r ts-node/register/transpile-only scripts/soak/recording-status-soak.ts
//   env: SOAK_MINUTES (125) SOAK_WARMUP_MIN (10) SOAK_CONCURRENCY (48)
//        SOAK_OUT (docs/soak/<date>-recording-status.md) SOAK_SLOPE_MB_PER_H (1.0)

process.env.LOG_LEVEL = process.env.LOG_LEVEL ?? 'silent';
process.env.REALTIME_JWT_SECRET = 'soak-only-secret-not-a-user-key-recording-status';
process.env.NODE_ENV = 'test';
delete process.env.SKIP_AUTH;

import express from 'express';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import jwt from 'jsonwebtoken';
import { EventBus } from 'distributed-core';
import { installRecordingStatusRoute } from '../../src/routes/recordingStatus';
import { recordingStatus } from '../../src/recordings/recordingStatus';
import { NotFoundError, errorHandler } from '../../src/middleware/error-handler';
import type { CallRecording } from '../../src/repositories/CallRecordingRepository';

const gc = (globalThis as { gc?: () => void }).gc;
if (!gc) { console.error('run with node --expose-gc'); process.exit(2); }

const MINUTES = Number(process.env.SOAK_MINUTES ?? 125);
const WARMUP = Number(process.env.SOAK_WARMUP_MIN ?? 10);
const CONCURRENCY = Number(process.env.SOAK_CONCURRENCY ?? 48);
const SLOPE_BOUND = Number(process.env.SOAK_SLOPE_MB_PER_H ?? 1.0);
const SAMPLE_MS = Number(process.env.SOAK_SAMPLE_MS ?? 60_000);
const MIN_CYCLES = 1_000;
const started = new Date();
const date = started.toLocaleDateString('en-CA'); // local YYYY-MM-DD
const OUT = process.env.SOAK_OUT ?? path.resolve(__dirname, '../../docs/soak', `${date}-recording-status.md`);
const SECRET = process.env.REALTIME_JWT_SECRET!;

// ─── Recording store, refused ids ─────────────────────────────────────────
const RECORDINGS = 64;
const rows = new Map<string, CallRecording>();
for (let i = 0; i < RECORDINGS; i++) {
  rows.set(`rec-${i}`, {
    recordingId: `rec-${i}`, callId: `call-${i}`, lobbyName: i % 16 === 15 ? 'orgiq:initiative:refused' : `orgiq:initiative:${i}`,
    composeStatus: 'ok', transcriptStatus: 'processing', summaryRunId: `run-${i}`, processingAttemptId: 'gen-1',
  } as CallRecording);
}
const revisionOf = (id: string) => recordingStatus(rows.get(id)!).revision;
const oldRevision = 'f'.repeat(64);
let slowAuthorize = 0; // when >0, the next authorize calls wait (for aborts during the authority read)

// ─── Fake BFF lobby authority ─────────────────────────────────────────────
const bff = http.createServer((req, res) => {
  let body = '';
  req.on('data', (c) => { body += c; });
  req.on('end', () => {
    const lobby = (() => { try { return JSON.parse(body).lobbyName as string; } catch { return ''; } })();
    const allowed = lobby !== 'orgiq:initiative:refused';
    res.writeHead(allowed ? 200 : 403, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ allowed }));
  });
});

// ─── In-memory bus transport (distributed-core EventBus on top) ───────────
type Listener = (topic: string, payload: unknown, meta: Record<string, unknown>) => Promise<void>;
const handlers = new Map<string, Listener>();
let seq = 0;
const transport = {
  subscribe: (_t: string, l: Listener) => { const id = String(++seq); handlers.set(id, l); return Promise.resolve(id); },
  unsubscribe: async (id: string) => { handlers.delete(id); },
  publish: async (topic: string, payload: unknown, meta: Record<string, unknown> = {}) => {
    for (const l of [...handlers.values()]) await l(topic, payload, meta);
  },
};
const bus = new EventBus<Record<string, Record<string, unknown>>>(transport as never, 'soak-recording-status', { topic: 'soak.recording.status' });
let busSubscriptions = 0;

// ─── App ──────────────────────────────────────────────────────────────────
const app = express();
installRecordingStatusRoute(app, {
  timeoutMs: 1_500,
  authorize: async (req) => {
    if (slowAuthorize > 0) { slowAuthorize--; await new Promise((r) => setTimeout(r, 80)); }
    const row = rows.get(String(req.params.recordingId));
    if (!row) throw new NotFoundError('recording not found');
    return row;
  },
  source: () => ({
    subscribeAll: (listener) => { busSubscriptions++; return bus.subscribeAll(listener as never); },
    unsubscribe: (id) => { busSubscriptions--; bus.unsubscribe(id); },
  }),
});
app.use(errorHandler);

const token = (expiresIn: number | string = '10m') => jwt.sign(
  { sub: 'soak-reader', org: 'orgiq', tenants: ['orgiq'], tokenKind: 'orgiq-hs256' }, SECRET,
  { algorithm: 'HS256', issuer: 'orgiq-middleware', audience: 'realtime', expiresIn: expiresIn as never });
let goodToken = token();
const expiredToken = jwt.sign({ sub: 'soak-reader', org: 'orgiq', tenants: ['orgiq'], tokenKind: 'orgiq-hs256', exp: Math.floor(Date.now() / 1000) - 60 },
  SECRET, { algorithm: 'HS256', issuer: 'orgiq-middleware', audience: 'realtime' });

// ─── Driver ───────────────────────────────────────────────────────────────
type Kind = 'woken' | 'deadline' | 'changed' | 'refused-participant' | 'refused-host' | 'expired' | 'aborted-wait' | 'aborted-read';
const counts: Record<string, number> = {};
const statusCounts: Record<string, number> = {};
let inFlight = 0;
let running = true;
let base = '';
const agent = new http.Agent({ keepAlive: true, maxSockets: CONCURRENCY * 2 });

function pick(): Kind {
  const r = Math.random();
  if (r < 0.40) return 'woken';
  if (r < 0.50) return 'deadline';
  if (r < 0.60) return 'changed';
  if (r < 0.68) return 'refused-participant';
  if (r < 0.76) return 'refused-host';
  if (r < 0.82) return 'expired';
  if (r < 0.94) return 'aborted-wait';
  return 'aborted-read';
}

function one(kind: Kind): Promise<void> {
  // Ordinary recordings: 1..63 minus the host-refused ones (n % 16 === 15).
  let n = 1 + Math.floor(Math.random() * (RECORDINGS - 1));
  if (n % 16 === 15) n -= 1;
  let id = `rec-${n}`;
  if (kind === 'refused-participant') id = `missing-${n}`;
  if (kind === 'refused-host') id = 'rec-15';
  // 'deadline' uses a recording the waker never publishes for.
  if (kind === 'deadline') id = 'rec-0';
  const after = kind === 'changed' ? oldRevision : rows.has(id) ? revisionOf(id) : oldRevision;
  const bearer = kind === 'expired' ? expiredToken : goodToken;
  if (kind === 'aborted-read') slowAuthorize++;
  return new Promise<void>((resolve) => {
    inFlight++;
    let done = false;
    const finish = (label: string) => {
      if (done) return; done = true; inFlight--;
      counts[kind] = (counts[kind] ?? 0) + 1;
      statusCounts[label] = (statusCounts[label] ?? 0) + 1;
      resolve();
    };
    const req = http.get(`${base}/recordings/${id}/status?after=${after}`, { agent, headers: { authorization: `Bearer ${bearer}` } }, (res) => {
      res.resume();
      res.on('end', () => finish(String(res.statusCode)));
      res.on('error', () => finish('res-error'));
    });
    req.on('error', () => finish('aborted'));
    if (kind === 'aborted-wait') setTimeout(() => req.destroy(), 30 + Math.random() * 300);
    if (kind === 'aborted-read') setTimeout(() => req.destroy(), 20);
  });
}

async function worker(): Promise<void> {
  while (running) await one(pick());
}

// Wake-ups: every 20 ms publish a matching step event for a random recording
// (never rec-0, so 'deadline' polls run out), plus unrelated noise.
function startWaker(): NodeJS.Timeout {
  return setInterval(() => {
    const n = 1 + Math.floor(Math.random() * (RECORDINGS - 1));
    void bus.publish('pipeline:step:completed', { runId: `run-${n}`, stepId: 'store', output: { callId: `call-${n}`, recordingId: `rec-${n}` } }).catch(() => undefined);
    void bus.publish('pipeline:step:completed', { runId: 'unrelated', stepId: 'noop' }).catch(() => undefined);
  }, 20);
}

// ─── Sampling, verdict, record ────────────────────────────────────────────
interface Sample { minute: number; heapMB: number; rssMB: number; externalMB: number; subscriptions: number; inFlight: number; handles: number; cycles: number; perKind: Record<string, number> }
const samples: Sample[] = [];
let lastTotal = 0;

function slope(xs: number[], ys: number[]): number {
  const n = xs.length; if (n < 2) return 0;
  const mx = xs.reduce((a, b) => a + b, 0) / n, my = ys.reduce((a, b) => a + b, 0) / n;
  let num = 0, den = 0;
  for (let i = 0; i < n; i++) { num += (xs[i] - mx) * (ys[i] - my); den += (xs[i] - mx) ** 2; }
  return den === 0 ? 0 : num / den;
}

function evaluate(final: boolean) {
  const post = samples.filter((s) => s.minute > WARMUP);
  const heapSlopePerH = slope(post.map((s) => s.minute), post.map((s) => s.heapMB)) * 60;
  const handleSlopePerH = slope(post.map((s) => s.minute), post.map((s) => s.handles)) * 60;
  const maxOrphans = Math.max(0, ...samples.map((s) => s.subscriptions - s.inFlight));
  const minCycles = post.length ? Math.min(...post.map((s) => s.cycles)) : 0;
  const elapsed = samples.length ? samples[samples.length - 1].minute : 0;
  const checks = {
    heapFlat: post.length >= 2 && heapSlopePerH <= SLOPE_BOUND,
    handlesFlat: post.length >= 2 && handleSlopePerH <= 1,
    noOrphanSubscriptions: maxOrphans <= 0,
    rate: post.length > 0 && minCycles >= MIN_CYCLES,
    duration: elapsed >= MINUTES,
  };
  const ok = Object.values(checks).every(Boolean);
  const verdict = ok ? 'pass' : (!final && checks.heapFlat !== false && !checks.duration && checks.noOrphanSubscriptions) ? 'pending' : 'fail';
  return { heapSlopePerH, handleSlopePerH, maxOrphans, minCycles, elapsed, checks, verdict: final ? (ok ? 'pass' : 'fail') : (ok ? 'pass' : verdict) };
}

function curve(values: number[], width = 60, height = 10): string {
  if (!values.length) return '';
  const step = Math.max(1, Math.ceil(values.length / width));
  const pts: number[] = [];
  for (let i = 0; i < values.length; i += step) pts.push(values[i]);
  const lo = Math.min(...pts), hi = Math.max(...pts), span = hi - lo || 1;
  const rowsOut: string[] = [];
  for (let r = height - 1; r >= 0; r--) {
    const label = (lo + (span * r) / (height - 1)).toFixed(1).padStart(7);
    rowsOut.push(`${label} | ${pts.map((v) => (Math.round(((v - lo) / span) * (height - 1)) === r ? '*' : ' ')).join('')}`);
  }
  rowsOut.push(`${' '.repeat(7)} +${'-'.repeat(pts.length + 1)}`);
  rowsOut.push(`${' '.repeat(9)}minute 1 … ${values.length} (${step} min per column)`);
  return rowsOut.join('\n');
}

function writeRecord(final: boolean) {
  const v = evaluate(final);
  const total = Object.values(counts).reduce((a, b) => a + b, 0);
  const lines = [
    `# Recording-status long-poll heap soak — ${date} (lane P1)`,
    '',
    `verdict: ${v.verdict}`,
    '',
    `- Script: \`scripts/soak/recording-status-soak.ts\` (real route, \`requireAuth\`, \`errorHandler\`, distributed-core EventBus on an in-memory transport, in-process fake BFF for lobby authority). Node ${process.version}, pid ${process.pid}.`,
    `- Started ${started.toISOString()}; ran ${v.elapsed} of ${MINUTES} planned minutes${final ? '' : ' (in progress — the record is rewritten every minute)'}.`,
    `- Load: ${CONCURRENCY} concurrent clients, ${total.toLocaleString('en-US')} cycles so far; resync deadline 1.5 s; a matching bus wake-up every 20 ms.`,
    `- Cycle mix: ${Object.entries(counts).sort().map(([k, n]) => `${k} ${n.toLocaleString('en-US')}`).join(', ')}.`,
    `- HTTP outcomes: ${Object.entries(statusCounts).sort().map(([k, n]) => `${k} ${n.toLocaleString('en-US')}`).join(', ')}.`,
    '',
    '## Definition of flat',
    '',
    `After a ${WARMUP}-minute warm-up, the least-squares slope of post-GC \`heapUsed\` (sampled once a minute after \`global.gc()\`) must be at most **${SLOPE_BOUND} MB/hour** — under 30 MB across the 29 hours the original OOM took. Also: live bus subscriptions never exceed requests in flight (no orphaned subscription), the active-handle count does not trend up (slope at most 1/hour), every post-warm-up minute completes at least ${MIN_CYCLES.toLocaleString('en-US')} cycles, and the run reaches its planned ${MINUTES} minutes.`,
    '',
    '| check | value | pass |',
    '|---|---|---|',
    `| heap slope after warm-up | ${v.heapSlopePerH.toFixed(3)} MB/h (bound ${SLOPE_BOUND}) | ${v.checks.heapFlat ? 'yes' : 'no'} |`,
    `| active-handle slope | ${v.handleSlopePerH.toFixed(3)} /h (bound 1) | ${v.checks.handlesFlat ? 'yes' : 'no'} |`,
    `| max subscriptions beyond in-flight | ${v.maxOrphans} | ${v.checks.noOrphanSubscriptions ? 'yes' : 'no'} |`,
    `| min cycles per post-warm-up minute | ${v.minCycles.toLocaleString('en-US')} (≥ ${MIN_CYCLES}) | ${v.checks.rate ? 'yes' : 'no'} |`,
    `| duration | ${v.elapsed} / ${MINUTES} min | ${v.checks.duration ? 'yes' : 'no'} |`,
    '',
    '## Post-GC heapUsed (MB) per minute',
    '',
    '```',
    curve(samples.map((s) => s.heapMB)),
    '```',
    '',
    '## Samples',
    '',
    '| min | heapUsed MB | rss MB | external MB | bus subs | in flight | handles | cycles/min |',
    '|---|---|---|---|---|---|---|---|',
    ...samples.map((s) => `| ${s.minute} | ${s.heapMB.toFixed(2)} | ${s.rssMB.toFixed(1)} | ${s.externalMB.toFixed(2)} | ${s.subscriptions} | ${s.inFlight} | ${s.handles} | ${s.cycles} |`),
    '',
    'Scope: a local, in-process soak of the route and its bus subscription lifecycle. It does not exercise the deployed media stack (SFU, native recording executors, real Redis transport); that confirmation is blocked on cloud access.',
    '',
  ];
  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, lines.join('\n'));
  return v;
}

function sample(minute: number) {
  gc!(); gc!();
  const m = process.memoryUsage();
  const total = Object.values(counts).reduce((a, b) => a + b, 0);
  const getRes = (process as unknown as { getActiveResourcesInfo?: () => string[] }).getActiveResourcesInfo;
  samples.push({
    minute, heapMB: m.heapUsed / 2 ** 20, rssMB: m.rss / 2 ** 20, externalMB: m.external / 2 ** 20,
    subscriptions: busSubscriptions, inFlight, handles: getRes ? getRes().filter((r) => r !== 'Timeout').length : 0,
    cycles: Math.round((total - lastTotal) * 60_000 / SAMPLE_MS), perKind: { ...counts },
  });
  lastTotal = total;
}

async function main() {
  await bus.start();
  await new Promise<void>((r) => bff.listen(0, '127.0.0.1', r));
  process.env.LOBBY_AUTHORITY_URL = `http://127.0.0.1:${(bff.address() as { port: number }).port}/api/realtime/lobby-access`;
  const server = app.listen(0, '127.0.0.1');
  await new Promise<void>((r) => server.once('listening', r));
  base = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  console.log(`[soak] pid ${process.pid}, ${MINUTES} min, record ${OUT}`);

  const waker = startWaker();
  const refresher = setInterval(() => { goodToken = token(); }, 5 * 60_000);
  const workers = Array.from({ length: CONCURRENCY }, () => worker());
  let minute = 0;
  await new Promise<void>((resolve) => {
    const timer = setInterval(() => {
      minute++;
      sample(minute);
      const v = writeRecord(false);
      console.log(`[soak] min ${minute} heap ${samples[samples.length - 1].heapMB.toFixed(2)} MB cycles ${samples[samples.length - 1].cycles} slope ${v.heapSlopePerH.toFixed(3)} MB/h subs ${busSubscriptions} inflight ${inFlight}`);
      if (minute >= MINUTES) { clearInterval(timer); resolve(); }
    }, SAMPLE_MS);
    const stop = () => { clearInterval(timer); resolve(); };
    process.once('SIGTERM', stop);
    process.once('SIGINT', stop);
  });
  running = false;
  clearInterval(waker); clearInterval(refresher);
  await Promise.all(workers);
  const v = writeRecord(true);
  console.log(`[soak] done: verdict ${v.verdict} after ${v.elapsed} min`);
  agent.destroy();
  server.closeAllConnections(); server.close(); bff.close();
  await bus.stop();
  process.exit(v.verdict === 'pass' ? 0 : 1);
}

void main().catch((err) => { console.error(err); process.exit(1); });
