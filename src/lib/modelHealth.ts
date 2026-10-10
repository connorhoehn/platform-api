// Is the model reachable, and should a call even try? (realtime-examples NFR #225)
//
// 2026-09-24: cluster DNS broke, then the mounted Claude token expired and a
// failed refresh blanked it. For hours every deck draft silently fell back to
// the offline fixture ("the model did not answer within 30000ms", then "Not
// logged in"), every revise waited its full 45 s timeout, and nothing said so.
//
// Two pieces, both with their state in the CACHE Redis so every replica sees
// the same thing (a lost key reads as "closed / unknown", which is safe):
//
//   - The probe (`platform:model-probe`): every sweep interval (60 s) on the
//     sweep leader only. It spends no tokens: a DNS lookup of the provider's
//     host, a TCP connect to :443, and — for the claude-code provider — the
//     mounted credentials file (is there an access token; has it expired with
//     no refresh token). A definite failure (DNS, network, auth) opens the
//     breaker at once, with the reason, before any run has to time out.
//
//   - The circuit breaker (`platform:model-breaker`): every real model call
//     goes through `guardModelCall`. After MODEL_BREAKER_THRESHOLD (3)
//     consecutive transport failures — timeout, DNS, auth, rate limit,
//     network — it opens for MODEL_BREAKER_COOLDOWN_MS (60 s) and every call
//     fails at once with `ModelUnavailableError` ("Model unavailable: <reason>")
//     instead of waiting. After the cool-down ONE call is let through as the
//     half-open trial (the others still fail fast); its success closes the
//     breaker, its failure opens it again. A model that answered with bad JSON
//     is not a transport failure and does not count.
//
// The state is one JSON value written by compare-and-set (a 5-line Lua
// script), so the transitions are plain functions below and are tested
// without Redis. With no Redis at all the same functions run on a
// process-local value.
//
// Test hook (never touches credentials): set `forced` on the breaker value —
// scripts/dev/model-breaker.sh in realtime-examples does it with redis-cli —
// and every replica fails fast with that reason until it is cleared.
//
// Surfaces: `/health` check `model` (degraded, never failing the pod), and
// /internal/metrics: model_breaker_state, model_breaker_failures,
// model_probe_ok, model_probe_age_seconds, model_calls_rejected_total,
// model_unavailable{kind}.

import os from 'os';
import dns from 'dns';
import net from 'net';
import { readFile } from 'fs/promises';
import path from 'path';
import { getRedisCacheClient } from './redis-client';
import { getRegistry } from '../observability/metrics';
import log from './logger';

export type ModelFailureKind = 'dns' | 'auth' | 'timeout' | 'rate-limit' | 'network' | 'forced';

export interface ModelFailure { kind: ModelFailureKind; reason: string }

/**
 * A model call's error → why the model is unavailable, or null when the error
 * is not about reaching the model (bad JSON, a refusal, a validation miss).
 */
export function classifyModelFailure(message: string): ModelFailure | null {
  const text = String(message ?? '').trim();
  const first = text.split('\n').find((l) => l.trim())?.trim().slice(0, 240) ?? '';
  if (/ENOTFOUND|EAI_AGAIN|getaddrinfo|name resolution|could not resolve|dns/i.test(text)) {
    return { kind: 'dns', reason: `DNS lookup failed — the model's host cannot be resolved (${first})` };
  }
  if (/not logged in|\/login\b|not authenticated|invalid[ _]api[ _]key|authentication_error|oauth token has expired|no access token|token expired|credentials/i.test(text)) {
    return { kind: 'auth', reason: 'not logged in — the mounted Claude credentials are missing or expired (re-extract them: tilt trigger claude-creds-extract)' };
  }
  if (/\b429\b|rate[ _-]?limit|usage limit|overloaded|\b529\b/i.test(text)) {
    return { kind: 'rate-limit', reason: `rate limited by the model provider (${first})` };
  }
  if (/did not answer within|timed? ?out|ETIMEDOUT|deadline/i.test(text)) {
    return { kind: 'timeout', reason: `the model did not answer in time (${first})` };
  }
  if (/ECONNREFUSED|ECONNRESET|EHOSTUNREACH|ENETUNREACH|socket hang up|connection error|fetch failed|network/i.test(text)) {
    return { kind: 'network', reason: `cannot reach the model provider (${first})` };
  }
  return null;
}

// ── Breaker state and its transitions (pure) ────────────────────────────────

export interface BreakerState {
  state: 'closed' | 'open' | 'half-open';
  failures: number;
  kind?: ModelFailureKind;
  reason?: string;
  openedAt?: number;
  /** open: fail fast until then. */
  until?: number;
  /** half-open: the trial call's lease. */
  trialUntil?: number;
  /** What opened it: repeated call failures, or the probe. */
  source?: 'calls' | 'probe';
  /** Test hook: fail every call with this reason until cleared. */
  forced?: { reason: string; at?: number };
  updatedAt?: number;
  node?: string;
}

export interface BreakerConfig { threshold: number; cooldownMs: number; trialMs: number }

const num = (value: string | undefined, fallback: number) => {
  const n = Number(value);
  return value !== undefined && value !== '' && Number.isFinite(n) ? n : fallback;
};

export function breakerConfig(): BreakerConfig {
  return {
    threshold: Math.max(1, Math.floor(num(process.env.MODEL_BREAKER_THRESHOLD, 3))),
    cooldownMs: Math.max(1000, num(process.env.MODEL_BREAKER_COOLDOWN_MS, 60_000)),
    // A trial may take a whole model call (revise: 45 s) before it says anything.
    trialMs: Math.max(1000, num(process.env.MODEL_BREAKER_TRIAL_MS, 60_000)),
  };
}

export const CLOSED: BreakerState = { state: 'closed', failures: 0 };

export type Admission =
  | { allow: true; trial: boolean; next: BreakerState | null }
  | { allow: false; kind: ModelFailureKind; reason: string; retryInMs?: number; next: null };

/** May a call go? `next` is the value to write (null: unchanged). */
export function admit(s: BreakerState, now: number, cfg: BreakerConfig): Admission {
  if (s.forced) return { allow: false, kind: 'forced', reason: s.forced.reason || 'forced open by an operator', next: null };
  if (s.state === 'closed') return { allow: true, trial: false, next: null };
  const kind = s.kind ?? 'network';
  const reason = s.reason ?? 'the model is unreachable';
  if (s.state === 'open' && now < (s.until ?? 0)) return { allow: false, kind, reason, retryInMs: (s.until ?? now) - now, next: null };
  if (s.state === 'half-open' && now < (s.trialUntil ?? 0)) return { allow: false, kind, reason, retryInMs: (s.trialUntil ?? now) - now, next: null };
  return { allow: true, trial: true, next: { ...s, state: 'half-open', trialUntil: now + cfg.trialMs, updatedAt: now } };
}

export function onFailure(s: BreakerState, now: number, failure: ModelFailure, cfg: BreakerConfig): BreakerState {
  if (s.forced) return s;
  const failures = s.failures + 1;
  if (s.state !== 'closed' || failures >= cfg.threshold) {
    return {
      ...s, state: 'open', failures, kind: failure.kind, reason: failure.reason, source: 'calls',
      openedAt: s.state === 'closed' ? now : (s.openedAt ?? now), until: now + cfg.cooldownMs, trialUntil: undefined, updatedAt: now,
    };
  }
  return { ...s, failures, kind: failure.kind, reason: failure.reason, updatedAt: now };
}

export function onSuccess(s: BreakerState, now: number): BreakerState | null {
  if (s.forced) return null;
  if (s.state === 'closed' && s.failures === 0) return null;
  return { state: 'closed', failures: 0, updatedAt: now };
}

/** A trial that ended with no verdict on the model (a slot timeout, a caller abort): let the next call try. */
export function onNeutral(s: BreakerState, now: number): BreakerState | null {
  return s.state === 'half-open' ? { ...s, trialUntil: now, updatedAt: now } : null;
}

/** The probe found a definite failure: open now, with its reason. */
export function onProbeFailure(s: BreakerState, now: number, failure: ModelFailure, cfg: BreakerConfig): BreakerState | null {
  if (s.forced) return null;
  if (s.state === 'open' && s.kind === failure.kind && s.reason === failure.reason && now < (s.until ?? 0)) {
    return { ...s, until: now + cfg.cooldownMs, updatedAt: now };
  }
  return {
    ...s, state: 'open', kind: failure.kind, reason: failure.reason, source: 'probe',
    openedAt: s.state === 'closed' ? now : (s.openedAt ?? now), until: now + cfg.cooldownMs, trialUntil: undefined, updatedAt: now,
  };
}

/** The probe passed: a breaker IT opened may try a call now instead of waiting out the cool-down. */
export function onProbeOk(s: BreakerState, now: number): BreakerState | null {
  if (s.forced || s.state !== 'open' || s.source !== 'probe' || (s.until ?? 0) <= now) return null;
  return { ...s, until: now, updatedAt: now };
}

// ── Storage: the cache Redis (compare-and-set), or this process ─────────────

export const BREAKER_KEY = 'platform:model-breaker';
export const PROBE_KEY = 'platform:model-probe';
const BREAKER_TTL_MS = 24 * 3600_000;
const NODE = process.env.WSG_NODE_ID || os.hostname();

const CAS = `local cur = redis.call('GET', KEYS[1]) or ''
if cur ~= ARGV[1] then return 0 end
if ARGV[2] == '' then redis.call('DEL', KEYS[1]) else redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3]) end
return 1`;

type Cmd = { sendCommand(args: string[]): Promise<unknown> };
const localBreakers = new Map<string, string>();
export type ModelBreakerScope = 'anthropic' | 'bedrock' | 'codex' | 'claude-code';

function breakerKey(scope?: ModelBreakerScope): string {
  if (scope && !['anthropic', 'bedrock', 'codex', 'claude-code'].includes(scope)) throw new Error('Unknown model breaker scope');
  // The default provider retains its proactive probe and existing operator controls.
  return !scope || scope === (process.env.PIPELINE_LLM_PROVIDER || 'anthropic')
    ? BREAKER_KEY : `${BREAKER_KEY}:${scope}`;
}
let localProbe = '';

async function client(): Promise<Cmd | null> {
  return (await getRedisCacheClient().catch(() => null)) as unknown as Cmd | null;
}

function parse(raw: string): BreakerState {
  if (!raw) return { ...CLOSED };
  try {
    const v = JSON.parse(raw) as BreakerState;
    return v && typeof v === 'object' && typeof v.state === 'string' ? v : { ...CLOSED };
  } catch { return { ...CLOSED }; }
}

async function readRaw(scope?: ModelBreakerScope): Promise<{ raw: string; redis: Cmd | null }> {
  const key = breakerKey(scope);
  const redis = await client();
  if (!redis) return { raw: localBreakers.get(key) || '', redis: null };
  const raw = await redis.sendCommand(['GET', key]).catch(() => null);
  return { raw: typeof raw === 'string' ? raw : '', redis };
}

export async function readBreaker(scope?: ModelBreakerScope): Promise<BreakerState> {
  return parse((await readRaw(scope)).raw);
}

/** Read → transition → compare-and-set, a few times over a concurrent writer. */
async function update(step: (s: BreakerState) => BreakerState | null, scope?: ModelBreakerScope): Promise<BreakerState> {
  const key = breakerKey(scope);
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const { raw, redis } = await readRaw(scope);
    const cur = parse(raw);
    const next = step(cur);
    if (!next) return cur;
    const value = JSON.stringify({ ...next, node: NODE });
    if (!redis) { localBreakers.set(key, value); return next; }
    const ok = await redis.sendCommand(['EVAL', CAS, '1', key, raw, value, String(BREAKER_TTL_MS)]).catch(() => null);
    if (Number(ok) === 1) return next;
  }
  return readBreaker(scope);
}

// ── The guard every real model call goes through ────────────────────────────

export class ModelUnavailableError extends Error {
  constructor(readonly kind: ModelFailureKind, readonly reason: string, readonly retryInMs?: number) {
    super(`Model unavailable: ${reason}`);
    this.name = 'ModelUnavailableError';
  }
}

/** Errors that say nothing about the model (a slot wait, a caller abort). */
export type NeutralCheck = (err: unknown) => boolean;

let rejected = 0;

export interface ModelCallTicket {
  /** The call answered: close the breaker if it was counting failures. */
  succeeded(): Promise<void>;
  /** The call failed: a transport failure counts toward opening; a neutral one (slot wait, abort) does not. */
  failed(err: unknown, isNeutral?: NeutralCheck): Promise<void>;
}

/**
 * The breaker's admission for one model call, split from the call itself so
 * a streaming caller (modelGate.ts) can hold it across an async generator.
 * Open: throws ModelUnavailableError at once.
 */
export async function admitModelCall(scope?: ModelBreakerScope): Promise<ModelCallTicket> {
  const scoped = breakerKey(scope) !== BREAKER_KEY;
  // An operator's fleet-wide stop applies even to independently configured providers.
  if (scoped) {
    const global = await readBreaker();
    if (global.forced) {
      rejected += 1; gauges().rejected.inc(1);
      throw new ModelUnavailableError('forced', global.forced.reason);
    }
  }
  const cfg = breakerConfig();
  let verdict: Admission | null = null;
  await update((s) => { verdict = admit(s, Date.now(), cfg); return verdict.next; }, scope).catch(() => null);
  const v = verdict as Admission | null;
  if (v && !v.allow) {
    rejected += 1;
    gauges().rejected.inc(1);
    throw new ModelUnavailableError(v.kind, v.reason, v.retryInMs);
  }
  const trial = Boolean(v?.allow && v.trial);
  if (trial) log.info({ node: NODE, provider: scope }, 'model breaker: half-open trial call');
  return {
    async succeeded() {
      await update((s) => onSuccess(s, Date.now()), scope).then((s) => { if (trial) log.info({ node: NODE, provider: scope, state: s.state }, 'model breaker: trial succeeded'); }).catch(() => undefined);
    },
    async failed(err: unknown, isNeutral: NeutralCheck = () => false) {
      const failure = isNeutral(err) ? null : classifyModelFailure(err instanceof Error ? err.message : String(err));
      if (failure) {
        if (scope && failure.kind === 'auth') failure.reason = `The configured ${scope} provider could not authenticate; check its login or bridge configuration.`;
        const after = await update((s) => onFailure(s, Date.now(), failure, cfg), scope).catch(() => null);
        if (after?.state === 'open') log.warn({ node: NODE, provider: scope, kind: failure.kind, reason: failure.reason, failures: after.failures, untilMs: (after.until ?? 0) - Date.now() }, 'model breaker: open');
      } else {
        await update((s) => onNeutral(s, Date.now()), scope).catch(() => undefined);
      }
    },
  };
}

/**
 * Runs `fn` (one model call) past the breaker. Open: throws
 * ModelUnavailableError at once. A transport failure counts toward opening
 * it; a success closes it. `isNeutral` errors neither count nor close.
 */
export async function guardModelCall<T>(fn: () => Promise<T>, isNeutral: NeutralCheck = () => false): Promise<T> {
  const ticket = await admitModelCall();
  try {
    const out = await fn();
    await ticket.succeeded();
    return out;
  } catch (err) {
    await ticket.failed(err, isNeutral);
    throw err;
  }
}

/**
 * The breaker's verdict without taking a trial: throws ModelUnavailableError
 * when a call would be refused right now. For a caller with slow work before
 * its model call (a revise reads its sources first) to fail before that work.
 */
export async function assertModelAvailable(): Promise<void> {
  const s = await readBreaker().catch(() => CLOSED);
  const v = admit(s, Date.now(), breakerConfig());
  if (!v.allow) {
    gauges().rejected.inc(1);
    rejected += 1;
    throw new ModelUnavailableError(v.kind, v.reason, v.retryInMs);
  }
}

/** Operator/test hook: force every replica's calls to fail fast (or clear it). */
export async function forceBreaker(reason: string | null): Promise<BreakerState> {
  return update((s) => (reason ? { ...s, forced: { reason, at: Date.now() }, updatedAt: Date.now() } : { ...CLOSED, updatedAt: Date.now() }));
}

// ── The probe (no tokens) ───────────────────────────────────────────────────

export interface ProbeResult {
  ok: boolean;
  kind?: ModelFailureKind;
  reason?: string;
  /** Not a failure, worth knowing (an expired token the CLI will refresh). */
  note?: string;
  host: string;
  provider: string;
  checkedAt: number;
  ms: number;
  node: string;
}

export interface ProbeDeps {
  lookup?: (host: string) => Promise<unknown>;
  connect?: (host: string, port: number, timeoutMs: number) => Promise<void>;
  readCredentials?: () => Promise<string | null>;
  env?: NodeJS.ProcessEnv;
  now?: () => number;
}

function withTimeout<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout;
  return Promise.race([p, new Promise<T>((_, reject) => { timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms); })])
    .finally(() => clearTimeout(timer));
}

function tcpConnect(host: string, port: number, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const socket = net.connect({ host, port });
    const done = (err?: Error) => { socket.destroy(); if (err) reject(err); else resolve(); };
    socket.setTimeout(timeoutMs, () => done(new Error(`connect ${host}:${port} ETIMEDOUT`)));
    socket.once('connect', () => done());
    socket.once('error', (err) => done(err));
  });
}

export function probeTarget(env: NodeJS.ProcessEnv = process.env): { provider: string; host: string; bedrock: boolean } {
  const provider = env.PIPELINE_LLM_PROVIDER || 'anthropic';
  const bedrock = provider === 'bedrock' || env.CLAUDE_CODE_USE_BEDROCK === '1' || env.CLAUDE_CODE_USE_BEDROCK?.toLowerCase() === 'true';
  const region = env.PIPELINE_LLM_AWS_REGION || env.AWS_REGION || 'us-east-1';
  const host = env.MODEL_PROBE_HOST?.trim() || (bedrock ? `bedrock-runtime.${region}.amazonaws.com` : 'api.anthropic.com');
  return { provider, host, bedrock };
}

/** The mounted CLI credentials → a failure, a note, or nothing. Never logs or returns the token. */
export function checkCredentials(raw: string | null, now: number): { failure?: ModelFailure; note?: string } {
  const relogin = '(re-extract them: tilt trigger claude-creds-extract)';
  if (!raw) return { failure: { kind: 'auth', reason: `not logged in — no Claude credentials are mounted ${relogin}` } };
  let oauth: { accessToken?: unknown; refreshToken?: unknown; expiresAt?: unknown } | undefined;
  try { oauth = (JSON.parse(raw) as { claudeAiOauth?: typeof oauth }).claudeAiOauth; } catch { /* below */ }
  if (!oauth || typeof oauth.accessToken !== 'string' || !oauth.accessToken) {
    return { failure: { kind: 'auth', reason: `not logged in — the mounted Claude credentials have no access token ${relogin}` } };
  }
  const expiresAt = Number(oauth.expiresAt);
  if (Number.isFinite(expiresAt) && expiresAt > 0 && expiresAt <= now) {
    if (typeof oauth.refreshToken !== 'string' || !oauth.refreshToken) {
      return { failure: { kind: 'auth', reason: `token expired at ${new Date(expiresAt).toISOString()} and there is no refresh token ${relogin}` } };
    }
    return { note: `access token expired at ${new Date(expiresAt).toISOString()}; the CLI refreshes it on the next call` };
  }
  return {};
}

export async function probeModel(deps: ProbeDeps = {}): Promise<ProbeResult> {
  const env = deps.env ?? process.env;
  const now = deps.now ?? Date.now;
  const started = now();
  const { provider, host, bedrock } = probeTarget(env);
  const out = (r: Partial<ProbeResult>): ProbeResult => ({ ok: !r.kind, host, provider, checkedAt: started, ms: now() - started, node: NODE, ...r });

  let note: string | undefined;
  if (provider === 'claude-code' && !bedrock) {
    const read = deps.readCredentials ?? (async () => {
      const dir = env.CLAUDE_CONFIG_DIR || path.join(env.HOME || os.homedir(), '.claude');
      return readFile(path.join(dir, '.credentials.json'), 'utf8').catch(() => null);
    });
    const creds = checkCredentials(await read(), started);
    if (creds.failure) return out(creds.failure);
    note = creds.note;
  } else if (provider === 'anthropic' && !env.ANTHROPIC_API_KEY) {
    return out({ kind: 'auth', reason: 'no ANTHROPIC_API_KEY is set on this deployment' });
  }

  const lookup = deps.lookup ?? ((h: string) => dns.promises.lookup(h));
  try {
    await withTimeout(Promise.resolve(lookup(host)), 3000, `DNS lookup of ${host}`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return out({ kind: 'dns', reason: `DNS lookup of ${host} failed — the cluster cannot resolve the model's host (${msg.slice(0, 160)})`, ...(note ? { note } : {}) });
  }
  const connect = deps.connect ?? tcpConnect;
  try {
    await connect(host, 443, 3000);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return out({ kind: 'network', reason: `cannot connect to ${host}:443 (${msg.slice(0, 160)})`, ...(note ? { note } : {}) });
  }
  return out(note ? { note } : {});
}

/** One probe on the sweep leader: store the result for every replica, and move the breaker. */
export async function runModelProbe(deps: ProbeDeps = {}): Promise<ProbeResult> {
  const result = await probeModel(deps);
  const cfg = breakerConfig();
  const ttl = String(Math.max(180_000, 3 * num(process.env.PLATFORM_CLUSTER_SWEEP_INTERVAL_MS, 60_000)));
  const value = JSON.stringify(result);
  const redis = await client();
  if (redis) await redis.sendCommand(['SET', PROBE_KEY, value, 'PX', ttl]).catch(() => undefined);
  else localProbe = value;
  if (!result.ok && result.kind && result.reason) {
    const failure = { kind: result.kind, reason: result.reason };
    const after = await update((s) => onProbeFailure(s, Date.now(), failure, cfg)).catch(() => null);
    log.warn({ kind: result.kind, reason: result.reason, host: result.host, breaker: after?.state }, 'model probe: unavailable');
  } else {
    await update((s) => onProbeOk(s, Date.now())).catch(() => undefined);
  }
  await refreshModelHealthGauges().catch(() => undefined);
  return result;
}

export async function readProbe(): Promise<ProbeResult | null> {
  const redis = await client();
  const raw = redis ? await redis.sendCommand(['GET', PROBE_KEY]).catch(() => null) : localProbe;
  if (typeof raw !== 'string' || !raw) return null;
  try { return JSON.parse(raw) as ProbeResult; } catch { return null; }
}

// ── What /health, metrics and the UI read ───────────────────────────────────

export interface ModelHealth {
  available: boolean;
  /** "Model unavailable — <reason>" when not available. */
  message?: string;
  kind?: ModelFailureKind;
  reason?: string;
  breaker: BreakerState;
  probe: ProbeResult | null;
}

export async function readModelHealth(): Promise<ModelHealth> {
  const [breaker, probe] = await Promise.all([readBreaker(), readProbe()]);
  const now = Date.now();
  let kind: ModelFailureKind | undefined;
  let reason: string | undefined;
  if (breaker.forced) { kind = 'forced'; reason = breaker.forced.reason || 'forced open by an operator'; }
  else if (breaker.state !== 'closed') { kind = breaker.kind; reason = breaker.reason; }
  else if (probe && !probe.ok && now - probe.checkedAt < 5 * 60_000) { kind = probe.kind; reason = probe.reason; }
  const available = !reason;
  return { available, ...(reason ? { message: `Model unavailable — ${reason}`, kind, reason } : {}), breaker, probe };
}

const LABELS = { service: process.env.WSG_SERVICE_NAME || 'platform-api', node_id: NODE };
const KINDS: ModelFailureKind[] = ['dns', 'auth', 'timeout', 'rate-limit', 'network', 'forced'];
function gauges() {
  const registry = getRegistry();
  return {
    state: registry.gauge('model_breaker_state', LABELS, 'Model circuit breaker, fleet-wide: 0 closed, 1 half-open, 2 open (or forced).'),
    failures: registry.gauge('model_breaker_failures', LABELS, 'Consecutive model transport failures the breaker has counted.'),
    probeOk: registry.gauge('model_probe_ok', LABELS, 'Last model reachability probe (DNS, TCP, credentials): 1 ok, 0 failing, -1 none yet.'),
    probeAge: registry.gauge('model_probe_age_seconds', LABELS, 'Seconds since the last model reachability probe, on any replica.'),
    rejected: registry.counter('model_calls_rejected_total', LABELS, 'Model calls this replica failed fast because the breaker was open.'),
    kind: (k: ModelFailureKind) => registry.gauge('model_unavailable', { ...LABELS, kind: k }, 'Why the model is unavailable (1 on the current reason).'),
  };
}

export async function refreshModelHealthGauges(): Promise<ModelHealth> {
  const health = await readModelHealth();
  const g = gauges();
  const b = health.breaker;
  g.state.set(b.forced || b.state === 'open' ? 2 : b.state === 'half-open' ? 1 : 0);
  g.failures.set(b.failures);
  g.probeOk.set(health.probe ? (health.probe.ok ? 1 : 0) : -1);
  g.probeAge.set(health.probe ? Math.round((Date.now() - health.probe.checkedAt) / 1000) : -1);
  for (const k of KINDS) g.kind(k).set(health.kind === k ? 1 : 0);
  return health;
}

/** Every replica keeps its gauges current from the shared state (the probe itself runs on the leader). */
export function startModelHealthGauges(everyMs = 15_000): () => void {
  const timer = setInterval(() => { void refreshModelHealthGauges().catch(() => undefined); }, everyMs);
  timer.unref?.();
  void refreshModelHealthGauges().catch(() => undefined);
  return () => clearInterval(timer);
}

/** Tests. */
export function resetModelHealth(): void { localBreakers.clear(); localProbe = ''; rejected = 0; }
export function rejectedCount(): number { return rejected; }
