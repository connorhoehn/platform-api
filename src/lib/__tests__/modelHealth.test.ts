// The model reachability probe, the circuit breaker and the reason mapping
// (realtime-examples NFR #225). No Redis here: the same transitions run on the
// process-local value; the fleet-wide check (2 replicas, one breaker) is the
// Loop 36 ppt done-check in docs/design/nfr.
jest.mock('../redis-client', () => ({ getRedisCacheClient: async () => null, getRedisClient: async () => null }));
import {
  admitModelCall, admit, checkCredentials, classifyModelFailure, CLOSED, forceBreaker, guardModelCall, ModelUnavailableError,
  onFailure, onProbeFailure, onProbeOk, onSuccess, probeModel, readBreaker, readModelHealth, rejectedCount,
  resetModelHealth, runModelProbe, type BreakerConfig,
} from '../modelHealth';
import { classifyPipelineError, classifyRetryability } from '../../pipeline/error-classification';

const cfg: BreakerConfig = { threshold: 3, cooldownMs: 60_000, trialMs: 50_000 };

describe('classifyModelFailure', () => {
  it.each([
    ['getaddrinfo EAI_AGAIN api.anthropic.com', 'dns'],
    ['claude is not authenticated in this environment: Not logged in · Please run /login', 'auth'],
    ['OAuth token has expired', 'auth'],
    ['the model did not answer within 30000ms', 'timeout'],
    ['API Error: 429 rate_limit_error', 'rate-limit'],
    ['Claude AI usage limit reached', 'rate-limit'],
    ['connect ECONNREFUSED 1.2.3.4:443', 'network'],
  ])('%s → %s', (message, kind) => {
    expect(classifyModelFailure(message)?.kind).toBe(kind);
  });

  it('is null for an answer that was merely bad (the model was reachable)', () => {
    expect(classifyModelFailure('the model answered with no JSON object')).toBeNull();
    expect(classifyModelFailure("slides[0]: unknown field 'subtitle'")).toBeNull();
  });

  it('says what to do about a login, not the CLI\'s "/login"', () => {
    expect(classifyModelFailure('Not logged in · Please run /login')?.reason).toMatch(/claude-creds-extract/);
  });
});

describe('breaker transitions', () => {
  const t = 1_000_000;
  const timeout = { kind: 'timeout' as const, reason: 'the model did not answer in time' };

  it('opens after N consecutive failures, and a success in between resets the count', () => {
    let s = onFailure(CLOSED, t, timeout, cfg);
    s = onFailure(s, t, timeout, cfg);
    expect(s.state).toBe('closed');
    s = onSuccess(s, t) ?? s;
    expect(s.failures).toBe(0);
    for (let i = 0; i < 3; i += 1) s = onFailure(s, t + i, timeout, cfg);
    expect(s).toMatchObject({ state: 'open', kind: 'timeout', until: t + 2 + 60_000, source: 'calls' });
  });

  it('fails fast while open, lets ONE trial through after the cool-down, and closes on its success', () => {
    const open = { ...CLOSED, state: 'open' as const, failures: 3, kind: 'dns' as const, reason: 'DNS lookup failed', until: t + 60_000 };
    expect(admit(open, t + 1000, cfg)).toMatchObject({ allow: false, kind: 'dns', reason: 'DNS lookup failed', retryInMs: 59_000 });
    const trial = admit(open, t + 60_000, cfg);
    expect(trial).toMatchObject({ allow: true, trial: true });
    const half = (trial as { next: typeof open }).next;
    expect(half.state).toBe('half-open');
    expect(admit(half, t + 61_000, cfg)).toMatchObject({ allow: false, kind: 'dns' });
    expect(onSuccess(half, t + 62_000)).toMatchObject({ state: 'closed', failures: 0 });
    expect(onFailure(half, t + 62_000, timeout, cfg)).toMatchObject({ state: 'open', until: t + 62_000 + 60_000 });
  });

  it('a forced breaker refuses everything and ignores outcomes', () => {
    const forced = { ...CLOSED, forced: { reason: 'test' } };
    expect(admit(forced, t, cfg)).toMatchObject({ allow: false, kind: 'forced', reason: 'test' });
    expect(onFailure(forced, t, timeout, cfg)).toBe(forced);
    expect(onSuccess(forced, t)).toBeNull();
  });

  it('the probe opens it at once, and a later passing probe lets a trial go without the cool-down', () => {
    const s = onProbeFailure(CLOSED, t, { kind: 'auth', reason: 'not logged in' }, cfg)!;
    expect(s).toMatchObject({ state: 'open', source: 'probe', kind: 'auth' });
    const ok = onProbeOk(s, t + 5000)!;
    expect(ok.until).toBe(t + 5000);
    expect(admit(ok, t + 5000, cfg)).toMatchObject({ allow: true, trial: true });
    // A breaker the CALLS opened waits out its cool-down: the probe proves DNS, not the model.
    expect(onProbeOk({ ...s, source: 'calls' }, t + 5000)).toBeNull();
  });
});

describe('guardModelCall (process-local store)', () => {
  const env = { ...process.env };
  beforeEach(() => { resetModelHealth(); process.env.MODEL_BREAKER_THRESHOLD = '2'; process.env.MODEL_BREAKER_COOLDOWN_MS = '60000'; });
  afterEach(() => { process.env = { ...env }; });

  it('after N transport failures every call fails fast with the reason, without running', async () => {
    const dead = () => Promise.reject(new Error('the model did not answer within 45000ms'));
    await expect(guardModelCall(dead)).rejects.toThrow('did not answer');
    await expect(guardModelCall(dead)).rejects.toThrow('did not answer');
    const fn = jest.fn(async () => 'x');
    const started = Date.now();
    for (let i = 0; i < 10; i += 1) {
      const err = await guardModelCall(fn).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(ModelUnavailableError);
      expect((err as Error).message).toMatch(/^Model unavailable: the model did not answer in time/);
    }
    expect(Date.now() - started).toBeLessThan(1000);
    expect(fn).not.toHaveBeenCalled();
    expect(rejectedCount()).toBe(10);
  });

  it('a bad answer and a neutral error do not count', async () => {
    for (let i = 0; i < 5; i += 1) await guardModelCall(() => Promise.reject(new Error('the model answered with no JSON object'))).catch(() => undefined);
    for (let i = 0; i < 5; i += 1) await guardModelCall(() => Promise.reject(new Error('timed out waiting for a slot')), () => true).catch(() => undefined);
    expect((await readBreaker()).state).toBe('closed');
  });

  it('forced open (the test hook) fails fast; cleared, calls run again', async () => {
    await forceBreaker('probe pointed at a bad endpoint (test)');
    await expect(guardModelCall(async () => 'x')).rejects.toThrow('Model unavailable: probe pointed at a bad endpoint (test)');
    expect((await readModelHealth()).message).toBe('Model unavailable — probe pointed at a bad endpoint (test)');
    await forceBreaker(null);
    await expect(guardModelCall(async () => 'x')).resolves.toBe('x');
    expect((await readModelHealth()).available).toBe(true);
  });
});

describe('the probe (no tokens)', () => {
  const now = Date.parse('2026-09-24T12:00:00Z');
  const env = { PIPELINE_LLM_PROVIDER: 'claude-code', HOME: '/root' };
  const good = JSON.stringify({ claudeAiOauth: { accessToken: 'a', refreshToken: 'r', expiresAt: now + 3600_000 } });
  const ok = { lookup: async () => ({}), connect: async () => undefined, env, now: () => now };

  beforeEach(() => resetModelHealth());

  it('passes with a live token, a resolvable host and an open port', async () => {
    expect(await probeModel({ ...ok, readCredentials: async () => good })).toMatchObject({ ok: true, host: 'api.anthropic.com' });
  });

  it('a blank token (yesterday\'s failed refresh) is "not logged in"', async () => {
    const blank = JSON.stringify({ claudeAiOauth: { accessToken: '', expiresAt: now } });
    expect(await probeModel({ ...ok, readCredentials: async () => blank })).toMatchObject({ ok: false, kind: 'auth' });
    expect(await probeModel({ ...ok, readCredentials: async () => null })).toMatchObject({ ok: false, kind: 'auth' });
  });

  it('an expired token with a refresh token is a note, not a failure', () => {
    const expired = JSON.stringify({ claudeAiOauth: { accessToken: 'a', refreshToken: 'r', expiresAt: now - 1 } });
    expect(checkCredentials(expired, now)).toEqual({ note: expect.stringMatching(/refreshes it/) });
  });

  it('DNS and connect failures name the host; MODEL_PROBE_HOST points it elsewhere', async () => {
    const dns = await probeModel({ ...ok, env: { ...env, MODEL_PROBE_HOST: 'model.invalid' }, readCredentials: async () => good, lookup: async () => { throw new Error('getaddrinfo ENOTFOUND model.invalid'); } });
    expect(dns).toMatchObject({ ok: false, kind: 'dns', host: 'model.invalid' });
    expect(dns.reason).toMatch(/DNS lookup of model.invalid failed/);
    const tcp = await probeModel({ ...ok, readCredentials: async () => good, connect: async () => { throw new Error('connect ETIMEDOUT'); } });
    expect(tcp).toMatchObject({ ok: false, kind: 'network' });
  });

  it('a failing probe opens the shared breaker with its reason; /health reads it', async () => {
    await runModelProbe({ ...ok, now: Date.now, readCredentials: async () => null });
    expect((await readBreaker())).toMatchObject({ state: 'open', source: 'probe', kind: 'auth' });
    const health = await readModelHealth();
    expect(health.available).toBe(false);
    expect(health.message).toMatch(/^Model unavailable — not logged in/);
    await expect(guardModelCall(async () => 'x')).rejects.toThrow(/^Model unavailable: not logged in/);
  });
});

describe('pipeline classification', () => {
  it('a model-unavailable step is terminal (a retry would be refused the same way)', () => {
    const msg = 'deck-revise model-unavailable: Model unavailable: not logged in. Nothing was changed.';
    expect(classifyPipelineError(msg)).toBe('ModelUnavailable');
    expect(classifyRetryability('ModelUnavailable', msg)).toBe('terminal');
  });
});


describe('explicit provider breaker isolation', () => {
  const env = { ...process.env };
  beforeEach(() => { resetModelHealth(); process.env.PIPELINE_LLM_PROVIDER = 'claude-code'; process.env.MODEL_BREAKER_THRESHOLD = '1'; });
  afterEach(() => { process.env = { ...env }; });
  it('a failed Claude credential probe cannot disable Codex, and Codex success cannot clear it', async () => {
    await runModelProbe({ env: { PIPELINE_LLM_PROVIDER: 'claude-code' }, readCredentials: async () => null,
      lookup: async () => undefined, connect: async () => undefined });
    await expect(admitModelCall('claude-code')).rejects.toBeInstanceOf(ModelUnavailableError);
    const codex = await admitModelCall('codex'); await codex.succeeded();
    expect(await readBreaker()).toMatchObject({ state: 'open', source: 'probe' });
    expect(await readBreaker('codex')).toMatchObject({ state: 'closed', failures: 0 });
  });
  it('counts independent provider failures while preserving the global operator stop', async () => {
    const codex = await admitModelCall('codex'); await codex.failed(new Error('Codex timed out'));
    await expect(admitModelCall('codex')).rejects.toBeInstanceOf(ModelUnavailableError);
    await expect(admitModelCall('bedrock')).resolves.toBeDefined();
    expect(await readBreaker()).toMatchObject({ state: 'closed', failures: 0 });
    await forceBreaker('maintenance');
    await expect(admitModelCall('bedrock')).rejects.toThrow('maintenance');
    await forceBreaker(null);
    await expect(admitModelCall('codex')).rejects.toBeInstanceOf(ModelUnavailableError);
  });
});
