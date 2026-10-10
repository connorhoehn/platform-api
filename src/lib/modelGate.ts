// One door for every model call (realtime-examples NFR Loop 39).
//
// Before this, only deck drafts and revises went through the model circuit
// breaker (modelHealth.ts) and the cross-replica RedisSemaphore
// (deck/modelSlots.ts). The /agent planner, the change verdict, the proposal
// drafter, the document agent and the pipeline LLM steps called the model
// directly: with the model down each of them waited out its own timeout, and
// nothing bounded how many ran at once across replicas.
//
// Every model call now goes through here, in this order:
//
//   1. single-flight (gated*Client({ singleFlight: true })): identical input
//      on this process shares one call — the joiners get the answer replayed;
//   2. the breaker: open → ModelUnavailableError ("Model unavailable: <reason>")
//      at once, before any wait;
//   3. a slot from the purpose's own pool (modelSlots.ts), so a flood of one
//      kind (deck revises) cannot starve another (the planner);
//   4. a timeout (MODEL_TIMEOUT_MS_<PURPOSE>, optional) that aborts the call;
//   5. metrics on /internal/metrics: model_gate_calls_total{purpose},
//      model_gate_failures_total{purpose,reason}, plus the pool gauges.
//
// Callers get an LLMClient / AgentClient that looks like the one they had
// (`gatedLLMClient`, `gatedAgentClient`), so streaming stays streaming.

import { createHash } from 'crypto';
import os from 'os';
import type { LLMChunk, LLMClient, LLMStreamOptions } from '../pipeline/LLMClient';
import type { AgentClient, AgentEvent, AgentRunOptions } from '../pipeline/agent/types';
import { acquireModelSlot, ModelSlotTimeoutError, type ModelPurpose, type ModelSlotOptions } from '../deck/modelSlots';
import { admitModelCall, ModelUnavailableError, type ModelBreakerScope } from './modelHealth';
import { getRegistry } from '../observability/metrics';

export type { ModelPurpose } from '../deck/modelSlots';

const LABELS = { service: process.env.WSG_SERVICE_NAME || 'platform-api', node_id: process.env.WSG_NODE_ID || os.hostname() };

export class ModelTimeoutError extends Error {
  constructor(readonly purpose: ModelPurpose, readonly timeoutMs: number) {
    super(`the model did not answer within ${Math.round(timeoutMs / 1000)} s`);
    this.name = 'ModelTimeoutError';
  }
}

export type GateFailureReason = 'unavailable' | 'busy' | 'timeout' | 'aborted' | 'error';

export function gateFailureReason(err: unknown): GateFailureReason {
  if (err instanceof ModelUnavailableError) return 'unavailable';
  if (err instanceof ModelSlotTimeoutError) return 'busy';
  if (err instanceof ModelTimeoutError) return 'timeout';
  if ((err as Error)?.name === 'AbortError') return 'aborted';
  return 'error';
}

function metrics(purpose: ModelPurpose) {
  const r = getRegistry();
  return {
    calls: r.counter('model_gate_calls_total', { ...LABELS, purpose }, 'Model calls that entered the gate, by purpose.'),
    joined: r.counter('model_gate_single_flight_joined_total', { ...LABELS, purpose }, 'Model calls answered by an identical call already in flight on this replica.'),
    failures: (reason: GateFailureReason) => r.counter('model_gate_failures_total', { ...LABELS, purpose, reason }, 'Model calls that failed, by purpose and reason.'),
  };
}

function timeoutFor(purpose: ModelPurpose): number {
  const n = Number(process.env[`MODEL_TIMEOUT_MS_${purpose.toUpperCase()}`]);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

const neutral = (err: unknown) => err instanceof ModelSlotTimeoutError || (err as Error)?.name === 'AbortError';

export type GateOptions = Omit<ModelSlotOptions, 'pool'> & { breakerScope?: ModelBreakerScope };

/**
 * Runs one model call as a stream through the gate. `open` gets the signal
 * the gate aborts on its timeout (combined with the caller's).
 */
export async function* gateStream<E>(
  purpose: ModelPurpose,
  open: (signal: AbortSignal | undefined) => AsyncIterable<E>,
  opts: GateOptions = {},
): AsyncGenerator<E> {
  const m = metrics(purpose);
  m.calls.inc(1);
  try {
    const ticket = await admitModelCall(opts.breakerScope);
    let lease;
    try {
      lease = await acquireModelSlot({ ...opts, pool: purpose });
    } catch (err) {
      await ticket.failed(err, neutral);
      throw err;
    }
    const timeoutMs = timeoutFor(purpose);
    const controller = timeoutMs || opts.signal ? new AbortController() : null;
    let timedOut = false;
    const timer = timeoutMs ? setTimeout(() => { timedOut = true; controller!.abort(); }, timeoutMs) : null;
    const onAbort = () => controller?.abort();
    opts.signal?.addEventListener('abort', onAbort, { once: true });
    try {
      for await (const ev of open(controller?.signal)) yield ev;
      if (timedOut) throw new ModelTimeoutError(purpose, timeoutMs);
      await ticket.succeeded();
    } catch (err) {
      const e = timedOut ? new ModelTimeoutError(purpose, timeoutMs) : err;
      await ticket.failed(e, neutral);
      throw e;
    } finally {
      if (timer) clearTimeout(timer);
      opts.signal?.removeEventListener('abort', onAbort);
      await lease.release();
    }
  } catch (err) {
    m.failures(gateFailureReason(err)).inc(1);
    throw err;
  }
}

// ── single-flight (this process) ────────────────────────────────────────────

const inFlight = new Map<string, Promise<unknown[]>>();

async function* singleFlight<E>(purpose: ModelPurpose, key: string, run: () => AsyncIterable<E>): AsyncGenerator<E> {
  const k = `${purpose}:${key}`;
  let shared = inFlight.get(k) as Promise<E[]> | undefined;
  if (shared) {
    metrics(purpose).joined.inc(1);
  } else {
    shared = (async () => { const out: E[] = []; for await (const ev of run()) out.push(ev); return out; })();
    inFlight.set(k, shared);
    shared.finally(() => { if (inFlight.get(k) === shared) inFlight.delete(k); }).catch(() => undefined);
  }
  for (const ev of await shared) yield ev;
}

export function inputKey(...parts: unknown[]): string {
  return createHash('sha256').update(JSON.stringify(parts)).digest('hex').slice(0, 32);
}

// ── Drop-in clients ─────────────────────────────────────────────────────────

export interface GatedClientOptions {
  /** Share one call between identical requests (buffers the answer; use where only the final text is read). */
  singleFlight?: boolean;
  /** Isolate an explicit provider from failures of another configured provider. */
  breakerScope?: ModelBreakerScope;
}

/** An LLMClient whose every stream goes through the gate under `purpose`. */
export function gatedLLMClient(inner: LLMClient, purpose: ModelPurpose, options: GatedClientOptions = {}): LLMClient {
  return {
    provider: inner.provider,
    stream(model: string, systemPrompt: string, userPrompt: string, opts: LLMStreamOptions = {}): AsyncIterable<LLMChunk> {
      const run = () => gateStream<LLMChunk>(purpose, (signal) => inner.stream(model, systemPrompt, userPrompt, { ...opts, ...(signal ? { signal } : {}) }), { signal: opts.signal, breakerScope: options.breakerScope });
      if (!options.singleFlight) return run();
      const { signal: _s, ...rest } = opts;
      return singleFlight(purpose, inputKey(options.breakerScope, inner.provider, model, systemPrompt, userPrompt, rest), run);
    },
  };
}

/** An AgentClient whose every run goes through the gate under `purpose`. */
export function gatedAgentClient(inner: AgentClient, purpose: ModelPurpose, options: GatedClientOptions = {}): AgentClient {
  return {
    provider: inner.provider,
    run(prompt: string, opts: AgentRunOptions = {}): AsyncIterable<AgentEvent> {
      const run = () => gateStream<AgentEvent>(purpose, (signal) => inner.run(prompt, { ...opts, ...(signal ? { signal } : {}) }), { signal: opts.signal, breakerScope: options.breakerScope });
      if (!options.singleFlight) return run();
      const { signal: _s, ...rest } = opts;
      return singleFlight(purpose, inputKey(options.breakerScope, inner.provider, prompt, rest), run);
    },
  };
}

/** One non-streaming model call through the gate. */
export async function withModelGate<T>(purpose: ModelPurpose, fn: (signal: AbortSignal | undefined) => Promise<T>, opts: GateOptions = {}): Promise<T> {
  let out: T | undefined;
  for await (const v of gateStream<T>(purpose, (signal) => (async function* one() { yield await fn(signal); })(), opts)) out = v;
  return out as T;
}

/** Is this the gate refusing because the model is down? Its message is "Model unavailable: <reason>". */
export function isModelUnavailable(err: unknown): err is ModelUnavailableError {
  return err instanceof ModelUnavailableError;
}

/** Tests. */
export function resetModelGate(): void { inFlight.clear(); }
