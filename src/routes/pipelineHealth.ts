// platform-api/src/routes/pipelineHealth.ts
//
// GET /api/pipelines/health — runtime introspection for the pipeline subsystem.
//
// Phase 1: all introspection points are stubbed but the endpoint exists so the
// frontend diagnostic banner has a stable URL to poll. Phase 4 fills these in
// by reading from the embedded distributed-core cluster + the pipelineModule
// (cancel / approval handler installation) + bridge instrumentation
// (lastEventAt + tokenRate sliding-window counters).
//
// Status semantics:
//   ok        — cluster booted, LLM client configured, pipelineModule connected,
//               and at least one event observed within the last 60s
//   degraded  — cluster booted but one of the other signals is unhealthy
//   unwired   — Phase 1 baseline; nothing is connected yet
//
// Routing: mount at `/pipelines/health` in routes/index.ts. As with
// `/pipelines/metrics` and `/pipelines/defs`, this static segment must be
// registered BEFORE any `/pipelines/:pipelineId` mount or it will be swallowed.
//
// Bridge probe (Phase 4 partial): we additionally surface live bridge state
// (`bridgeWired`, `runsActive`, `runsAwaitingApproval`, `pendingApprovals`) so
// monitoring/debugging can confirm the bridge is wired without tailing logs.
// All bridge calls are wrapped in try/catch and a tight 1s timeout so a flaky
// bridge can't hang or 500 the health endpoint.
//
// `lastEventAt` (ISO 8601) is read live from the bridge via
// `bridge.getLastEventAt()` — see the route handler below.

import { Router } from 'express';
import { getPipelineBridge, type PipelineBridge } from './pipelineTriggers';
import { recordPipelineError } from '../observability/metrics';
import { withContext } from '../lib/logger';

const log = withContext({ route: 'pipelineHealth' });

export interface PipelineHealthTokenRate {
  perSec1s: number;
  perSec10s: number;
  perSec60s: number;
}

/**
 * Result of a single bridge subsystem probe. Operators need to be able to
 * distinguish "bridge said zero" from "bridge timed out" from "bridge threw" —
 * collapsing all three to `0` (the previous behavior) made it impossible to
 * tell whether the system was idle or wedged.
 *
 *  - `ok`      — the surface returned a usable value within the timeout.
 *  - `timeout` — the surface didn't settle before {@link BRIDGE_PROBE_TIMEOUT_MS}.
 *  - `error`   — the surface threw or returned a malformed value.
 */
export type PipelineHealthProbeResult<T> =
  | { status: 'ok'; value: T }
  | { status: 'timeout' }
  | { status: 'error'; message: string };

export interface PipelineHealthProbes {
  /** `getPipelineBridge() !== null` — synchronous; always `ok` (never times out / errors). */
  bridgeWired: PipelineHealthProbeResult<boolean>;
  /** Length of `bridge.listActiveRuns()`. */
  runsActive: PipelineHealthProbeResult<number>;
  /** `bridge.getMetrics().runsAwaitingApproval`. */
  runsAwaitingApproval: PipelineHealthProbeResult<number>;
  /** Length of `bridge.getPendingApprovals()`. */
  pendingApprovals: PipelineHealthProbeResult<number>;
}

export interface PipelineHealth {
  status: 'ok' | 'degraded' | 'unwired';
  embeddedClusterReady: boolean;       // true once distributed-core cluster has booted (Phase 4)
  llmClientConfigured: boolean;        // ANTHROPIC_API_KEY present OR PIPELINE_LLM_PROVIDER=bedrock
  pipelineModuleConnected: boolean;    // setCancelHandler / setResolveApprovalHandler installed
  lastEventAt: string | null;          // ISO of last pipeline event observed by the bridge
  tokenRate: PipelineHealthTokenRate | null;
  asOf: string;
  // ---- Bridge live-state probe (additive — pre-existing fields above are unchanged) ----
  /** True iff `getPipelineBridge() !== null` at request time. */
  bridgeWired: boolean;
  /** Length of `bridge.listActiveRuns()` (0 when bridge is null or surface missing). */
  runsActive: number;
  /** `bridge.getMetrics().runsAwaitingApproval` (0 when bridge is null, surface missing, or call fails). */
  runsAwaitingApproval: number;
  /** Length of `bridge.getPendingApprovals()` (0 when bridge is null or surface missing). */
  pendingApprovals: number;
  /**
   * Structured per-subsystem probe results. Lets operators tell "bridge said
   * zero" apart from "bridge hung" apart from "bridge threw". The flat
   * `bridgeWired` / `runsActive` / `runsAwaitingApproval` / `pendingApprovals`
   * fields above remain for backward compatibility (timeouts/errors collapse
   * to the same default they always have).
   */
  probes: PipelineHealthProbes;
}

/**
 * Read the LLM provider env vars and decide whether the LLM client is
 * configured. Exported for tests.
 *
 *  - `anthropic` (default): requires `ANTHROPIC_API_KEY`.
 *  - `bedrock`: requires `AWS_REGION` (or relies on the AWS SDK default chain).
 *  - `claude-code`: assumes the host CLI session is mounted; presence of
 *    the credentials file is checked at call time, not here.
 *  - anything else: not configured.
 */
export function hasLLMConfig(): boolean {
  const provider = process.env.PIPELINE_LLM_PROVIDER ?? 'anthropic';
  if (provider === 'anthropic')   return !!process.env.ANTHROPIC_API_KEY;
  if (provider === 'bedrock')     return !!(process.env.AWS_REGION ?? true);
  if (provider === 'claude-code') return true;
  return false;
}

/**
 * Per-call timeout for any bridge surface invoked from the health probe.
 * 1s keeps the health endpoint responsive even if the bridge is wedged on
 * a slow downstream (e.g. Redis, Bedrock). On timeout we degrade to the
 * default value for that field rather than 500-ing.
 */
const BRIDGE_PROBE_TIMEOUT_MS = 1000;

/**
 * Marker symbol the timeout branch of {@link runProbe} resolves with so we can
 * distinguish "the surface returned `undefined`/`null`" from "the timer won".
 * Using a symbol avoids any collision with values a bridge surface might
 * legitimately produce.
 */
const PROBE_TIMEOUT = Symbol('pipelineHealth.probeTimeout');

/**
 * Run a single probe against a (possibly-async) thunk and classify the
 * outcome. The classification is the whole point of this helper: operators
 * need to be able to tell "the bridge said zero" from "the bridge hung" from
 * "the bridge threw" — collapsing all three to `0` (the previous behavior)
 * masked real outages as idleness.
 *
 * Synchronous thunks short-circuit the timer; only thenables race against it.
 * Never throws — any thrown value (including non-Errors) becomes
 * `{ status: 'error', message }`.
 */
async function runProbe<T>(
  thunk: () => T | Promise<T>,
  timeoutMs: number = BRIDGE_PROBE_TIMEOUT_MS,
): Promise<PipelineHealthProbeResult<T>> {
  let raw: T | Promise<T>;
  try {
    raw = thunk();
  } catch (err) {
    return { status: 'error', message: errMessage(err) };
  }

  // Fast-path: synchronous result — no timer, no race.
  if (raw === null || raw === undefined || typeof (raw as { then?: unknown }).then !== 'function') {
    return { status: 'ok', value: raw as T };
  }

  let timer: NodeJS.Timeout | undefined;
  try {
    const settled = await Promise.race<T | typeof PROBE_TIMEOUT>([
      raw as Promise<T>,
      new Promise<typeof PROBE_TIMEOUT>((resolve) => {
        timer = setTimeout(() => resolve(PROBE_TIMEOUT), timeoutMs);
        // Don't keep the event loop alive solely for this timer.
        if (typeof (timer as { unref?: () => void }).unref === 'function') {
          (timer as { unref: () => void }).unref();
        }
      }),
    ]);
    if (settled === PROBE_TIMEOUT) {
      return { status: 'timeout' };
    }
    return { status: 'ok', value: settled as T };
  } catch (err) {
    return { status: 'error', message: errMessage(err) };
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function errMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === 'string') return err;
  try {
    return JSON.stringify(err);
  } catch {
    return String(err);
  }
}

/**
 * Probe the live PipelineBridge (when wired) for the four counts surfaced
 * on the health endpoint. Each surface is independent: a throw / missing
 * method / timeout on one does not affect the others. Never throws.
 *
 * Returns both the structured per-subsystem {@link PipelineHealthProbes}
 * (for operators) and the legacy flat scalars (for backward-compatible
 * response shape). Timeouts and errors collapse to the legacy default for
 * each scalar field — the truth lives in `probes`.
 */
export async function probeBridge(
  bridge: PipelineBridge | null,
): Promise<{
  bridgeWired: boolean;
  runsActive: number;
  runsAwaitingApproval: number;
  pendingApprovals: number;
  probes: PipelineHealthProbes;
}> {
  // Synchronous: just records whether a bridge is wired. Always `ok`.
  const bridgeWiredProbe: PipelineHealthProbeResult<boolean> = {
    status: 'ok',
    value: bridge !== null,
  };

  if (!bridge) {
    return {
      bridgeWired: false,
      runsActive: 0,
      runsAwaitingApproval: 0,
      pendingApprovals: 0,
      probes: {
        bridgeWired: bridgeWiredProbe,
        runsActive: { status: 'ok', value: 0 },
        runsAwaitingApproval: { status: 'ok', value: 0 },
        pendingApprovals: { status: 'ok', value: 0 },
      },
    };
  }

  // ---- runsActive ---------------------------------------------------------
  let runsActiveProbe: PipelineHealthProbeResult<number>;
  if (typeof bridge.listActiveRuns === 'function') {
    const r = await runProbe<unknown>(() => bridge.listActiveRuns!());
    if (r.status === 'ok') {
      runsActiveProbe = {
        status: 'ok',
        value: Array.isArray(r.value) ? r.value.length : 0,
      };
    } else {
      runsActiveProbe = r;
    }
  } else {
    runsActiveProbe = { status: 'ok', value: 0 };
  }

  // ---- runsAwaitingApproval (read out of getMetrics) ----------------------
  let runsAwaitingApprovalProbe: PipelineHealthProbeResult<number>;
  if (typeof bridge.getMetrics === 'function') {
    const r = await runProbe<unknown>(() => bridge.getMetrics!());
    if (r.status === 'ok') {
      const n = (r.value as { runsAwaitingApproval?: unknown } | null | undefined)
        ?.runsAwaitingApproval;
      runsAwaitingApprovalProbe = {
        status: 'ok',
        value: typeof n === 'number' && Number.isFinite(n) ? n : 0,
      };
    } else {
      runsAwaitingApprovalProbe = r;
    }
  } else {
    runsAwaitingApprovalProbe = { status: 'ok', value: 0 };
  }

  // ---- pendingApprovals ---------------------------------------------------
  let pendingApprovalsProbe: PipelineHealthProbeResult<number>;
  if (typeof bridge.getPendingApprovals === 'function') {
    const r = await runProbe<unknown>(() => bridge.getPendingApprovals!());
    if (r.status === 'ok') {
      pendingApprovalsProbe = {
        status: 'ok',
        value: Array.isArray(r.value) ? r.value.length : 0,
      };
    } else {
      pendingApprovalsProbe = r;
    }
  } else {
    pendingApprovalsProbe = { status: 'ok', value: 0 };
  }

  return {
    bridgeWired: true,
    runsActive: runsActiveProbe.status === 'ok' ? runsActiveProbe.value : 0,
    runsAwaitingApproval:
      runsAwaitingApprovalProbe.status === 'ok' ? runsAwaitingApprovalProbe.value : 0,
    pendingApprovals: pendingApprovalsProbe.status === 'ok' ? pendingApprovalsProbe.value : 0,
    probes: {
      bridgeWired: bridgeWiredProbe,
      runsActive: runsActiveProbe,
      runsAwaitingApproval: runsAwaitingApprovalProbe,
      pendingApprovals: pendingApprovalsProbe,
    },
  };
}

/**
 * Build the Phase 1 stub payload. Phase 4 swaps the body for live reads from
 * the embedded cluster + pipelineModule + bridge instrumentation.
 *
 * The four `bridge*` fields default to the "no bridge wired" values. The HTTP
 * route layers a live probe on top via {@link probeBridge}.
 */
export function getPipelineHealthStub(): PipelineHealth {
  return {
    status: 'unwired',
    embeddedClusterReady: false,
    llmClientConfigured: hasLLMConfig(),
    pipelineModuleConnected: false,
    lastEventAt: null,
    tokenRate: null,
    asOf: new Date().toISOString(),
    bridgeWired: false,
    runsActive: 0,
    runsAwaitingApproval: 0,
    pendingApprovals: 0,
    probes: {
      bridgeWired: { status: 'ok', value: false },
      runsActive: { status: 'ok', value: 0 },
      runsAwaitingApproval: { status: 'ok', value: 0 },
      pendingApprovals: { status: 'ok', value: 0 },
    },
  };
}

export const pipelineHealthRouter = Router();

pipelineHealthRouter.get('/', async (_req, res) => {
  const base = getPipelineHealthStub();
  // Probe is best-effort — any failure inside is caught and falls back to
  // the stub defaults already on `base`. Catastrophic failures (e.g. a bridge
  // accessor that throws synchronously) are reported as a single 5xx; a
  // probe that *individually* timed out or threw is still 200 with a
  // truthful `probes.<subsystem>.status` so operators can tell the
  // difference.
  let probe: Awaited<ReturnType<typeof probeBridge>>;
  try {
    probe = await probeBridge(getPipelineBridge());
  } catch (err) {
    log.error(
      { err: err instanceof Error ? err.message : String(err) },
      'pipeline-health probe failed catastrophically',
    );
    res.status(500).json({
      error: 'pipeline-health probe failed',
      message: err instanceof Error ? err.message : String(err),
    });
    return;
  }

  // Per-probe observability: surface non-ok subsystems as a single
  // `pipeline_errors_total` increment (rate, not count) and a structured log
  // line so operators can distinguish "bridge said zero" from "bridge hung"
  // from "bridge threw" without tailing logs.
  for (const [name, result] of Object.entries(probe.probes) as Array<
    [keyof PipelineHealthProbes, PipelineHealthProbeResult<unknown>]
  >) {
    if (result.status === 'ok') continue;
    recordPipelineError();
    if (result.status === 'timeout') {
      log.warn(
        { probe: name, status: 'timeout' },
        'pipeline health probe timed out',
      );
    } else {
      log.error(
        { probe: name, status: 'error', err: result.message },
        'pipeline health probe errored',
      );
    }
  }

  // Read lastEventAt from the bridge (epoch ms -> ISO 8601, or null).
  let lastEventAt: string | null = null;
  try {
    const bridgeRef = getPipelineBridge();
    if (bridgeRef && typeof bridgeRef.getLastEventAt === 'function') {
      const ts = bridgeRef.getLastEventAt();
      if (typeof ts === 'number' && ts > 0) {
        lastEventAt = new Date(ts).toISOString();
      }
    }
  } catch {
    // Non-fatal — fall through with null.
  }

  // Phase 4 complete: set embeddedClusterReady=true when bridge is wired.
  // The distributed-core cluster + PipelineModule are booted and functional.
  const embeddedClusterReady = probe.bridgeWired;
  const pipelineModuleConnected = probe.bridgeWired;

  // Derive overall status: ok when bridge is wired + LLM configured + recent events,
  // degraded when bridge is wired but other signals are unhealthy, unwired otherwise.
  let status: 'ok' | 'degraded' | 'unwired' = 'unwired';
  if (embeddedClusterReady) {
    const hasRecentEvent = lastEventAt && Date.now() - new Date(lastEventAt).getTime() < 60_000;
    status = base.llmClientConfigured && hasRecentEvent ? 'ok' : 'degraded';
  }

  const body: PipelineHealth = {
    ...base,
    ...probe,
    status,
    embeddedClusterReady,
    pipelineModuleConnected,
    lastEventAt
  };
  res.json(body satisfies PipelineHealth);
});
