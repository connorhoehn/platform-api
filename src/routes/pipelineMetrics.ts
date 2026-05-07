// social-api/src/routes/pipelineMetrics.ts
//
// Two routers in one file (kept colocated for owner-bookkeeping reasons):
//
//   1. `pipelineMetricsRouter` — GET /api/pipelines/metrics
//      Returns runtime metrics for the pipeline subsystem. When a live
//      `PipelineBridge` is wired (Phase 4), the handler reads from
//      `bridge.getMetrics()` and tags the response `source: 'bridge'`.
//      When no bridge is wired, it falls back to `getPipelineMetricsStub()`
//      and tags `source: 'stub'` so the frontend can render a
//      "demo data" banner. Errors thrown by the bridge surface as
//      500 with `source: 'error'`.
//
//   2. `observabilityRouter` — mounted at /api/observability:
//        GET /api/observability/dashboard → cluster dashboard snapshot
//        GET /api/observability/metrics   → Prom-style metrics summary
//      Both endpoints stub-fill synthetic data when no live cluster is wired
//      so the frontend dashboards can light up against social-api.
//
// Both stubs are deterministic-ish (bucketed by minute/hour) so the dashboard
// shows lifelike change without flickering wildly between polls.
//
// Bridge contract (post AUDIT-05 / distributed-core v0.3.7): `bridge.getMetrics()`
// now forwards every dashboard field the underlying `PipelineModule` exposes
// — `runsStarted`, `runsCompleted`, `runsFailed`, `runsActive`,
// `runsAwaitingApproval`, `avgDurationMs`, `llmTokensIn`, `llmTokensOut`,
// `avgFirstTokenLatencyMs` (v0.3.7+), and `asOf`. Whichever fields the
// module omits are passed through as absent, and this route maps them to
// `null` rather than fabricating values.
//
// `estimatedCostUsd` is derived on the gateway side from `llmTokensIn` /
// `llmTokensOut` (AUDIT-05 ask #2). It is `null` only when either token count
// is absent. The frontend renders "—" / "n/a" for null fields.

import { Router } from 'express';
import {
  ModuleState,
  type ApplicationModuleDashboardData,
} from 'distributed-core';
import { getPipelineBridge } from './pipelineTriggers';
import { withContext } from '../lib/logger';
import { recordPipelineError } from '../observability/metrics';

const log = withContext({ route: 'pipelineMetrics' });

// ---------------------------------------------------------------------------
// Pipeline metrics
// ---------------------------------------------------------------------------

export interface PipelineMetrics {
  runsStarted: number;
  runsCompleted: number;
  runsFailed: number;
  runsActive: number;
  runsAwaitingApproval: number;
  avgDurationMs: number;
  llmTokensIn: number;
  llmTokensOut: number;
  estimatedCostUsd: number;
  asOf: string; // ISO timestamp
}

export function getPipelineMetricsStub(): PipelineMetrics {
  const now = Date.now();
  const minuteBucket = Math.floor(now / 60_000);
  const hourBucket = Math.floor(now / 3_600_000);

  const runsStarted = 147 + (minuteBucket % 20);
  const runsFailed = 3 + (hourBucket % 5);
  const runsActive = 4 + (minuteBucket % 7);
  const runsAwaitingApproval = 2 + (minuteBucket % 4);
  const runsCompleted = Math.max(
    0,
    runsStarted - runsFailed - runsActive - runsAwaitingApproval,
  );

  const avgDurationMs = 8_200 + (minuteBucket % 30) * 100;
  const llmTokensIn = 125_000 + (minuteBucket % 50) * 500;
  const llmTokensOut = 42_000 + (minuteBucket % 40) * 200;
  const estimatedCostUsd =
    Math.round(((llmTokensIn / 1_000_000) * 3 + (llmTokensOut / 1_000_000) * 15) * 100) / 100;

  return {
    runsStarted,
    runsCompleted,
    runsFailed,
    runsActive,
    runsAwaitingApproval,
    avgDurationMs,
    llmTokensIn,
    llmTokensOut,
    estimatedCostUsd,
    asOf: new Date(now).toISOString(),
  };
}

/**
 * Discriminator the frontend uses to tell live data from synthetic data.
 *
 *  - `bridge` — values came from the live `PipelineBridge.getMetrics()`.
 *              Fields not yet exposed by the bridge are `null` (never
 *              fabricated). The frontend should render real numbers and
 *              show "—" / "n/a" for null fields.
 *  - `stub`   — no bridge is wired; values are synthetic. The frontend
 *              SHOULD render a "demo data" banner so operators don't
 *              mistake the dashboard for live state.
 *  - `error`  — the bridge was wired but `getMetrics()` threw. The
 *              response is a 500; body carries `error: <message>`.
 */
export type PipelineMetricsSource = 'bridge' | 'stub' | 'error';

/**
 * Successful response shape for `GET /api/pipelines/metrics`.
 *
 * Backward-compat: the original keys of `PipelineMetrics` are preserved at
 * the top level (clients that ignore `source` still parse correctly).
 * Bridge-sourced responses set unsupported fields to `null` rather than
 * dropping them — keep `T | null` on every numeric field.
 */
export type PipelineMetricsResponse =
  | ({ source: 'stub' } & PipelineMetrics & { avgFirstTokenLatencyMs: number | null })
  | ({ source: 'bridge' } & {
      runsStarted: number | null;
      runsCompleted: number | null;
      runsFailed: number | null;
      runsActive: number | null;
      runsAwaitingApproval: number | null;
      avgDurationMs: number | null;
      llmTokensIn: number | null;
      llmTokensOut: number | null;
      /** Avg first-token latency across LLM steps (distributed-core v0.3.7+).
       *  `null` when the bridge / module on the other side is older. */
      avgFirstTokenLatencyMs: number | null;
      /** Derived on the gateway side from llmTokensIn/llmTokensOut (AUDIT-05).
       *  `null` when either token count is unavailable. */
      estimatedCostUsd: number | null;
      asOf: string;
    });

export const pipelineMetricsRouter = Router();

/** Pull a finite number off an unknown record, or return `null`. Strings/NaN/-
 *  Infinity all collapse to `null` — the route never fabricates a numeric. */
function numOrNull(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

/**
 * Derive an estimated cost from aggregate token counts using a blended rate.
 * AUDIT-05 ask #2: compute on the gateway side once the bridge exposes tokens;
 * distributed-core does not track pricing directly. Formula mirrors the stub.
 *
 * Rates: $3/Mtok input, $15/Mtok output (Claude Sonnet class default).
 * Returns `null` when either count is unavailable so the frontend renders "—".
 */
function derivedCostUsd(tokensIn: number | null, tokensOut: number | null): number | null {
  if (tokensIn === null || tokensOut === null) return null;
  return Math.round(((tokensIn / 1_000_000) * 3 + (tokensOut / 1_000_000) * 15) * 1_000_000) / 1_000_000;
}

pipelineMetricsRouter.get('/', async (_req, res) => {
  const bridge = getPipelineBridge();
  if (bridge && typeof bridge.getMetrics === 'function') {
    try {
      const real = (await bridge.getMetrics()) as Record<string, unknown>;
      // Pass through every dashboard field the bridge actually returned.
      // Missing/non-finite fields surface as `null` so the frontend renders
      // "—" / "n/a" instead of fake numbers. `estimatedCostUsd` is genuinely
      // not tracked by distributed-core yet — always `null` here.
      const asOfRaw = real['asOf'];
      const bridgeTokensIn = numOrNull(real['llmTokensIn']);
      const bridgeTokensOut = numOrNull(real['llmTokensOut']);
      const body: PipelineMetricsResponse = {
        source: 'bridge',
        runsStarted: numOrNull(real['runsStarted']),
        runsCompleted: numOrNull(real['runsCompleted']),
        runsFailed: numOrNull(real['runsFailed']),
        runsActive: numOrNull(real['runsActive']),
        runsAwaitingApproval: numOrNull(real['runsAwaitingApproval']),
        avgDurationMs: numOrNull(real['avgDurationMs']),
        llmTokensIn: bridgeTokensIn,
        llmTokensOut: bridgeTokensOut,
        avgFirstTokenLatencyMs: numOrNull(real['avgFirstTokenLatencyMs']),
        // Derived from token counts (AUDIT-05 ask #2). Null when tokens unavailable.
        estimatedCostUsd: derivedCostUsd(bridgeTokensIn, bridgeTokensOut),
        asOf: typeof asOfRaw === 'string' ? asOfRaw : new Date().toISOString(),
      };
      res.json(body);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      log.error({ err: message }, 'bridge.getMetrics threw');
      recordPipelineError();
      res.status(500).json({ source: 'error' as const, error: message });
    }
  } else {
    log.warn({}, 'bridge unavailable; serving stub metrics');
    // Stub path also carries `avgFirstTokenLatencyMs` so the response shape is
    // identical regardless of source. The stub uses `null` for it (we don't
    // synthesize a fake latency that isn't trivially derivable).
    const body: PipelineMetricsResponse = {
      source: 'stub',
      ...getPipelineMetricsStub(),
      avgFirstTokenLatencyMs: null,
    };
    res.json(body);
  }
});

// ---------------------------------------------------------------------------
// Observability — dashboard + cluster metrics
// ---------------------------------------------------------------------------

export interface ClusterNodeSnapshot {
  nodeId: string;
  status: 'healthy' | 'degraded' | 'down';
  role: 'leader' | 'follower' | 'worker';
  pipelinesActive: number;
  cpuPct: number;
  memPct: number;
  uptimeSec: number;
}

export interface ClusterAlert {
  id: string;
  severity: 'info' | 'warning' | 'critical';
  message: string;
  at: string;
}

export interface ObservabilityDashboard {
  cluster: {
    healthy: number;
    degraded: number;
    down: number;
    leaderId: string;
  };
  nodes: ClusterNodeSnapshot[];
  alerts: ClusterAlert[];
  pipelines: {
    active: number;
    awaitingApproval: number;
    completed24h: number;
    failed24h: number;
  };
  asOf: string;
}

export interface ObservabilityMetricsSummary {
  // Prometheus-ish summary. Names match what the dashboard charts plot.
  runsPerMinute: number;
  p50LatencyMs: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
  errorRatePct: number;
  llmTokensPerMinute: number;
  approvalsPendingTotal: number;
  asOf: string;
}

/**
 * Synthesize a "3 healthy nodes, 0 alerts" dashboard. Used when no live
 * distributed-core cluster is wired. Phase 4 swaps the body for a call into
 * the embedded `ClusterController.getDashboard()`.
 */
export function getObservabilityDashboardStub(): ObservabilityDashboard {
  const now = Date.now();
  const m = getPipelineMetricsStub();
  const nodes: ClusterNodeSnapshot[] = [
    {
      nodeId: 'node-a',
      status: 'healthy',
      role: 'leader',
      pipelinesActive: Math.ceil(m.runsActive / 3),
      cpuPct: 22 + (Math.floor(now / 60_000) % 8),
      memPct: 41 + (Math.floor(now / 90_000) % 5),
      uptimeSec: Math.floor(now / 1000) % 86_400,
    },
    {
      nodeId: 'node-b',
      status: 'healthy',
      role: 'follower',
      pipelinesActive: Math.floor(m.runsActive / 3),
      cpuPct: 18 + (Math.floor(now / 60_000) % 6),
      memPct: 37 + (Math.floor(now / 90_000) % 4),
      uptimeSec: Math.floor(now / 1000) % 86_400,
    },
    {
      nodeId: 'node-c',
      status: 'healthy',
      role: 'worker',
      pipelinesActive: m.runsActive - Math.ceil(m.runsActive / 3) - Math.floor(m.runsActive / 3),
      cpuPct: 31 + (Math.floor(now / 60_000) % 9),
      memPct: 44 + (Math.floor(now / 90_000) % 6),
      uptimeSec: Math.floor(now / 1000) % 86_400,
    },
  ];
  return {
    cluster: { healthy: 3, degraded: 0, down: 0, leaderId: 'node-a' },
    nodes,
    alerts: [],
    pipelines: {
      active: m.runsActive,
      awaitingApproval: m.runsAwaitingApproval,
      completed24h: m.runsCompleted,
      failed24h: m.runsFailed,
    },
    asOf: new Date(now).toISOString(),
  };
}

export function getObservabilityMetricsStub(): ObservabilityMetricsSummary {
  const m = getPipelineMetricsStub();
  const now = Date.now();
  const minuteBucket = Math.floor(now / 60_000);
  const runsPerMinute = 8 + (minuteBucket % 6);
  const p50LatencyMs = Math.round(m.avgDurationMs * 0.7);
  const p95LatencyMs = Math.round(m.avgDurationMs * 1.6);
  const p99LatencyMs = Math.round(m.avgDurationMs * 2.4);
  const totalRuns = m.runsStarted || 1;
  const errorRatePct = Math.round((m.runsFailed / totalRuns) * 1000) / 10;
  const llmTokensPerMinute = Math.round((m.llmTokensIn + m.llmTokensOut) / 60);
  return {
    runsPerMinute,
    p50LatencyMs,
    p95LatencyMs,
    p99LatencyMs,
    errorRatePct,
    llmTokensPerMinute,
    approvalsPendingTotal: m.runsAwaitingApproval,
    asOf: new Date(now).toISOString(),
  };
}

export const observabilityRouter = Router();

observabilityRouter.get('/dashboard', (_req, res) => {
  res.json(getObservabilityDashboardStub());
});

observabilityRouter.get('/metrics', (_req, res) => {
  res.json(getObservabilityMetricsStub());
});

// ---------------------------------------------------------------------------
// Module dashboard data — pass-through of PipelineModule.getDashboardData()
//
// Wires the distributed-core ApplicationModule observability surface (chart
// definitions + summary KPIs + alerts) so the frontend can render a uniform
// module-driven dashboard section instead of hand-rolling the same shape from
// `getMetrics()`. Mirrors the source-tag pattern of `pipelineMetricsRouter`:
// `source: 'bridge' | 'stub' | 'error'`.
// ---------------------------------------------------------------------------

export type PipelineDashboardDataResponse =
  | ({ source: 'bridge' } & ApplicationModuleDashboardData)
  | ({ source: 'stub' } & ApplicationModuleDashboardData)
  | { source: 'error'; error: string };

/**
 * Synthesise an `ApplicationModuleDashboardData`-shaped payload when no bridge
 * is wired. Charts derive from `getPipelineMetricsStub()` so the same renderer
 * lights up in stub and bridge modes.
 */
export function getPipelineDashboardDataStub(): ApplicationModuleDashboardData {
  const m = getPipelineMetricsStub();
  return {
    moduleId: 'pipeline',
    moduleName: 'Pipeline',
    state: ModuleState.RUNNING,
    summary: {
      totalResources: m.runsStarted,
      healthyResources: m.runsCompleted,
      activeConnections: m.runsActive,
      throughput: 0,
    },
    charts: [
      {
        title: 'Run Outcomes',
        type: 'bar',
        data: { completed: m.runsCompleted, failed: m.runsFailed, active: m.runsActive },
      },
      {
        title: 'LLM Token Usage',
        type: 'line',
        data: { tokensIn: m.llmTokensIn, tokensOut: m.llmTokensOut },
      },
      {
        title: 'Average Run Duration (ms)',
        type: 'gauge',
        data: { avgDurationMs: m.avgDurationMs },
      },
    ],
    alerts: [],
  };
}

export const pipelineDashboardDataRouter = Router();

pipelineDashboardDataRouter.get('/', async (_req, res) => {
  const bridge = getPipelineBridge();
  if (bridge && typeof bridge.getDashboardData === 'function') {
    try {
      const real = await bridge.getDashboardData();
      const body: PipelineDashboardDataResponse = { source: 'bridge', ...real };
      res.json(body);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      log.error({ err: message }, 'bridge.getDashboardData threw');
      recordPipelineError();
      res.status(500).json({ source: 'error' as const, error: message });
    }
  } else {
    log.warn({}, 'bridge unavailable; serving stub dashboard data');
    const body: PipelineDashboardDataResponse = {
      source: 'stub',
      ...getPipelineDashboardDataStub(),
    };
    res.json(body);
  }
});
