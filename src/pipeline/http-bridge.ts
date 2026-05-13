// platform-api/src/pipeline/http-bridge.ts
//
// SPIKE (M4 Block 1) — HTTPPipelineBridge: proof that platform-api's
// PipelineBridge interface can be satisfied by forwarding to a remote
// pipeline-worker over HTTP. This file demonstrates the wire format and
// per-method shape; it is NOT wired into setPipelineBridge anywhere. The
// goal is to confirm interface-satisfiability, not to ship a transport.
//
// =====================================================================
// METHOD-BY-METHOD ASSESSMENT (HTTP-FRIENDLINESS)
// =====================================================================
//
// EASY — pure request/response, JSON in / JSON out, no streaming:
//   trigger              → POST /internal/pipeline/trigger
//   resolveBreakpoint    → POST /internal/pipeline/resolveBreakpoint
//   resumeFromStep       → POST /internal/pipeline/resumeFromStep
//   patchPausedContext   → POST /internal/pipeline/patchPausedContext
//   revertContextPatch   → POST /internal/pipeline/revertContextPatch
//   getRun               → POST /internal/pipeline/getRun
//   resolveApproval      → POST /internal/pipeline/resolveApproval
//   listRuns             → POST /internal/pipeline/listRuns
//   listActiveRuns       → POST /internal/pipeline/listActiveRuns
//   cancelRun            → POST /internal/pipeline/cancelRun
//   getPendingApprovals  → POST /internal/pipeline/getPendingApprovals
//   getMetrics           → POST /internal/pipeline/getMetrics
//   getDashboardData     → POST /internal/pipeline/getDashboardData
//   getLastEventAt       → POST /internal/pipeline/getLastEventAt
//   getHistory           → POST /internal/pipeline/getHistory
//                          NOTE: technically a paged read (fromVersion cursor).
//                          History payloads stay small (hundreds of events,
//                          not millions) so plain JSON is fine for v1; if a
//                          replay tool ever wants whole runs at once, switch
//                          this endpoint to NDJSON streaming. Spike keeps it
//                          easy.
//
// AWKWARD — synchronous interfaces over a non-RPC seam. Each needs a
// transport-shape decision before M4.2 wires up the worker for real:
//
//   getInspector(): QueueInspector<PipelineRunSnapshot>
//     The inspector is a stateful object with methods like
//     paginatePending() / findStuckLeases() — caller expects a live handle,
//     not a snapshot. Sketch: implement a *proxy* QueueInspector whose
//     methods each map to one HTTP call against the worker's inspector
//     surface (POST /internal/pipeline/inspector/<methodName>). The bridge
//     can synthesise the proxy once and cache it; per-call latency is the
//     only cost. Alternate transport: push QueueSummary as a periodic
//     `inspector.snapshot` event over the durable bus and let platform-api
//     subscribe — turns N inspector RPCs into one fan-out subscription.
//
//   getDLQ(): DeadLetterQueue<DCBusEvent> | null
//     Same story as inspector — stateful handle with put/list/redrive/purge
//     verbs. Sketch: HTTP proxy with one endpoint per DLQ verb. Redrive in
//     particular is a long-running operation (could touch many entries);
//     consider returning a job-id and polling, or moving DLQ control-plane
//     over event-catalog instead of synchronous HTTP entirely.
//
// LIVE-EVENTS GAPS — methods the interface DOESN'T expose today but a
// remote worker setup will eventually need:
//
//   * `pipeline.run.reassigned` and other PublicBusEvent fan-out. Today
//     gateway-side bridges subscribe to module.getEventBus(). When the
//     worker is remote there is no in-process bus, so platform-api will
//     need either a server-sent-events / WebSocket tail of the worker's
//     bus, or (preferred) consumption from a durable transport
//     (event-catalog → SQS). The HTTP bridge does NOT cover this surface;
//     it's intentionally out of scope for the request/response spike.
//
// =====================================================================
//
// WIRE PROTOCOL (v1, spike-only — not a stable contract):
//   POST ${workerBaseUrl}/internal/pipeline/<methodName>
//   Content-Type: application/json
//   X-Service-Auth: <signEnvelope output>   (when signOpts provided)
//   Body: { args: [...] } where args is the positional argument list of
//   the source method. Worker decodes positionally.
//
// Response shape (worker side, spike convention):
//   200 → { result: <JSON-serialised return value> }
//   4xx / 5xx → { error: { code?, message } }
// On non-200, the bridge throws BridgeRpcError carrying status + body.

import { signEnvelope } from '../lib/service-auth';
import type {
  PipelineBridge,
  PipelineBridgeMetrics,
  PipelineRunSnapshot,
  PendingApprovalRow,
  BusEvent,
  ContextPatch,
  PatchResult,
} from './bridge-types';
import type {
  ApplicationModuleDashboardData,
  QueueInspector,
  DeadLetterQueue,
  BusEvent as DCBusEvent,
} from 'distributed-core';

/**
 * Error thrown when a remote pipeline-worker call fails (non-2xx response,
 * network failure, timeout/abort). Carries enough info that routes can map
 * to an HTTP status without re-parsing the wire body.
 */
export class BridgeRpcError extends Error {
  public readonly status: number;
  public readonly method: string;
  public readonly cause?: unknown;
  public readonly body?: unknown;

  constructor(
    method: string,
    status: number,
    message: string,
    opts: { cause?: unknown; body?: unknown } = {},
  ) {
    super(`HTTPPipelineBridge.${method} failed (${status}): ${message}`);
    this.name = 'BridgeRpcError';
    this.status = status;
    this.method = method;
    if (opts.cause !== undefined) this.cause = opts.cause;
    if (opts.body !== undefined) this.body = opts.body;
  }
}

export interface HTTPPipelineBridgeOptions {
  /** Base URL of the remote pipeline-worker, e.g. `http://pipeline-worker:8088`. */
  workerBaseUrl: string;
  /** Override the global `fetch` (handy for tests / undici injection). */
  fetch?: typeof fetch;
  /** Per-request timeout (default 5000ms). */
  timeoutMs?: number;
  /**
   * Optional HMAC signing. When provided, every request carries an
   * `X-Service-Auth` header signed over the canonicalised body.
   * The worker side verifies via lib/service-auth#verifyEnvelope.
   */
  signOpts?: {
    secret: string;
    serviceId: string;
  };
}

/**
 * HTTPPipelineBridge — implements every method of `PipelineBridge` by
 * forwarding to a remote pipeline-worker over plain HTTP/JSON.
 *
 * Construction is cheap: no I/O happens until a method is called. The class
 * carries no per-instance state beyond config, so a single instance can be
 * shared across the platform-api process.
 */
export class HTTPPipelineBridge implements PipelineBridge {
  private readonly workerBaseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMs: number;
  private readonly signOpts: HTTPPipelineBridgeOptions['signOpts'];
  // Inspector / DLQ proxies are stateful handles; we cache so callers that
  // hold a reference observe the same object. Lazily constructed.
  private inspectorProxy: QueueInspector<PipelineRunSnapshot> | null = null;

  constructor(opts: HTTPPipelineBridgeOptions) {
    if (!opts || typeof opts.workerBaseUrl !== 'string' || !opts.workerBaseUrl) {
      throw new Error('HTTPPipelineBridge: workerBaseUrl is required');
    }
    // Strip trailing slash so URL joins stay predictable.
    this.workerBaseUrl = opts.workerBaseUrl.replace(/\/+$/, '');
    this.fetchImpl = opts.fetch ?? globalThis.fetch;
    if (typeof this.fetchImpl !== 'function') {
      throw new Error(
        'HTTPPipelineBridge: no fetch implementation available (Node 18+ or pass opts.fetch)',
      );
    }
    this.timeoutMs = opts.timeoutMs ?? 5000;
    this.signOpts = opts.signOpts;
  }

  // -------------------------------------------------------------------
  // Core RPC plumbing
  // -------------------------------------------------------------------

  /**
   * POSTs `{ args }` to `/internal/pipeline/<methodName>` and returns the
   * `result` field of the worker's JSON response. Non-2xx, network errors,
   * and timeouts are all surfaced as `BridgeRpcError`.
   *
   * Generic over R only — we don't validate at runtime per the spike spec;
   * caller-cast is acceptable for proof-of-interface work.
   */
  private async rpc<R>(method: string, args: unknown[]): Promise<R> {
    const url = `${this.workerBaseUrl}/internal/pipeline/${method}`;
    const body = JSON.stringify({ args });
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };
    if (this.signOpts) {
      headers['X-Service-Auth'] = signEnvelope(body, this.signOpts);
    }

    let response: Response;
    try {
      response = await this.fetchImpl(url, {
        method: 'POST',
        headers,
        body,
        signal: AbortSignal.timeout(this.timeoutMs),
      });
    } catch (err) {
      // AbortError (from AbortSignal.timeout) and network errors both land
      // here. Preserve the original error name so callers can distinguish
      // timeout from connection refused.
      const cause = err as Error;
      const name = cause?.name ?? 'Error';
      const msg = cause?.message ?? String(err);
      throw new BridgeRpcError(method, 0, `${name}: ${msg}`, { cause });
    }

    if (!response.ok) {
      let parsed: unknown;
      try {
        parsed = await response.json();
      } catch {
        parsed = undefined;
      }
      const msg =
        parsed && typeof parsed === 'object' && 'error' in parsed
          ? (((parsed as { error?: { message?: string } }).error?.message) ?? response.statusText)
          : response.statusText;
      throw new BridgeRpcError(method, response.status, msg, { body: parsed });
    }

    let parsed: unknown;
    try {
      parsed = await response.json();
    } catch (err) {
      throw new BridgeRpcError(method, response.status, 'invalid JSON in response', {
        cause: err,
      });
    }
    // The wire format wraps the return value in `{ result }`. `undefined`
    // returns (void methods) round-trip as missing-key, which `as R` casts
    // to `undefined` — fine for the spike.
    return (parsed as { result?: R }).result as R;
  }

  // -------------------------------------------------------------------
  // PipelineBridge surface
  // -------------------------------------------------------------------

  async trigger(args: {
    pipelineId: string;
    definition?: unknown;
    triggerPayload?: unknown;
    triggeredBy: { userId: string };
    breakpoints?: string[];
  }): Promise<{ runId: string }> {
    return this.rpc<{ runId: string }>('trigger', [args]);
  }

  // Per the interface this is sync `void`. We fire-and-await internally so
  // failures still surface to callers via the returned promise being
  // observed by `void` patterns — matching how createBridge.ts handles it.
  async resolveBreakpoint(
    runId: string,
    stepId: string,
    action: 'continue' | 'step_over' | 'resume_all' = 'continue',
  ): Promise<void> {
    await this.rpc<void>('resolveBreakpoint', [runId, stepId, action]);
  }

  async resumeFromStep(
    runId: string,
    fromNodeId: string,
    options?: { breakpoints?: string[]; allowCompleted?: boolean },
  ): Promise<unknown> {
    return this.rpc<unknown>('resumeFromStep', [runId, fromNodeId, options]);
  }

  async patchPausedContext(
    runId: string,
    stepId: string,
    patch: ContextPatch,
    appliedBy?: string,
  ): Promise<PatchResult> {
    return this.rpc<PatchResult>('patchPausedContext', [runId, stepId, patch, appliedBy]);
  }

  async revertContextPatch(
    runId: string,
    stepId: string,
    patchId: string,
  ): Promise<void> {
    await this.rpc<void>('revertContextPatch', [runId, stepId, patchId]);
  }

  async getRun(runId: string): Promise<PipelineRunSnapshot | null> {
    return this.rpc<PipelineRunSnapshot | null>('getRun', [runId]);
  }

  async getHistory(runId: string, fromVersion: number): Promise<BusEvent[]> {
    return this.rpc<BusEvent[]>('getHistory', [runId, fromVersion]);
  }

  async resolveApproval(
    runId: string,
    stepId: string,
    userId: string,
    decision: 'approve' | 'reject',
    comment?: string,
  ): Promise<void> {
    await this.rpc<void>('resolveApproval', [runId, stepId, userId, decision, comment]);
  }

  async listRuns(
    pipelineId: string,
    opts: { limit: number; cursor?: string },
  ): Promise<{ runs: PipelineRunSnapshot[]; nextCursor?: string }> {
    return this.rpc<{ runs: PipelineRunSnapshot[]; nextCursor?: string }>('listRuns', [
      pipelineId,
      opts,
    ]);
  }

  async listActiveRuns(): Promise<PipelineRunSnapshot[]> {
    return this.rpc<PipelineRunSnapshot[]>('listActiveRuns', []);
  }

  async cancelRun(runId: string, userId: string): Promise<void> {
    await this.rpc<void>('cancelRun', [runId, userId]);
  }

  async getPendingApprovals(): Promise<PendingApprovalRow[]> {
    return this.rpc<PendingApprovalRow[]>('getPendingApprovals', []);
  }

  // PipelineBridge declares this as `number | null` (sync). We expose an
  // async variant here because the value lives on the remote worker; the
  // interface allows sync returns but doesn't forbid async (TS structural
  // typing accepts `Promise<number | null>` for a method typed
  // `(): number | null` only via the union in some bridge methods — this
  // one is strict). Callers of this remote bridge must therefore treat
  // getLastEventAt as awaitable. Documented here so the eventual
  // setPipelineBridge wire-in either bumps the interface or wraps with a
  // cache.
  //
  // We satisfy the interface signature via a cast at the route boundary
  // (createBridge / setPipelineBridge wire-in is the responsibility of a
  // future, non-spike PR).
  async getLastEventAtAsync(): Promise<number | null> {
    return this.rpc<number | null>('getLastEventAt', []);
  }
  // Sync stub to satisfy the interface — returns `null` (caller should
  // prefer getLastEventAtAsync for the real value). Documented above.
  getLastEventAt(): number | null {
    return null;
  }

  async getMetrics(): Promise<PipelineBridgeMetrics> {
    return this.rpc<PipelineBridgeMetrics>('getMetrics', []);
  }

  async getDashboardData(): Promise<ApplicationModuleDashboardData> {
    return this.rpc<ApplicationModuleDashboardData>('getDashboardData', []);
  }

  /**
   * Inspector proxy — see the awkward-methods note at the top of this file.
   * Returns a stateful handle whose methods each translate to a single HTTP
   * call against the worker's inspector surface. Spike implements just
   * enough of the QueueInspector contract to demonstrate the pattern; the
   * full surface is left for the production wire-in.
   */
  getInspector(): QueueInspector<PipelineRunSnapshot> {
    if (this.inspectorProxy) return this.inspectorProxy;
    const rpc = this.rpc.bind(this);
    // Cast through unknown — real impl will need every QueueInspector
    // method; for the spike we just prove the shape is reachable.
    this.inspectorProxy = {
      paginatePending: (opts: unknown) =>
        rpc('inspector.paginatePending', [opts]),
      peekPending: (opts: unknown) => rpc('inspector.peekPending', [opts]),
      listInflight: (opts: unknown) => rpc('inspector.listInflight', [opts]),
      findStuckLeases: (opts: unknown) =>
        rpc('inspector.findStuckLeases', [opts]),
      summarizeQueue: (opts: unknown) =>
        rpc('inspector.summarizeQueue', [opts]),
      aggregateFailures: (opts: unknown) =>
        rpc('inspector.aggregateFailures', [opts]),
    } as unknown as QueueInspector<PipelineRunSnapshot>;
    return this.inspectorProxy;
  }

  /**
   * DLQ proxy — same awkward-shape note as `getInspector`. Returns `null`
   * to mean "no DLQ wired"; once the worker is real, this becomes another
   * proxy handle whose verbs map to per-method HTTP calls. Spike returns
   * `null` so routes that null-check the DLQ (every existing call site)
   * still behave correctly.
   */
  getDLQ(): DeadLetterQueue<DCBusEvent> | null {
    return null;
  }
}
