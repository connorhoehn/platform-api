/**
 * Shared observability conventions (spike) — Block 0.
 *
 * One-stop interface for `createLogger` + `makeHealthHandler` so every
 * service in the platform (gateway, platform-api, future split-outs)
 * agrees on the same structured-log shape and the same /health JSON.
 *
 * Why pino?
 *   - platform-api already pins pino in deps; gateway also installs it.
 *   - pino is the stricter standard (typed Logger, child-loggers, fast
 *     JSON output). Gateway's plain-JS `structured-logger.js` already
 *     wraps pino under the hood — adopting this API is mostly removing
 *     the bespoke wrapper.
 *
 * Migration sketch for `websocket-gateway/src/utils/structured-logger.js`
 * (3–5 call sites — do NOT migrate in this spike, just enumerated):
 *   1. The module-level `require('./structured-logger')` becomes
 *      `import { createLogger } from '@platform/observability-base'`
 *      (or `require` until ESM lands). Same `service` base field, just
 *      passed via opts now.
 *   2. The custom `.withContext(ctx)` method goes away — call sites use
 *      pino's standard `.child(ctx)` directly (already what withContext
 *      delegated to).
 *   3. `pino-pretty` dev-mode auto-detect stays — same try/catch dance
 *      lives inside `createLogger` here.
 *   4. The exported singleton becomes a factory call once at startup
 *      (`const log = createLogger({ service: 'gateway' })`); the rest
 *      of the gateway can keep importing that singleton from one file.
 *   5. `WSG_SERVICE_NAME` env var stops being read here — caller passes
 *      `service` explicitly; env-name choice moves to call site.
 *
 * Why this health JSON shape?
 *   - `service` + `version` make every probe self-describing — operators
 *     skimming JSON in a terminal know which service replied.
 *   - `status` is a tri-state enum (`healthy` | `degraded` | `unhealthy`)
 *     so we can introduce ok-with-warning later without breaking the
 *     contract. v1 only emits `healthy` and `unhealthy`.
 *   - `checks[]` carries per-dep `ok` + `durationMs` + `error` so a
 *     dashboard can chart latency per check and surface failure reasons
 *     without re-running the probe.
 *   - `timestamp` is ISO-8601 so it round-trips through any log pipeline.
 *
 * NOT in scope (deferred deliberately):
 *   - Metrics: prom-client is already widely used; a shared metrics
 *     convention should follow a separate audit of label cardinality.
 *   - Tracing: needs a broader OTEL decision (collector? exporter?
 *     sampling?). Spike-locally not worth it.
 */

import pino, { Logger, LoggerOptions } from 'pino';

export type { Logger };

export interface CreateLoggerOptions {
  service: string;
  level?: string;
  context?: Record<string, unknown>;
}

/**
 * Build a pino logger with the platform-wide base shape:
 *   { service, ...context } merged into every log line.
 *
 * Level defaults to `LOG_LEVEL` env (then 'info'). In NODE_ENV=development
 * we try-require pino-pretty for human-friendly output; production stays
 * pure JSON.
 */
export function createLogger(opts: CreateLoggerOptions): Logger {
  const level = opts.level ?? process.env.LOG_LEVEL ?? 'info';
  const isDev = process.env.NODE_ENV === 'development';

  const baseFields: Record<string, unknown> = { service: opts.service };
  if (opts.context) {
    for (const [k, v] of Object.entries(opts.context)) {
      baseFields[k] = v;
    }
  }

  const baseOptions: LoggerOptions = {
    level,
    base: baseFields,
    timestamp: pino.stdTimeFunctions.isoTime,
  };

  if (isDev) {
    let prettyTransport: LoggerOptions['transport'];
    try {
      // try-require pino-pretty; if not installed, fall back to JSON.
      require.resolve('pino-pretty');
      prettyTransport = {
        target: 'pino-pretty',
        options: {
          colorize: true,
          translateTime: 'SYS:standard',
          ignore: 'pid,hostname',
        },
      };
    } catch {
      prettyTransport = undefined;
    }
    return prettyTransport
      ? pino({ ...baseOptions, transport: prettyTransport })
      : pino(baseOptions);
  }
  return pino(baseOptions);
}

// --------------------------------------------------------------------
// Health handler
// --------------------------------------------------------------------

export interface HealthCheckResult {
  ok: boolean;
  details?: unknown;
  durationMs?: number;
}

export interface HealthCheck {
  name: string;
  check: () => Promise<HealthCheckResult>;
}

export type HealthStatus = 'healthy' | 'degraded' | 'unhealthy';

export interface HealthCheckReport {
  name: string;
  ok: boolean;
  details?: unknown;
  durationMs: number;
  error?: string;
}

export interface HealthBody {
  service: string;
  version: string;
  status: HealthStatus;
  checks: HealthCheckReport[];
  timestamp: string;
}

export interface MakeHealthHandlerOptions {
  service: string;
  version: string;
  checks: HealthCheck[];
  /** Per-check timeout in ms. Defaults to 5000. */
  timeoutMs?: number;
}

// Minimal duck-typed request/response shapes so we don't drag in
// express types. Both Node's http and express satisfy these.
export interface MinimalRequest {
  url?: string;
  method?: string;
}

export interface MinimalResponse {
  status: (code: number) => MinimalResponse;
  json: (body: unknown) => MinimalResponse | void;
}

const DEFAULT_TIMEOUT_MS = 5000;

/**
 * Run a single HealthCheck with a hard timeout. Always resolves — never
 * throws — so one slow dep can't hang the whole probe.
 */
async function runCheckWithTimeout(
  check: HealthCheck,
  timeoutMs: number,
): Promise<HealthCheckReport> {
  const start = Date.now();
  let timer: NodeJS.Timeout | undefined;
  try {
    const result = await Promise.race<HealthCheckResult>([
      check.check(),
      new Promise<HealthCheckResult>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error(`health check '${check.name}' timed out after ${timeoutMs}ms`)),
          timeoutMs,
        );
      }),
    ]);
    const durationMs = typeof result.durationMs === 'number' ? result.durationMs : Date.now() - start;
    const report: HealthCheckReport = {
      name: check.name,
      ok: result.ok === true,
      durationMs,
    };
    if (result.details !== undefined) {
      report.details = result.details;
    }
    return report;
  } catch (err) {
    return {
      name: check.name,
      ok: false,
      durationMs: Date.now() - start,
      error: err instanceof Error ? err.message : String(err),
    };
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/**
 * Build an Express-compatible (also raw-http-compatible) handler that
 * runs every configured check in parallel and returns a normalized
 * health JSON body. Returns 200 for healthy/degraded, 503 for unhealthy.
 */
export function makeHealthHandler(
  opts: MakeHealthHandlerOptions,
): (req: MinimalRequest, res: MinimalResponse) => Promise<void> {
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;

  return async function healthHandler(_req: MinimalRequest, res: MinimalResponse): Promise<void> {
    const reports = await Promise.all(
      opts.checks.map((c) => runCheckWithTimeout(c, timeoutMs)),
    );

    const anyFailed = reports.some((r) => !r.ok);
    // v1: any failure ⇒ unhealthy. 'degraded' is reserved for future
    // ok-with-warning signal but never emitted today.
    const status: HealthStatus = anyFailed ? 'unhealthy' : 'healthy';
    const httpCode = status === 'unhealthy' ? 503 : 200;

    const body: HealthBody = {
      service: opts.service,
      version: opts.version,
      status,
      checks: reports,
      timestamp: new Date().toISOString(),
    };

    res.status(httpCode).json(body);
  };
}
