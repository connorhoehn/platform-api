// OpenTelemetry tracing bootstrap.
//
// Thin wrapper around `startTracing` from distributed-core (v0.22+) which
// consolidates the NodeSDK + OTLPTraceExporter + auto-instrumentations boot
// previously duplicated across gateway, platform-api, message-queue, and MNS.
//
// Must be initialized BEFORE any other application imports so that
// auto-instrumentation can monkey-patch http, express, etc. before they
// are require()'d by the rest of the codebase.
//
// Env (unchanged contract):
//   OTEL_TRACING_DISABLED=true      → skip entirely (no-op shutdown)
//   OTEL_SERVICE_NAME               → service.name attribute (default: platform-api)
//   OTEL_EXPORTER_OTLP_ENDPOINT     → OTLP HTTP collector base URL
//                                     (default: http://localhost:4318)

import { inspect } from 'node:util';
import { startTracing } from 'distributed-core';
import log from './lib/logger';

export type TracingShutdown = () => Promise<void>;

/**
 * How long the tracing flush gets before shutdown gives up on it.
 *
 * Observed: ~8.3s on every restart in local dev, because the OTLP exporter
 * retries against `localhost:4318` where no collector is listening. Nothing
 * downstream benefits from that wait — the spans have nowhere to go — and it
 * is added to every single restart.
 */
const TRACING_SHUTDOWN_TIMEOUT_MS = 2000;

/**
 * Bound the tracing shutdown and report what actually went wrong.
 *
 * Two separate defects, both upstream in distributed-core
 * (`observability/tracing-bootstrap.js:193`) and therefore mitigated here at
 * the boundary rather than fixed at the source:
 *
 *  1. It can hang for seconds flushing to a collector that is not there. This
 *     wrapper stops waiting and lets the process exit.
 *  2. It logs `[tracing] shutdown failed: ${err?.message ?? String(err)}`,
 *     which renders as an EMPTY string for a rejection that is not an Error
 *     and for an Error with no message — so the one line telling you tracing
 *     failed tells you nothing about why. This logs the constructor name and
 *     the inspected value, which are what actually identify it.
 *
 * Never rethrows: a failure to flush traces must not turn a clean shutdown
 * into a dirty one.
 */
function boundShutdown(shutdown: TracingShutdown): TracingShutdown {
  return async () => {
    let timer: NodeJS.Timeout | undefined;
    try {
      const timeout = new Promise<'timeout'>((resolve) => {
        timer = setTimeout(() => resolve('timeout'), TRACING_SHUTDOWN_TIMEOUT_MS);
      });
      const result = await Promise.race([shutdown().then(() => 'done' as const), timeout]);
      if (result === 'timeout') {
        log.warn(
          { timeoutMs: TRACING_SHUTDOWN_TIMEOUT_MS },
          '[tracing] shutdown did not finish in time; abandoning the span flush ' +
            'and continuing. Unexported spans are lost, which is the right trade ' +
            'against holding up every restart.',
        );
      }
    } catch (err) {
      log.error(
        {
          err: err instanceof Error ? err.message : String(err),
          errType: err?.constructor?.name ?? typeof err,
          // The upstream log collapses to '' for exactly these cases; keeping
          // the inspected value is what makes the line worth reading.
          errInspect: inspect(err, { depth: 2 }),
        },
        '[tracing] shutdown failed',
      );
    } finally {
      if (timer) clearTimeout(timer);
    }
  };
}

export function start(): TracingShutdown {
  const bootstrap = startTracing({
    serviceName: 'platform-api',
    defaultEndpoint: 'http://localhost:4318',
    logger: log,
  });
  return boundShutdown(bootstrap.shutdown);
}
