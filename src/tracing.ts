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

import { startTracing } from 'distributed-core';
import log from './lib/logger';

export type TracingShutdown = () => Promise<void>;

export function start(): TracingShutdown {
  const bootstrap = startTracing({
    serviceName: 'platform-api',
    defaultEndpoint: 'http://localhost:4318',
    logger: log,
  });
  return bootstrap.shutdown;
}
