// OpenTelemetry tracing bootstrap.
//
// Must be initialized BEFORE any other application imports so that
// auto-instrumentation can monkey-patch http, express, etc. before they
// are required by the rest of the codebase.
//
// Env:
//   OTEL_TRACING_DISABLED=true      → skip entirely (no-op shutdown)
//   OTEL_SERVICE_NAME               → service.name attribute (default: social-api)
//   OTEL_EXPORTER_OTLP_ENDPOINT     → OTLP HTTP collector base URL
//                                     (default: http://localhost:4318)

import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import log from './lib/logger';

export type TracingShutdown = () => Promise<void>;

const NOOP_SHUTDOWN: TracingShutdown = async () => {
  /* no tracing active */
};

export function start(): TracingShutdown {
  if (process.env.OTEL_TRACING_DISABLED === 'true') {
    return NOOP_SHUTDOWN;
  }

  const serviceName = process.env.OTEL_SERVICE_NAME ?? 'social-api';
  const endpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? 'http://localhost:4318';

  // OTLPTraceExporter expects the traces signal URL. The OTLP HTTP convention
  // is `${base}/v1/traces`; the SDK also picks this up from the env var
  // OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, but we wire it explicitly for clarity.
  const traceExporter = new OTLPTraceExporter({
    url: `${endpoint.replace(/\/$/, '')}/v1/traces`,
  });

  // Service name is conventionally set via env so resource detection picks it up.
  if (!process.env.OTEL_SERVICE_NAME) {
    process.env.OTEL_SERVICE_NAME = serviceName;
  }

  const sdk = new NodeSDK({
    traceExporter,
    instrumentations: [getNodeAutoInstrumentations()],
  });

  sdk.start();
  log.info({ serviceName, endpoint }, 'OpenTelemetry started');

  return async () => {
    try {
      await sdk.shutdown();
    } catch (err) {
      log.error({ err }, 'tracing shutdown failed');
    }
  };
}
