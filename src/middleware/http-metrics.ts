import type { Request, Response, NextFunction } from 'express';
import { recordHttpHandlerDuration } from '../observability/metrics';

/**
 * HTTP handler duration middleware (hub#1005).
 *
 * Observes every request's wall-clock duration into the
 * `http_handler_duration_seconds` histogram, labeled by a bounded
 * `route_group` enum so cardinality stays ≤ ~10 groups regardless of
 * how many routes the service grows.
 *
 * Separate from request-logger.ts on purpose:
 *   - request-logger emits a per-request log line (pino).
 *   - http-metrics emits a numeric observation into the registry.
 * They use the same finish hook but serve different consumers (pino
 * vs Prometheus / Grafana), and keeping them split means a logger
 * config change can't accidentally drop metric coverage.
 */
export function httpMetrics(req: Request, res: Response, next: NextFunction): void {
  const startMs = Date.now();
  res.on('finish', () => {
    const durationSec = (Date.now() - startMs) / 1000;
    recordHttpHandlerDuration(deriveRouteGroup(req.originalUrl || req.url || ''), durationSec);
  });
  next();
}

/**
 * Map a URL path to a bounded route_group label. The enum is the
 * authoritative cardinality cap — extending it is a deliberate edit
 * here, NOT a per-route opt-in. Anything unmatched falls into `other`.
 *
 * Exported for unit-testing the mapping directly.
 */
export function deriveRouteGroup(url: string): string {
  const path = url.split('?')[0] ?? url;
  if (path.startsWith('/health') || path === '/api/health') return 'health';
  if (path.startsWith('/internal')) return 'internal';
  if (path.startsWith('/hooks/')) return 'hooks';
  if (path.startsWith('/api/admin/')) return 'api_admin';
  if (path.startsWith('/api/pipelines')) return 'api_pipelines';
  if (path.startsWith('/api/video/')) return 'api_video';
  if (path.startsWith('/api/recordings')) return 'api_recordings';
  if (path.startsWith('/api/rooms')) return 'api_rooms';
  if (path.startsWith('/api/document-types')) return 'api_document_types';
  if (path.startsWith('/api/typed-documents')) return 'api_typed_documents';
  if (path.startsWith('/api/capabilities')) return 'api_capabilities';
  if (path.startsWith('/api/')) return 'api_other';
  return 'other';
}
