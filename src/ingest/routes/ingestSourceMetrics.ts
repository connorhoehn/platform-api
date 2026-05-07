// social-api/src/ingest/routes/ingestSourceMetrics.ts
//
// Per-source SLO panel data — AGENTIC_ETL_VISION §F. Returns three time
// series operators read to spot a connector dragging vs. healthy state:
//   - p95 ingest latency (ms)
//   - error rate (%, 0..100)
//   - queue depth (count of non-terminal items)
//
// Endpoint: GET /api/ingest/sources/:id/metrics?window=1h
//
// Implementation note: today this is derived from the IngestItem repo
// (no metric history table), bucketing items by `updatedAt` into 5-min
// windows over the requested wall-clock window. Operators reading the
// Grafana dashboards in `.planning/grafana-ingest-dashboard.json` get the
// same signals; this endpoint exposes them inside the operator UI for
// non-Grafana users.

import { Router, type Request, type Response } from 'express';
import { asyncHandler } from '../../middleware/error-handler';
import { withContext } from '../../lib/logger';
import { ingestItemRepo } from '../repositories';
import type { IngestItem } from '../types';

const log = withContext({ route: 'ingestSourceMetrics' });

export const ingestSourceMetricsRouter = Router({ mergeParams: true });

/** Bucket size for the time series — 5 minutes. */
const BUCKET_MS = 5 * 60 * 1000;

/** Map of operator-friendly window identifiers → ms. */
const WINDOWS: Record<string, number> = {
  '15m': 15 * 60 * 1000,
  '1h': 60 * 60 * 1000,
  '6h': 6 * 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000,
};

export interface TimeSeriesPoint {
  /** ISO timestamp at the start of the bucket. */
  at: string;
  /** Numeric value for that bucket. Null when no samples landed in the window. */
  value: number | null;
}

export interface SourceMetricsResponse {
  sourceId: string;
  window: string;
  bucketSizeMs: number;
  latencyP95: TimeSeriesPoint[];
  errorRate: TimeSeriesPoint[];
  queueDepth: TimeSeriesPoint[];
}

function p95(samples: number[]): number {
  if (samples.length === 0) return 0;
  const sorted = [...samples].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95));
  return sorted[idx];
}

ingestSourceMetricsRouter.get(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const sourceId = req.params.id;
    const windowKey = typeof req.query.window === 'string' && req.query.window in WINDOWS
      ? req.query.window
      : '1h';
    const windowMs = WINDOWS[windowKey];
    const now = Date.now();
    const start = now - windowMs;

    const items = await ingestItemRepo.listBySource(sourceId);
    const inWindow = items.filter((i) => new Date(i.updatedAt).getTime() >= start);

    // Bucket items by 5-min windows starting at `start`.
    const bucketCount = Math.ceil(windowMs / BUCKET_MS);
    const latencyBuckets: number[][] = Array.from({ length: bucketCount }, () => []);
    const errorBuckets: { failed: number; total: number }[] = Array.from(
      { length: bucketCount },
      () => ({ failed: 0, total: 0 }),
    );
    const queueDepthBuckets: number[] = Array.from({ length: bucketCount }, () => 0);

    for (const item of inWindow) {
      const ts = new Date(item.updatedAt).getTime();
      const bucket = Math.min(bucketCount - 1, Math.floor((ts - start) / BUCKET_MS));
      const created = new Date(item.createdAt).getTime();
      const lat = ts - created;
      if (Number.isFinite(lat) && lat >= 0 && (item.status === 'done' || item.status === 'failed')) {
        latencyBuckets[bucket].push(lat);
      }
      errorBuckets[bucket].total += 1;
      if (item.status === 'failed') errorBuckets[bucket].failed += 1;
      if (item.status !== 'done' && item.status !== 'failed') {
        queueDepthBuckets[bucket] += 1;
      }
    }

    const latencyP95: TimeSeriesPoint[] = latencyBuckets.map((samples, i) => ({
      at: new Date(start + i * BUCKET_MS).toISOString(),
      value: samples.length > 0 ? p95(samples) : null,
    }));
    const errorRate: TimeSeriesPoint[] = errorBuckets.map(({ failed, total }, i) => ({
      at: new Date(start + i * BUCKET_MS).toISOString(),
      value: total > 0 ? Math.round((failed / total) * 1000) / 10 : null,
    }));
    const queueDepth: TimeSeriesPoint[] = queueDepthBuckets.map((count, i) => ({
      at: new Date(start + i * BUCKET_MS).toISOString(),
      value: count,
    }));

    log.info(
      { sourceId, window: windowKey, samples: inWindow.length, action: 'source.metrics' },
      'served source metrics',
    );
    res.json({ sourceId, window: windowKey, bucketSizeMs: BUCKET_MS, latencyP95, errorRate, queueDepth } as SourceMetricsResponse);
  }),
);
