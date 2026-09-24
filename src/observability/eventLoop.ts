// realtime-examples NFR #202: event-loop delay and utilization on
// `GET /internal/metrics`, so a loop can watch the platform's loop under run
// load without attaching a profiler.
//
// A `monitorEventLoopDelay` histogram and `performance.eventLoopUtilization()`
// are sampled over fixed windows (default 10 s, unref'd timer). At the end of
// each window the gauges below are set from that window alone and the
// histogram is reset, so a scrape always reads the most recent full window —
// not a lifetime average that hides a busy minute.
//
//   eventloop_delay_ms{quantile="0.5"|"0.95"|"0.99"|"max"}
//   eventloop_utilization              (0..1, fraction of the window the loop ran JS)
//   eventloop_window_seconds           (the window length the two above cover)
//
// Label cardinality is fixed (four quantiles), the same bounded-label rule as
// the route_group histogram (NFR #8). Pre-registered so a scrape sees zeros
// before the first window closes.

import { monitorEventLoopDelay, performance, type EventLoopUtilization } from 'perf_hooks';
import { getRegistry } from './metrics';
import os from 'os';

const BASE_LABELS = {
  service: process.env.WSG_SERVICE_NAME || 'platform-api',
  node_id: process.env.WSG_NODE_ID || os.hostname(),
};
const QUANTILES = [['0.5', 50], ['0.95', 95], ['0.99', 99]] as const;

let started = false;

export interface EventLoopWindow {
  windowSeconds: number;
  utilization: number;
  delayMs: { p50: number; p95: number; p99: number; max: number };
}

let last: EventLoopWindow | null = null;

/** The most recent closed window (null before the first one closes). */
export function lastEventLoopWindow(): EventLoopWindow | null {
  return last;
}

export function startEventLoopMetrics(windowMs = Number(process.env.EVENT_LOOP_METRICS_WINDOW_MS) || 10_000): void {
  if (started) return;
  started = true;
  const registry = getRegistry();
  const gauge = (q: string) => registry.gauge('eventloop_delay_ms', { ...BASE_LABELS, quantile: q }, 'Event-loop delay over the last window, in ms (monitorEventLoopDelay, 10 ms resolution).');
  const delay = {
    p50: gauge('0.5'), p95: gauge('0.95'), p99: gauge('0.99'), max: gauge('max'),
  };
  const utilization = registry.gauge('eventloop_utilization', BASE_LABELS, 'Event-loop utilization over the last window (0..1): the fraction of time the loop was running JS rather than waiting.');
  const windowGauge = registry.gauge('eventloop_window_seconds', BASE_LABELS, 'Length of the window eventloop_delay_ms and eventloop_utilization cover.');
  windowGauge.set(windowMs / 1000);

  const histogram = monitorEventLoopDelay({ resolution: 10 });
  histogram.enable();
  let eluMark: EventLoopUtilization = performance.eventLoopUtilization();
  const ms = (ns: number) => Math.round(ns / 1e5) / 10;

  const timer = setInterval(() => {
    const elu = performance.eventLoopUtilization(eluMark);
    eluMark = performance.eventLoopUtilization();
    const window: EventLoopWindow = {
      windowSeconds: windowMs / 1000,
      utilization: Math.round(elu.utilization * 1000) / 1000,
      delayMs: {
        p50: ms(histogram.percentile(QUANTILES[0][1])),
        p95: ms(histogram.percentile(QUANTILES[1][1])),
        p99: ms(histogram.percentile(QUANTILES[2][1])),
        max: ms(histogram.max),
      },
    };
    histogram.reset();
    last = window;
    delay.p50.set(window.delayMs.p50);
    delay.p95.set(window.delayMs.p95);
    delay.p99.set(window.delayMs.p99);
    delay.max.set(window.delayMs.max);
    utilization.set(window.utilization);
  }, windowMs);
  timer.unref();
}
