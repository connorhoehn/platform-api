// platform-api/src/lib/__tests__/webhook-replay.test.ts
//
// Unit tests for the shared replay-window guard (hub#1260). Pins the
// helper contract so downstream call sites (LVS, pipeline, ingest
// webhooks) can rely on uniform behavior.

import {
  checkReplayWindow,
  WEBHOOK_TIMESTAMP_HEADER,
  DEFAULT_MAX_SKEW_MS,
} from '../webhook-replay';

describe('checkReplayWindow', () => {
  const FIXED_NOW = 1717_200_000_000; // 2024-06-01T00:00:00Z in ms
  const now = () => FIXED_NOW;

  test('exports the canonical header name', () => {
    expect(WEBHOOK_TIMESTAMP_HEADER).toBe('x-webhook-timestamp');
  });

  test('exports a five-minute default skew', () => {
    expect(DEFAULT_MAX_SKEW_MS).toBe(300_000);
  });

  test('rejects when header is missing', () => {
    expect(checkReplayWindow(undefined, undefined, now)).toEqual({
      ok: false,
      reason: 'missing x-webhook-timestamp header',
    });
  });

  test('rejects when header is empty string', () => {
    expect(checkReplayWindow('', undefined, now)).toEqual({
      ok: false,
      reason: 'missing x-webhook-timestamp header',
    });
  });

  test('rejects an unparseable string', () => {
    expect(checkReplayWindow('not-a-date', undefined, now)).toEqual({
      ok: false,
      reason: 'invalid x-webhook-timestamp',
    });
  });

  test('accepts an ISO-8601 timestamp inside the window', () => {
    const iso = new Date(FIXED_NOW - 60_000).toISOString(); // 1 min ago
    expect(checkReplayWindow(iso, undefined, now)).toEqual({ ok: true });
  });

  test('accepts a unix-ms timestamp inside the window', () => {
    const ts = String(FIXED_NOW - 60_000);
    expect(checkReplayWindow(ts, undefined, now)).toEqual({ ok: true });
  });

  test('accepts a unix-seconds timestamp inside the window', () => {
    // Seconds-precision Stripe-style: < 1e12, upscaled internally.
    const ts = String(Math.floor((FIXED_NOW - 60_000) / 1000));
    expect(checkReplayWindow(ts, undefined, now)).toEqual({ ok: true });
  });

  test('rejects a timestamp older than the skew', () => {
    const iso = new Date(FIXED_NOW - 6 * 60_000).toISOString(); // 6 min ago
    expect(checkReplayWindow(iso, undefined, now)).toEqual({
      ok: false,
      reason: 'x-webhook-timestamp too old',
    });
  });

  test('rejects a timestamp too far in the future', () => {
    const iso = new Date(FIXED_NOW + 6 * 60_000).toISOString();
    expect(checkReplayWindow(iso, undefined, now)).toEqual({
      ok: false,
      reason: 'x-webhook-timestamp too far in future',
    });
  });

  test('accepts exactly at the skew boundary', () => {
    const iso = new Date(FIXED_NOW - DEFAULT_MAX_SKEW_MS).toISOString();
    expect(checkReplayWindow(iso, undefined, now)).toEqual({ ok: true });
  });

  test('takes the first value when header is an array (express variant)', () => {
    const iso = new Date(FIXED_NOW).toISOString();
    expect(checkReplayWindow([iso, 'ignored'], undefined, now)).toEqual({
      ok: true,
    });
  });

  test('honors an explicit maxSkewMs override', () => {
    const iso = new Date(FIXED_NOW - 90_000).toISOString(); // 90s ago
    // With 60s window this is too old.
    expect(checkReplayWindow(iso, 60_000, now)).toEqual({
      ok: false,
      reason: 'x-webhook-timestamp too old',
    });
    // With 300s window the same timestamp is fresh.
    expect(checkReplayWindow(iso, 300_000, now)).toEqual({ ok: true });
  });
});
