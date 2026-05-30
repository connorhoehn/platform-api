// Unit tests for the shared SFU CircuitBreaker.
//
// The point of extracting `callSfu` into its own module (hub#1001) was so
// that every LVS-touching route shares the SAME breaker — one route's
// repeated failures should fast-fail subsequent callers on every route.
// This test exercises that shared-state property.

const mockFetch = jest.fn();
(globalThis as unknown as { fetch: typeof mockFetch }).fetch = mockFetch;

import {
  callSfu,
  BreakerOpenError,
  __resetSfuBreakerForTests,
  __configureSfuBreakerForTests,
} from '../sfu-breaker';

beforeEach(() => {
  mockFetch.mockReset();
  __resetSfuBreakerForTests();
  // Open after 2 consecutive 5xx failures so we don't need 5 iterations
  // to trip the breaker in a unit test.
  __configureSfuBreakerForTests({
    failureThreshold: 2,
    successThreshold: 1,
    resetTimeout: 60_000,
    halfOpenMaxCalls: 1,
  });
});

afterEach(() => {
  __resetSfuBreakerForTests();
});

it('one caller tripping the breaker fast-fails subsequent callers (shared state)', async () => {
  // Stub fetch to always return an upstream 5xx — counted as a breaker
  // failure (see callSfu's `if (res.status >= 500) throw`).
  mockFetch.mockResolvedValue({ ok: false, status: 503 } as unknown as Response);

  // First caller hits the upstream and trips the breaker after 2 failures.
  // Both rejections surface the upstream-5xx shape (UpstreamServerError),
  // NOT BreakerOpenError — that's the documented contract: the failure
  // that *trips* the breaker still surfaces its real shape to that
  // caller; only *subsequent* callers see fast-fail.
  await expect(callSfu('https://video.test/api/channels/x')).rejects.toMatchObject({
    name: 'UpstreamServerError',
  });
  await expect(callSfu('https://video.test/api/channels/x')).rejects.toMatchObject({
    name: 'UpstreamServerError',
  });

  // Breaker is now OPEN. A *different* caller (e.g. recordings.ts hitting
  // /playback-tokens vs videoSessions.ts hitting /channels) gets the
  // BreakerOpenError fast-fail immediately — without fetch being called.
  const beforeCallCount = mockFetch.mock.calls.length;
  await expect(
    callSfu('https://video.test/api/channels/y/playback-tokens', { method: 'POST' }),
  ).rejects.toBeInstanceOf(BreakerOpenError);
  expect(mockFetch.mock.calls.length).toBe(beforeCallCount); // no new fetch
});
