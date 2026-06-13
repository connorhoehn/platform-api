// Regression guard for hub#971 — AbortSignal listener leak in
// @connorhoehn/event-catalog v0.3.48 DurableTransport.sleep().
//
// The unpatched sleep() attaches an `addEventListener('abort', ..., { once: true })`
// per call. `once: true` only auto-removes the listener if abort actually
// fires — on the normal-timeout path (the common case in a poll loop) the
// listener stays attached to the long-lived signal forever. The patch
// removes the listener on the normal-timeout success path.
//
// Two layers of assertion:
// 1. Behavioral: N sleep() calls against the same long-lived signal must
//    leave the abort-listener count bounded — this is the load-bearing
//    soak-test guard (LIVE-AUDIT C2). Uses the EventTarget listener probe
//    (events.getEventListeners, available without flags on modern Node)
//    to count residual 'abort' listeners after the loop.
// 2. Source-level: the patched body must contain a removeEventListener
//    on abort — protects against pin slip back to an unpatched build.

import { getEventListeners } from 'node:events';
import { DurableTransport } from '@connorhoehn/event-catalog';

describe('event-catalog DurableTransport AbortSignal listener bound (hub#971)', () => {
  let transport: { sleep(ms: number, signal: AbortSignal): Promise<void> };

  beforeAll(() => {
    transport = new DurableTransport({ url: 'http://localhost:0' }) as unknown as typeof transport;
  });

  it('sleep() body contains removeEventListener on abort (postinstall patch landed)', () => {
    const src = (transport as unknown as { sleep: { toString(): string } }).sleep.toString();
    expect(src).toMatch(/removeEventListener\(\s*['"]abort['"]/);
  });

  it('N sleep() calls against the same signal keep abort-listener count bounded', async () => {
    const controller = new AbortController();
    // Pre-set a low max so leaked listeners would emit MaxListenersExceeded.
    // Cast through unknown — Node's AbortSignal extends EventTarget at
    // runtime but TS DOM types don't surface setMaxListeners.
    (controller.signal as unknown as { setMaxListeners?: (n: number) => void })
      .setMaxListeners?.(5);

    // The LIVE-AUDIT C2 soak symptom was 1857+ accumulated abort listeners
    // at ~1/sec poll cadence. Run an iteration count well past the default
    // EventEmitter cap (10) so an unpatched build would both blow the cap
    // and leave a listener count == iterations.
    const ITERATIONS = 200;
    for (let i = 0; i < ITERATIONS; i++) {
      await transport.sleep(0, controller.signal);
    }

    // Runtime probe: count residual 'abort' listeners on the long-lived
    // signal. A correct (patched/upstream-fixed) sleep() removes its
    // listener on the normal-timeout success path, so this must stay near
    // zero regardless of how many iterations ran. The unpatched build
    // would report ~ITERATIONS here.
    const residual = getEventListeners(
      controller.signal as unknown as NodeJS.EventEmitter,
      'abort',
    ).length;

    // Bounded: allow a tiny slack for any in-flight timer races, but it
    // must NOT scale with iteration count.
    expect(residual).toBeLessThanOrEqual(2);
  });
});
