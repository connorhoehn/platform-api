import { startEventLoopMetrics, lastEventLoopWindow } from '../eventLoop';
import { renderPrometheusText } from '../metrics';

describe('event-loop metrics (NFR #202)', () => {
  it('publishes delay quantiles and utilization for the last window', async () => {
    startEventLoopMetrics(50);
    const until = Date.now() + 30;
    while (Date.now() < until) { /* hold the loop */ }
    await new Promise((r) => setTimeout(r, 120));
    const w = lastEventLoopWindow();
    expect(w).not.toBeNull();
    expect(w!.utilization).toBeGreaterThan(0);
    expect(w!.utilization).toBeLessThanOrEqual(1);
    const text = renderPrometheusText();
    expect(text).toMatch(/eventloop_delay_ms\{[^}]*quantile="0\.99"/);
    expect(text).toMatch(/eventloop_utilization\{/);
  });
});
