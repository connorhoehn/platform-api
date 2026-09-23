import { describe, it, expect } from '@jest/globals';
import { hashViewBase, publishViewBase } from '../viewBase';

describe('view base hash (NFR #85)', () => {
  it('matches the gateway copy byte for byte (same vector in realtime-examples test/work-graph/view-base.test.ts)', () => {
    expect(hashViewBase({
      organizationId: 'dev-org', viewerId: 'dev-frank', personId: 'dev-connor',
      day: '2026-09-23', timezone: 'America/New_York', watermark: 42,
    }, '{"efforts":[]}')).toBe('D8Nh9fdeLnkp3gCYOGuD_RHZJDVI_yfxvTWRnbxtkMA');
  });

  it('gives up on a store that does not answer in time instead of holding the snapshot', async () => {
    const started = Date.now();
    const hash = await publishViewBase({ set: () => new Promise(() => undefined) }, {
      organizationId: 'o', viewerId: 'v', personId: 'p', day: '2026-09-23', timezone: 'UTC', watermark: 1,
    }, { query: {}, temporal: {}, efforts: [], details: [], operations: [], eventBuckets: [] } as never);
    expect(hash).toBeNull();
    expect(Date.now() - started).toBeLessThan(1000);
  });
});
