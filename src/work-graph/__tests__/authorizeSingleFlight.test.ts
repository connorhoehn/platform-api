/**
 * NFR #94: every open Active Now stream is re-checked each second and each
 * check loads and authorizes the whole day. Identical checks that overlap
 * share one pass; nothing is kept after it settles.
 */
const mockSend = jest.fn();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: (...args: unknown[]) => mockSend(...args) },
  ddbClient: {},
}));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (base: string) => `test-${base}` }));

import { authorizeGatewayWorkGraph } from '../runtime';

const query = { personId: 'dev-connor', day: '2026-09-23', timezone: 'America/New_York' };

describe('authorizeGatewayWorkGraph single flight', () => {
  beforeEach(() => {
    mockSend.mockReset();
    // No identity row: the pass ends in a denial after its reads.
    mockSend.mockImplementation(async () => ({ Items: [] }));
  });

  it('overlapping identical checks share one pass', async () => {
    const [a, b] = await Promise.all([
      authorizeGatewayWorkGraph('dev-org', 'dev-frank', query),
      authorizeGatewayWorkGraph('dev-org', 'dev-frank', query),
    ]);
    expect(a).toBe(b);
    const shared = mockSend.mock.calls.length;
    expect(shared).toBeGreaterThan(0);

    mockSend.mockClear();
    await authorizeGatewayWorkGraph('dev-org', 'dev-frank', query);
    expect(mockSend.mock.calls.length).toBe(shared); // settled: the next check reads again
  });

  it('different viewers or days are separate passes', async () => {
    const [a, b] = await Promise.all([
      authorizeGatewayWorkGraph('dev-org', 'dev-frank', query),
      authorizeGatewayWorkGraph('dev-org', 'dev-connor', query),
    ]);
    expect(a).not.toBe(b);
  });
});
