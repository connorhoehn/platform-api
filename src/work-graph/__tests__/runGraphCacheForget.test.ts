// realtime-examples NFR #196: a grant write drops the owner's cached run sets,
// so a run the grant just added is not refused from a pass taken before it.
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { forgetRunGraphsOf, _rememberRunGraphForTest } from '../runtime';

describe('forgetRunGraphsOf (NFR #196)', () => {
  it('drops every viewer and day of that owner, and nothing else', () => {
    _rememberRunGraphForTest('org', 'dev-frank', 'dev-connor', '2026-09-24', 'America/New_York');
    _rememberRunGraphForTest('org', 'dev-alice', 'dev-connor', '2026-09-23', 'UTC');
    _rememberRunGraphForTest('org', 'dev-frank', 'dev-alice', '2026-09-24', 'UTC');
    _rememberRunGraphForTest('other-org', 'dev-frank', 'dev-connor', '2026-09-24', 'UTC');
    expect(forgetRunGraphsOf('org', 'dev-connor')).toBe(2);
    expect(forgetRunGraphsOf('org', 'dev-connor')).toBe(0);
    expect(forgetRunGraphsOf('org', 'dev-alice')).toBe(1);
    expect(forgetRunGraphsOf('other-org', 'dev-connor')).toBe(1);
  });
});
