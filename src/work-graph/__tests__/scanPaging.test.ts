/**
 * A Scan's `Limit` counts rows EXAMINED, not rows the filter keeps. Sizing it
 * by how many MATCHES a caller wants makes an authorization that wants at most
 * two rows walk the whole table two at a time — correct, and slow enough to
 * turn into withheld nodes the moment a request runs out of time.
 */
import { ScanCommand } from '@aws-sdk/lib-dynamodb';

const mockSend = jest.fn();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: (...args: unknown[]) => mockSend(...args) },
  ddbClient: {},
}));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (base: string) => `test-${base}` }));

import { workGraphOrchestrator } from '../runtime';

interface Cmd { constructor: { name: string }; input: Record<string, unknown> }

function scans(): Cmd[] {
  return mockSend.mock.calls
    .map((call) => call[0] as Cmd)
    .filter((cmd) => cmd?.constructor?.name === ScanCommand.name);
}

beforeEach(() => { mockSend.mockReset(); });

describe('authorization scans', () => {
  it('examines a full page per round trip, not one per match wanted', async () => {
    // The whole table is terminal rows that do not match; the ACL wants one.
    let page = 0;
    mockSend.mockImplementation(async (cmd: Cmd) => {
      if (cmd.constructor.name !== ScanCommand.name) return {};
      page += 1;
      return page < 3 ? { Items: [], LastEvaluatedKey: { id: `${page}` } } : { Items: [] };
    });
    // Reach the scan through the orchestrator's own directory lookup.
    await workGraphOrchestrator.listPeople({ viewerId: 'dev-connor', organizationId: 'dev-org', conversationIds: [] })
      .catch(() => undefined);
    for (const scan of scans()) {
      expect(scan.input.Limit).toBeGreaterThanOrEqual(100);
    }
  });

  it('stops walking rather than following an unbounded table', async () => {
    mockSend.mockImplementation(async (cmd: Cmd) => (
      cmd.constructor.name === ScanCommand.name
        ? { Items: [], LastEvaluatedKey: { id: 'always-more' } }
        : {}
    ));
    await workGraphOrchestrator.listPeople({ viewerId: 'dev-connor', organizationId: 'dev-org', conversationIds: [] })
      .catch(() => undefined);
    // Bounded: a hit past the cap is not found, rather than scanned for ever.
    expect(scans().length).toBeLessThanOrEqual(25);
    expect(scans().length).toBeGreaterThan(1);
  });
});
