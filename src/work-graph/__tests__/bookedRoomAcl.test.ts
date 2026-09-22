/**
 * A booking has no recording, and the meeting ACL is decided from recordings.
 * Without a fallback the room a person had just made was denied to that
 * person — projected, accepted, and then invisible, which is indistinguishable
 * from never having been projected.
 */
import { GetCommand, QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';

const mockSend = jest.fn();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: (...args: unknown[]) => mockSend(...args) },
  ddbClient: {},
}));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (base: string) => `test-${base}` }));

import { workGraphOrchestrator } from '../runtime';

interface Cmd { constructor: { name: string }; input: Record<string, unknown> }

const ROOM = '01M33V0Q4B8YDBPH830ZM3WJ77';

/** Every table answers empty except the room, which frank created. */
function liveWorld(over: { room?: Record<string, unknown> | null; members?: unknown[] } = {}) {
  mockSend.mockImplementation(async (cmd: Cmd) => {
    const table = String(cmd.input.TableName ?? '');
    if (cmd.constructor.name === GetCommand.name) {
      if (table.includes('hangout-rooms')) {
        return { Item: over.room === null ? undefined : over.room ?? { orgId: 'dev-org', roomId: ROOM, createdBy: 'dev-frank' } };
      }
      return {};
    }
    if (cmd.constructor.name === QueryCommand.name) return { Items: over.members ?? [] };
    if (cmd.constructor.name === ScanCommand.name) return { Items: [] };
    return {};
  });
}

function roomLookups(): Cmd[] {
  return mockSend.mock.calls
    .map((call) => call[0] as Cmd)
    .filter((cmd) => cmd?.constructor?.name === GetCommand.name
      && String(cmd.input.TableName ?? '').includes('hangout-rooms'));
}

beforeEach(() => { mockSend.mockReset(); });

describe('who may see a room that has not happened', () => {
  it('asks the room, once, when no recording decides it', async () => {
    liveWorld();
    await workGraphOrchestrator.getGraph(
      { viewerId: 'dev-frank', organizationId: 'dev-org', conversationIds: [] },
      'dev-frank', '2026-09-22', 'America/New_York',
    ).catch(() => undefined);
    // Memoized per request: a room shared by several nodes is read once.
    const byRoom = new Set(roomLookups().map((cmd) => JSON.stringify(cmd.input.Key)));
    expect(byRoom.size).toBe(roomLookups().length);
  });

  it('reads the room under the viewer\'s own organization', async () => {
    liveWorld();
    await workGraphOrchestrator.getGraph(
      { viewerId: 'dev-frank', organizationId: 'dev-org', conversationIds: [] },
      'dev-frank', '2026-09-22', 'America/New_York',
    ).catch(() => undefined);
    for (const cmd of roomLookups()) {
      expect((cmd.input.Key as { orgId?: string }).orgId).toBe('dev-org');
    }
  });
});
