/**
 * A room a loop booked has not happened yet, so it has no recording — and the
 * meeting source was decided entirely from recordings. The room was projected,
 * published and accepted, and then withheld from the very person who booked
 * it, which looks from outside exactly like never having projected it.
 */
import { GetCommand, QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';

const mockSend = jest.fn();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: (...args: unknown[]) => mockSend(...args) },
  ddbClient: {},
}));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (base: string) => `test-${base}` }));

import { DynamoWorkActivityReader } from '../sources/activityReader';

interface Cmd { constructor: { name: string }; input: Record<string, unknown> }

const ROOM = '01M33TCS3NJY6BTGT7CM5E9MHW';

function respond(over: { room?: Record<string, unknown> | null; members?: unknown[]; profiles?: Record<string, string> } = {}) {
  mockSend.mockImplementation(async (cmd: Cmd) => {
    const table = String(cmd.input.TableName ?? '');
    if (cmd.constructor.name === ScanCommand.name) {
      if (table.includes('video-call-recordings')) return { Items: [] };
      if (table.includes('hangout-rooms')) return { Items: over.room === null ? [] : [over.room ?? { roomId: ROOM, createdBy: 'dev-frank', name: 'Platform team sync' }] };
      return { Items: [] };
    }
    if (cmd.constructor.name === QueryCommand.name) return { Items: over.members ?? [{ userId: 'dev-nina' }] };
    if (cmd.constructor.name === GetCommand.name) {
      const userId = String((cmd.input.Key as { userId?: string } | undefined)?.userId ?? '');
      const label = (over.profiles ?? { 'dev-frank': 'Frank Castle', 'dev-nina': 'Nina Lopez' })[userId];
      return { Item: label ? { userId, displayName: label } : undefined };
    }
    return {};
  });
}

beforeEach(() => { mockSend.mockReset(); });

describe('the facts a booked room has', () => {
  it('names the people expected on it, from the room and its roster', async () => {
    respond();
    const facts = await new DynamoWorkActivityReader({ send: mockSend } as never).meeting(`meeting:${ROOM}`);
    expect(facts?.participants?.map((person) => person.label).sort()).toEqual(['Frank Castle', 'Nina Lopez']);
  });

  it('claims nothing about when it happens, because the room does not say', async () => {
    // `createdAt` is when the room was MADE. Reporting it as the start would
    // be a plausible wrong answer, which is worse than no line at all.
    respond();
    const facts = await new DynamoWorkActivityReader({ send: mockSend } as never).meeting(`meeting:${ROOM}`);
    expect(facts?.startedAt).toBeUndefined();
    expect(facts?.endedAt).toBeUndefined();
    expect(facts?.recorded).toBe(false);
    expect(facts?.transcriptAvailable).toBe(false);
  });

  it('discloses nothing at all for a room that does not exist', async () => {
    respond({ room: null });
    expect(await new DynamoWorkActivityReader({ send: mockSend } as never).meeting('meeting:nope')).toBeNull();
  });

  it('leaves out an attendee the directory cannot name', async () => {
    // Naming them by account id would put an identifier on the card.
    respond({ profiles: { 'dev-frank': 'Frank Castle' } });
    const facts = await new DynamoWorkActivityReader({ send: mockSend } as never).meeting(`meeting:${ROOM}`);
    expect(facts?.participants?.map((person) => person.label)).toEqual(['Frank Castle']);
  });
});
