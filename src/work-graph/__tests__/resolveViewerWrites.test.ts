/**
 * A browser read used to start with seven writes: the identity row and six
 * create-only principal joins, each failing its condition and falling back to
 * a Get once the rows existed. Reads must only read.
 */
import { GetCommand, PutCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';

const mockSend = jest.fn();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: (...args: unknown[]) => mockSend(...args) },
  ddbClient: {},
}));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (base: string) => `test-${base}` }));

import { DynamoWorkGraphDirectory, IDENTITY_REFRESH_MS, PRINCIPALS_VERIFIED_TTL_MS } from '../runtime';

interface Cmd { constructor: { name: string }; input: Record<string, any> }

/** An in-memory table that honours the create-only condition. */
function fakeTable() {
  const rows = new Map<string, Record<string, unknown>>();
  const key = (k: Record<string, unknown>) => `${k.PK}|${k.SK}`;
  const send = jest.fn(async (cmd: Cmd) => {
    const name = cmd.constructor.name;
    if (name === PutCommand.name) {
      const k = key(cmd.input.Item);
      if (String(cmd.input.ConditionExpression ?? '').startsWith('attribute_not_exists') && rows.has(k)) {
        throw Object.assign(new Error('conditional'), { name: 'ConditionalCheckFailedException' });
      }
      rows.set(k, cmd.input.Item);
      return {};
    }
    if (name === GetCommand.name) return { Item: rows.get(key(cmd.input.Key)) };
    if (name === ScanCommand.name) return { Items: [] };
    return {};
  });
  const count = (cmdName: string) => send.mock.calls.filter(([cmd]) => cmd.constructor.name === cmdName).length;
  return { rows, send, count };
}

describe('resolveViewer does not write on every read', () => {
  it('creates missing principals once, then a second resolve issues 0 Puts and 0 principal Gets', async () => {
    const table = fakeTable();
    let now = 1_000_000;
    const directory = new DynamoWorkGraphDirectory({ send: table.send } as never, () => now);

    const first = await directory.resolveViewer('dev-frank', 'dev-org');
    expect(first?.viewerId).toBe('dev-frank');
    expect(table.count(PutCommand.name)).toBe(7); // identity + six joins, first time only

    table.send.mockClear();
    now += 1_000;
    const second = await directory.resolveViewer('dev-frank', 'dev-org');
    expect(second?.viewerId).toBe('dev-frank');
    expect(table.count(PutCommand.name)).toBe(0);
    expect(table.count(GetCommand.name)).toBe(0);
  });

  it('a fresh process with existing rows reads the joins and writes none of them', async () => {
    const table = fakeTable();
    await new DynamoWorkGraphDirectory({ send: table.send } as never).resolveViewer('dev-frank', 'dev-org');
    table.send.mockClear();

    const restarted = new DynamoWorkGraphDirectory({ send: table.send } as never);
    expect(await restarted.resolveViewer('dev-frank', 'dev-org')).not.toBeNull();
    const puts = table.send.mock.calls.filter(([cmd]) => cmd.constructor.name === PutCommand.name);
    expect(puts).toHaveLength(1); // the identity heartbeat only
    expect(puts[0][0].input.Item.PK).toBe('WGIDENTITY#dev-org');
    expect(table.count(GetCommand.name)).toBe(6);
  });

  it('refreshes lastSeenAt at most once per refresh window and re-verifies after the TTL', async () => {
    const table = fakeTable();
    let now = 5_000_000;
    const directory = new DynamoWorkGraphDirectory({ send: table.send } as never, () => now);
    await directory.resolveViewer('dev-frank', 'dev-org');
    table.send.mockClear();

    now += IDENTITY_REFRESH_MS + 1;
    await directory.resolveViewer('dev-frank', 'dev-org');
    expect(table.count(PutCommand.name)).toBe(1);
    expect(table.count(GetCommand.name)).toBe(0);

    table.send.mockClear();
    now += PRINCIPALS_VERIFIED_TTL_MS;
    await directory.resolveViewer('dev-frank', 'dev-org');
    expect(table.count(GetCommand.name)).toBe(6);
    expect(table.count(PutCommand.name)).toBe(1);
  });

  it('still refuses a viewer whose join points at someone else, and does not cache the refusal as good', async () => {
    const table = fakeTable();
    table.rows.set('WGACCOUNT#document#dev-org|SUBJECT#dev-frank', {
      PK: 'WGACCOUNT#document#dev-org',
      SK: 'SUBJECT#dev-frank',
      entityType: 'work-source-account',
      mapping: {
        source: 'document', producerServiceId: 'websocket-gateway', producerOrganizationId: 'dev-org',
        sourceSubject: 'dev-frank', organizationId: 'dev-org', actorId: 'dev-mallory', revision: 1,
        updatedAt: '2026-09-22T00:00:00.000Z',
      },
    });
    const directory = new DynamoWorkGraphDirectory({ send: table.send } as never);
    expect(await directory.resolveViewer('dev-frank', 'dev-org')).toBeNull();
    table.send.mockClear();
    expect(await directory.resolveViewer('dev-frank', 'dev-org')).toBeNull();
    expect(table.count(GetCommand.name)).toBe(6);
  });
});
