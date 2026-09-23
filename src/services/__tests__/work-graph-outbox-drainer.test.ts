import { DeleteCommand, DynamoDBDocumentClient, QueryCommand, ScanCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { createWorkGraphOutboxDrainer } from '../work-graph-outbox-drainer';

jest.mock('../../lib/work-graph-dynamo', () => ({
  WORK_GRAPH_TABLE: 'work-graph-test',
  WORK_GRAPH_OUTBOX_SHARDS: 4,
  workGraphOutboxPartition: (shard: number) => `OUTBOX#${shard}`,
}));
jest.mock('../../lib/redis-client', () => ({ getRedisClient: async () => null }));
jest.mock('../../lib/logger', () => ({
  __esModule: true,
  default: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
}));

const row = {
  PK: 'WG#org-1#actor-1#2026-09-19',
  SK: 'OUTBOX#000000000001#event-1',
  itemKind: 'outbox',
  eventType: 'work-graph.projection.changed.v1',
  organizationId: 'org-1',
  actorId: 'actor-1',
  day: '2026-09-19',
  revision: 1,
  sourceEventId: 'event-1',
  createdAt: '2026-09-19T20:00:00.000Z',
};

describe('work-graph outbox drainer', () => {
  it('advances across empty filtered pages and wraps after the last page', async () => {
    const firstKey = { PK: 'NODE#1', SK: 'META' };
    const secondKey = { PK: 'NODE#2', SK: 'META' };
    const pages = [
      { Items: [], LastEvaluatedKey: firstKey },
      { Items: [], LastEvaluatedKey: secondKey },
      { Items: [row] },
      { Items: [] },
    ];
    const send = jest.fn(async (command: unknown) => command instanceof ScanCommand ? pages.shift()! : {});
    const publish = jest.fn().mockResolvedValue(undefined);
    const drainer = createWorkGraphOutboxDrainer({ client: { send } as unknown as DynamoDBDocumentClient, publish });
    for (const count of [0, 0, 1, 0]) await expect(drainer.drainOnce()).resolves.toBe(count);
    const scans = send.mock.calls.map(([command]) => command).filter((command): command is ScanCommand => command instanceof ScanCommand);
    expect(scans.map((command) => command.input.ExclusiveStartKey)).toEqual([undefined, firstKey, secondKey, undefined]);
    expect(publish).toHaveBeenCalledTimes(1);
  });

  it('retries the same scan page after a transient scan failure', async () => {
    const key = { PK: 'NODE#1', SK: 'META' };
    let scans = 0;
    const send = jest.fn(async (command: unknown) => {
      if (!(command instanceof ScanCommand)) return {};
      if (++scans === 1) return { Items: [], LastEvaluatedKey: key };
      if (scans === 2) throw new Error('DynamoDB unavailable');
      return { Items: [row] };
    });
    const drainer = createWorkGraphOutboxDrainer({ client: { send } as unknown as DynamoDBDocumentClient, publish: jest.fn() });
    await drainer.drainOnce();
    await expect(drainer.drainOnce()).rejects.toThrow('DynamoDB unavailable');
    await expect(drainer.drainOnce()).resolves.toBe(1);
    const scanCalls = send.mock.calls.map(([c]) => c).filter((c): c is ScanCommand => c instanceof ScanCommand);
    expect(scanCalls[2].input.ExclusiveStartKey).toEqual(key);
  });

  it('coalesces overlapping timer and manual drains so the scan cursor cannot race', async () => {
    let finish!: (page: { Items: typeof row[] }) => void;
    const send = jest.fn((command: unknown) => command instanceof ScanCommand
      ? new Promise<{ Items: typeof row[] }>((resolve) => { finish = resolve; })
      : Promise.resolve({}));
    const publish = jest.fn().mockResolvedValue(undefined);
    const drainer = createWorkGraphOutboxDrainer({ client: { send } as unknown as DynamoDBDocumentClient, publish });
    const first = drainer.drainOnce();
    const second = drainer.drainOnce();
    // The shard Queries run first; wait until the legacy Scan is in flight.
    while (!finish) await new Promise((resolve) => setImmediate(resolve));
    finish({ Items: [row] });
    await expect(Promise.all([first, second])).resolves.toEqual([1, 1]);
    expect(send.mock.calls.filter(([command]) => command instanceof ScanCommand)).toHaveLength(1);
    expect(publish).toHaveBeenCalledTimes(1);
  });

  it('claims, publishes, and conditionally removes a projection event', async () => {
    const send = jest.fn(async (command: unknown) => command instanceof ScanCommand ? { Items: [row] } : {});
    const publish = jest.fn().mockResolvedValue(undefined);
    const drainer = createWorkGraphOutboxDrainer({
      client: { send } as unknown as DynamoDBDocumentClient,
      publish,
      owner: 'worker-1',
      now: () => 1_000,
    });
    await expect(drainer.drainOnce()).resolves.toBe(1);
    // After the four (empty) shard Queries: Scan, claim, delete.
    const writes = send.mock.calls.map(([c]) => c).filter((c) => !(c instanceof QueryCommand));
    expect(writes[1]).toBeInstanceOf(UpdateCommand);
    expect(publish).toHaveBeenCalledWith('work-graph.projection.changed.v1', {
      organizationId: 'org-1', actorId: 'actor-1', day: '2026-09-19', revision: 1, sourceEventId: 'event-1',
    });
    const removal = writes[2] as DeleteCommand;
    expect(removal.input.ConditionExpression).toContain('outboxLeaseOwner = :owner');
  });

  it('does not publish a row claimed by another replica', async () => {
    const send = jest.fn(async (command: unknown) => {
      if (command instanceof ScanCommand) return { Items: [row] };
      if (command instanceof UpdateCommand) throw Object.assign(new Error('claimed'), { name: 'ConditionalCheckFailedException' });
      return {};
    });
    const publish = jest.fn();
    const drainer = createWorkGraphOutboxDrainer({ client: { send } as unknown as DynamoDBDocumentClient, publish });
    await expect(drainer.drainOnce()).resolves.toBe(0);
    expect(publish).not.toHaveBeenCalled();
  });

  it('keeps a claimed row when publish fails so lease expiry enables recovery', async () => {
    const send = jest.fn(async (command: unknown) => command instanceof ScanCommand ? { Items: [row] } : {});
    const drainer = createWorkGraphOutboxDrainer({
      client: { send } as unknown as DynamoDBDocumentClient,
      publish: jest.fn().mockRejectedValue(new Error('queue unavailable')),
    });
    await expect(drainer.drainOnce()).resolves.toBe(0);
    expect(send.mock.calls.some(([command]) => command instanceof DeleteCommand)).toBe(false);
  });

  it('signals every gateway after a durable publish, and a failed signal does not fail the drain', async () => {
    const send = jest.fn(async (command: unknown) => command instanceof ScanCommand ? { Items: [row] } : {});
    const publish = jest.fn().mockResolvedValue(undefined);
    const signal = jest.fn().mockRejectedValue(new Error('redis down'));
    const drainer = createWorkGraphOutboxDrainer({ client: { send } as unknown as DynamoDBDocumentClient, publish, signal });
    await expect(drainer.drainOnce()).resolves.toBe(1);
    expect(signal).toHaveBeenCalledWith(expect.objectContaining({ organizationId: 'org-1', actorId: 'actor-1', day: '2026-09-19', revision: 1 }));
    // The row was still deleted: the durable event is what counts.
    expect(send.mock.calls.some(([command]) => command instanceof DeleteCommand)).toBe(true);
  });

  it('sends no signal when the durable publish fails', async () => {
    const send = jest.fn(async (command: unknown) => command instanceof ScanCommand ? { Items: [row] } : {});
    const signal = jest.fn().mockResolvedValue(undefined);
    const drainer = createWorkGraphOutboxDrainer({
      client: { send } as unknown as DynamoDBDocumentClient,
      publish: jest.fn().mockRejectedValue(new Error('mq down')),
      signal,
    });
    await expect(drainer.drainOnce()).resolves.toBe(0);
    expect(signal).not.toHaveBeenCalled();
  });
});

describe('NFR #27 — outbox shards are queried, the table scan retires', () => {
  const shardRow = { ...row, PK: 'OUTBOX#2', SK: 'OUTBOX#2026-09-19T20:00:00.000Z#000000000001#event-1#WG#org-1#actor-1#2026-09-19' };

  it('publishes a row from its shard with one Query per shard, whatever the table size', async () => {
    const send = jest.fn(async (command: unknown) => {
      if (command instanceof QueryCommand) return command.input.ExpressionAttributeValues?.[':pk'] === 'OUTBOX#2' ? { Items: [shardRow] } : { Items: [] };
      if (command instanceof ScanCommand) return { Items: [] };
      return {};
    });
    const publish = jest.fn().mockResolvedValue(undefined);
    const drainer = createWorkGraphOutboxDrainer({ client: { send } as unknown as DynamoDBDocumentClient, publish });
    await expect(drainer.drainOnce()).resolves.toBe(1);
    const queries = send.mock.calls.filter(([c]) => c instanceof QueryCommand);
    expect(queries).toHaveLength(4);
    expect(publish).toHaveBeenCalledWith('work-graph.projection.changed.v1', expect.objectContaining({ sourceEventId: 'event-1' }));
    expect(send.mock.calls.some(([c]) => c instanceof DeleteCommand && c.input.Key?.PK === 'OUTBOX#2')).toBe(true);
  });

  it('stops scanning after one full pass finds no legacy rows', async () => {
    const send = jest.fn(async (command: unknown) => (command instanceof QueryCommand || command instanceof ScanCommand) ? { Items: [] } : {});
    const drainer = createWorkGraphOutboxDrainer({ client: { send } as unknown as DynamoDBDocumentClient, publish: jest.fn() });
    for (let i = 0; i < 3; i++) await drainer.drainOnce();
    expect(send.mock.calls.filter(([c]) => c instanceof ScanCommand)).toHaveLength(1);
    expect(send.mock.calls.filter(([c]) => c instanceof QueryCommand)).toHaveLength(12);
  });
});
