import { DeleteCommand, DynamoDBDocumentClient, ScanCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { createWorkGraphOutboxDrainer } from '../work-graph-outbox-drainer';

jest.mock('../../lib/work-graph-dynamo', () => ({ WORK_GRAPH_TABLE: 'work-graph-test' }));
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
    expect(send.mock.calls[1][0]).toBeInstanceOf(UpdateCommand);
    expect(publish).toHaveBeenCalledWith('work-graph.projection.changed.v1', {
      organizationId: 'org-1', actorId: 'actor-1', day: '2026-09-19', revision: 1, sourceEventId: 'event-1',
    });
    const removal = send.mock.calls[2][0] as DeleteCommand;
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
});
