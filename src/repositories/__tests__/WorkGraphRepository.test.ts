import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { GetCommand, QueryCommand, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import {
  WorkGraphConcurrencyError,
  WorkGraphDataIntegrityError,
  WorkGraphRepository,
  type AuthenticatedWorkEvent,
  type WorkGraphReducer,
  type WorkProjectionState,
} from '../WorkGraphRepository';

const event: AuthenticatedWorkEvent = {
  schemaVersion: 1,
  eventId: 'evt-1',
  idempotencyKey: 'idem-1',
  source: 'pipeline',
  sourceSequence: '42',
  occurredAt: '2026-09-19T23:30:00.000-04:00',
  receivedAt: '2026-09-20T03:30:01.000Z',
  actor: { actorId: 'actor-1', organizationId: 'org-1', sourceSubject: 'subject-1' },
  producer: { serviceId: 'platform-api', organizationId: 'org-1', credentialId: 'cred-1' },
  resource: { source: 'pipeline', resourceId: 'run-1' },
  payload: { kind: 'pipeline', lifecycle: 'started', pipelineId: 'pipeline-1', runId: 'run-1', attempt: 1 },
};

const emptyState = (revision = 0): WorkProjectionState => ({
  schemaVersion: 1,
  organizationId: 'org-1',
  actorId: 'actor-1',
  revision,
  sessions: {},
  nodes: {},
  edges: {},
  sourceCheckpoints: {},
  appliedEventIds: [],
});

const reducer: WorkGraphReducer = (current, incoming) => ({
  ...current,
  revision: current.revision + 1,
  nodes: {
    ...current.nodes,
    'node-1': {
      id: 'node-1',
      organizationId: incoming.actor.organizationId,
      actorId: incoming.actor.actorId,
      kind: 'run',
      sourceRef: incoming.resource,
      policyRef: 'pipeline:run',
      title: 'Pipeline run',
      status: 'running',
      updatedAt: incoming.occurredAt,
      sourceEventId: incoming.eventId,
      revision: 1,
    },
  },
  sourceCheckpoints: { ...current.sourceCheckpoints, pipeline: incoming.sourceSequence },
  appliedEventIds: [...current.appliedEventIds, incoming.eventId],
});

type Send = jest.Mock<Promise<Record<string, unknown>>, [unknown]>;

function client(send: Send): DynamoDBDocumentClient {
  return { send } as unknown as DynamoDBDocumentClient;
}

function meta(revision = 1, organizationId = 'org-1'): Record<string, unknown> {
  return {
    PK: `WG#${organizationId}#actor-1#2026-09-20`,
    SK: 'META',
    itemKind: 'meta',
    schemaVersion: 1,
    organizationId,
    actorId: 'actor-1',
    day: '2026-09-20',
    revision,
    sourceCheckpoints: { pipeline: '42' },
    appliedEventIds: ['evt-1'],
  };
}

describe('WorkGraphRepository', () => {
  it('commits graph mutation, checkpoint, revision, and 30-day dedupe row in one transaction', async () => {
    const send: Send = jest.fn(async (command: unknown) => {
      if (command instanceof GetCommand) return {};
      if (command instanceof QueryCommand) return { Items: [] };
      return {};
    });
    const repository = new WorkGraphRepository(client(send), reducer);

    const result = await repository.commitEvent(event, 0);

    expect(result.revision).toBe(1);
    expect(result.sourceCheckpoints).toEqual({ pipeline: '42' });
    const transaction = send.mock.calls.map(([command]) => command)
      .find((command): command is TransactWriteCommand => command instanceof TransactWriteCommand);
    expect(transaction).toBeDefined();
    const writes = transaction!.input.TransactItems!;
    expect(writes).toHaveLength(3);
    expect(writes.map((write) => write.Put?.Item?.['SK'])).toEqual([
      'NODE#node-1',
      'EVENT#pipeline#evt-1',
      'META',
    ]);
    const eventPut = writes[1].Put!;
    expect(eventPut.ConditionExpression).toBe('attribute_not_exists(PK)');
    expect(eventPut.Item?.['expiresAt']).toBe(
      Math.floor(Date.parse(event.receivedAt) / 1_000) + 30 * 24 * 60 * 60,
    );
    expect(writes[2].Put?.ConditionExpression).toBe('attribute_not_exists(PK)');
    expect(writes[2].Put?.Item).toMatchObject({
      PK: 'WG#org-1#actor-1#2026-09-20',
      revision: 1,
      sourceCheckpoints: { pipeline: '42' },
      appliedEventIds: ['evt-1'],
    });
  });

  it('reconstructs a complete projection across query pages after restart', async () => {
    const send: Send = jest.fn()
      .mockResolvedValueOnce({
        Items: [meta(), {
          PK: 'WG#org-1#actor-1#2026-09-20',
          SK: 'NODE#node-1',
          itemKind: 'node',
          id: 'node-1',
          organizationId: 'org-1',
          actorId: 'actor-1',
          title: 'Run',
        }],
        LastEvaluatedKey: { PK: 'WG#org-1#actor-1#2026-09-20', SK: 'NODE#node-1' },
      })
      .mockResolvedValueOnce({
        Items: [{
          PK: 'WG#org-1#actor-1#2026-09-20',
          SK: 'EDGE#edge-1',
          itemKind: 'edge',
          id: 'edge-1',
          organizationId: 'org-1',
          actorId: 'actor-1',
          fromId: 'node-1',
          toId: 'node-2',
        }],
      });

    const restartedRepository = new WorkGraphRepository(client(send), reducer);
    const state = await restartedRepository.loadProjection('org-1', 'actor-1', '2026-09-20');

    expect(state).toMatchObject({ revision: 1, sourceCheckpoints: { pipeline: '42' } });
    expect(Object.keys(state!.nodes)).toEqual(['node-1']);
    expect(Object.keys(state!.edges)).toEqual(['edge-1']);
    const secondQuery = send.mock.calls[1][0] as QueryCommand;
    expect(secondQuery.input.ExclusiveStartKey).toEqual({
      PK: 'WG#org-1#actor-1#2026-09-20',
      SK: 'NODE#node-1',
    });
  });

  it('returns the durable state for a duplicate event without reducing or writing', async () => {
    const reduce = jest.fn(reducer);
    const send: Send = jest.fn(async (command: unknown) => {
      if (command instanceof GetCommand) return { Item: { PK: 'p', SK: 'EVENT#pipeline#evt-1' } };
      if (command instanceof QueryCommand) return { Items: [meta()] };
      throw new Error('duplicate must not write');
    });
    const repository = new WorkGraphRepository(client(send), reduce);

    const state = await repository.commitEvent(event, 0);

    expect(state.revision).toBe(1);
    expect(reduce).not.toHaveBeenCalled();
    expect(send.mock.calls.some(([command]) => command instanceof TransactWriteCommand)).toBe(false);
  });

  it('converts a failed expected-revision transaction into a typed conflict', async () => {
    let getCount = 0;
    const send: Send = jest.fn(async (command: unknown) => {
      if (command instanceof GetCommand) {
        getCount += 1;
        return {};
      }
      if (command instanceof QueryCommand) return { Items: [meta(1)] };
      if (command instanceof TransactWriteCommand) {
        throw Object.assign(new Error('cancelled'), {
          name: 'TransactionCanceledException',
          CancellationReasons: [{ Code: 'None' }, { Code: 'None' }, { Code: 'ConditionalCheckFailed' }],
        });
      }
      return {};
    });
    const repository = new WorkGraphRepository(client(send), reducer);

    await expect(repository.commitEvent({ ...event, eventId: 'evt-2' }, 1))
      .rejects.toBeInstanceOf(WorkGraphConcurrencyError);
    expect(getCount).toBe(2);
  });

  it('bounds the persisted applied event IDs', async () => {
    const manyIds = Array.from({ length: 1_000 }, (_, index) => `old-${index}`);
    const send: Send = jest.fn(async (command: unknown) => {
      if (command instanceof GetCommand) return {};
      if (command instanceof QueryCommand) return { Items: [{ ...meta(4), appliedEventIds: manyIds }] };
      return {};
    });
    const repository = new WorkGraphRepository(client(send), reducer);

    const state = await repository.commitEvent(event, 4);

    expect(state.appliedEventIds).toHaveLength(1_000);
    expect(state.appliedEventIds[0]).toBe('old-1');
    expect(state.appliedEventIds.at(-1)).toBe('evt-1');
  });

  it('locks, paginates, and conditionally erases the entire actor/day partition', async () => {
    const pageOne = [
      meta(7),
      { PK: 'p', SK: 'NODE#one', itemKind: 'node' },
    ];
    const pageTwo = [
      { PK: 'p', SK: 'EDGE#one', itemKind: 'edge' },
      { PK: 'p', SK: 'EVENT#pipeline#evt-1', itemKind: 'event' },
    ];
    let queryCount = 0;
    const send: Send = jest.fn(async (command: unknown) => {
      if (command instanceof QueryCommand) {
        queryCount += 1;
        return queryCount === 1
          ? { Items: pageOne, LastEvaluatedKey: { PK: 'p', SK: 'NODE#one' } }
          : { Items: pageTwo };
      }
      return {};
    });
    const repository = new WorkGraphRepository(client(send), reducer);

    await repository.eraseActorDay('org-1', 'actor-1', '2026-09-20', 7);

    const transactions = send.mock.calls.map(([command]) => command)
      .filter((command): command is TransactWriteCommand => command instanceof TransactWriteCommand);
    expect(transactions).toHaveLength(3);
    expect(transactions[0].input.TransactItems?.[0].Update).toMatchObject({
      Key: { PK: 'WG#org-1#actor-1#2026-09-20', SK: 'META' },
      ConditionExpression: '#revision = :expected AND attribute_not_exists(#erasing)',
    });
    expect(transactions[1].input.TransactItems?.map((write) => write.Delete?.Key?.['SK']))
      .toEqual(['NODE#one', 'EDGE#one', 'EVENT#pipeline#evt-1']);
    expect(transactions[2].input.TransactItems?.[0].Delete).toMatchObject({
      Key: { PK: 'WG#org-1#actor-1#2026-09-20', SK: 'META' },
      ConditionExpression: '#revision = :locked AND #erasing = :true',
    });
  });

  it('keeps organizations in distinct partitions and rejects crossed stored data', async () => {
    const send: Send = jest.fn()
      .mockResolvedValueOnce({ Items: [] })
      .mockResolvedValueOnce({ Items: [{ ...meta(1, 'org-1'), PK: 'WG#org-2#actor-1#2026-09-20' }] });
    const repository = new WorkGraphRepository(client(send), reducer);

    await expect(repository.loadProjection('org-1', 'actor-1', '2026-09-20')).resolves.toBeNull();
    await expect(repository.loadProjection('org-2', 'actor-1', '2026-09-20'))
      .rejects.toBeInstanceOf(WorkGraphDataIntegrityError);
    const keys = send.mock.calls.map(([command]) => (command as QueryCommand).input.ExpressionAttributeValues?.[':pk']);
    expect(keys).toEqual([
      'WG#org-1#actor-1#2026-09-20',
      'WG#org-2#actor-1#2026-09-20',
    ]);
  });

  it('rejects an erase at a stale revision before issuing any delete', async () => {
    const send: Send = jest.fn().mockResolvedValue({ Items: [meta(3)] });
    const repository = new WorkGraphRepository(client(send), reducer);

    await expect(repository.eraseActorDay('org-1', 'actor-1', '2026-09-20', 2))
      .rejects.toBeInstanceOf(WorkGraphConcurrencyError);
    expect(send).toHaveBeenCalledTimes(1);
  });

  it('treats an erase retry after the partition is gone as success', async () => {
    const send: Send = jest.fn().mockResolvedValue({ Items: [] });
    const repository = new WorkGraphRepository(client(send), reducer);

    await expect(repository.eraseActorDay('org-1', 'actor-1', '2026-09-20', 7))
      .resolves.toBeUndefined();
    expect(send).toHaveBeenCalledTimes(1);
  });
});
