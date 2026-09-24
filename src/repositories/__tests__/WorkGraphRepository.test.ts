import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { GetCommand, QueryCommand, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import {
  WorkGraphConcurrencyError,
  WorkGraphDataIntegrityError,
  WorkGraphRepository,
  decodeLocalDayWatermark,
  encodeLocalDayWatermark,
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
    expect(writes).toHaveLength(5);
    expect(writes.map((write) => write.Put?.Item?.['SK'])).toEqual([
      'NODE#node-1',
      'EVENT#pipeline#evt-1',
      'DELTA#000000000001',
      'OUTBOX#2026-09-20T03:30:01.000Z#000000000001#evt-1#WG#org-1#actor-1#2026-09-20',
      'META',
    ]);
    // NFR #27: the outbox row lives in a drainer partition, pointing back.
    const outbox = writes.find((write) => write.Put?.Item?.['itemKind'] === 'outbox')!.Put!.Item!;
    expect(outbox['PK']).toMatch(/^OUTBOX#[0-3]$/);
    expect(outbox['projectionPK']).toBe('WG#org-1#actor-1#2026-09-20');
    const eventPut = writes[1].Put!;
    expect(eventPut.ConditionExpression).toBe('attribute_not_exists(PK)');
    expect(eventPut.Item?.['ttl']).toBe(
      Math.floor(Date.parse(event.receivedAt) / 1_000) + 30 * 24 * 60 * 60,
    );
    expect(writes[2].Put?.ConditionExpression).toBe('attribute_not_exists(PK)');
    expect(writes[2].Put?.Item).toMatchObject({
      previousWatermark: 0,
      watermark: 1,
      // NFR #22: a node new to the partition is flagged so replay can prove
      // that an authorization revision moved only because of additions.
      operations: [{ kind: 'upsert-node', created: true, node: expect.objectContaining({ id: 'node-1' }) }],
    });
    expect(writes[3].Put?.ConditionExpression).toBe('attribute_not_exists(PK)');
    expect(writes[3].Put?.Item).toMatchObject({
      eventType: 'work-graph.projection.changed.v1',
      organizationId: 'org-1',
      actorId: 'actor-1',
      revision: 1,
    });
    expect(writes[4].Put?.Item).toMatchObject({
      PK: 'WG#org-1#actor-1#2026-09-20',
      revision: 1,
      sourceCheckpoints: { pipeline: '42' },
      appliedEventIds: ['evt-1'],
    });
  });

  it('reconstructs a complete projection across query pages after restart', async () => {
    const send: Send = jest.fn()
      .mockResolvedValueOnce({ Item: meta() })
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
      })
      .mockResolvedValueOnce({ Item: meta() });

    const restartedRepository = new WorkGraphRepository(client(send), reducer);
    const state = await restartedRepository.loadProjection('org-1', 'actor-1', '2026-09-20');

    expect(state).toMatchObject({ revision: 1, sourceCheckpoints: { pipeline: '42' } });
    expect(Object.keys(state!.nodes)).toEqual(['node-1']);
    expect(Object.keys(state!.edges)).toEqual(['edge-1']);
    const secondQuery = send.mock.calls.map(([command]) => command)
      .filter((command): command is QueryCommand => command instanceof QueryCommand)[1];
    expect(secondQuery.input.ExclusiveStartKey).toEqual({
      PK: 'WG#org-1#actor-1#2026-09-20',
      SK: 'NODE#node-1',
    });
  });

  it('retries a paginated read when its META revision changes mid-read', async () => {
    const metaTwo = { ...meta(2), sourceCheckpoints: { pipeline: '43' } };
    let getCount = 0;
    let queryCount = 0;
    const send: Send = jest.fn(async (command: unknown) => {
      if (command instanceof GetCommand) {
        getCount += 1;
        return { Item: getCount === 1 ? meta(1) : metaTwo };
      }
      if (command instanceof QueryCommand) {
        queryCount += 1;
        return queryCount === 1
          ? { Items: [meta(1), {
            PK: 'WG#org-1#actor-1#2026-09-20', SK: 'NODE#old', itemKind: 'node',
            id: 'old', organizationId: 'org-1', actorId: 'actor-1', updatedAt: '2026-09-20T00:00:00.000Z',
          }] }
          : { Items: [metaTwo, {
            PK: 'WG#org-1#actor-1#2026-09-20', SK: 'NODE#new', itemKind: 'node',
            id: 'new', organizationId: 'org-1', actorId: 'actor-1', updatedAt: '2026-09-20T01:00:00.000Z',
          }] };
      }
      return {};
    });
    const repository = new WorkGraphRepository(client(send), reducer);

    await expect(repository.loadProjection('org-1', 'actor-1', '2026-09-20')).resolves.toMatchObject({
      revision: 2,
      nodes: { new: expect.any(Object) },
    });
    expect(queryCount).toBe(2);
  });

  it('joins only the UTC partitions overlapping an IANA local day', async () => {
    const itemFor = (day: string, revision: number, nodes: Array<Record<string, unknown>>) => [
      {
        ...meta(revision),
        PK: `WG#org-1#actor-1#${day}`,
        day,
      },
      ...nodes.map((node) => ({
        PK: `WG#org-1#actor-1#${day}`,
        SK: `NODE#${node.id}`,
        itemKind: 'node',
        organizationId: 'org-1',
        actorId: 'actor-1',
        ...node,
      })),
    ];
    const rows = new Map([
      ['2026-09-20', itemFor('2026-09-20', 2, [
        {
          id: 'before-local-day', startedAt: '2026-09-20T03:30:00.000Z',
          updatedAt: '2026-09-20T03:30:00.000Z', endedAt: '2026-09-20T04:00:00.000Z',
        },
        { id: 'inside-first-utc-day', updatedAt: '2026-09-20T04:30:00.000Z' },
      ])],
      ['2026-09-21', itemFor('2026-09-21', 3, [
        { id: 'inside-second-utc-day', updatedAt: '2026-09-21T03:30:00.000Z' },
      ])],
    ]);
    const send: Send = jest.fn(async (command: unknown) => {
      const pk = command instanceof GetCommand
        ? String(command.input.Key?.['PK'])
        : command instanceof QueryCommand
          ? String(command.input.ExpressionAttributeValues?.[':pk'])
          : '';
      const day = pk.slice(-10);
      const items = rows.get(day) ?? [];
      if (command instanceof GetCommand) return { Item: items[0] };
      if (command instanceof QueryCommand) return { Items: items };
      return {};
    });
    const repository = new WorkGraphRepository(client(send), reducer);

    const state = await repository.loadProjectionForLocalDay(
      'org-1', 'actor-1', '2026-09-20', 'America/New_York',
    );
    expect(Object.keys(state!.nodes).sort()).toEqual([
      'inside-first-utc-day',
      'inside-second-utc-day',
    ]);
    // Packed, not summed: replay splits it back into one cursor per partition.
    expect(state!.revision).toBe(encodeLocalDayWatermark(2, 3));
    expect(decodeLocalDayWatermark(state!.revision)).toEqual([2, 3]);
  });

  it('re-reads an unmoved partition as one META read, and a moved one in full (NFR #94)', async () => {
    const node = (id: string, updatedAt: string) => ({
      PK: 'WG#org-1#actor-1#2026-09-20', SK: `NODE#${id}`, itemKind: 'node',
      organizationId: 'org-1', actorId: 'actor-1', id, updatedAt,
    });
    let revision = 2;
    let items: Array<Record<string, unknown>> = [node('first', '2026-09-20T12:00:00.000Z')];
    const send: Send = jest.fn(async (command: unknown) => {
      const head = { ...meta(revision), PK: 'WG#org-1#actor-1#2026-09-20' };
      if (command instanceof GetCommand) return { Item: head };
      if (command instanceof QueryCommand) return { Items: [head, ...items] };
      return {};
    });
    const repository = new WorkGraphRepository(client(send), reducer);
    const load = () => repository.loadProjectionForLocalDay('org-1', 'actor-1', '2026-09-20', 'UTC');
    const queries = () => send.mock.calls.filter(([command]) => command instanceof QueryCommand).length;

    expect(Object.keys((await load())!.nodes)).toEqual(['first']);
    expect(queries()).toBe(1);
    send.mockClear();
    expect(Object.keys((await load())!.nodes)).toEqual(['first']);
    expect(queries()).toBe(0);
    expect(send).toHaveBeenCalledTimes(1);

    revision = 3;
    items = [...items, node('second', '2026-09-20T13:00:00.000Z')];
    send.mockClear();
    expect(Object.keys((await load())!.nodes).sort()).toEqual(['first', 'second']);
    expect(queries()).toBe(1);
  });

  describe('local-day replay (NFR #22)', () => {
    const delta = (watermark: number, committedAt: string, id: string) => ({
      itemKind: 'delta',
      previousWatermark: watermark - 1,
      watermark,
      committedAt,
      operations: [{ kind: 'upsert-node', node: { id } }],
    });
    const logs: Record<string, Array<Record<string, unknown>>> = {
      '2026-09-20': [delta(3, '2026-09-20T14:00:00.000Z', 'a3'), delta(4, '2026-09-20T16:00:00.000Z', 'a4')],
      '2026-09-21': [delta(2, '2026-09-21T01:00:00.000Z', 'b2')],
    };
    const repositoryWithLogs = () => {
      const send: Send = jest.fn(async (command: unknown) => {
        if (!(command instanceof QueryCommand)) return {};
        const values = command.input.ExpressionAttributeValues ?? {};
        const day = String(values[':pk']).slice(-10);
        const from = Number(String(values[':from']).split('#')[1]);
        const to = Number(String(values[':through']).split('#')[1]);
        return { Items: (logs[day] ?? []).filter((row) => Number(row.watermark) >= from && Number(row.watermark) <= to) };
      });
      return { repository: new WorkGraphRepository(client(send), reducer), send };
    };

    it('merges both UTC partitions into single-step batches with packed watermarks', async () => {
      const { repository } = repositoryWithLogs();
      const result = await repository.listLocalDayDeltas(
        'org-1', 'actor-1', '2026-09-20', 'America/New_York',
        encodeLocalDayWatermark(2, 1), encodeLocalDayWatermark(4, 2), 1_001,
      );
      expect(result.complete).toBe(true);
      expect(result.batches.map((batch) => [
        decodeLocalDayWatermark(batch.previousWatermark),
        decodeLocalDayWatermark(batch.watermark),
        (batch.operations[0] as { node: { id: string } }).node.id,
      ])).toEqual([
        [[2, 1], [3, 1], 'a3'],
        [[3, 1], [4, 1], 'a4'],
        [[4, 1], [4, 2], 'b2'],
      ]);
      for (let index = 1; index < result.batches.length; index += 1) {
        expect(result.batches[index]!.previousWatermark).toBe(result.batches[index - 1]!.watermark);
      }
    });

    it('answers incomplete for a cursor from the old summed encoding instead of guessing a split', async () => {
      const { repository, send } = repositoryWithLogs();
      await expect(repository.listLocalDayDeltas(
        'org-1', 'actor-1', '2026-09-20', 'America/New_York', 3, encodeLocalDayWatermark(4, 2), 1_001,
      )).resolves.toEqual({ batches: [], complete: false });
      expect(send).not.toHaveBeenCalled();
    });

    it('is incomplete when one partition has a gap (expired delta rows)', async () => {
      const { repository } = repositoryWithLogs();
      const result = await repository.listLocalDayDeltas(
        'org-1', 'actor-1', '2026-09-20', 'America/New_York',
        encodeLocalDayWatermark(1, 1), encodeLocalDayWatermark(4, 2), 1_001,
      );
      expect(result.complete).toBe(false);
    });

    it('reads a UTC day exactly like listDeltas', async () => {
      const { repository } = repositoryWithLogs();
      const result = await repository.listLocalDayDeltas('org-1', 'actor-1', '2026-09-20', 'UTC', 2, 4, 1_001);
      expect(result.complete).toBe(true);
      expect(result.batches.map((batch) => batch.watermark)).toEqual([3, 4]);
    });

    it('treats 0 as the empty two-partition day and rejects out-of-range revisions', () => {
      expect(decodeLocalDayWatermark(0)).toEqual([0, 0]);
      expect(encodeLocalDayWatermark(0, 0)).toBeGreaterThan(Number.MAX_SAFE_INTEGER / 2);
      expect(() => encodeLocalDayWatermark(2 ** 26, 0)).toThrow(WorkGraphDataIntegrityError);
      expect(encodeLocalDayWatermark(1, 0)).toBeGreaterThan(encodeLocalDayWatermark(0, 2 ** 26 - 1));
    });
  });

  it('returns the durable state for a duplicate event without reducing or writing', async () => {
    const reduce = jest.fn(reducer);
    let getCount = 0;
    const send: Send = jest.fn(async (command: unknown) => {
      if (command instanceof GetCommand) {
        getCount += 1;
        return getCount === 1
          ? { Item: { PK: 'p', SK: 'EVENT#pipeline#evt-1' } }
          : { Item: meta() };
      }
      if (command instanceof QueryCommand) return { Items: [meta()] };
      throw new Error('duplicate must not write');
    });
    const repository = new WorkGraphRepository(client(send), reduce);

    const state = await repository.commitEvent(event, 0);

    expect(state.revision).toBe(1);
    expect(reduce).not.toHaveBeenCalled();
    expect(send.mock.calls.some(([command]) => command instanceof TransactWriteCommand)).toBe(false);
  });

  it('nudges live viewers after a commit, and not after a failed one', async () => {
    const committed = jest.fn();
    const ok: Send = jest.fn(async (command: unknown) => {
      if (command instanceof QueryCommand) return { Items: [] };
      return {};
    });
    await new WorkGraphRepository(client(ok), reducer, committed).commitEvent(event, 0);
    expect(committed).toHaveBeenCalledWith(expect.objectContaining({ revision: 1, day: expect.stringMatching(/^\d{4}-\d{2}-\d{2}$/) }));

    const failing: Send = jest.fn(async (command: unknown) => {
      if (command instanceof QueryCommand) return { Items: [] };
      if (command instanceof TransactWriteCommand) throw new Error('throttled');
      return {};
    });
    committed.mockClear();
    await expect(new WorkGraphRepository(client(failing), reducer, committed).commitEvent(event, 0)).rejects.toThrow('throttled');
    expect(committed).not.toHaveBeenCalled();
  });

  it('a nudge that throws does not fail the commit', async () => {
    const send: Send = jest.fn(async (command: unknown) => (command instanceof QueryCommand ? { Items: [] } : {}));
    const repository = new WorkGraphRepository(client(send), reducer, () => { throw new Error('redis gone'); });
    await expect(repository.commitEvent(event, 0)).resolves.toMatchObject({ revision: 1 });
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
    let getCount = 0;
    const send: Send = jest.fn(async (command: unknown) => {
      if (command instanceof GetCommand) {
        getCount += 1;
        return getCount === 1 ? {} : { Item: { ...meta(4), appliedEventIds: manyIds } };
      }
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
      { ...meta(8), erasing: true, eraseExpectedRevision: 7 },
      { PK: 'p', SK: 'NODE#one', itemKind: 'node' },
    ];
    const pageTwo = [
      { PK: 'p', SK: 'EDGE#one', itemKind: 'edge' },
      { PK: 'p', SK: 'EVENT#pipeline#evt-1', itemKind: 'event' },
    ];
    let queryCount = 0;
    const send: Send = jest.fn(async (command: unknown) => {
      if (command instanceof GetCommand) return { Item: meta(7) };
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
    let getCount = 0;
    const send: Send = jest.fn(async (command: unknown) => {
      if (command instanceof GetCommand) {
        getCount += 1;
        if (getCount === 1) return {};
        return { Item: { ...meta(1, 'org-1'), PK: 'WG#org-2#actor-1#2026-09-20' } };
      }
      if (command instanceof QueryCommand) {
        return { Items: [{ ...meta(1, 'org-1'), PK: 'WG#org-2#actor-1#2026-09-20' }] };
      }
      return {};
    });
    const repository = new WorkGraphRepository(client(send), reducer);

    await expect(repository.loadProjection('org-1', 'actor-1', '2026-09-20')).resolves.toBeNull();
    await expect(repository.loadProjection('org-2', 'actor-1', '2026-09-20'))
      .rejects.toBeInstanceOf(WorkGraphDataIntegrityError);
    const keys = send.mock.calls.map(([command]) => command)
      .filter((command): command is QueryCommand => command instanceof QueryCommand)
      .map((command) => command.input.ExpressionAttributeValues?.[':pk']);
    expect(keys).toEqual(['WG#org-2#actor-1#2026-09-20']);
  });

  it('rejects an erase at a stale revision before issuing any delete', async () => {
    const send: Send = jest.fn(async (command: unknown) => command instanceof GetCommand
      ? { Item: meta(3) }
      : {});
    const repository = new WorkGraphRepository(client(send), reducer);

    await expect(repository.eraseActorDay('org-1', 'actor-1', '2026-09-20', 2))
      .rejects.toBeInstanceOf(WorkGraphConcurrencyError);
    expect(send).toHaveBeenCalledTimes(1);
  });

  it('treats an erase retry after the partition is gone as success', async () => {
    const send: Send = jest.fn().mockResolvedValue({});
    const repository = new WorkGraphRepository(client(send), reducer);

    await expect(repository.eraseActorDay('org-1', 'actor-1', '2026-09-20', 7))
      .resolves.toBeUndefined();
    expect(send).toHaveBeenCalledTimes(1);
  });

  describe('locateTarget', () => {
    const lookup = (day: string, targetActorId = 'actor-1'): Record<string, unknown> => ({
      targetLookupPk: 'WGTARGET#org-1',
      targetLookupSk: 'NODE#wg_node_deck',
      targetActorId,
      day,
    });
    const locate = (items: Array<Record<string, unknown>>) => new WorkGraphRepository(
      client(jest.fn(async (command: unknown) => (command instanceof QueryCommand ? { Items: items } : {}))),
      reducer,
    ).locateTarget('org-1', { kind: 'node', id: 'wg_node_deck' });

    it('finds a target that is still being worked on after a day boundary', async () => {
      // The seeded Sprint review deck: one document node projected into both
      // 2026-09-21 and 2026-09-22. It is one entity, not an ambiguous id, and
      // refusing it is what made "Open presentation" answer 404.
      await expect(locate([lookup('2026-09-21'), lookup('2026-09-22')]))
        .resolves.toEqual({ personId: 'actor-1', day: '2026-09-22', timezone: 'UTC' });
    });

    it('still refuses a target two different people both claim', async () => {
      await expect(locate([lookup('2026-09-21'), lookup('2026-09-21', 'actor-2')])).resolves.toBeNull();
    });

    it('discloses nothing for an unknown target or a row with no usable day', async () => {
      await expect(locate([])).resolves.toBeNull();
      await expect(locate([lookup('yesterday')])).resolves.toBeNull();
    });
  });

  describe('writer path (NFR #202)', () => {
    it('reads only projection items, reuses the caller\'s load, and holds the committed state', async () => {
      let revision = 0;
      const send: Send = jest.fn(async (command: unknown) => {
        if (command instanceof GetCommand) {
          if (String(command.input.Key?.['SK']).startsWith('EVENT#')) return {};
          return revision ? { Item: meta(revision) } : {};
        }
        if (command instanceof QueryCommand) return { Items: revision ? [meta(revision)] : [] };
        if (command instanceof TransactWriteCommand) { revision += 1; return {}; }
        return {};
      });
      const repository = new WorkGraphRepository(client(send), reducer);
      const count = (type: unknown) => send.mock.calls.filter(([c]) => c instanceof (type as never)).length;

      const first = await repository.commitEvent(event, 0, null);
      expect(first.revision).toBe(1);
      send.mockClear();

      // The next writer load is one META Get: the committed state is held.
      const current = await repository.loadCurrentProjection('org-1', 'actor-1', '2026-09-20');
      expect(current?.revision).toBe(1);
      expect(count(QueryCommand)).toBe(0);
      expect(count(GetCommand)).toBe(1);

      // Passing it to commitEvent skips a second partition read.
      send.mockClear();
      await repository.commitEvent({ ...event, eventId: 'evt-2', idempotencyKey: 'idem-2' }, 1, current);
      expect(count(QueryCommand)).toBe(0);
      expect(count(TransactWriteCommand)).toBe(1);

      // A full read filters events and deltas out server-side.
      send.mockClear();
      await repository.loadProjection('org-1', 'actor-1', '2026-09-20');
      const query = send.mock.calls.map(([c]) => c).find((c): c is QueryCommand => c instanceof QueryCommand)!;
      expect(query.input.FilterExpression).toBe('itemKind IN (:meta, :session, :node, :edge)');
    });

    it('ignores a caller\'s load at another revision and reads afresh', async () => {
      const send: Send = jest.fn(async (command: unknown) => {
        if (command instanceof GetCommand) return String(command.input.Key?.['SK']).startsWith('EVENT#') ? {} : { Item: meta(1) };
        if (command instanceof QueryCommand) return { Items: [meta(1)] };
        return {};
      });
      const repository = new WorkGraphRepository(client(send), reducer);
      await repository.commitEvent(event, 1, emptyState(0));
      expect(send.mock.calls.filter(([c]) => c instanceof QueryCommand)).toHaveLength(1);
    });
  });
});
