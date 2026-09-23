/**
 * NFR 34b: every step transition of a run reaches the work-graph projection's
 * queue, exactly once, however large the outbox table is.
 *
 * The whole producer path runs for real — run persistence → the platform
 * work-graph source publisher (real `mapPipeline`) → the outbox row →
 * the outbox drainer → the durable publish the projection consumes. Only
 * DynamoDB (an in-memory table that pages like a Scan) and the MQ publish are
 * fakes.
 *
 * The bug this pins: each drainer poll walked the table from the top and gave
 * up after MAX_SCAN_PAGES × SCAN_PAGE_SIZE rows (4000), so on a table holding
 * a day of terminal rows any row stored past that point was never drained.
 * On the live run the graph had no "load" node and "write" stuck `running`.
 */
type Row = Record<string, unknown> & { outboxId: string };

const mockTable = new Map<string, Row>();
const mockDelivered: Array<{ eventName: string; payload: Record<string, unknown> }> = [];
/** Items each read path returned/examined — the cost of finding pending rows. */
const mockReads = { scanned: 0, queried: 0, scanEnds: 0 };

jest.mock('../../lib/ddb-table-name', () => ({ tableName: (base: string) => base }));
jest.mock('../../lib/aws-clients', () => ({
  ddbClient: { send: async () => ({ Table: { GlobalSecondaryIndexes: [{ IndexName: 'pending-index', IndexStatus: 'ACTIVE' }] } }) },
  docClient: {
    send: async (cmd: { constructor: { name: string }; input: Record<string, any> }) => {
      const input = cmd.input;
      const key: string | undefined = input.Item?.outboxId ?? input.Key?.outboxId;
      // The drainer's lease lives in the same table; it always wins here.
      if (typeof key === 'string' && key.startsWith('OUTBOX_LOCK#')) return {};
      switch (cmd.constructor.name) {
        case 'PutCommand':
          mockTable.set(input.Item.outboxId, { ...input.Item });
          return {};
        case 'UpdateCommand': {
          const row = mockTable.get(input.Key.outboxId);
          const values = input.ExpressionAttributeValues ?? {};
          if (String(input.UpdateExpression) === 'REMOVE #p') {
            if (!row || row.status === 'UNPROCESSED') {
              throw Object.assign(new Error('conditional'), { name: 'ConditionalCheckFailedException' });
            }
            delete row.pendingShard;
            return {};
          }
          const adopting = ':shard' in values;
          if (!row || row.status !== 'UNPROCESSED' || (adopting && row.pendingShard !== undefined)) {
            throw Object.assign(new Error('conditional'), { name: 'ConditionalCheckFailedException' });
          }
          if (adopting) row.pendingShard = values[':shard'];
          row.status = values[':processed'] ?? values[':failed'] ?? row.status;
          if (String(input.UpdateExpression).includes('REMOVE #p')) delete row.pendingShard;
          return {};
        }
        case 'ScanCommand': {
          // Table order, `Limit` rows examined, the filter applied afterwards.
          const all = [...mockTable.values()];
          const start = input.ExclusiveStartKey
            ? all.findIndex((row) => row.outboxId === input.ExclusiveStartKey.outboxId) + 1
            : 0;
          const page = all.slice(start, start + input.Limit);
          const more = start + input.Limit < all.length;
          const unsharded = String(input.FilterExpression).includes('attribute_not_exists(#p)');
          mockReads.scanned += page.length;
          if (!more) mockReads.scanEnds += 1;
          return {
            Items: page
              .filter((row) => row.status === 'UNPROCESSED' && (!unsharded || row.pendingShard === undefined))
              .map((row) => ({ ...row })),
            ...(more ? { LastEvaluatedKey: { outboxId: page[page.length - 1].outboxId } } : {}),
          };
        }
        case 'QueryCommand': {
          // pending-index: only rows carrying the shard, in ulid order.
          const shard = input.ExpressionAttributeValues[':shard'];
          const items = [...mockTable.values()]
            .filter((row) => row.pendingShard === shard)
            .sort((a, b) => (a.outboxId < b.outboxId ? -1 : 1))
            .slice(0, input.Limit);
          mockReads.queried += items.length;
          return { Items: items.map((row) => ({ ...row })) };
        }
        default:
          return {};
      }
    },
  },
}));

jest.mock('../../lib/event-catalog', () => {
  let sequence = 0;
  return {
    // The real helper's row, written through the (fake) table.
    publishDurable: async (eventType: string, payload: Record<string, unknown>) => {
      const { docClient } = jest.requireMock('../../lib/aws-clients') as { docClient: { send: (c: unknown) => Promise<unknown> } };
      const { pendingFields } = jest.requireActual('../../lib/outbox-pending') as typeof import('../../lib/outbox-pending');
      sequence += 1;
      const outboxId = `new-${String(sequence).padStart(4, '0')}`;
      await docClient.send({
        constructor: { name: 'PutCommand' },
        input: {
          Item: {
            outboxId, ...pendingFields(outboxId),
            status: 'UNPROCESSED', eventType, queueName: 'platform-events',
            payload: JSON.stringify(payload), createdAt: new Date().toISOString(),
          },
        },
      });
    },
    catalog: {
      publish: async (eventName: string, payload: Record<string, unknown>) => {
        mockDelivered.push({ eventName, payload });
      },
    },
  };
});

jest.mock('../../work-graph/sources/platformPublisher', () => {
  const { PlatformWorkGraphSourcePublisher } = jest.requireActual('../../work-graph/sources/publisher') as typeof import('../../work-graph/sources/publisher');
  const { publishDurable } = jest.requireMock('../../lib/event-catalog') as { publishDurable: (n: string, p: unknown) => Promise<void> };
  return {
    platformWorkGraphSourcePublisher: new PlatformWorkGraphSourcePublisher({
      accounts: { resolve: async () => ({ organizationId: 'dev-org', actorId: 'dev-frank' }) } as never,
      publisher: { publish: async (eventName: string, payload: unknown) => publishDurable(eventName, payload) },
      credentialId: 'test',
    }),
  };
});

import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { RunPersistenceService } from '../runPersistence';
import { startOutboxDrainer } from '../../services/outbox-drainer';
import type { PipelineRunRepository } from '../../repositories/PipelineRunRepository';

type Handler = (event: { id?: string; payload: Record<string, unknown> }) => Promise<void>;

const RUN = 'run-34b';
const STEPS = ['load', 'propose', 'write'];
const plan = {
  title: 'Edit audit-deck-check',
  steps: [
    { stepId: 'load', title: 'Read the deck', dependsOn: [] },
    { stepId: 'propose', title: 'Propose the edit', dependsOn: ['load'] },
    { stepId: 'write', title: 'Save the new revision', dependsOn: ['propose'] },
  ],
};

async function runThreeStepPipeline(): Promise<void> {
  const handlers = new Map<string, Handler>();
  const eventBus = { subscribe: (name: string, handler: Handler) => { handlers.set(name, handler); return name; } };
  const service = new RunPersistenceService({
    eventBus: eventBus as never,
    repository: { put: jest.fn(async () => undefined) } as unknown as PipelineRunRepository,
  });
  service.start();
  let n = 0;
  const at = (ms: number) => new Date(Date.parse('2026-09-23T04:41:07.000Z') + ms).toISOString();
  const emit = async (name: string, payload: Record<string, unknown>) => {
    n += 1;
    await handlers.get(name)!({ id: `bus-${n}`, payload });
  };
  service.registerTrigger({
    runId: RUN, pipelineId: 'deck-revise', userId: 'dev-frank', triggerType: 'manual',
    at: at(0), organizationId: 'dev-org', workGraph: plan,
  });
  await emit('pipeline:run:started', {
    runId: RUN, pipelineId: 'deck-revise', at: at(0),
    triggeredBy: { triggerType: 'manual', userId: 'dev-frank', payload: { instruction: 'tighten the title' } },
  });
  let t = 1;
  for (const stepId of STEPS) {
    await emit('pipeline:step:started', { runId: RUN, stepId, at: at(t++) });
    await emit('pipeline:step:completed', { runId: RUN, stepId, output: {}, durationMs: 1, at: at(t++) });
  }
  await emit('pipeline:run:completed', { runId: RUN, durationMs: t, at: at(t) });
}

async function until(predicate: () => boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate() && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 5));
}

describe('NFR 34b — every step transition reaches the projection queue exactly once', () => {
  beforeEach(() => {
    mockTable.clear();
    mockDelivered.length = 0;
    Object.assign(mockReads, { scanned: 0, queried: 0, scanEnds: 0 });
  });

  it('a 3-step run on an outbox holding a day of terminal rows delivers started and completed for every step', async () => {
    // More terminal rows than one poll may examine (20 pages × 200), ahead of
    // the run's rows in the table's order.
    for (let i = 0; i < 5_000; i += 1) {
      const id = `old-${String(i).padStart(5, '0')}`;
      mockTable.set(id, { outboxId: id, status: 'PROCESSED', eventType: 'x', payload: '{}', createdAt: '2026-09-22T00:00:00.000Z' });
    }

    await runThreeStepPipeline();
    const enqueued = [...mockTable.values()].filter((row) => row.status === 'UNPROCESSED').length;
    expect(enqueued).toBeGreaterThan(0);

    const drainer = startOutboxDrainer({ pollIntervalMs: 5, batchSize: 25 });
    try {
      await until(() => [...mockTable.values()].every((row) => row.status !== 'UNPROCESSED'));
    } finally {
      drainer.stop();
    }

    const lifecycle = mockDelivered
      .filter((entry) => entry.eventName === 'work-graph.pipeline.lifecycle.v1')
      .map((entry) => entry.payload as { eventId: string; payload: { stepId?: string; lifecycle?: string } });
    expect(lifecycle).toHaveLength(enqueued);

    const ids = lifecycle.map((event) => event.eventId);
    expect(new Set(ids).size).toBe(ids.length);

    for (const stepId of STEPS) {
      const of = (phase: string) => ids.filter((id) => id.includes(`::${stepId}:1:${phase}:`));
      expect({ stepId, started: of('started').length, completed: of('completed').length })
        .toEqual({ stepId, started: 1, completed: 1 });
    }
    expect(ids.filter((id) => id.startsWith(`pipeline:run:${RUN}:1:started:`))).toHaveLength(1);
    expect(ids.filter((id) => id.startsWith(`pipeline:run:${RUN}:1:completed:`))).toHaveLength(1);
  });

  const fillProcessed = (count: number): void => {
    for (let i = 0; i < count; i += 1) {
      const id = `old-${String(i).padStart(5, '0')}`;
      mockTable.set(id, { outboxId: id, status: 'PROCESSED', eventType: 'x', payload: '{}', createdAt: '2026-09-22T00:00:00.000Z' });
    }
  };

  it('NFR #34c: a new row behind 8,000 processed ones is found by reading only the pending rows', async () => {
    fillProcessed(8_000);
    const drainer = startOutboxDrainer({ pollIntervalMs: 5, batchSize: 25 });
    try {
      // The one-time stray walk (rows written before the index existed) sees
      // the whole table once, finds none, and switches itself off.
      await until(() => mockReads.scanEnds >= 1);
      await new Promise((resolve) => setTimeout(resolve, 50));
      const scansAtRest = mockReads.scanned;
      mockReads.queried = 0;

      await runThreeStepPipeline();
      const enqueued = [...mockTable.values()].filter((row) => row.status === 'UNPROCESSED').length;
      await until(() => [...mockTable.values()].every((row) => row.status !== 'UNPROCESSED'));
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockDelivered).toHaveLength(enqueued);
      // No table walk after the stray check, and the index returned only the
      // run's own rows (each at most once more if a poll overlapped the write).
      expect(mockReads.scanned).toBe(scansAtRest);
      expect(mockReads.queried).toBeLessThanOrEqual(enqueued * 2);
      expect([...mockTable.values()].filter((row) => row.pendingShard !== undefined)).toHaveLength(0);
    } finally {
      drainer.stop();
    }
  });

  it('NFR #34c: a row written without a shard (older build) is adopted and delivered once', async () => {
    fillProcessed(5_000);
    mockTable.set('stray-1', {
      outboxId: 'stray-1', status: 'UNPROCESSED', eventType: 'social.room.join',
      payload: JSON.stringify({ roomId: 'r' }), createdAt: '2026-09-23T00:00:00.000Z',
    });
    const drainer = startOutboxDrainer({ pollIntervalMs: 5, batchSize: 25 });
    try {
      await until(() => mockTable.get('stray-1')!.status !== 'UNPROCESSED');
    } finally {
      drainer.stop();
    }
    expect(mockDelivered.map((entry) => entry.eventName)).toEqual(['social.room.join']);
    expect(mockTable.get('stray-1')!.pendingShard).toBeUndefined();
  });

  it('NFR #34c: terminal rows still carrying a shard do not starve the pending ones behind them', async () => {
    // 200 PROCESSED rows that kept pendingShard (written by the new build,
    // processed by an older one), all older than the new pending rows.
    const { pendingFields } = jest.requireActual('../../lib/outbox-pending') as typeof import('../../lib/outbox-pending');
    for (let i = 0; i < 200; i += 1) {
      const id = `aaa-${String(i).padStart(4, '0')}`;
      mockTable.set(id, { outboxId: id, ...pendingFields(id), status: 'PROCESSED', eventType: 'x', payload: '{}', createdAt: '2026-09-23T00:00:00.000Z' });
    }
    const drainer = startOutboxDrainer({ pollIntervalMs: 5, batchSize: 25 });
    try {
      await runThreeStepPipeline();
      await until(() => [...mockTable.values()].every((row) => row.status !== 'UNPROCESSED' && row.pendingShard === undefined));
    } finally {
      drainer.stop();
    }
    expect([...mockTable.values()].filter((row) => row.status === 'UNPROCESSED')).toHaveLength(0);
    expect([...mockTable.values()].filter((row) => row.pendingShard !== undefined)).toHaveLength(0);
  });
});
