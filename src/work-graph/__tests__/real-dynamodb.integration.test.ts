import { randomUUID } from 'crypto';
import type { EventEnvelope } from '@connorhoehn/event-catalog';
import { projectWorkEvent, type AuthenticatedWorkEvent } from '@connorhoehn/realtime-modules/work-graph';
import { ddbClient, docClient } from '../../lib/aws-clients';
import { ensureWorkGraphTable } from '../../lib/work-graph-dynamo';
import { WorkGraphRepository } from '../../repositories/WorkGraphRepository';
import { WorkSourceAccountRepository } from '../../repositories/WorkSourceAccountRepository';
import { startWorkGraphConsumer } from '../../services/work-graph-consumer';
import { createWorkGraphOutboxDrainer } from '../../services/work-graph-outbox-drainer';

const realDescribe = process.env.WORK_GRAPH_REAL_DDB === 'true' ? describe : describe.skip;

realDescribe('work graph real DynamoDB integration', () => {
  jest.setTimeout(30_000);

  it('joins producer identity, commits atomically, replays, locates, dedupes, and drains outbox', async () => {
    await ensureWorkGraphTable(ddbClient);
    const suffix = randomUUID();
    const organizationId = `org-${suffix}`;
    const actorId = `actor-${suffix}`;
    const sourceSubject = `subject-${suffix}`;
    const now = new Date().toISOString();
    const day = now.slice(0, 10);
    const accounts = new WorkSourceAccountRepository(docClient);
    await accounts.put({
      source: 'pipeline',
      producerServiceId: 'platform-api',
      producerOrganizationId: organizationId,
      sourceSubject,
      organizationId,
      actorId,
      revision: 1,
      updatedAt: now,
    }, null);

    const repository = new WorkGraphRepository(docClient, projectWorkEvent);
    let consume!: (envelope: EventEnvelope<unknown>) => Promise<void>;
    const consumer = startWorkGraphConsumer({
      transport: {
        subscribe: (_queue, handler) => {
          consume = handler;
          return () => undefined;
        },
      },
      repository,
      accountResolver: accounts,
    });
    const payload: AuthenticatedWorkEvent = {
      schemaVersion: 1,
      eventId: `event-${suffix}`,
      idempotencyKey: `idem-${suffix}`,
      source: 'pipeline',
      sourceSequence: '1',
      occurredAt: now,
      receivedAt: now,
      actor: { actorId, organizationId, sourceSubject },
      producer: { serviceId: 'platform-api', organizationId, credentialId: `credential-${suffix}` },
      resource: { source: 'pipeline', resourceId: `run-${suffix}`, resourceVersion: '1' },
      payload: {
        kind: 'pipeline', lifecycle: 'started', pipelineId: `pipeline-${suffix}`,
        runId: `run-${suffix}`, attempt: 1, safeLabel: 'Integration run',
      },
    };
    const envelope: EventEnvelope<unknown> = {
      eventName: 'work-graph.pipeline.lifecycle.v1',
      payload,
      metadata: { id: `envelope-${suffix}`, producer: 'platform-api', timestamp: Date.now() },
    };

    await consume(envelope);
    await consume(envelope);
    const projection = await repository.loadProjection(organizationId, actorId, day);
    expect(projection?.revision).toBe(1);
    expect(Object.values(projection?.nodes ?? {})).toEqual(expect.arrayContaining([
      expect.objectContaining({ kind: 'run', title: 'Integration run' }),
    ]));
    const nodeId = Object.keys(projection?.nodes ?? {})[0];
    expect(await repository.locateTarget(organizationId, { kind: 'node', id: nodeId })).toEqual({
      personId: actorId,
      day,
      timezone: 'UTC',
    });
    const replay = await repository.listDeltas(organizationId, actorId, day, 0, 1, 10);
    expect(replay.complete).toBe(true);
    expect(replay.batches).toEqual([
      expect.objectContaining({ previousWatermark: 0, watermark: 1 }),
    ]);

    const publish = jest.fn().mockResolvedValue(undefined);
    const outbox = createWorkGraphOutboxDrainer({ client: docClient, publish, batchSize: 100 });
    // This isolated local namespace is reusable across integration runs, so a
    // prior process may have left valid durable outbox rows. Drain bounded
    // batches until empty before checking this run's unique actor row.
    let drained = 0;
    for (let attempt = 0; attempt < 10; attempt += 1) {
      const count = await outbox.drainOnce();
      drained += count;
      if (count === 0) break;
    }
    expect(drained).toBeGreaterThanOrEqual(1);
    expect(publish).toHaveBeenCalledWith('work-graph.projection.changed.v1', expect.objectContaining({
      organizationId,
      actorId,
      revision: 1,
    }));
    await consumer.stop();
  });
});
