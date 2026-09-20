import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  WorkSharingConflictError,
  WorkSharingRepository,
  type WorkHistoryGrant,
  type WorkSharingGrant,
} from '../WorkSharingRepository';

const CREATED_AT = '2026-09-19T12:00:00.000Z';
const NOW = '2026-09-19T12:30:00.000Z';
const EXPIRES_AT = '2026-09-19T13:00:00.000Z';

function liveGrant(overrides: Partial<WorkSharingGrant> = {}): WorkSharingGrant {
  return {
    schemaVersion: 1,
    id: 'grant-1',
    organizationId: 'org-1',
    ownerId: 'owner-1',
    audience: { kind: 'people', personIds: ['viewer-1'] },
    selection: { nodeIds: ['node-1'], edgeIds: ['edge-1'], disclosure: 'details' },
    state: 'active',
    createdAt: CREATED_AT,
    updatedAt: CREATED_AT,
    expiresAt: EXPIRES_AT,
    revision: 1,
    ...overrides,
  };
}

function historyGrant(overrides: Partial<WorkHistoryGrant> = {}): WorkHistoryGrant {
  return {
    ...liveGrant({
      id: 'history-1',
      expiresAt: '2026-09-21T12:00:00.000Z',
      ...overrides,
    }),
    dayFrom: '2026-09-17',
    dayThrough: '2026-09-19',
    ...overrides,
  };
}

interface MemoryClient {
  client: DynamoDBDocumentClient;
  send: jest.Mock;
  rows: Map<string, Record<string, unknown>>;
}

function createMemoryClient(): MemoryClient {
  const rows = new Map<string, Record<string, unknown>>();
  const send = jest.fn(async (command: PutCommand | QueryCommand | GetCommand) => {
    if (command instanceof PutCommand) {
      const input = command.input;
      const item = input.Item as Record<string, unknown>;
      const key = `${String(item['PK'])}|${String(item['SK'])}`;
      const current = rows.get(key);
      if (input.ConditionExpression?.startsWith('attribute_not_exists')) {
        if (current) throw Object.assign(new Error('conditional create'), { name: 'ConditionalCheckFailedException' });
      } else {
        const currentRevision = (current?.['grant'] as { revision?: number } | undefined)?.revision;
        if (!current || currentRevision !== input.ExpressionAttributeValues?.[':expectedRevision']) {
          throw Object.assign(new Error('conditional update'), { name: 'ConditionalCheckFailedException' });
        }
      }
      rows.set(key, item);
      return {};
    }

    if (command instanceof QueryCommand) {
      const values = command.input.ExpressionAttributeValues ?? {};
      const all = [...rows.values()];
      if (command.input.IndexName) {
        const matches = all.filter((item) => item['grantLookupPk'] === values[':organization']
          && item['grantLookupSk'] === values[':grant']);
        return { Items: matches.slice(0, command.input.Limit) };
      }
      return { Items: all.filter((item) => item['PK'] === values[':partition']) };
    }

    if (command instanceof GetCommand) {
      const key = `${String(command.input.Key?.['PK'])}|${String(command.input.Key?.['SK'])}`;
      return { Item: rows.get(key) };
    }

    throw new Error('Unexpected DynamoDB command.');
  });
  return { client: { send } as unknown as DynamoDBDocumentClient, send, rows };
}

describe('WorkSharingRepository', () => {
  it('uses frozen keys, lookup index attributes, TTL cleanup metadata, and create/update conditions', async () => {
    const memory = createMemoryClient();
    const repository = new WorkSharingRepository(memory.client, 'test-work-graph');
    await repository.putGrant(liveGrant(), null);

    const create = memory.send.mock.calls[0][0] as PutCommand;
    expect(create.input).toMatchObject({
      TableName: 'test-work-graph',
      ConditionExpression: 'attribute_not_exists(PK) AND attribute_not_exists(SK)',
      Item: {
        PK: 'WGGRANT#org-1#owner-1',
        SK: 'GRANT#grant-1',
        grantLookupPk: 'WGGRANT#org-1',
        grantLookupSk: 'GRANT#grant-1',
        entityType: 'work-sharing-grant',
        ttl: Date.parse(EXPIRES_AT) / 1_000,
      },
    });

    const updated = liveGrant({ revision: 2, updatedAt: '2026-09-19T12:10:00.000Z', state: 'paused' });
    await repository.putGrant(updated, 1);
    const update = memory.send.mock.calls[1][0] as PutCommand;
    expect(update.input.ConditionExpression).toBe(
      'attribute_exists(PK) AND attribute_exists(SK) AND #grant.#revision = :expectedRevision',
    );
    expect(update.input.ExpressionAttributeNames).toEqual({ '#grant': 'grant', '#revision': 'revision' });
    expect(update.input.ExpressionAttributeValues).toEqual({ ':expectedRevision': 1 });
  });

  it('maps stale conditional writes to a typed revision conflict', async () => {
    const memory = createMemoryClient();
    const repository = new WorkSharingRepository(memory.client, 'test-work-graph');
    await repository.putGrant(liveGrant(), null);

    await expect(repository.putGrant(
      liveGrant({ revision: 3, updatedAt: '2026-09-19T12:10:00.000Z' }),
      2,
    )).rejects.toMatchObject({
      name: 'WorkSharingConflictError',
      code: 'WORK_SHARING_REVISION_CONFLICT',
      grantId: 'grant-1',
      expectedRevision: 2,
    });
    await expect(repository.putGrant(
      liveGrant({ revision: 2, updatedAt: '2026-09-19T12:10:00.000Z' }),
      1,
    )).resolves.toMatchObject({ revision: 2 });
    expect(WorkSharingConflictError.prototype).toBeInstanceOf(Error);
  });

  it('denies an expired row synchronously even while DynamoDB still retains it', async () => {
    const memory = createMemoryClient();
    const repository = new WorkSharingRepository(memory.client, 'test-work-graph');
    await repository.putGrant(liveGrant(), null);
    await repository.putGrant(liveGrant({ id: 'explicit-expired', state: 'expired' }), null);

    expect(memory.rows.size).toBe(2);
    await expect(repository.getGrant('org-1', 'explicit-expired', NOW)).resolves.toBeNull();
    await expect(repository.getGrant('org-1', 'grant-1', EXPIRES_AT)).resolves.toBeNull();
    await expect(repository.listEffectiveGrants(
      'org-1',
      'owner-1',
      { viewerId: 'viewer-1', conversationIds: [] },
      EXPIRES_AT,
    )).resolves.toEqual([]);
    expect(memory.rows.size).toBe(2);
  });

  it('persists stopped state across repository instances and never lists it as effective', async () => {
    const memory = createMemoryClient();
    const firstProcess = new WorkSharingRepository(memory.client, 'test-work-graph');
    await firstProcess.putGrant(liveGrant(), null);
    await firstProcess.putGrant(liveGrant({
      state: 'stopped',
      revision: 2,
      updatedAt: '2026-09-19T12:15:00.000Z',
    }), 1);

    const restartedProcess = new WorkSharingRepository(memory.client, 'test-work-graph');
    await expect(restartedProcess.getGrant('org-1', 'grant-1', NOW)).resolves.toMatchObject({
      state: 'stopped',
      revision: 2,
    });
    await expect(restartedProcess.listEffectiveGrants(
      'org-1',
      'owner-1',
      { viewerId: 'viewer-1', conversationIds: [] },
      NOW,
    )).resolves.toEqual([]);
    await expect(restartedProcess.listOwnedGrants('org-1', 'owner-1', NOW)).resolves.toEqual([]);
  });

  it('lists only the authenticated owner current mutable grants', async () => {
    const memory = createMemoryClient();
    const repository = new WorkSharingRepository(memory.client, 'test-work-graph');
    await repository.putGrant(liveGrant(), null);
    await repository.putGrant(liveGrant({ id: 'paused', state: 'paused' }), null);
    await repository.putGrant(liveGrant({ id: 'stopped', state: 'stopped' }), null);
    await expect(repository.listOwnedGrants('org-1', 'owner-1', NOW)).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: 'grant-1', state: 'active' }),
        expect.objectContaining({ id: 'paused', state: 'paused' }),
      ]),
    );
    expect((await repository.listOwnedGrants('org-1', 'owner-1', NOW)).map((grant) => grant.id))
      .not.toContain('stopped');
    await expect(repository.listOwnedGrants('org-1', 'other-owner', NOW)).resolves.toEqual([]);
  });

  it('isolates organization and owner partitions and fails closed on a corrupted stored scope', async () => {
    const memory = createMemoryClient();
    const repository = new WorkSharingRepository(memory.client, 'test-work-graph');
    await repository.putGrant(liveGrant(), null);

    await expect(repository.getGrant('org-2', 'grant-1', NOW)).resolves.toBeNull();
    await expect(repository.listEffectiveGrants(
      'org-1',
      'owner-2',
      { viewerId: 'viewer-1', conversationIds: [] },
      NOW,
    )).resolves.toEqual([]);

    const row = [...memory.rows.values()][0];
    (row['grant'] as WorkSharingGrant).organizationId = 'org-2';
    await expect(repository.listEffectiveGrants(
      'org-1',
      'owner-1',
      { viewerId: 'viewer-1', conversationIds: [] },
      NOW,
    )).resolves.toEqual([]);
  });

  it('matches only server-resolved direct, conversation, or same-organization audiences', async () => {
    const memory = createMemoryClient();
    const repository = new WorkSharingRepository(memory.client, 'test-work-graph');
    const grants: WorkSharingGrant[] = [
      liveGrant({ id: 'direct', audience: { kind: 'people', personIds: ['viewer-1'] } }),
      liveGrant({ id: 'conversation', audience: { kind: 'conversation', conversationIds: ['room-1'] } }),
      liveGrant({ id: 'organization', audience: { kind: 'organization' } }),
    ];
    for (const grant of grants) await repository.putGrant(grant, null);

    const direct = await repository.listEffectiveGrants(
      'org-1', 'owner-1', { viewerId: 'viewer-1', conversationIds: [] }, NOW,
    );
    expect(direct.map((grant) => grant.id).sort()).toEqual(['direct', 'organization']);

    const conversation = await repository.listEffectiveGrants(
      'org-1', 'owner-1', { viewerId: 'viewer-2', conversationIds: ['room-1'] }, NOW,
    );
    expect(conversation.map((grant) => grant.id).sort()).toEqual(['conversation', 'organization']);

    const sameOrganization = await repository.listEffectiveGrants(
      'org-1', 'owner-1', { viewerId: 'viewer-3', conversationIds: [] }, NOW,
    );
    expect(sameOrganization.map((grant) => grant.id)).toEqual(['organization']);
    await expect(repository.listEffectiveGrants(
      'org-2', 'owner-1', { viewerId: 'viewer-3', conversationIds: [] }, NOW,
    )).resolves.toEqual([]);
  });

  it('retains the explicit history day range and evaluates its audience independently', async () => {
    const memory = createMemoryClient();
    const repository = new WorkSharingRepository(memory.client, 'test-work-graph');
    const history = historyGrant({ audience: { kind: 'people', personIds: ['history-viewer'] } });
    await repository.putGrant(history, null);

    await expect(repository.getGrant('org-1', 'history-1', NOW)).resolves.toEqual(history);
    const effective = await repository.listEffectiveGrants(
      'org-1',
      'owner-1',
      { viewerId: 'history-viewer', conversationIds: [] },
      NOW,
    );
    expect(effective).toEqual([history]);
    expect(effective[0]).toMatchObject({ dayFrom: '2026-09-17', dayThrough: '2026-09-19' });
  });

  it('uses the GSI only to discover a key, then authorizes from a consistent base read', async () => {
    const memory = createMemoryClient();
    const repository = new WorkSharingRepository(memory.client, 'test-work-graph');
    await repository.putGrant(liveGrant(), null);
    await repository.getGrant('org-1', 'grant-1', NOW);

    const lookup = memory.send.mock.calls.map(([command]) => command)
      .find((command): command is QueryCommand => command instanceof QueryCommand)!;
    expect(lookup.input).toMatchObject({
      TableName: 'test-work-graph',
      IndexName: 'work-graph-grant-lookup',
      KeyConditionExpression: 'grantLookupPk = :organization AND grantLookupSk = :grant',
      ExpressionAttributeValues: {
        ':organization': 'WGGRANT#org-1',
        ':grant': 'GRANT#grant-1',
      },
      Limit: 2,
    });
    const baseRead = memory.send.mock.calls.map(([command]) => command)
      .find((command): command is GetCommand => command instanceof GetCommand)!;
    expect(baseRead.input).toMatchObject({
      TableName: 'test-work-graph',
      Key: { PK: 'WGGRANT#org-1#owner-1', SK: 'GRANT#grant-1' },
      ConsistentRead: true,
    });

    const existing = [...memory.rows.values()][0];
    memory.rows.set('duplicate', { ...existing });
    await expect(repository.getGrant('org-1', 'grant-1', NOW)).resolves.toBeNull();
  });

  it('does not authorize an active GSI image after the base grant was paused', async () => {
    const memory = createMemoryClient();
    const repository = new WorkSharingRepository(memory.client, 'test-work-graph');
    await repository.putGrant(liveGrant(), null);
    const stale = [...memory.rows.values()][0];
    const current = { ...stale };
    current['grant'] = {
      ...(current['grant'] as WorkSharingGrant),
      state: 'paused',
      revision: 2,
      updatedAt: '2026-09-19T12:10:00.000Z',
    } satisfies WorkSharingGrant;

    memory.send.mockImplementation(async (command: PutCommand | QueryCommand | GetCommand) => {
      if (command instanceof QueryCommand) return { Items: [stale] };
      if (command instanceof GetCommand) return { Item: current };
      throw new Error('unexpected write');
    });
    await expect(repository.getGrant('org-1', 'grant-1', NOW)).resolves.toMatchObject({
      state: 'paused',
      revision: 2,
    });
  });

  it('does not authorize a page-one grant revoked before the next query page', async () => {
    const memory = createMemoryClient();
    const repository = new WorkSharingRepository(memory.client, 'test-work-graph');
    await repository.putGrant(liveGrant(), null);
    await repository.putGrant(liveGrant({ id: 'grant-2', selection: { nodeIds: ['node-2'], edgeIds: [], disclosure: 'details' } }), null);
    const first = memory.rows.get('WGGRANT#org-1#owner-1|GRANT#grant-1')!;
    const second = memory.rows.get('WGGRANT#org-1#owner-1|GRANT#grant-2')!;
    memory.send.mockImplementation(async (command: PutCommand | QueryCommand | GetCommand) => {
      if (command instanceof QueryCommand) {
        expect(command.input.ConsistentRead).toBe(true);
        if (!command.input.ExclusiveStartKey) {
          return { Items: [first], LastEvaluatedKey: { PK: first['PK'], SK: first['SK'] } };
        }
        memory.rows.set('WGGRANT#org-1#owner-1|GRANT#grant-1', {
          ...first,
          grant: liveGrant({ state: 'stopped', revision: 2, updatedAt: NOW }),
        });
        return { Items: [second] };
      }
      if (command instanceof GetCommand) {
        expect(command.input.ConsistentRead).toBe(true);
        const key = `${String(command.input.Key?.['PK'])}|${String(command.input.Key?.['SK'])}`;
        return { Item: memory.rows.get(key) };
      }
      throw new Error('unexpected write');
    });

    await expect(repository.listEffectiveGrants(
      'org-1', 'owner-1', { viewerId: 'viewer-1', conversationIds: [] }, NOW,
    )).resolves.toEqual([second['grant']]);
  });

  it('never returns unknown persisted properties', async () => {
    const memory = createMemoryClient();
    const repository = new WorkSharingRepository(memory.client, 'test-work-graph');
    await repository.putGrant(liveGrant(), null);
    const row = [...memory.rows.values()][0];
    (row['grant'] as unknown as Record<string, unknown>)['credential'] = 'secret';

    await expect(repository.getGrant('org-1', 'grant-1', NOW)).resolves.toBeNull();
  });
});
