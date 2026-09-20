import {
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  WorkCollaborationRepository,
  type WorkCollaborationRequest,
} from '../WorkCollaborationRepository';

const CREATED = '2026-09-19T12:00:00.000Z';
const NOW = '2026-09-19T12:30:00.000Z';

function request(overrides: Partial<WorkCollaborationRequest> = {}): WorkCollaborationRequest {
  return {
    schemaVersion: 1,
    id: 'request-1',
    organizationId: 'org-1',
    requesterId: 'viewer-1',
    ownerId: 'owner-1',
    target: { kind: 'node', id: 'node-1' },
    capability: 'control-terminal',
    state: 'requested',
    createdAt: CREATED,
    updatedAt: CREATED,
    expiresAt: '2026-09-19T13:00:00.000Z',
    revision: 1,
    ...overrides,
  };
}

function memoryClient(): {
  client: DynamoDBDocumentClient;
  rows: Map<string, Record<string, unknown>>;
} {
  const rows = new Map<string, Record<string, unknown>>();
  const send = jest.fn(async (command: PutCommand | QueryCommand) => {
    if (command instanceof PutCommand) {
      const item = command.input.Item as Record<string, unknown>;
      const key = `${String(item['PK'])}|${String(item['SK'])}`;
      const current = rows.get(key);
      if (command.input.ConditionExpression?.startsWith('attribute_not_exists')) {
        if (current) throw Object.assign(new Error('create conflict'), { name: 'ConditionalCheckFailedException' });
      } else {
        const revision = (current?.['request'] as { revision?: number } | undefined)?.revision;
        if (!current || revision !== command.input.ExpressionAttributeValues?.[':expectedRevision']) {
          throw Object.assign(new Error('update conflict'), { name: 'ConditionalCheckFailedException' });
        }
      }
      rows.set(key, item);
      return {};
    }
    const values = command.input.ExpressionAttributeValues ?? {};
    const all = [...rows.values()];
    if (command.input.IndexName) {
      return { Items: all.filter((item) => item['collaborationLookupPk'] === values[':organization']
        && item['collaborationLookupSk'] === values[':request']).slice(0, command.input.Limit) };
    }
    return { Items: all.filter((item) => item['PK'] === values[':partition']) };
  });
  return { client: { send } as unknown as DynamoDBDocumentClient, rows };
}

describe('WorkCollaborationRepository', () => {
  it('persists immutable capability requests with optimistic revisions', async () => {
    const memory = memoryClient();
    const repository = new WorkCollaborationRepository(memory.client, 'test-work-graph');
    await repository.putRequest(request(), null);
    await repository.putRequest(request({ state: 'accepted', revision: 2, updatedAt: NOW }), 1);
    await expect(repository.getRequest('org-1', 'request-1')).resolves.toMatchObject({
      capability: 'control-terminal',
      state: 'accepted',
      revision: 2,
    });
    await expect(repository.putRequest(
      request({ state: 'revoked', revision: 2, updatedAt: '2026-09-19T12:40:00.000Z' }),
      1,
    )).rejects.toMatchObject({
      code: 'WORK_COLLABORATION_REVISION_CONFLICT',
      requestId: 'request-1',
      expectedRevision: 1,
    });
  });

  it('finds only an accepted exact-capability grant before synchronous expiry', async () => {
    const memory = memoryClient();
    const repository = new WorkCollaborationRepository(memory.client, 'test-work-graph');
    await repository.putRequest(request({ state: 'accepted' }), null);

    await expect(repository.findAcceptedRequest(
      'org-1', 'owner-1', 'viewer-1', { kind: 'node', id: 'node-1' }, 'control-terminal', NOW,
    )).resolves.toMatchObject({ id: 'request-1', state: 'accepted' });
    await expect(repository.findAcceptedRequest(
      'org-1', 'owner-1', 'viewer-1', { kind: 'node', id: 'node-1' }, 'observe-terminal', NOW,
    )).resolves.toBeNull();
    await expect(repository.findAcceptedRequest(
      'org-1', 'owner-1', 'viewer-1', { kind: 'node', id: 'node-1' }, 'control-terminal', '2026-09-19T13:00:00.000Z',
    )).resolves.toBeNull();
  });

  it('lists only live incoming requests for the authenticated owner', async () => {
    const memory = memoryClient();
    const repository = new WorkCollaborationRepository(memory.client, 'test-work-graph');
    await repository.putRequest(request(), null);
    await repository.putRequest(request({ id: 'accepted-1', state: 'accepted' }), null);
    await repository.putRequest(request({ id: 'revoked-1', state: 'revoked' }), null);
    const owned = await repository.listOwnedRequests('org-1', 'owner-1', NOW);
    expect(owned).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: 'request-1', state: 'requested' }),
      expect.objectContaining({ id: 'accepted-1', state: 'accepted' }),
    ]));
    expect(owned.map((item) => item.id)).not.toContain('revoked-1');
    await expect(repository.listOwnedRequests('org-1', 'other-owner', NOW)).resolves.toEqual([]);
  });

  it('fails closed on cross-org lookup and unknown persisted fields', async () => {
    const memory = memoryClient();
    const repository = new WorkCollaborationRepository(memory.client, 'test-work-graph');
    await repository.putRequest(request(), null);
    await expect(repository.getRequest('org-2', 'request-1')).resolves.toBeNull();
    const row = [...memory.rows.values()][0];
    (row['request'] as unknown as Record<string, unknown>)['credential'] = 'secret';
    await expect(repository.getRequest('org-1', 'request-1')).resolves.toBeNull();
  });
});
