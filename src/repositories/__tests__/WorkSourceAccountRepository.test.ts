import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import {
  WorkSourceAccountConflictError,
  WorkSourceAccountRepository,
  type WorkSourceAccountMapping,
} from '../WorkSourceAccountRepository';

function mapping(overrides: Partial<WorkSourceAccountMapping> = {}): WorkSourceAccountMapping {
  return {
    source: 'cloud-compute',
    producerServiceId: 'aws-agentcore',
    producerOrganizationId: 'source-org-1',
    sourceSubject: 'cognito:user-1',
    organizationId: 'platform-org-1',
    actorId: 'user-1',
    revision: 1,
    updatedAt: '2026-09-19T20:00:00.000Z',
    ...overrides,
  };
}

describe('WorkSourceAccountRepository', () => {
  it('resolves only an exact producer/source/subject join', async () => {
    const stored = mapping();
    const send = jest.fn(async (command: GetCommand) => ({
      Item: {
        PK: 'WGACCOUNT#cloud-compute#source-org-1',
        SK: 'SUBJECT#cognito:user-1',
        entityType: 'work-source-account',
        mapping: stored,
      },
    }));
    const repository = new WorkSourceAccountRepository(
      { send } as unknown as DynamoDBDocumentClient,
      'work-graph-test',
    );

    await expect(repository.resolve(
      'cloud-compute', 'aws-agentcore', 'source-org-1', 'cognito:user-1',
    )).resolves.toEqual({ organizationId: 'platform-org-1', actorId: 'user-1' });
    expect((send.mock.calls[0][0] as GetCommand).input).toMatchObject({
      TableName: 'work-graph-test',
      ConsistentRead: true,
      Key: { PK: 'WGACCOUNT#cloud-compute#source-org-1', SK: 'SUBJECT#cognito:user-1' },
    });
  });

  it('fails closed for corrupt or mismatched producer rows', async () => {
    const send = jest.fn(async () => ({
      Item: {
        PK: 'WGACCOUNT#cloud-compute#source-org-1',
        SK: 'SUBJECT#cognito:user-1',
        entityType: 'work-source-account',
        mapping: mapping({ producerServiceId: 'impostor-service' }),
      },
    }));
    const repository = new WorkSourceAccountRepository({ send } as unknown as DynamoDBDocumentClient);
    await expect(repository.resolve(
      'cloud-compute', 'aws-agentcore', 'source-org-1', 'cognito:user-1',
    )).resolves.toBeNull();
  });

  it('uses optimistic create/update conditions and maps stale writes', async () => {
    const send = jest.fn()
      .mockResolvedValueOnce({})
      .mockRejectedValueOnce(Object.assign(new Error('stale'), { name: 'ConditionalCheckFailedException' }));
    const repository = new WorkSourceAccountRepository(
      { send } as unknown as DynamoDBDocumentClient,
      'work-graph-test',
    );
    await repository.put(mapping(), null);
    expect((send.mock.calls[0][0] as PutCommand).input.ConditionExpression)
      .toBe('attribute_not_exists(PK) AND attribute_not_exists(SK)');
    await expect(repository.put(mapping({ revision: 2 }), 1))
      .rejects.toBeInstanceOf(WorkSourceAccountConflictError);
  });

  it('creates only a same-subject platform join and refuses to repoint a conflicting row', async () => {
    const existing = mapping({
      source: 'pipeline', producerServiceId: 'platform-api', producerOrganizationId: 'org-1',
      sourceSubject: 'user-1', organizationId: 'org-1', actorId: 'someone-else',
    });
    const send = jest.fn()
      .mockRejectedValueOnce(Object.assign(new Error('exists'), { name: 'ConditionalCheckFailedException' }))
      .mockResolvedValueOnce({
        Item: {
          PK: 'WGACCOUNT#pipeline#org-1', SK: 'SUBJECT#user-1',
          entityType: 'work-source-account', mapping: existing,
        },
      });
    const repository = new WorkSourceAccountRepository({ send } as unknown as DynamoDBDocumentClient);
    await expect(repository.ensureVerifiedPrincipal({
      source: 'pipeline', organizationId: 'org-1', subject: 'user-1', updatedAt: '2026-09-19T20:00:00.000Z',
    })).resolves.toBe(false);
    expect((send.mock.calls[0][0] as PutCommand).input.Item).toMatchObject({
      mapping: expect.objectContaining({ actorId: 'user-1', sourceSubject: 'user-1' }),
    });
  });

  it('allows only the fixed producer for externally verified source principals', async () => {
    const send = jest.fn().mockResolvedValue({});
    const repository = new WorkSourceAccountRepository({ send } as unknown as DynamoDBDocumentClient);
    await expect(repository.ensureVerifiedSourcePrincipal({
      source: 'conversation',
      producerServiceId: 'websocket-gateway',
      organizationId: 'org-1',
      subject: 'user-1',
    })).resolves.toBe(true);
    expect((send.mock.calls[0][0] as PutCommand).input.Item).toMatchObject({
      mapping: expect.objectContaining({
        producerServiceId: 'websocket-gateway',
        organizationId: 'org-1',
        actorId: 'user-1',
        sourceSubject: 'user-1',
      }),
    });

    await expect(repository.ensureVerifiedSourcePrincipal({
      source: 'conversation',
      producerServiceId: 'aws-agentcore',
      organizationId: 'org-1',
      subject: 'user-1',
    })).resolves.toBe(false);
    expect(send).toHaveBeenCalledTimes(1);
  });
});
