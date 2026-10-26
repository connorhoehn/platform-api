import { CreateTableCommand, DeleteTableCommand, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { HANGOUT_ROOMS_TABLE, HangoutRoomRepository } from '../HangoutRoomRepository.js';
import { readHangoutPlan } from '../../lib/hangout-plan.js';

const run = process.env.HANGOUT_PLAN_ACCEPTANCE_ENDPOINT ? describe : describe.skip;
run('saved layout compare-and-set in actual DynamoDB', () => {
  let clients: DynamoDBClient[] = [];
  let documents: DynamoDBDocumentClient[];
  let repositories: HangoutRoomRepository[];
  let created = false;
  const key = { orgId: 'isolated-org', roomId: 'isolated-room' };
  const read = async () => (await documents[0]!.send(new GetCommand({ TableName: HANGOUT_ROOMS_TABLE, Key: key, ConsistentRead: true }))).Item;
  beforeAll(async () => {
    const endpoint = new URL(process.env.HANGOUT_PLAN_ACCEPTANCE_ENDPOINT!);
    if (!['127.0.0.1', 'localhost'].includes(endpoint.hostname)
      || !process.env.DDB_TABLE_PREFIX?.startsWith('chassis_c8_rooms_')) throw new Error('Unique local room acceptance configuration required');
    clients = [0, 1].map(() => new DynamoDBClient({ endpoint: endpoint.href, region: 'us-east-1',
      credentials: { accessKeyId: 'synthetic', secretAccessKey: 'synthetic' } }));
    documents = clients.map(client => DynamoDBDocumentClient.from(client, { marshallOptions: { removeUndefinedValues: true } }));
    repositories = documents.map(client => new HangoutRoomRepository(client));
    await clients[0]!.send(new CreateTableCommand({ TableName: HANGOUT_ROOMS_TABLE, BillingMode: 'PAY_PER_REQUEST',
      KeySchema: [{ AttributeName: 'orgId', KeyType: 'HASH' }, { AttributeName: 'roomId', KeyType: 'RANGE' }],
      AttributeDefinitions: [{ AttributeName: 'orgId', AttributeType: 'S' }, { AttributeName: 'roomId', AttributeType: 'S' }],
    }));
    created = true;
  });
  beforeEach(async () => {
    await documents[0]!.send(new PutCommand({ TableName: HANGOUT_ROOMS_TABLE, Item: { ...key, slug: 'design', name: 'Design',
      createdBy: 'owner', createdAt: '2026-10-26T00:00:00Z', visibility: 'org', state: 'active',
      settings: { audioOnly: false, recordingEnabled: false, softCap: 25 }, lastActiveAt: '2026-10-26T00:00:00Z' } }));
  });
  afterAll(async () => {
    try { if (created) await clients[0]!.send(new DeleteTableCommand({ TableName: HANGOUT_ROOMS_TABLE })); }
    finally { for (const client of clients) client.destroy(); }
  });

  test.each([0, 1])('two independent clients racing expectedVersion=%s have one winner; stale writes change nothing', async expectedVersion => {
    const input = { expectedVersion, mode: 'assigned', allowReturnToMain: true,
      rooms: [{ id: 'design', name: 'Design', members: ['alice'] }] };
    if (expectedVersion === 1) await repositories[0]!.updateRoom(key.orgId, key.roomId,
      { plan: readHangoutPlan({ ...input, expectedVersion: 0 }, 'owner') });
    const plans = ['left', 'right'].map(actor => readHangoutPlan(input, actor));
    const results = await Promise.allSettled(repositories.map((repository, index) =>
      repository.updateRoom(key.orgId, key.roomId, { plan: plans[index], name: `winner-${index}` })));
    const winners = results.flatMap((result, index) => result.status === 'fulfilled' ? [index] : []);
    expect(winners).toHaveLength(1);
    const loser = results.find(result => result.status === 'rejected') as PromiseRejectedResult;
    expect(loser.reason.name).toBe('ConditionalCheckFailedException');
    const stored = await read();
    expect(stored?.plan).toEqual(plans[winners[0]!]);
    expect(stored?.name).toBe(`winner-${winners[0]}`);
    await expect(repositories[0]!.updateRoom(key.orgId, key.roomId, { plan: plans[0], name: 'stale overwrite' }))
      .rejects.toMatchObject({ name: 'ConditionalCheckFailedException' });
    expect(await read()).toEqual(stored);
  });

  test('a plan save cannot manufacture a missing room or another tenant row', async () => {
    const plan = readHangoutPlan({ expectedVersion: 0, mode: 'self-select', allowReturnToMain: false, rooms: [] }, 'owner');
    for (const [orgId, roomId] of [[key.orgId, 'absent'], ['other-org', key.roomId]]) {
      await expect(repositories[0]!.updateRoom(orgId!, roomId!, { plan })).rejects.toMatchObject({ name: 'ConditionalCheckFailedException' });
    }
    expect((await read())?.plan).toBeUndefined();
  });
});
