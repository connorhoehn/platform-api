// Opt-in actual Dynamo expression acceptance in a uniquely owned local table.
// No primary/native runtime APIs or accepted seats are manufactured here.
import { DynamoDBClient, CreateTableCommand, DeleteTableCommand } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, type UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { VideoSessionRepository, VIDEO_SESSIONS_TABLE } from '../VideoSessionRepository';
import { VideoCallBindingConflict, type VideoSessionRecord, type VideoParticipantCallBinding } from '../VideoSessionStore';
const run = process.env.NATIVE_BINDING_ACCEPTANCE_ENDPOINT ? describe : describe.skip;
run('actual isolated Dynamo participant call binding', () => {
  let ddb: DynamoDBClient, docClient: DynamoDBDocumentClient, repo: VideoSessionRepository, created = false;
  const binding: VideoParticipantCallBinding = { callId: 'call-one', clientId: 'client-one', userId: 'alice', tenant: 'orgiq', epoch: 2,
    sessionId: 'session-one', channelArn: 'arn-one', lobbyName: 'orgiq:initiative:p-observability', boundAt: '2026-10-10T12:01:00Z' };
  const participant = { participantId: 'pid-one', userId: 'alice', clientId: 'client-one', displayName: 'Alice', joinedAt: '2026-10-10T12:00:00Z' };
  const row: VideoSessionRecord = { documentId: binding.lobbyName, sessionId: binding.sessionId, channelArn: binding.channelArn, streamKey: '', orgId: 'orgiq',
    startedBy: 'alice', status: 'active', startedAt: participant.joinedAt, participants: [participant], participantHistory: { 'pid-one': { ...participant } } };
  beforeAll(async () => {
    const endpoint = new URL(process.env.NATIVE_BINDING_ACCEPTANCE_ENDPOINT!);
    if (!['127.0.0.1', 'localhost'].includes(endpoint.hostname) || !process.env.DDB_TABLE_PREFIX?.startsWith('meeting_binding_acceptance_')) throw new Error('Unique local binding acceptance configuration required');
    ddb = new DynamoDBClient({ endpoint: endpoint.href, region: 'us-east-1', credentials: { accessKeyId: 'synthetic', secretAccessKey: 'synthetic' } });
    await ddb.send(new CreateTableCommand({ TableName: VIDEO_SESSIONS_TABLE, BillingMode: 'PAY_PER_REQUEST', KeySchema: [
      { AttributeName: 'documentId', KeyType: 'HASH' }, { AttributeName: 'sessionId', KeyType: 'RANGE' } ], AttributeDefinitions: [
      { AttributeName: 'documentId', AttributeType: 'S' }, { AttributeName: 'sessionId', AttributeType: 'S' } ] }));
    created = true; docClient = DynamoDBDocumentClient.from(ddb, { marshallOptions: { removeUndefinedValues: true } }); repo = new VideoSessionRepository(docClient);
  });
  afterAll(async () => { if (created) await ddb.send(new DeleteTableCommand({ TableName: VIDEO_SESSIONS_TABLE })); ddb?.destroy(); });
  test('first write, idempotent retry, conflicting call and connection leave all preserve immutable attendance', async () => {
    await repo.createSession(row);
    const bind = (value = binding) => repo.bindCallParticipant(row.documentId, row.sessionId, 'pid-one', row.channelArn, value);
    expect(await bind()).toEqual(binding);
    expect(await bind({ ...binding, boundAt: '2026-10-10T12:02:00Z' })).toEqual(binding);
    await expect(bind({ ...binding, callId: 'call-other' })).rejects.toBeInstanceOf(VideoCallBindingConflict);
    expect((await repo.getSession(row.documentId, row.sessionId))!.participantHistory!['pid-one']!.callBinding).toEqual(binding);
    await repo.removeParticipant(row.documentId, row.sessionId, 'pid-one');
    expect(await bind()).toBeNull();
    await repo.addOrReplaceParticipant(row.documentId, row.sessionId, { ...participant, participantId: 'pid-two', clientId: 'client-two' });
    expect(await bind()).toBeNull();
    expect(await repo.bindCallParticipant(row.documentId, row.sessionId, 'pid-two', row.channelArn, binding)).toBeNull();
    const next = { ...binding, clientId: 'client-two', callId: 'call-two', boundAt: '2026-10-10T12:03:00Z' };
    expect(await repo.bindCallParticipant(row.documentId, row.sessionId, 'pid-two', row.channelArn, next)).toEqual(next);
    const stored = (await repo.getSession(row.documentId, row.sessionId))!;
    expect(stored.participantHistory!['pid-one']!.callBinding).toEqual(binding);
    expect(stored.participantHistory!['pid-one']!.leftAt).toBeDefined();
    expect(stored.participantHistory!['pid-two']!.callBinding).toEqual(next);
  });
  test('actual conditional write rejects a participant replacement between read and write', async () => {
    const sessionId = 'session-race'; await repo.createSession({ ...row, sessionId });
    let changed = false;
    // Inject connection churn at the real AWS-driver boundary; the condition
    // is still evaluated by actual Dynamo, not a mocked expression evaluator.
    const driver = { send: async (command: UpdateCommand) => {
      if (!changed && command.input.UpdateExpression?.includes('#binding')) {
        changed = true;
        await repo.addOrReplaceParticipant(row.documentId, sessionId, { ...participant, participantId: 'pid-new' });
      }
      return docClient.send(command);
    } } as unknown as DynamoDBDocumentClient;
    const raced = new VideoSessionRepository(driver);
    expect(await raced.bindCallParticipant(row.documentId, sessionId, 'pid-one', row.channelArn, { ...binding, sessionId })).toBeNull();
    expect(changed).toBe(true);
    const stored = (await repo.getSession(row.documentId, sessionId))!;
    expect(stored.participants.map(p => p.participantId)).toEqual(['pid-new']);
    expect(stored.participantHistory!['pid-one']!.callBinding).toBeUndefined();
    expect(stored.participantHistory!['pid-one']!.leftAt).toBeDefined();
    expect(stored.participantHistory!['pid-new']!.callBinding).toBeUndefined();
  });
});
