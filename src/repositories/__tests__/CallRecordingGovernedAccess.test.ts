import { BatchGetCommand, DeleteCommand, GetCommand, PutCommand, QueryCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { CallRecordingRepository, type CallRecording } from '../CallRecordingRepository';

jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));
jest.mock('../../work-graph/sources/recordingLocator', () => ({ recordingLocator: { noteWrite: jest.fn() } }));

const primaryTable = 'test-video-call-recordings', indexTable = 'test-video-call-recording-participants';
function database() {
  const tables = new Map<string, Array<Record<string, unknown>>>();
  const matches = (row: Record<string, unknown>, key: Record<string, unknown>) => Object.entries(key).every(([field, value]) => row[field] === value);
  const send = jest.fn(async (command: unknown) => {
    const input = (command as { input: any }).input;
    const rows = tables.get(input.TableName) ?? [];
    if (command instanceof GetCommand) return { Item: rows.find(row => matches(row, input.Key)) };
    if (command instanceof PutCommand) {
      const row = structuredClone(input.Item);
      const key = input.TableName === indexTable ? { userId: row.userId, startedAtRecordingId: row.startedAtRecordingId }
        : { callId: row.callId, recordingId: row.recordingId };
      tables.set(input.TableName, [...rows.filter(existing => !matches(existing, key)), row]); return {};
    }
    if (command instanceof DeleteCommand) { tables.set(input.TableName, rows.filter(row => !matches(row, input.Key))); return {}; }
    if (command instanceof QueryCommand) return { Items: rows.filter(row => input.ExpressionAttributeValues[':r'] !== undefined
      ? row.recordingId === input.ExpressionAttributeValues[':r'] : row.userId === input.ExpressionAttributeValues[':u']) };
    if (command instanceof BatchGetCommand) return { Responses: Object.fromEntries(Object.entries(input.RequestItems as Record<string, any>)
      .map(([table, request]) => [table, (tables.get(table) ?? []).filter(row => request.Keys.some((key: Record<string, unknown>) => matches(row, key)))])) };
    throw new Error('Unexpected native database command');
  });
  return { send, client: { send } as unknown as DynamoDBDocumentClient };
}

function recording(): Omit<CallRecording, 'ttl'> {
  return { callId: 'canonical-call', recordingId: 'exact-recording', lobbyName: 'orgiq:initiative:p-one', channelArn: 'channel-one',
    recordingGoverned: true, callBindingStatus: 'bound', startedAt: '2026-10-10T10:00:00Z', endedAt: '2026-10-10T10:01:00Z',
    durationMs: 60_000, destination: 'local', totalBytes: 1234, segments: [], startedBy: 'old-starter',
    participants: [{ userId: 'old-starter' }, { userId: 'still-recorded' }],
    tracks: [{ userId: 'old-starter', participantId: 'old-seat', tokenParticipantId: 'old-seat', key: 'old.mp4', url: '', size: 600 },
      { userId: 'still-recorded', participantId: 'current-seat', tokenParticipantId: 'current-seat', key: 'current.mp4', url: '', size: 634 }] };
}
function smaller(row: Omit<CallRecording, 'ttl'>): Omit<CallRecording, 'ttl'> {
  // Even a stale caller/display projection must not restore the old owner.
  return { ...row, participants: [{ userId: 'still-recorded' }], tracks: row.tracks!.slice(1) };
}

it('removes stale governed fan-out on same-startedAt replay and excludes an unrecorded initiator', async () => {
  const db = database(), repo = new CallRecordingRepository(db.client), row = recording();
  await repo.putRecording(row);
  expect(await repo.resolveForUser('old-starter', row.recordingId)).toMatchObject({ isParticipant: true });
  await repo.putRecording(smaller(row));
  expect(await repo.resolveForUser('old-starter', row.recordingId)).toMatchObject({ isParticipant: false });
  expect(await repo.resolveForUser('still-recorded', row.recordingId)).toMatchObject({ isParticipant: true });
  const deletes = db.send.mock.calls.map(([command]) => command).filter(command => command instanceof DeleteCommand) as DeleteCommand[];
  expect(deletes.map(command => command.input)).toContainEqual({ TableName: indexTable,
    Key: { userId: 'old-starter', startedAtRecordingId: `${row.startedAt}#${row.recordingId}` } });
  expect(db.send.mock.calls.some(([command]) => command instanceof GetCommand
    && command.input.TableName === primaryTable && command.input.ConsistentRead === true)).toBe(true);
});

it('denies stale index detail and list reads from the current canonical primary, preserving untag denial', async () => {
  const db = database(), repo = new CallRecordingRepository(db.client), row = recording();
  await repo.putRecording(row);
  // Simulate an incomplete/lost secondary-index cleanup after a primary write.
  await db.client.send(new PutCommand({ TableName: primaryTable, Item: { ...smaller(row), ttl: 2_000_000_000 } }));
  expect(await repo.hasParticipantRow('old-starter', row.startedAt, row.recordingId)).toBe(true);
  expect(await repo.resolveForUser('old-starter', row.recordingId)).toMatchObject({ isParticipant: false });
  expect((await repo.listByUser('old-starter')).items).toEqual([]);
  expect((await repo.listByUser('still-recorded')).items).toHaveLength(1);
  await db.client.send(new DeleteCommand({ TableName: indexTable,
    Key: { userId: 'still-recorded', startedAtRecordingId: `${row.startedAt}#${row.recordingId}` } }));
  expect(await repo.resolveForUser('still-recorded', row.recordingId)).toMatchObject({ isParticipant: false });
});

it('fails new governed writes/read lists closed when canonical storage is unavailable', async () => {
  const db = database(), repo = new CallRecordingRepository(db.client), row = recording();
  await repo.putRecording(row);
  const nativeSend = db.send.getMockImplementation()!;
  db.send.mockImplementation(async command => {
    if (command instanceof GetCommand && command.input.TableName === primaryTable) throw new Error('primary-unavailable');
    if (command instanceof BatchGetCommand) throw new Error('primary-unavailable');
    return nativeSend(command);
  });
  await expect(repo.putRecording(smaller(row))).rejects.toThrow('primary-unavailable');
  await expect(repo.resolveForUser('old-starter', row.recordingId)).rejects.toThrow('primary-unavailable');
  expect((await repo.listByUser('old-starter')).items).toEqual([]);
});

it('cannot bypass current governed entitlement with a stale legacy GSI projection or unmarked old index', async () => {
  const db = database(), repo = new CallRecordingRepository(db.client), row = recording();
  const legacy = { ...row }; delete legacy.callBindingStatus; delete legacy.recordingGoverned;
  await repo.putRecording(legacy);
  // The primary has advanced; GSI and fan-out have not caught up yet.
  await db.client.send(new PutCommand({ TableName: primaryTable, Item: { ...smaller(row), ttl: 2_000_000_000 } }));
  const nativeSend = db.send.getMockImplementation()!;
  db.send.mockImplementation(async command => command instanceof QueryCommand && command.input.TableName === primaryTable
    ? { Items: [{ ...legacy, ttl: 2_000_000_000 }] } : nativeSend(command));
  expect(await repo.resolveForUser('old-starter', row.recordingId)).toMatchObject({ isParticipant: false });
  expect((await repo.listByUser('old-starter')).items).toEqual([]);
  db.send.mockImplementation(async command => {
    if (command instanceof BatchGetCommand) throw new Error('current-primary-unavailable');
    return nativeSend(command);
  });
  expect((await repo.listByUser('old-starter')).items).toEqual([]);
});

it('does not rewrite historical index authorization or initiator behavior', async () => {
  const db = database(), repo = new CallRecordingRepository(db.client), row = recording();
  delete row.callBindingStatus; delete row.recordingGoverned;
  await repo.putRecording(row);
  await repo.putRecording(smaller(row));
  expect(await repo.resolveForUser('old-starter', row.recordingId)).toMatchObject({ isParticipant: true });
  expect((await repo.listByUser('old-starter')).items).toHaveLength(1);
  expect(db.send.mock.calls.filter(([command]) => command instanceof DeleteCommand)).toHaveLength(0);
});
