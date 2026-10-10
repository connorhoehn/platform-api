// Real native resolve/persist steps and participant-index writer: Leave must
// not remove replay entitlement, and a reused channel must not grant outsiders.
import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { PipelineNode } from 'distributed-core/applications/pipeline';
import { MemoryVideoSessionStore } from '../../repositories/MemoryVideoSessionStore';
import { CallRecordingRepository } from '../../repositories/CallRecordingRepository';
import type { VideoSessionRecord } from '../../repositories/VideoSessionStore';

let mockSessions: MemoryVideoSessionStore;
let mockRecordings: CallRecordingRepository;
jest.mock('../../repositories', () => ({
  videoSessionRepo: { findByChannelArn: (arn: string) => mockSessions.findByChannelArn(arn) },
  callRecordingRepo: { putRecording: (row: never) => mockRecordings.putRecording(row) },
  callReactionRepo: {},
}));
jest.mock('../../lib/redis-client', () => ({ getRedisClient: jest.fn().mockResolvedValue(null) }));
jest.mock('../../lib/aws-clients', () => ({ ddbClient: { send: jest.fn().mockResolvedValue({ Items: [] }) }, docClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));
jest.mock('../../gateway/GatewayServiceClient', () => ({ postChatMessage: jest.fn(), PIPELINE_IDENTITY: { userId: 'pipeline', displayName: 'Pipeline' } }));
jest.mock('../../work-graph/sources/recordingLocator', () => ({ recordingLocator: { noteWrite: jest.fn() } }));
jest.mock('../../work-graph/sources/platformPublisher', () => ({ platformWorkGraphSourcePublisher: {
  meeting: jest.fn().mockResolvedValue({ published: false, reason: 'isolated-unmapped-fixture' }),
} }));

import { buildRecordingPipelineHandlers, KINDS } from '../recordingPipelines';
import { recordingSessionParticipants } from '../../lib/recording-truth';
import { ddbClient } from '../../lib/aws-clients';

function recordingClient(): DynamoDBDocumentClient {
  const tables = new Map<string, Array<Record<string, unknown>>>();
  const matches = (row: Record<string, unknown>, key: Record<string, unknown>) => Object.entries(key).every(([field, value]) => row[field] === value);
  return { send: jest.fn(async (command: { constructor: { name: string }; input: any }) => {
    const input = command.input;
    const rows = tables.get(input.TableName) ?? [];
    if (command.constructor.name === 'GetCommand') return { Item: rows.find(row => matches(row, input.Key)) };
    if (command.constructor.name === 'PutCommand') {
      const row = input.Item;
      const key = row.userId ? { userId: row.userId, startedAtRecordingId: row.startedAtRecordingId }
        : { callId: row.callId, recordingId: row.recordingId };
      tables.set(input.TableName, [...rows.filter(existing => !matches(existing, key)), structuredClone(row)]);
      return {};
    }
    if (command.constructor.name === 'QueryCommand') return { Items: rows.filter(row => row.recordingId === input.ExpressionAttributeValues[':r']) };
    throw new Error(`Unexpected native recording command ${command.constructor.name}`);
  }) } as unknown as DynamoDBDocumentClient;
}

const ARN = 'arn:local:ivs:channel/reused';
const LOBBY = 'orgiq:initiative:p-observability';
const START = '2026-10-09T19:41:00.000Z', END = '2026-10-09T19:42:00.000Z';
const vera = { userId: 'u-vera', participantId: 'opaque-vera-leg', displayName: 'Vera Green', joinedAt: START };
// Deliberately misleading spelling: its owner comes from the authenticated
// join, never from a participantId prefix.
const nora = { userId: 'u-nora', participantId: 'unrelated-user-looking-leg', displayName: 'Nora DaSilva', joinedAt: START };
const node = { id: 'test', type: 'test', position: { x: 0, y: 0 }, data: { type: 'test' } } as unknown as PipelineNode;

function session(id: string, startedAt = START): VideoSessionRecord {
  return { documentId: LOBBY, sessionId: id, channelArn: ARN, streamKey: '', status: 'active', startedAt, startedBy: 'u-vera', participants: [] };
}

beforeEach(() => {
  jest.clearAllMocks();
  (ddbClient.send as jest.Mock).mockReset().mockResolvedValue({ Items: [] });
  mockSessions = new MemoryVideoSessionStore();
  mockRecordings = new CallRecordingRepository(recordingClient());
});

it.each(['orgiq:initiative:p-observability', 'assessment:team:TEAM-005', 'assessment:doc:one'])('does not use the latest lobby signaling call for governed %s', async lobby => {
  await mockSessions.createSession({ ...session('actual'), documentId: lobby, orgId: 'org-one',
    participantHistory: { [vera.participantId]: vera, [nora.participantId]: nora } });
  (ddbClient.send as jest.Mock).mockResolvedValueOnce({ Items: [{ callId: { S: 'unrelated-newest-call' } }] });
  const resolved = await buildRecordingPipelineHandlers().get(KINDS.resolve)!.execute(node, {
    arn: ARN, recordingId: 'actual-recording', callId: 'untrusted-call', detail: { startedAt: START, endedAt: END,
      tracks: [vera, nora].map(actor => ({ participantId: actor.participantId, userId: actor.userId })) },
  }, {} as never);
  expect(resolved.output).toMatchObject({ callId: `unknown-${ARN}-actual-recording`, callBindingStatus: 'unresolved',
    callBindingReason: 'missing-recorded-participant', sessionId: 'actual' });
  expect(ddbClient.send).not.toHaveBeenCalled();
});

it('uses only the immutable canonical native seats after leave and persists association confidence', async () => {
  await mockSessions.createSession({ ...session('actual'), orgId: 'org-one' });
  for (const actor of [vera, nora]) {
    const clientId = `tab-${actor.participantId}`;
    await mockSessions.addOrReplaceParticipant(LOBBY, 'actual', { ...actor, clientId });
    await mockSessions.bindCallParticipant(LOBBY, 'actual', actor.participantId, ARN, {
      callId: 'canonical-adopted-call', clientId, userId: actor.userId, tenant: 'org-one', epoch: 1,
      sessionId: 'actual', channelArn: ARN, lobbyName: LOBBY, boundAt: START,
    });
    await mockSessions.removeParticipant(LOBBY, 'actual', actor.participantId);
  }
  await mockSessions.endSession(LOBBY, 'actual', END);
  const context = { arn: ARN, recordingId: 'canonical-recording', detail: { startedAt: START, endedAt: END,
    durationMs: 60_000, tracks: [...[vera, nora].map(actor => ({ participantId: actor.participantId, tokenParticipantId: actor.participantId, userId: actor.userId })),
      { participantId: 'opaque-independent-screen-transport', tokenParticipantId: nora.participantId, userId: nora.userId }] } };
  const handlers = buildRecordingPipelineHandlers();
  const resolved = await handlers.get(KINDS.resolve)!.execute(node, context, {} as never);
  expect(resolved.output).toMatchObject({ callId: 'canonical-adopted-call', callBindingStatus: 'bound', sessionId: 'actual' });
  expect(ddbClient.send).not.toHaveBeenCalled();
  await handlers.get(KINDS.persist)!.execute(node, { ...context, ...resolved.output as Record<string, unknown> }, {} as never);
  const row = await mockRecordings.findByRecordingId('canonical-recording');
  expect(row).toMatchObject({ callId: 'canonical-adopted-call', callBindingStatus: 'bound', participantCount: 2, recordingGoverned: true });
  expect(row!.tracks).toHaveLength(3);
});

it('keeps an explicit legacy signaling lookup outside the governed tenants', async () => {
  await mockSessions.createSession({ ...session('actual'), documentId: 'dm:u-vera:u-nora',
    participantHistory: { [vera.participantId]: vera, [nora.participantId]: nora } });
  (ddbClient.send as jest.Mock).mockResolvedValueOnce({ Items: [{ callId: { S: 'legacy-call' } }] });
  const resolved = await buildRecordingPipelineHandlers().get(KINDS.resolve)!.execute(node, { arn: ARN, recordingId: 'legacy-recording',
    detail: { startedAt: START, endedAt: END, tracks: [{ participantId: vera.participantId }] } }, {} as never);
  expect(resolved.output).toMatchObject({ callId: 'legacy-call', callBindingStatus: 'legacy', callBindingReason: 'legacy-lobby-lookup' });
  expect(ddbClient.send).toHaveBeenCalledTimes(1);
});

it('does not grant governed replay access from a spoofable recorded pid without a native token user', async () => {
  await mockSessions.createSession({ ...session('actual'), orgId: 'org-one',
    participantHistory: { [vera.participantId]: vera, [nora.participantId]: nora } });
  const context = { arn: ARN, recordingId: 'no-trusted-user', detail: { startedAt: START, endedAt: END,
    tracks: [{ participantId: vera.participantId }], durationMs: 60_000 } };
  const handlers = buildRecordingPipelineHandlers();
  const resolved = await handlers.get(KINDS.resolve)!.execute(node, context, {} as never);
  expect(resolved.output).toMatchObject({ callBindingStatus: 'unresolved', recordingGoverned: true, startedBy: null, participants: [] });
  await handlers.get(KINDS.persist)!.execute(node, { ...context, ...resolved.output as Record<string, unknown>,
    // Historical display fields cannot restore an entitlement after resolve.
    startedBy: vera.userId, participants: [vera, nora] }, {} as never);
  expect(await mockRecordings.findByRecordingId(context.recordingId)).toMatchObject({ participants: [], participantCount: 0 });
  for (const user of [vera.userId, nora.userId, vera.participantId]) {
    expect(await mockRecordings.resolveForUser(user, context.recordingId)).toMatchObject({ isParticipant: false });
  }
});

it('does not grant an old session starter or a mismatched pid owner access to another recorded user', async () => {
  await mockSessions.createSession({ ...session('actual'), orgId: 'org-one',
    participantHistory: { [vera.participantId]: vera, [nora.participantId]: nora } });
  const context = { arn: ARN, recordingId: 'mismatched-trusted-user', detail: { startedAt: START, endedAt: END,
    tracks: [{ participantId: vera.participantId, userId: nora.userId }], durationMs: 60_000 } };
  const handlers = buildRecordingPipelineHandlers();
  const resolved = await handlers.get(KINDS.resolve)!.execute(node, context, {} as never);
  expect(resolved.output).toMatchObject({ callBindingStatus: 'unresolved', startedBy: null });
  expect((resolved.output as { participants: Array<{ userId: string }> }).participants.map(person => person.userId)).toEqual([nora.userId]);
  await handlers.get(KINDS.persist)!.execute(node, { ...context, ...resolved.output as Record<string, unknown>,
    startedBy: vera.userId, participants: [vera, nora] }, {} as never);
  expect(await mockRecordings.findByRecordingId(context.recordingId)).toMatchObject({ participants: [expect.objectContaining({ userId: nora.userId })], participantCount: 1 });
  expect(await mockRecordings.resolveForUser(nora.userId, context.recordingId)).toMatchObject({ isParticipant: true });
  expect(await mockRecordings.resolveForUser(vera.userId, context.recordingId)).toMatchObject({ isParticipant: false });
});

it('two authenticated actors join, leave, and retain native recording index authorization; other sessions do not', async () => {
  // Earlier and newer sessions reuse the SAME lobby/channel; neither may
  // supply the recording owner or participant authorization for this window.
  await mockSessions.createSession({ ...session('previous', '2026-10-09T19:00:00Z'), status: 'ended', endedAt: '2026-10-09T19:10:00Z', startedBy: 'previous-user',
    participantHistory: { 'previous-leg': { userId: 'previous-user', participantId: 'previous-leg', displayName: 'Previous', joinedAt: '2026-10-09T19:00:00Z' } } });
  await mockSessions.createSession({ ...session('future', '2026-10-09T19:50:00Z'), startedBy: 'future-user',
    participantHistory: { 'future-leg': { userId: 'future-user', participantId: 'future-leg', displayName: 'Future', joinedAt: '2026-10-09T19:50:00Z' } } });
  await mockSessions.createSession(session('actual'));
  await Promise.all([vera, nora].map(actor => mockSessions.addOrReplaceParticipant(LOBBY, 'actual', actor)));
  await Promise.all([vera, nora].map(actor => mockSessions.removeParticipant(LOBBY, 'actual', actor.participantId)));
  await mockSessions.endSession(LOBBY, 'actual', END);
  expect((await mockSessions.getSession(LOBBY, 'actual'))!.participants).toEqual([]);

  const ctx: Record<string, unknown> = { arn: ARN, recordingId: 'actual-recording', detail: {
    startedAt: START, endedAt: END, durationMs: 60_000,
    tracks: [vera, nora].map(actor => ({ participantId: actor.participantId, userId: actor.userId, durationMs: 60_000, startedAtMs: 0, endedAtMs: 60_000 })),
  } };
  const handlers = buildRecordingPipelineHandlers();
  const resolved = await handlers.get(KINDS.resolve)!.execute(node, ctx, {} as never);
  expect(resolved.status).toBe('completed');
  expect(resolved.output).toMatchObject({ sessionId: 'actual', startedBy: 'u-vera' });
  Object.assign(ctx, resolved.output);
  const persisted = await handlers.get(KINDS.persist)!.execute(node, ctx, {} as never);
  expect(persisted.status).toBe('completed');
  for (const user of ['u-vera', 'u-nora']) {
    expect(await mockRecordings.resolveForUser(user, 'actual-recording')).toMatchObject({ isParticipant: true });
  }
  for (const user of ['previous-user', 'future-user', 'unrelated-user', 'unrelated-user-looking-leg']) {
    expect(await mockRecordings.resolveForUser(user, 'actual-recording')).toMatchObject({ isParticipant: false });
  }
  const row = await mockRecordings.findByRecordingId('actual-recording');
  expect(row!.participants.map(actor => actor.userId).sort()).toEqual(['u-nora', 'u-vera']);
});

it('an earlier attendee of the same session cannot gain a later recording without a matching recorded leg', () => {
  const source = { ...session('actual'), participants: [], participantHistory: {
    [vera.participantId]: vera,
    'earlier-leg': { userId: 'earlier-user', participantId: 'earlier-leg', joinedAt: '2026-10-09T19:00:00Z', leftAt: '2026-10-09T19:05:00Z' },
  } };
  expect(recordingSessionParticipants(source, { startedAt: START, endedAt: END, tracks: [{ participantId: vera.participantId }] }).map(p => p.userId)).toEqual(['u-vera']);
  expect(recordingSessionParticipants(source, { startedAt: START, endedAt: END }).map(p => p.userId)).toEqual(['u-vera']);
});

it('missing times cannot select a later starter or expand DM members from an unrelated recorded leg', async () => {
  await mockSessions.createSession({ ...session('later'), documentId: 'dm:previous-user:future-user', startedBy: 'future-user',
    participants: [], participantHistory: { 'future-leg': { userId: 'future-user', participantId: 'future-leg', displayName: 'Future', joinedAt: START } } });
  const handlers = buildRecordingPipelineHandlers();
  for (const detail of [{ tracks: [{ participantId: 'an-older-unrelated-leg' }] }, {}]) {
    const resolved = await handlers.get(KINDS.resolve)!.execute(node, { arn: ARN, recordingId: 'older-recording', detail }, {} as never);
    expect(resolved.output).toMatchObject({ sessionId: null, startedBy: null, participants: [] });
  }
});

it('exact trusted recorded legs can identify the original session when times are unavailable', async () => {
  await mockSessions.createSession({ ...session('later', '2026-10-09T19:50:00Z'), startedBy: 'future-user',
    participantHistory: { 'future-leg': { userId: 'future-user', participantId: 'future-leg', displayName: 'Future', joinedAt: '2026-10-09T19:50:00Z' } } });
  await mockSessions.createSession({ ...session('actual'), participantHistory: { [vera.participantId]: vera, [nora.participantId]: nora } });
  const resolved = await buildRecordingPipelineHandlers().get(KINDS.resolve)!.execute(node, {
    arn: ARN, recordingId: 'actual-recording', detail: { tracks: [{ participantId: vera.participantId, userId: vera.userId }, { participantId: nora.participantId, userId: nora.userId }] },
  }, {} as never);
  expect(resolved.output).toMatchObject({ sessionId: 'actual', startedBy: 'u-vera' });
  expect((resolved.output as any).participants.map((p: any) => p.userId).sort()).toEqual(['u-nora', 'u-vera']);
});
