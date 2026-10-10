#!/usr/bin/env node
// Integration check against DynamoDB Local using temporary, uniquely named
// tables. It runs native repositories only, without starting a call or server.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';

const endpoint = new URL(process.env.ATTENDANCE_TEST_DDB_ENDPOINT ?? 'http://127.0.0.1:8000');
if (endpoint.protocol !== 'http:' || !['localhost', '127.0.0.1', 'orgiq-social-dynamodb'].includes(endpoint.hostname)) {
  throw new Error('This integration check supports only the existing local DynamoDB endpoint');
}
const prefix = `attendance-proof-${Date.now()}-${process.pid}-`;
process.env.DDB_TABLE_PREFIX = prefix;
const require = createRequire(resolve(process.cwd(), 'package.json'));
const { DynamoDBClient, CreateTableCommand, DeleteTableCommand } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
const { VideoSessionRepository } = require(resolve('dist/repositories/VideoSessionRepository.js'));
const { CallRecordingRepository } = require(resolve('dist/repositories/CallRecordingRepository.js'));
const { recordingSessionParticipants, reconcileParticipants } = require(resolve('dist/lib/recording-truth.js'));
const raw = new DynamoDBClient({ endpoint: endpoint.origin, region: 'us-east-1',
  credentials: { accessKeyId: 'attendancetest', secretAccessKey: 'attendancetest' } });
const client = DynamoDBDocumentClient.from(raw, { marshallOptions: { removeUndefinedValues: true } });
const created = [];
const checks = [];
try {
  for (const [base, pk, sk, index] of [
    ['document-video-sessions', 'documentId', 'sessionId'],
    ['video-call-recordings', 'callId', 'recordingId', true],
    ['video-call-recording-participants', 'userId', 'startedAtRecordingId'],
  ]) {
    const TableName = prefix + base;
    await raw.send(new CreateTableCommand({ TableName, BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [{ AttributeName: pk, AttributeType: 'S' }, { AttributeName: sk, AttributeType: 'S' }],
      KeySchema: [{ AttributeName: pk, KeyType: 'HASH' }, { AttributeName: sk, KeyType: 'RANGE' }],
      ...(index ? { GlobalSecondaryIndexes: [{ IndexName: 'recordingId-index', KeySchema: [{ AttributeName: 'recordingId', KeyType: 'HASH' }], Projection: { ProjectionType: 'ALL' } }] } : {}),
    }));
    created.push(TableName);
  }
  const sessions = new VideoSessionRepository(client), recordings = new CallRecordingRepository(client);
  const lobby = 'orgiq:initiative:attendance-test', sessionId = 'owned-session';
  const startedAt = new Date().toISOString();
  await sessions.createSession({ documentId: lobby, sessionId, channelArn: 'arn:local:ivs:channel/attendance-test',
    streamKey: '', status: 'active', startedAt, startedBy: 'u-vera', participants: [] });
  const vera = { userId: 'u-vera', participantId: 'opaque-A', clientId: 'tab-A', displayName: 'Vera Green', joinedAt: startedAt };
  const nora = { userId: 'u-nora', participantId: 'misleading-other-user-id', clientId: 'tab-B', displayName: 'Nora DaSilva', joinedAt: startedAt };
  await Promise.all([vera, nora].map(actor => sessions.addOrReplaceParticipant(lobby, sessionId, actor)));
  let session = await sessions.getSession(lobby, sessionId);
  assert.deepEqual(session.participants.map(p => p.userId).sort(), ['u-nora', 'u-vera']);
  assert.equal(Object.keys(session.participantHistory).length, 2);
  checks.push('concurrent authenticated joins preserve both live slots and both durable identities');
  await sessions.addOrReplaceParticipant(lobby, sessionId, nora);
  const rejoinedNora = { ...nora, participantId: 'opaque-B-rejoin', joinedAt: new Date().toISOString() };
  await sessions.addOrReplaceParticipant(lobby, sessionId, rejoinedNora);
  session = await sessions.getSession(lobby, sessionId);
  assert.equal(session.participants.length, 2);
  assert.equal(Object.keys(session.participantHistory).length, 3);
  assert.ok(session.participantHistory[nora.participantId].leftAt);
  checks.push('duplicate/rejoin preserves original attendance and closes the displaced exact leg');
  await Promise.all([vera, rejoinedNora].map(actor => sessions.removeParticipant(lobby, sessionId, actor.participantId)));
  const endedAt = new Date().toISOString();
  await sessions.endSession(lobby, sessionId, endedAt);
  session = await new VideoSessionRepository(client).getSession(lobby, sessionId);
  assert.equal(session.participants.length, 0);
  assert.ok(Object.values(session.participantHistory).every(p => p.leftAt));
  checks.push('concurrent leaves empty the live roster while attendance survives a fresh repository instance');
  const tracks = [vera, nora, rejoinedNora].map(p => ({ participantId: p.participantId, durationMs: 1000 }));
  const trusted = recordingSessionParticipants(session, { startedAt, endedAt, tracks });
  const roster = reconcileParticipants({ participants: trusted, tracks, startedAt, endedAt, startedBy: session.startedBy });
  await recordings.putRecording({ callId: 'owned-call', recordingId: 'owned-recording', channelArn: session.channelArn,
    lobbyName: lobby, sessionId, startedAt, endedAt, durationMs: 1000, totalBytes: 0, segments: [], destination: 'local',
    participants: roster.participants, participantCount: roster.participantCount, startedBy: session.startedBy });
  for (const actor of ['u-vera', 'u-nora']) {
    assert.equal((await recordings.resolveForUser(actor, 'owned-recording')).isParticipant, true);
  }
  for (const outsider of ['previous-session-user', 'future-session-user', 'misleading-other-user-id']) {
    assert.equal((await recordings.resolveForUser(outsider, 'owned-recording')).isParticipant, false);
  }
  checks.push('native fan-out index authorizes both actual users after Leave and denies unrelated/string-derived users');
  console.log(JSON.stringify({ kind: 'native-local-attendance-integration', passed: true, checks,
    isolatedTablePrefix: prefix, temporaryTables: created, existingSharedTablesModified: false,
    participantIds: roster.participants.map(p => p.userId).sort(), noCallsOrServersStarted: true }, null, 2));
} finally {
  for (const TableName of created.reverse()) await raw.send(new DeleteTableCommand({ TableName }));
  client.destroy(); raw.destroy();
}
