#!/usr/bin/env node
// Opt-in actual DynamoDB acceptance of native finalization/CAS transactions.
// Owned local instance and synthetic records only; no engine, model, gateway,
// calls, shared state, or primary services. Cache invalidation is inert here.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
if (process.env.RECORDING_FINALIZE_ACCEPTANCE_OPT_IN !== 'owned-native-dynamo') throw new Error('Explicit owned Dynamo acceptance opt-in required');
const resource = JSON.parse(fs.readFileSync(process.env.RECORDING_FINALIZE_ACCEPTANCE_RESOURCES, 'utf8'));
assert.equal(resource.isolated, true); assert.equal(resource.host, '127.0.0.1');
assert.equal(resource.label, 'native-recording-finalize-acceptance'); assert.match(resource.name, /^meeting-finalize-native-[a-f0-9]+-ddb$/);
assert(Number.isInteger(resource.port) && resource.port > 1024);
const prefix = 'finalize_acceptance_' + crypto.randomUUID().replaceAll('-', '') + '_';
Object.assign(process.env, { DDB_TABLE_PREFIX: prefix, AWS_EC2_METADATA_DISABLED: 'true', LOG_LEVEL: 'warn', NODE_ENV: 'test' });
require('ts-node/register/transpile-only');
const { DynamoDBClient, CreateTableCommand, DeleteTableCommand, ListTablesCommand } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, DeleteCommand, TransactWriteCommand } = require('@aws-sdk/lib-dynamodb');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const { recordingLocator } = require('../src/work-graph/sources/recordingLocator');
recordingLocator.noteWrite = async () => undefined; // only cache invalidation; no shared Redis connection
const { CallRecordingRepository, CALL_RECORDINGS_TABLE, CALL_RECORDING_PARTICIPANTS_TABLE } = require('../src/repositories/CallRecordingRepository');
const deadline = AbortSignal.timeout(120_000);
const ddb = new DynamoDBClient({ requestHandler: new NodeHttpHandler({ connectionTimeout: 5_000, requestTimeout: 15_000, throwOnRequestTimeout: true }), endpoint: `http://127.0.0.1:${resource.port}`, region: 'us-east-1', maxAttempts: 1,
  credentials: { accessKeyId: 'ownedLocalFinalizationFixture', secretAccessKey: 'owned-local-fixture-not-a-real-secret' } });
const client = DynamoDBDocumentClient.from(ddb, { marshallOptions: { removeUndefinedValues: true } });
// Bound every real request, including any deferred transaction. Cleanup gets
// its own short abort signal even if the acceptance deadline has elapsed.
for (const native of [ddb, client]) { const send = native.send.bind(native); native.send = (command, options = {}) => send(command, { ...options, abortSignal: options.abortSignal ?? deadline }); }
const repo = new CallRecordingRepository(client), checks = [], created = [];
const sorted = value => Array.isArray(value) ? value.map(sorted) : value && typeof value === 'object'
  ? Object.fromEntries(Object.keys(value).sort().map(key => [key, sorted(value[key])])) : value;
const hash = value => crypto.createHash('sha256').update(typeof value === 'string' ? value : JSON.stringify(sorted(value))).digest('hex');
const sourceFiles = ['src/repositories/CallRecordingRepository.ts', 'src/recordings/recordingPipelines.ts', 'src/lib/recording-truth.ts', 'scripts/recording-finalize-acceptance.cjs'];
const sourceHashes = Object.fromEntries(sourceFiles.map(file => [file, hash(fs.readFileSync(path.resolve(__dirname, '..', file), 'utf8'))]));
function row(id, count = 2) {
  const users = Array.from({ length: count }, (_, i) => `synthetic-reader-${i}`);
  return { callId: `synthetic-call-${id}`, recordingId: `synthetic-recording-${id}`, channelArn: `synthetic-channel-${id}`,
    lobbyName: 'orgiq:initiative:synthetic-owned-only', orgId: 'synthetic-tenant', callBindingStatus: 'bound', recordingGoverned: true,
    startedAt: '2026-10-10T10:00:00.000Z', endedAt: '2026-10-10T10:01:00.000Z', durationMs: 60_000, sessionDurationMs: 60_000,
    totalBytes: 1_000, segments: [], destination: 'local', startedBy: users[0], transcriptionRequested: true,
    participants: users.map(userId => ({ userId, displayName: userId })),
    tracks: users.map((userId, i) => ({ userId, participantId: `opaque-${i}`, tokenParticipantId: `signed-fixture-seat-${i}`, key: 'fixture.mp4', url: '', size: 1000 })) };
}
async function removeIndex(rec, userId = 'synthetic-reader-0') {
  await client.send(new DeleteCommand({ TableName: CALL_RECORDING_PARTICIPANTS_TABLE,
    Key: { userId, startedAtRecordingId: `${rec.startedAt}#${rec.recordingId}` } }));
}
async function main() {
  await ddb.send(new ListTablesCommand({}));
  for (const [TableName, pk, sk, gsi] of [[CALL_RECORDINGS_TABLE, 'callId', 'recordingId', true], [CALL_RECORDING_PARTICIPANTS_TABLE, 'userId', 'startedAtRecordingId', false]]) {
    await ddb.send(new CreateTableCommand({ TableName, BillingMode: 'PAY_PER_REQUEST', KeySchema: [{ AttributeName: pk, KeyType: 'HASH' }, { AttributeName: sk, KeyType: 'RANGE' }],
      AttributeDefinitions: [{ AttributeName: pk, AttributeType: 'S' }, { AttributeName: sk, AttributeType: 'S' }],
      ...(gsi ? { GlobalSecondaryIndexes: [{ IndexName: 'recordingId-index', KeySchema: [{ AttributeName: 'recordingId', KeyType: 'HASH' }], Projection: { ProjectionType: 'ALL' } }] } : {}) }));
    created.push(TableName);
  }
  const first = row('preserve');
  assert.equal((await repo.putFinalizedRecording(first)).replay, false);
  await repo.updateTranscriptStatus(first.callId, first.recordingId, { status: 'ready', processingAttemptId: 'saved-generation', transcriptJson: 'synthetic-ready-transcript',
    summaryStatus: 'ready', summaryRunId: 'saved-run', summaryMarkdown: 'synthetic-ready-summary' });
  await repo.updateComposeState(first.callId, first.recordingId, { composeStatus: 'ok', composedSegment: { key: 'original-fixture.mp4', size: 1000 } });
  const before = await repo.getRecording(first.callId, first.recordingId, { consistentRead: true });
  await removeIndex(first);
  assert.equal((await repo.putFinalizedRecording({ ...first, totalBytes: 9999 })).replay, true);
  assert.deepEqual(await repo.getRecording(first.callId, first.recordingId, { consistentRead: true }), before);
  assert.equal(await repo.hasParticipantRow('synthetic-reader-0', first.startedAt, first.recordingId), false);
  assert.equal((await repo.resolveForUser('synthetic-reader-0', first.recordingId)).isParticipant, false);
  checks.push({ name: 'actual-insert-once-ready-artifacts-completed-untag', pass: true, beforeHash: hash(before), afterHash: hash(await repo.getRecording(first.callId, first.recordingId)) });

  const late = row('late'); let entered, release;
  const arrived = new Promise(resolve => { entered = resolve; }), held = new Promise(resolve => { release = resolve; });
  let intercept = true;
  const pausedRepo = new CallRecordingRepository({ send: async command => {
    if (command instanceof TransactWriteCommand && intercept) { intercept = false; entered(); await held; }
    return client.send(command);
  } });
  const delayed = pausedRepo.putFinalizedRecording(late);
  await Promise.race([arrived, delayed.then(() => { throw new Error('expected paused initial transaction'); })]);
  try { await repo.putFinalizedRecording(late); await removeIndex(late); } finally { release(); }
  await delayed;
  assert.equal(await repo.hasParticipantRow('synthetic-reader-0', late.startedAt, late.recordingId), false);
  assert.equal((await repo.getRecording(late.callId, late.recordingId)).finalizationState, 'persisted');
  checks.push({ name: 'actual-transaction-rejects-late-repair-after-completed-untag', pass: true });

  const large = row('large', 150); let transactions = 0;
  const interruptedRepo = new CallRecordingRepository({ send: async command => {
    if (command instanceof TransactWriteCommand && ++transactions === 2) throw new Error('declared transport failure fixture before second transaction');
    return client.send(command);
  } });
  await assert.rejects(interruptedRepo.putFinalizedRecording(large), /declared transport failure fixture/);
  assert.equal((await repo.getRecording(large.callId, large.recordingId)).finalizationState, 'persisting');
  await repo.updateTranscriptStatus(large.callId, large.recordingId, { status: 'processing', processingAttemptId: 'accepted-during-repair' });
  await repo.putFinalizedRecording(large);
  assert.equal((await repo.getRecording(large.callId, large.recordingId)).processingAttemptId, 'accepted-during-repair');
  for (const person of large.participants) assert.equal(await repo.hasParticipantRow(person.userId, large.startedAt, large.recordingId), true);
  checks.push({ name: 'actual-100-item-transaction-bound-and-partial-transport-repair', pass: true, participants: 150, transportFailureIsInjected: true });

  const claim = row('claim'); await repo.putFinalizedRecording(claim);
  const competing = await Promise.allSettled(['one', 'two'].map(processingAttemptId => repo.updateTranscriptStatus(claim.callId, claim.recordingId,
    { status: 'pending', onlyIfUnclaimed: true, processingAttemptId, summaryStatus: 'pending', summaryMarkdown: null })));
  assert.equal(competing.filter(r => r.status === 'fulfilled').length, 1);
  assert.equal(competing.filter(r => r.status === 'rejected' && r.reason.name === 'ConditionalCheckFailedException').length, 1);
  const active = await repo.getRecording(claim.callId, claim.recordingId);
  await repo.updateTranscriptStatus(claim.callId, claim.recordingId, { status: 'ready', expectedProcessingAttemptId: active.processingAttemptId, transcriptJson: 'accepted-child-result', summaryStatus: 'ready', summaryMarkdown: 'accepted-child-summary' });
  await assert.rejects(repo.updateTranscriptStatus(claim.callId, claim.recordingId, { status: 'failed', expectedProcessingAttemptId: active.processingAttemptId, expectedTranscriptStatus: 'pending', transcriptError: 'lost acknowledgment' }), error => error.name === 'ConditionalCheckFailedException');
  assert.equal((await repo.getRecording(claim.callId, claim.recordingId)).transcriptStatus, 'ready');
  checks.push({ name: 'actual-single-automatic-claim-and-progress-aware-failure-fence', pass: true });

  await repo.updateTranscriptStatus(claim.callId, claim.recordingId, { status: 'pending', processingAttemptId: 'explicit-manual-generation', summaryStatus: 'pending', summaryMarkdown: null });
  const manual = await repo.getRecording(claim.callId, claim.recordingId);
  assert.equal(manual.processingAttemptId, 'explicit-manual-generation'); assert.equal(manual.summaryMarkdown, undefined);
  checks.push({ name: 'actual-explicit-manual-regeneration-compatible', pass: true });
  await repo.markFinalizationAnnounced(claim.callId, claim.recordingId);
  assert.equal((await repo.putFinalizedRecording(claim)).announcementPending, false);
  const empty = row('empty', 0); await repo.putFinalizedRecording(empty);
  assert.equal((await repo.getRecording(empty.callId, empty.recordingId)).finalizationState, 'persisted');
  checks.push({ name: 'actual-announcement-checkpoint-and-empty-index-completion', pass: true });
}
(async () => {
  let failure;
  try { await main(); } catch (error) { failure = error; }
  const cleanup = [];
  for (const TableName of created) { try { await ddb.send(new DeleteTableCommand({ TableName }), { abortSignal: AbortSignal.timeout(10_000) }); cleanup.push({ table: TableName, deleted: true }); } catch (error) { cleanup.push({ table: TableName, deleted: false, error: error.name }); failure ??= error; } }
  ddb.destroy();
  const proof = { at: new Date().toISOString(), pass: !failure, sourceHashes, resource: { name: resource.name, id: resource.id, image: resource.image, host: resource.host, port: resource.port },
    checks, cleanup, boundaries: ['Actual native repository + actual Dynamo transactions/conditional writes, synthetic authority fixtures.', 'No engine/model/gateway/call/media/shared service actions.', 'Cache invalidation inert; large-index interruption is explicit transport fault injection.', 'No full webhook-to-paid-transcription orchestration claim.'], ...(failure ? { error: { name: failure.name, message: failure.message } } : {}) };
  fs.writeFileSync(process.env.RECORDING_FINALIZE_ACCEPTANCE_OUTPUT, JSON.stringify(proof, null, 2) + '\n');
  console.log(JSON.stringify({ pass: proof.pass, checks: checks.length, tablesCleaned: cleanup.every(c => c.deleted), evidence: process.env.RECORDING_FINALIZE_ACCEPTANCE_OUTPUT }));
  if (failure) process.exitCode = 1;
})();
