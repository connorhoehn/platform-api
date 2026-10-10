#!/usr/bin/env node
// Opt-in native processing acceptance. Own DDB/Redis and a declared immutable
// file-transport fixture; real Whisper, native executor/repositories and Codex.
// No live call, media write, gateway message, primary service or shared row.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const http = require('node:http');
const crypto = require('node:crypto');
const { execFileSync } = require('node:child_process');
if (process.env.MEETING_ACCEPTANCE_OPT_IN !== 'owned-native-services') throw new Error('Explicit isolated native acceptance opt-in is required.');
const resources = JSON.parse(fs.readFileSync(process.env.MEETING_ACCEPTANCE_RESOURCES_JSON, 'utf8'));
assert.equal(resources.isolated, true);
for (const service of Object.values(resources.services)) { assert.match(service.name, /^meeting-pipeline-native-/); assert.equal(service.host, '127.0.0.1'); }
const privateConfig = JSON.parse(fs.readFileSync(process.env.MEETING_ACCEPTANCE_PRIVATE_CONFIG, 'utf8'));
assert.equal(typeof privateConfig.env.CODEX_EXEC_TOKEN, 'string');
assert.match(privateConfig.env.CODEX_EXEC_URL, /^http:\/\/127\.0\.0\.1:/);
Object.assign(process.env, privateConfig.env);
const output = path.resolve(process.env.MEETING_ACCEPTANCE_OUTPUT);
const media = path.resolve(process.env.MEETING_ACCEPTANCE_MEDIA_FILE);
const wantedHash = process.env.MEETING_ACCEPTANCE_MEDIA_SHA256;
assert.match(wantedHash, /^[a-f0-9]{64}$/);
const suffix = crypto.randomUUID();
const prefix = 'meeting_acceptance_' + suffix.replaceAll('-', '') + '_';
Object.assign(process.env, {
  DDB_TABLE_PREFIX: prefix, LOCAL_DDB_ENDPOINT: `http://127.0.0.1:${resources.services.ddb.port}`,
  REDIS_ENDPOINT: '127.0.0.1', REDIS_PORT: String(resources.services.redis.port),
  REDIS_CACHE_URL: `redis://127.0.0.1:${resources.services.redis.port}`,
  RECORDING_SUMMARY_PROVIDER: 'codex', RECORDING_SUMMARY_MODEL: '', VIDEO_SESSIONS_STORE: 'memory',
  LOG_LEVEL: 'warn', NODE_ENV: 'test', AWS_REGION: 'us-east-1',
});
assert.match(process.env.SPEECH_URL, /^http:\/\/127\.0\.0\.1:\d+\//);
const hashFile = async file => { const hash = crypto.createHash('sha256'); for await (const chunk of fs.createReadStream(file)) hash.update(chunk); return hash.digest('hex'); };
const hashValue = value => crypto.createHash('sha256').update(typeof value === 'string' ? value : JSON.stringify(value)).digest('hex');
const write = async (name, value) => fsp.writeFile(path.join(output, name), typeof value === 'string' ? value : JSON.stringify(value, null, 2) + '\n');
let fixtureServer, bus, pubsub;
async function main() {
  await fsp.mkdir(output, { recursive: true });
  const before = await hashFile(media); assert.equal(before, wantedHash);
  const probe = JSON.parse(execFileSync('ffprobe', ['-v', 'error', '-show_entries', 'format=duration', '-of', 'json', media], { encoding: 'utf8' }));
  const durationMs = Math.round(Number(probe.format.duration) * 1000); assert(durationMs > 0);
  const arn = 'arn:owned-acceptance:media:' + suffix;
  const callId = 'meeting-acceptance-call-' + suffix, recordingId = 'meeting-acceptance-recording-' + suffix;
  const lobbyName = 'orgiq:initiative:p-observability';
  const participants = ['meeting-reader-one', 'meeting-reader-two'];
  const transportToken = crypto.randomBytes(32).toString('hex');
  const route = `/api/channels/${encodeURIComponent(arn)}/recordings/${encodeURIComponent(recordingId)}/segments/composed.mp4`;
  let fileReads = 0;
  fixtureServer = http.createServer((req, res) => {
    if (req.method !== 'GET' || req.url !== route || req.headers.authorization !== 'Bearer ' + transportToken) { res.writeHead(404).end(); return; }
    fileReads++; res.writeHead(200, { 'Content-Type': 'video/mp4', 'Content-Length': fs.statSync(media).size });
    fs.createReadStream(media).pipe(res);
  });
  await new Promise(resolve => fixtureServer.listen(0, '127.0.0.1', resolve));
  const transportUrl = `http://127.0.0.1:${fixtureServer.address().port}`;
  // Any future card mapper still cannot reach a real gateway from this process.
  process.env.GATEWAY_WS_URL = transportUrl.replace('http:', 'ws:');
  process.env.VIDEO_API_URL = transportUrl;
  process.env.VIDEO_PUBLIC_URL = transportUrl;
  process.env.VIDEO_ADMIN_TOKEN = transportToken;
  require('ts-node/register/transpile-only');
  const { CreateTableCommand, DescribeTableCommand } = require('@aws-sdk/client-dynamodb');
  const { ddbClient } = require('../src/lib/aws-clients');
  const { callRecordingRepo } = require('../src/repositories');
  const { PipelineExecutor } = require('distributed-core/applications/pipeline');
  const { RedisPubSubManager, EventBus } = require('distributed-core');
  const { buildRecordingPipelineHandlers, startRecordingTranscription, KINDS } = require('../src/recordings/recordingPipelines');
  const { createDocumentApplyHandler, DOCUMENT_APPLY_KIND } = require('../src/documents/documentAgentPipeline');
  const definitions = [
    ['video-call-recordings', 'callId', 'recordingId', true],
    ['video-call-recording-participants', 'userId', 'startedAtRecordingId'],
    ['call-reactions', 'lobbyKey', 'sk'], ['work-graph', 'PK', 'SK'],
  ];
  for (const [name, pk, sk, index] of definitions) {
    await ddbClient.send(new CreateTableCommand({ TableName: prefix + name,
      KeySchema: [{ AttributeName: pk, KeyType: 'HASH' }, { AttributeName: sk, KeyType: 'RANGE' }],
      AttributeDefinitions: [{ AttributeName: pk, AttributeType: 'S' }, { AttributeName: sk, AttributeType: 'S' }],
      BillingMode: 'PAY_PER_REQUEST', ...(index ? { GlobalSecondaryIndexes: [{ IndexName: 'recordingId-index', KeySchema: [{ AttributeName: 'recordingId', KeyType: 'HASH' }], Projection: { ProjectionType: 'ALL' } }] } : {}),
    }));
    const table = await ddbClient.send(new DescribeTableCommand({ TableName: prefix + name }));
    assert.equal(table.Table.TableStatus, 'ACTIVE');
  }
  const now = Date.now();
  const startedAt = new Date(now - durationMs).toISOString(), endedAt = new Date(now).toISOString();
  const rec = { callId, recordingId, channelArn: arn, lobbyName, orgId: 'orgiq',
    startedAt, endedAt, durationMs, totalBytes: fs.statSync(media).size, segments: [], destination: 'local',
    startedBy: participants[0], participants: participants.map(userId => ({ userId, displayName: userId })),
    composeStatus: 'ok', composedSegment: { key: 'composed.mp4', size: fs.statSync(media).size } };
  await callRecordingRepo.putRecording(rec);
  // Seeded obsolete metadata is a concurrency fixture, never a model success.
  await callRecordingRepo.updateTranscriptStatus(callId, recordingId, { status: 'ready', processingAttemptId: 'obsolete-generation-fixture',
    summaryRunId: 'obsolete-run-fixture', summaryStatus: 'pending', summaryMarkdown: 'OBSOLETE FIXTURE ARTIFACT' });
  pubsub = new RedisPubSubManager({ localNodeId: 'owned-native-acceptance', url: process.env.REDIS_CACHE_URL,
    channelPrefix: prefix + ':events:' });
  await pubsub.start();
  bus = new EventBus(pubsub, 'owned-native-acceptance', { topic: prefix + 'pipeline-events', walFilePath: path.join(output, 'pipeline-events.wal'), awaitFanOut: true });
  await bus.start();
  const observed = [];
  bus.subscribeAll(async event => { observed.push({ type: event.type, at: event.payload?.at, stepId: event.payload?.stepId, runId: event.payload?.runId }); });
  const handlers = buildRecordingPipelineHandlers();
  handlers.set(DOCUMENT_APPLY_KIND, createDocumentApplyHandler());
  let executor, running;
  const started = await startRecordingTranscription(rec, participants[0], { startRun: async (pipelineId, definition, triggerPayload) => {
    executor = new PipelineExecutor({ definition, ownerNodeId: 'owned-native-acceptance', eventBus: bus,
      llmClient: { provider: 'not-used', stream() { throw new Error('Unexpected built-in model node.'); } },
      customStepHandlers: handlers, triggerPayload, triggeredBy: { userId: participants[0] },
      failureRateLLM: 0, failureRateOther: 0, simulateUnsupportedConditions: false, speedMultiplier: 0.02 });
    running = executor.run(); return executor.runId;
  } });
  const run = await running;
  await write('pipeline-result.json', run);
  assert.equal(run.status, 'completed');
  const ready = await callRecordingRepo.getRecording(callId, recordingId);
  assert.equal(ready.transcriptStatus, 'ready'); assert.equal(ready.summaryStatus, 'ready');
  assert.equal(ready.processingAttemptId, started.processingAttemptId); assert.equal(ready.summaryRunId, started.runId);
  const transcript = JSON.parse(ready.transcriptJson);
  assert(transcript.segments.length >= 2); assert.equal(transcript.timestampGranularity, 'segment');
  for (const segment of transcript.segments) { assert.equal(segment.timestampSource, 'engine'); assert.equal(segment.speakerId, 'call'); assert.equal(segment.speakerLabel, 'Call'); assert(segment.endMs >= segment.startMs); }
  assert.match(ready.summaryMarkdown, /^## Summary\n/); assert(ready.summaryMarkdown.length > 100);
  assert(!ready.summaryMarkdown.includes('OBSOLETE FIXTURE ARTIFACT'));
  const readyHash = hashValue({ transcript: ready.transcriptJson, summary: ready.summaryMarkdown, attempt: ready.processingAttemptId, run: ready.summaryRunId });
  const rejected = [];
  for (const [name, patch] of [
    ['obsolete-generation-transcript', { status: 'ready', expectedProcessingAttemptId: 'obsolete-generation-fixture', transcriptJson: '{"segments":[]}' }],
    ['obsolete-run-summary', { status: 'ready', expectedSummaryRunId: 'obsolete-run-fixture', summaryStatus: 'ready', summaryMarkdown: '## Summary\nObsolete answer.' }],
    ['changed-settled-result', { status: 'ready', expectedSummaryRunId: started.runId, expectedProcessingAttemptId: started.processingAttemptId, summaryStatus: 'ready', summaryMarkdown: '## Summary\nDifferent later answer.' }],
  ]) {
    await assert.rejects(callRecordingRepo.updateTranscriptStatus(callId, recordingId, patch), error => error.name === 'ConditionalCheckFailedException'); rejected.push(name);
  }
  await callRecordingRepo.updateTranscriptStatus(callId, recordingId, { status: 'ready', expectedSummaryRunId: started.runId, expectedProcessingAttemptId: started.processingAttemptId, summaryStatus: 'ready', summaryMarkdown: ready.summaryMarkdown });
  const after = await callRecordingRepo.getRecording(callId, recordingId);
  assert.equal(hashValue({ transcript: after.transcriptJson, summary: after.summaryMarkdown, attempt: after.processingAttemptId, run: after.summaryRunId }), readyHash);
  assert.equal(await hashFile(media), before); assert.equal(fileReads, 1);
  await write('recording-ready.json', after); await write('transcript.json', transcript); await write('summary.md', ready.summaryMarkdown);
  await write('observed-lifecycle.json', observed);
  const proof = { status: 'passed', actualNativeExecutor: true, actualNativeRepository: true, actualWhisper: true, actualCodexSubscription: true,
    noModelSimulation: true, transport: 'explicit isolated immutable media-file HTTP fixture', liveLvsReadOrMutation: false,
    callId, recordingId, lobbyName, organizationId: 'orgiq', participantSubjects: participants,
    tablePrefix: prefix, ddbEndpoint: process.env.LOCAL_DDB_ENDPOINT, redisPort: resources.services.redis.port,
    runId: started.runId, processingAttemptId: started.processingAttemptId, nativeTranscriptStatus: after.transcriptStatus, nativeSummaryStatus: after.summaryStatus,
    segments: transcript.segments.length, timestampGranularity: transcript.timestampGranularity, speakerAttribution: 'Call (mixed composed recording; no diarization)',
    summaryChars: after.summaryMarkdown.length, summarySHA256: hashValue(after.summaryMarkdown), transcriptSHA256: hashValue(transcript),
    immutableMediaSHA256: before, mediaFileReads: fileReads, staleWriteRejections: rejected,
    identicalResultReplayAccepted: true, readyArtifactHashUnchangedAfterRetries: true,
    documentWriteRequested: false, gatewayDeliveryTested: false, primaryServiceControl: false, sharedRowMutation: false,
    sourceFiles: Object.fromEntries(['src/recordings/recordingPipelines.ts','src/repositories/CallRecordingRepository.ts','src/pipeline/CodexLLMClient.ts','src/lib/modelGate.ts','src/lib/modelHealth.ts'].map(file => [file, hashValue(fs.readFileSync(path.join(__dirname, '..', file), 'utf8'))])) };
  await write('proof.json', proof); console.log(JSON.stringify(proof));
}
main().catch(async error => { await write('failure.json', { status: 'failed', name: error.name, error: error.message }).catch(() => {}); console.error('Owned native meeting pipeline acceptance failed:', error.name, error.message); process.exitCode = 1; })
.finally(async () => { if (bus) await bus.stop(); if (pubsub) await pubsub.stop(); if (fixtureServer) await new Promise(resolve => fixtureServer.close(resolve));
  const { getRedisClient, getRedisCacheClient } = require('../src/lib/redis-client');
  const clients = await Promise.all([getRedisClient(), getRedisCacheClient()]); for (const client of new Set(clients)) if (client?.isOpen) await client.quit();
});
