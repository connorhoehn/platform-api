#!/usr/bin/env node
// One opt-in end-to-end acceptance starting from the actual signed completion
// webhook, not a composed-row seed or manual transcription helper. Uses only
// uniquely owned DDB/Redis, synthetic accepted seats and immutable file media.
const assert = require('node:assert/strict');
const fs = require('node:fs'); const fsp = require('node:fs/promises');
const path = require('node:path'); const http = require('node:http'); const crypto = require('node:crypto');
const { execFileSync } = require('node:child_process');
if (process.env.MEETING_ACCEPTANCE_OPT_IN !== 'owned-native-services') throw new Error('Explicit owned native services opt-in required');
const resources = JSON.parse(fs.readFileSync(process.env.MEETING_ACCEPTANCE_RESOURCES_JSON, 'utf8'));
assert.equal(resources.isolated, true);
for (const service of Object.values(resources.services)) { assert.match(service.name, /^meeting-(?:pipeline|finalize)-native-/); assert.equal(service.host, '127.0.0.1'); }
const privateConfig = JSON.parse(fs.readFileSync(process.env.MEETING_ACCEPTANCE_PRIVATE_CONFIG, 'utf8'));
assert.equal(typeof privateConfig.env.CODEX_EXEC_TOKEN, 'string'); assert.match(privateConfig.env.CODEX_EXEC_URL, /^http:\/\/127\.0\.0\.1:/);
Object.assign(process.env, privateConfig.env);
const output = path.resolve(process.env.MEETING_ACCEPTANCE_OUTPUT), media = path.resolve(process.env.MEETING_ACCEPTANCE_MEDIA_FILE), wantedHash = process.env.MEETING_ACCEPTANCE_MEDIA_SHA256;
assert.match(wantedHash, /^[a-f0-9]{64}$/); assert.match(process.env.SPEECH_URL, /^http:\/\/127\.0\.0\.1:\d+\//);
const suffix = crypto.randomUUID(), prefix = 'automatic_finalize_' + suffix.replaceAll('-', '') + '_';
Object.assign(process.env, { DDB_TABLE_PREFIX: prefix, LOCAL_DDB_ENDPOINT: `http://127.0.0.1:${resources.services.ddb.port}`,
  REDIS_ENDPOINT: '127.0.0.1', REDIS_PORT: String(resources.services.redis.port), REDIS_CACHE_URL: `redis://127.0.0.1:${resources.services.redis.port}`,
  VIDEO_SESSIONS_STORE: 'ddb', RECORDING_SUMMARY_PROVIDER: 'codex', RECORDING_SUMMARY_MODEL: '', LOG_LEVEL: 'warn', NODE_ENV: 'test', AWS_REGION: 'us-east-1',
  LVS_WEBHOOK_SECRET: crypto.randomBytes(32).toString('hex'), SKIP_AUTH: 'false' });
const hashValue = value => crypto.createHash('sha256').update(typeof value === 'string' ? value : JSON.stringify(value)).digest('hex');
const hashFile = async file => { const hash = crypto.createHash('sha256'); for await (const c of fs.createReadStream(file)) hash.update(c); return hash.digest('hex'); };
const write = async (name, value) => fsp.writeFile(path.join(output, name), typeof value === 'string' ? value : JSON.stringify(value, null, 2)+'\n');
let fixtureServer, webhookServer, modelProxy, speechProxy, bus, pubsub, ddbClient;
const tables = [], executors = [], running = [], cards = [], graphEffects = [], modelRequests = [], speechRequests = [];
function forwardingServer(upstream, seen) {
  return http.createServer((req, res) => {
    if (req.method !== 'POST') { res.writeHead(404).end(); return; }
    seen.push({ method: req.method, at: new Date().toISOString() });
    const target = new URL(upstream);
    const request = http.request({ hostname: target.hostname, port: target.port, path: target.pathname, method: req.method, headers: req.headers }, response => { res.writeHead(response.statusCode, response.headers); response.pipe(res); });
    request.on('error', () => { if (!res.headersSent) res.writeHead(502).end(); else res.destroy(); }); req.pipe(request);
  });
}
const listen = server => new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
async function main() {
  await fsp.mkdir(output, { recursive: true });
  const before = await hashFile(media); assert.equal(before, wantedHash);
  const probe = JSON.parse(execFileSync('ffprobe', ['-v','error','-show_entries','format=duration','-of','json',media],{encoding:'utf8'}));
  const durationMs = Math.round(Number(probe.format.duration)*1000); assert(durationMs>0);
  const arn='arn:owned-automatic:media:'+suffix, callId='automatic-call-'+suffix, recordingId='automatic-recording-'+suffix, sessionId='automatic-session-'+suffix;
  const lobbyName='orgiq:initiative:p-observability', participants=['automatic-reader-one','automatic-reader-two'];
  const seats=['signed-native-seat-one','signed-native-seat-two'], token=crypto.randomBytes(32).toString('hex');
  const base=`/api/channels/${encodeURIComponent(arn)}/recordings`, segment=base+`/${encodeURIComponent(recordingId)}/segments/composed.mp4`;
  let fileReads=0, composeReads=0;
  fixtureServer=http.createServer((req,res)=>{
    if(req.method!=='GET'||req.headers.authorization!=='Bearer '+token){res.writeHead(404).end();return;}
    if(req.url===base){
      composeReads++;
      const row=composeReads===1 ? {recordingId,composeStatus:'queued'} : {recordingId,composeStatus:'ok',composedSegment:{key:'composed.mp4',size:fs.statSync(media).size}};
      res.writeHead(200,{'content-type':'application/json'}).end(JSON.stringify({recordings:[row]}));return;
    }
    if(req.url!==segment){res.writeHead(404).end();return;}
    fileReads++;res.writeHead(200,{'content-type':'video/mp4','content-length':fs.statSync(media).size});fs.createReadStream(media).pipe(res);
  }); await listen(fixtureServer);
  const fixtureUrl=`http://127.0.0.1:${fixtureServer.address().port}`;
  process.env.VIDEO_API_URL=fixtureUrl;process.env.VIDEO_PUBLIC_URL=fixtureUrl;process.env.VIDEO_ADMIN_TOKEN=token;process.env.GATEWAY_WS_URL=fixtureUrl.replace('http:','ws:');
  // Count actual HTTP inference requests without changing the input or response.
  modelProxy=forwardingServer(new URL('/complete',process.env.CODEX_EXEC_URL).toString(),modelRequests);await listen(modelProxy);process.env.CODEX_EXEC_URL=`http://127.0.0.1:${modelProxy.address().port}`;
  speechProxy=forwardingServer(process.env.SPEECH_URL,speechRequests);await listen(speechProxy);process.env.SPEECH_URL=`http://127.0.0.1:${speechProxy.address().port}/`;
  require('ts-node/register/transpile-only');
  const {CreateTableCommand,DescribeTableCommand}=require('@aws-sdk/client-dynamodb');
  ({ddbClient}=require('../src/lib/aws-clients'));
  const {callRecordingRepo,videoSessionRepo}=require('../src/repositories');
  // These are declared external delivery fixtures, not a real chat/work-graph.
  require('../src/gateway/GatewayServiceClient').postChatMessage=async input=>{cards.push({channel:input.channel,kind:input.metadata?.kind,runId:input.metadata?.runId,recordingId:input.metadata?.recordingId});};
  require('../src/work-graph/sources/platformPublisher').platformWorkGraphSourcePublisher.meeting=async input=>{graphEffects.push({recordingId:input.recordingId,kind:input.kind});return {published:false,reason:'owned-delivery-fixture'};};
  const {PipelineExecutor}=require('distributed-core/applications/pipeline'),{RedisPubSubManager,EventBus}=require('distributed-core');
  const {buildRecordingPipelineHandlers,CALL_TRANSCRIPTION_PIPELINE_ID,RECORDING_FINALIZE_PIPELINE_ID}=require('../src/recordings/recordingPipelines');
  const {createDocumentApplyHandler,DOCUMENT_APPLY_KIND}=require('../src/documents/documentAgentPipeline');
  const {setPipelineBridge}=require('../src/pipeline/bridge-types');
  const specs=[['video-call-recordings','callId','recordingId',true],['video-call-recording-participants','userId','startedAtRecordingId'],['document-video-sessions','documentId','sessionId'],['call-reactions','lobbyKey','sk']];
  for(const [name,pk,sk,index] of specs){const table=prefix+name;await ddbClient.send(new CreateTableCommand({TableName:table,KeySchema:[{AttributeName:pk,KeyType:'HASH'},{AttributeName:sk,KeyType:'RANGE'}],AttributeDefinitions:[{AttributeName:pk,AttributeType:'S'},{AttributeName:sk,AttributeType:'S'}],BillingMode:'PAY_PER_REQUEST',...(index?{GlobalSecondaryIndexes:[{IndexName:'recordingId-index',KeySchema:[{AttributeName:'recordingId',KeyType:'HASH'}],Projection:{ProjectionType:'ALL'}}]}:{})}));tables.push(table);assert.equal((await ddbClient.send(new DescribeTableCommand({TableName:table}))).Table.TableStatus,'ACTIVE');}
  const endedAt=new Date().toISOString(),startedAt=new Date(Date.now()-durationMs).toISOString();
  // Fixture accepted proof is explicit: repository conditions are real; this
  // harness does not claim an actual browser/gateway/JWT issuance ceremony.
  await videoSessionRepo.createSession({documentId:lobbyName,sessionId,channelArn:arn,orgId:'orgiq',startedBy:participants[0],startedAt,status:'active',streamKey:'',participants:[]});
  for(let n=0;n<participants.length;n++){
    const userId=participants[n],participantId=seats[n],clientId='owned-client-'+n+'-'+suffix;
    await videoSessionRepo.addOrReplaceParticipant(lobbyName,sessionId,{userId,participantId,clientId,displayName:userId,joinedAt:startedAt});
    assert(await videoSessionRepo.bindCallParticipant(lobbyName,sessionId,participantId,arn,{callId,clientId,userId,tenant:'orgiq',epoch:1,sessionId,channelArn:arn,lobbyName,boundAt:startedAt}));
    await videoSessionRepo.removeParticipant(lobbyName,sessionId,participantId);
  }await videoSessionRepo.endSession(lobbyName,sessionId,endedAt);
  assert.equal(await callRecordingRepo.getRecording(callId,recordingId),null);
  pubsub=new RedisPubSubManager({localNodeId:'owned-automatic-finalize',url:process.env.REDIS_CACHE_URL,channelPrefix:prefix+':events:'});await pubsub.start();
  bus=new EventBus(pubsub,'owned-automatic-finalize',{topic:prefix+'pipeline-events',walFilePath:path.join(output,'pipeline-events.wal'),awaitFanOut:true});await bus.start();
  const observed=[];bus.subscribeAll(async e=>{observed.push({type:e.type,runId:e.payload?.runId,stepId:e.payload?.stepId,at:e.payload?.at});});
  const handlers=buildRecordingPipelineHandlers();handlers.set(DOCUMENT_APPLY_KIND,createDocumentApplyHandler());
  const accepted=[];
  setPipelineBridge({trigger(input){
    const executor=new PipelineExecutor({definition:input.definition,ownerNodeId:'owned-automatic-finalize',eventBus:bus,llmClient:{provider:'not-used',stream(){throw new Error('Unexpected built-in model node');}},customStepHandlers:handlers,triggerPayload:input.triggerPayload,triggeredBy:input.triggeredBy,failureRateLLM:0,failureRateOther:0,simulateUnsupportedConditions:false,speedMultiplier:0.02});
    executors.push(executor);accepted.push({pipelineId:input.pipelineId,runId:executor.runId,processingAttemptId:input.triggerPayload.processingAttemptId});
    running.push(executor.run());return {runId:executor.runId};
  }});
  const express=require('express'),{lvsWebhooksRouter,getRecentDeliveries}=require('../src/routes/lvsWebhooks');
  const app=express();app.use('/hooks/lvs',lvsWebhooksRouter);webhookServer=http.createServer(app);await listen(webhookServer);
  const tracks=[...participants.map((userId,n)=>({participantId:'opaque-camera-'+n,tokenParticipantId:seats[n],userId,key:'camera-'+n+'.mp4',url:'',size:1,durationMs,startedAtMs:0,endedAtMs:durationMs})),{participantId:'opaque-screen-routing',tokenParticipantId:seats[0],userId:participants[0],key:'screen.mp4',url:'',size:1,durationMs,startedAtMs:0,endedAtMs:durationMs,kind:'video'}];
  const payload=JSON.stringify({type:'sfu.recording.completed',channelArn:arn,data:{recordingId,startedAt,endedAt,durationMs,totalSize:fs.statSync(media).size,tracks,segments:[],destination:'local',transcribe:true}});
  const deliver=async()=>{const response=await fetch(`http://127.0.0.1:${webhookServer.address().port}/hooks/lvs/recording`,{method:'POST',headers:{'content-type':'application/json','x-lvs-signature':'sha256='+crypto.createHmac('sha256',process.env.LVS_WEBHOOK_SECRET).update(payload).digest('hex'),'x-webhook-timestamp':new Date().toISOString()},body:payload,signal:AbortSignal.timeout(10_000)});assert.equal(response.status,204);return getRecentDeliveries().at(-1).runId;};
  const parentRunId=await deliver(); assert.equal(accepted[0].pipelineId,RECORDING_FINALIZE_PIPELINE_ID);
  // Parent completion can precede child completion; await every accepted actual
  // executor, including automatically added children, without manual start.
  for(let n=0;n<running.length;n++){const result=await running[n];await write('run-'+accepted[n].pipelineId+'-'+n+'.json',result);assert.equal(result.status,'completed');}
  const ready=await callRecordingRepo.getRecording(callId,recordingId,{consistentRead:true});
  assert.equal(ready.callBindingStatus,'bound');assert.equal(ready.recordingGoverned,true);assert.equal(ready.tracks.length,3);assert.equal(ready.participantCount,2);
  assert.equal(ready.transcriptionRequested,true);assert.equal(ready.transcriptStatus,'ready');assert.equal(ready.summaryStatus,'ready');
  const children=accepted.filter(r=>r.pipelineId===CALL_TRANSCRIPTION_PIPELINE_ID);assert.equal(children.length,1);assert.equal(ready.processingAttemptId,children[0].processingAttemptId);assert.equal(ready.summaryRunId,children[0].runId);
  const transcript=JSON.parse(ready.transcriptJson);assert(transcript.segments.length>=2);assert.equal(transcript.timestampGranularity,'segment');for(const s of transcript.segments){assert.equal(s.timestampSource,'engine');assert.equal(s.speakerId,'call');assert(s.endMs>=s.startMs);}assert.match(ready.summaryMarkdown,/^## Summary\n/);
  const beforeReplay=hashValue(ready),counters={fileReads,composeReads,cards:cards.length,graphEffects:graphEffects.length,speechRequests:speechRequests.length,modelRequests:modelRequests.length};
  assert.equal(fileReads,1);assert.equal(speechRequests.length,1);assert.equal(modelRequests.length,1);assert.equal(composeReads,2);
  const replayRunId=await deliver();const replay=await running.at(-1);assert.equal(replay.status,'completed');
  const after=await callRecordingRepo.getRecording(callId,recordingId,{consistentRead:true});assert.equal(hashValue(after),beforeReplay);
  assert.deepEqual({fileReads,composeReads,cards:cards.length,graphEffects:graphEffects.length,speechRequests:speechRequests.length,modelRequests:modelRequests.length},counters);
  assert.equal(accepted.filter(r=>r.pipelineId===CALL_TRANSCRIPTION_PIPELINE_ID).length,1);
  const reads=[];for(const user of participants){const result=await callRecordingRepo.resolveForUser(user,recordingId);assert.equal(result.isParticipant,true);assert.equal(result.detail.summaryMarkdown,after.summaryMarkdown);reads.push({userId:user,isParticipant:true,exactSummarySHA256:hashValue(result.detail.summaryMarkdown)});}assert.equal((await callRecordingRepo.resolveForUser('not-recorded',recordingId)).isParticipant,false);
  assert.equal(await hashFile(media),before);await write('recording-ready.private.json',after);await write('transcript.private.json',transcript);await write('summary.private.md',after.summaryMarkdown);await write('observed-lifecycle.json',observed);await write('native-deliveries.json',getRecentDeliveries());
  const sourceFiles=['src/recordings/recordingPipelines.ts','src/repositories/CallRecordingRepository.ts','src/repositories/VideoSessionRepository.ts','src/repositories/VideoSessionStore.ts','src/lib/recording-call-binding.ts','src/routes/lvsWebhooks.ts','src/pipeline/CodexLLMClient.ts','scripts/meeting-finalize-automatic-acceptance.cjs'];
  const proof={status:'passed',startsFromActualSignedWebhook:true,actualFinalizeExecutor:true,automaticNativeChild:true,recordingRowPreSeeded:false,manualTranscriptionHelperCalled:false,actualNativeDynamoTransactions:true,actualWhisper:true,actualCodexSubscription:true,simulatedModel:false,canonicalSignedParentMetadata:true,
    callId,recordingId,lobbyName,sessionId,parentRunId,childRunId:children[0].runId,replayRunId,processingAttemptId:ready.processingAttemptId,acceptedRuns:accepted,segments:transcript.segments.length,timestampGranularity:transcript.timestampGranularity,speakerAttribution:'Call: mixed composed media, no diarization',summaryChars:after.summaryMarkdown.length,summarySHA256:hashValue(after.summaryMarkdown),transcriptSHA256:hashValue(transcript),immutableMediaSHA256:before,readResults:reads,nonparticipantDenied:true,
    duplicateWebhookPreservesExactReadyRow:true,automaticGenerations:children.length,...counters,tablePrefix:prefix,ddbEndpoint:process.env.LOCAL_DDB_ENDPOINT,redisPort:resources.services.redis.port,sourceFiles:Object.fromEntries(sourceFiles.map(file=>[file,hashValue(fs.readFileSync(path.join(__dirname,'..',file),'utf8'))])),
    boundaries:['Owned bridge uses actual native PipelineExecutor/EventBus/WAL; production Raft bootstrap and RunPersistence not booted.','Accepted seat provenance fixtures are synthetic; actual repository binding/leave conditions and strict resolver run. No real gateway/browser/JWT mint ceremony.','Composition/file HTTP transport is declared immutable fixture; no actual SFU/ffmpeg/network recording.','Chat and work-graph delivery are intercepted only inside this owned fixture; no messages or external mutations.','Primary services, existing rows and original media remain unchanged.']};
  await write('proof.json',proof);console.log(JSON.stringify({status:proof.status,parentRunId,childRunId:proof.childRunId,automaticGenerations:children.length,segments:proof.segments,duplicateReadyPreserved:true,output}));
}
const deadline=setTimeout(()=>{console.error('Owned automatic finalizer acceptance deadline reached');process.exit(2);},240_000);
main().catch(async error=>{await fsp.mkdir(output,{recursive:true});await write('failure.json',{status:'failed',name:error.name,error:error.message,modelHTTPRequests:modelRequests.length,speechHTTPRequests:speechRequests.length,ownedTables:tables});console.error(error.name,error.message);process.exitCode=1;}).finally(async()=>{
  clearTimeout(deadline);require('../src/pipeline/bridge-types').setPipelineBridge(null);
  if(bus)await bus.stop();if(pubsub)await pubsub.stop();for(const s of [webhookServer,fixtureServer,modelProxy,speechProxy])if(s)await new Promise(resolve=>s.close(resolve));
  const {getRedisClient,getRedisCacheClient}=require('../src/lib/redis-client');for(const client of new Set(await Promise.all([getRedisClient(),getRedisCacheClient()])))if(client?.isOpen)await client.quit();
  const cleanup=[];if(ddbClient){const {DeleteTableCommand}=require('@aws-sdk/client-dynamodb');for(const table of tables){await ddbClient.send(new DeleteTableCommand({TableName:table}),{abortSignal:AbortSignal.timeout(10_000)});cleanup.push({table,deleted:true});}ddbClient.destroy();}await write('cleanup.json',{ownedTables:cleanup,sharedResourcesChanged:false});
});
