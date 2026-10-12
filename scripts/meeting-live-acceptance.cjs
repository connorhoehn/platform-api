#!/usr/bin/env node
// One opt-in end-to-end acceptance starting from the actual signed completion
// webhook, not a composed-row seed or manual transcription helper. Uses only
// uniquely owned DDB/Redis, real native SFU capture, and explicit gateway/host authority fixtures.
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
const output = path.resolve(process.env.MEETING_ACCEPTANCE_OUTPUT);
assert.match(process.env.SPEECH_URL, /^http:\/\/127\.0\.0\.1:\d+\//);
const suffix = crypto.randomUUID(), prefix = 'automatic_finalize_' + suffix.replaceAll('-', '') + '_';
Object.assign(process.env, { DDB_TABLE_PREFIX: prefix, LOCAL_DDB_ENDPOINT: `http://127.0.0.1:${resources.services.ddb.port}`,
  REDIS_ENDPOINT: '127.0.0.1', REDIS_PORT: String(resources.services.redis.port), REDIS_CACHE_URL: `redis://127.0.0.1:${resources.services.redis.port}`,
  VIDEO_SESSIONS_STORE: 'ddb', RECORDING_SUMMARY_PROVIDER: 'codex', RECORDING_SUMMARY_MODEL: '', LOG_LEVEL: 'warn', NODE_ENV: 'test', AWS_REGION: 'us-east-1',
  REALTIME_JWT_SECRET: crypto.randomBytes(32).toString('hex'), SKIP_AUTH: 'false' });
const hashValue = value => crypto.createHash('sha256').update(typeof value === 'string' ? value : JSON.stringify(value)).digest('hex');
const hashFile = async file => { const hash = crypto.createHash('sha256'); for await (const c of fs.createReadStream(file)) hash.update(c); return hash.digest('hex'); };
const write = async (name, value) => fsp.writeFile(path.join(output, name), typeof value === 'string' ? value : JSON.stringify(value, null, 2)+'\n');
let webhookServer, modelProxy, speechProxy, bus, pubsub, ddbClient, browser;
let arn, recordingId;
const nativeOrigin = process.env.VIDEO_API_URL;
assert.match(nativeOrigin, /^http:\/\/127\.0\.0\.1:\d+$/);
const statusReads = [], webhookReceipts = [];
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
  assert.match(process.env.NODE_ID ?? '', /^meeting-live-/);
  const health = await fetch(nativeOrigin+'/api/health', {signal:AbortSignal.timeout(5_000)});
  assert.equal(health.status, 200);
  assert.equal((await health.json()).nodeId, process.env.NODE_ID, 'Refuse an SFU outside this owned run');
  const callId='live-call-'+suffix, sessionId='live-session-'+suffix;
  const lobbyName='orgiq:initiative:owned-live-'+suffix, participants=['owned-live-one','owned-live-two'];
  const native = async (pathname, body, method = 'POST') => {
    const response = await fetch(nativeOrigin+pathname, {method, headers: {authorization:'Bearer '+(pathname.startsWith('/api/channels/') ? process.env.VIDEO_ADMIN_TOKEN : process.env.LVS_SERVICE_TOKEN), 'content-type':'application/json'}, ...(body ? {body:JSON.stringify(body)} : {}), signal:AbortSignal.timeout(15_000)});
    assert(response.ok, `Native ${method} ${pathname}: ${response.status}`); return response.json();
  };
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
  const app=express();
  app.use('/hooks/lvs', (req,res,next) => { const chunks=[]; req.on('data', c=>chunks.push(c)); req.on('end',()=>{ const body=Buffer.concat(chunks); let parsed; try {parsed=JSON.parse(body)}catch{}; webhookReceipts.push({at:new Date().toISOString(),type:parsed?.['detail-type']||parsed?.type,sha256:hashValue(body.toString())});  }); next(); }, lvsWebhooksRouter);
  const jwt=require('jsonwebtoken');
  const bearerFor=user=>jwt.sign({sub:user,org:'orgiq',tokenKind:'orgiq-hs256'}, process.env.REALTIME_JWT_SECRET,{algorithm:'HS256',issuer:'orgiq-middleware',audience:'realtime',expiresIn:'10m'});
  const bearers=participants.map(bearerFor);
  // Explicit isolated host authority: actual JWT and repository checks below;
  // this route stands in for the application's directory/Cedar admission.
  app.post('/owned-authority',express.json(),(req,res)=>{ let claims;try {claims=jwt.verify((req.headers.authorization||'').slice(7),process.env.REALTIME_JWT_SECRET)}catch{}; res.status(claims && participants.includes(claims.sub) && req.body.lobbyName===lobbyName ? 200 : 403).json({allowed:!!claims && participants.includes(claims.sub) && req.body.lobbyName===lobbyName}); });
  const {installRecordingStatusRoute}=require('../src/routes/recordingStatus');
  const {NotFoundError,errorHandler}=require('../src/middleware/error-handler');
  installRecordingStatusRoute(app,{source:()=>bus,authorize:async(req,id)=>{const resolved=await callRecordingRepo.resolveForUser(req.user.sub,id);if(!resolved.isParticipant||!resolved.detail)throw new NotFoundError('recording not found');return resolved.detail;}});
  app.use(errorHandler);
  webhookServer=http.createServer(app); await listen(webhookServer);
  const httpOrigin=`http://127.0.0.1:${webhookServer.address().port}`;
  process.env.LOBBY_AUTHORITY_URL=httpOrigin+'/owned-authority';
  const session=await native('/api/sessions',{creatorId:participants[0],lobbyName,name:'Owned live capture acceptance',metadata:{webhookUrl:httpOrigin+'/hooks/lvs/recording'},recordingConfiguration:{enabled:true,recordingProfile:'hangout',destination:'local',overrides:{transcribe:true}}});
  arn=session.channelArn; const seats=[];
  for(const userId of participants) seats.push(await native('/api/sessions/'+encodeURIComponent(arn)+'/join',{userId,displayName:userId,role:'publisher'}));
  const startedAt=new Date().toISOString();
  await videoSessionRepo.createSession({documentId:lobbyName,sessionId,channelArn:arn,orgId:'orgiq',startedBy:participants[0],startedAt,status:'active',streamKey:'',participants:[]});
  // The native SFU minted each signed media seat. Gateway acceptance remains
  // an explicit fixture; do not describe this as an application sign-in test.
  for(let n=0;n<participants.length;n++){
    const userId=participants[n], participantId=seats[n].participantId, clientId='owned-client-'+n+'-'+suffix;
    await videoSessionRepo.addOrReplaceParticipant(lobbyName,sessionId,{userId,participantId,clientId,displayName:userId,joinedAt:startedAt});
    assert(await videoSessionRepo.bindCallParticipant(lobbyName,sessionId,participantId,arn,{callId,clientId,userId,tenant:'orgiq',epoch:1,sessionId,channelArn:arn,lobbyName,boundAt:startedAt}));
  }
  const {chromium}=require(process.env.MEETING_ACCEPTANCE_PLAYWRIGHT_MODULE);
  browser=await chromium.launch({headless:true,args:['--autoplay-policy=no-user-gesture-required']});
  const publishers=[];
  for(let n=0;n<seats.length;n++){
    const page=await browser.newPage();
    await page.route(nativeOrigin+'/__owned-capture',r=>r.fulfill({contentType:'text/html',body:'<!doctype html><title>Owned recording acceptance</title>'}));
    await page.goto(nativeOrigin+'/__owned-capture');
    const wav=fs.readFileSync(path.join(process.env.MEETING_ACCEPTANCE_MEDIA_ROOT,'initiative',n?'u-nora.wav':'u-vera.wav')).toString('base64');
    await page.evaluate(async({wav,seat,origin,arn,n})=>{
      const audio=new AudioContext({sampleRate:48000}); const buffer=await audio.decodeAudioData(Uint8Array.from(atob(wav),c=>c.charCodeAt(0)).buffer);
      const destination=audio.createMediaStreamDestination(), source=audio.createBufferSource(); source.buffer=buffer; source.connect(destination);
      const canvas=document.createElement('canvas');canvas.width=640;canvas.height=360;document.body.append(canvas);const ctx=canvas.getContext('2d');
      const timer=setInterval(()=>{ctx.fillStyle=n?'#382b70':'#164d5d';ctx.fillRect(0,0,640,360);ctx.fillStyle='white';ctx.font='28px sans-serif';ctx.fillText('Owned participant '+(n+1),30,80);ctx.fillText(new Date().toISOString(),30,140)},60);
      const stream=new MediaStream([...canvas.captureStream(15).getVideoTracks(),...destination.stream.getAudioTracks()]);
      const pc=new RTCPeerConnection({iceServers:[]});for(const track of stream.getTracks())pc.addTrack(track,stream);
      const offer=await pc.createOffer();await pc.setLocalDescription(offer);
      const response=await fetch(origin+'/api/channels/'+encodeURIComponent(arn)+'/whip?participantId='+encodeURIComponent(seat.participantId),{method:'POST',headers:{authorization:'Bearer '+seat.token,'content-type':'application/sdp'},body:offer.sdp});
      if(!response.ok)throw new Error('WHIP admission '+response.status);await pc.setRemoteDescription({type:'answer',sdp:await response.text()});
      window.owned={pc,stream,audio,source,timer,started:false};
    },{wav,seat:seats[n],origin:nativeOrigin,arn,n}); publishers.push(page);
  }
  for(const page of publishers) await page.waitForFunction(()=>window.owned.pc.connectionState==='connected',{},{timeout:20_000});
  // Begin speech only once BOTH native transports are connected.
  for(const page of publishers) await page.evaluate(async()=>{await window.owned.audio.resume();window.owned.source.start();window.owned.started=true;});
  const captureDeadline=Date.now()+30_000;
  while(Date.now()<captureDeadline) await new Promise(r=>setTimeout(r,500));
  const captureStats=[];
  for(const page of publishers) captureStats.push(await page.evaluate(async()=>{const rows=[];for(const s of(await window.owned.pc.getStats()).values())if(s.type==='outbound-rtp')rows.push({kind:s.kind,bytesSent:s.bytesSent,packetsSent:s.packetsSent});return rows;}));
  assert(captureStats.every(rows=>rows.some(r=>r.kind==='audio'&&r.bytesSent>1000)&&rows.some(r=>r.kind==='video'&&r.bytesSent>1000)));
  await write('actual-browser-rtp.json',captureStats);
  await native('/api/channels/'+encodeURIComponent(arn)+'/recording-config',{enabled:false,destination:'local'},'PUT');
  await browser.close();browser=null;
  for(let n=0;n<participants.length;n++)await videoSessionRepo.removeParticipant(lobbyName,sessionId,seats[n].participantId);
  await videoSessionRepo.endSession(lobbyName,sessionId,new Date().toISOString());
  const deadline=Date.now()+90_000;
  while(!accepted.length && Date.now()<deadline)await new Promise(r=>setTimeout(r,200));
  assert.equal(accepted[0]?.pipelineId,RECORDING_FINALIZE_PIPELINE_ID,'native completion webhook must start finalize');
  const parentRunId=accepted[0].runId;
  while(!recordingId && Date.now()<deadline){const rows=await native('/api/channels/'+encodeURIComponent(arn)+'/recordings',null,'GET');recordingId=(Array.isArray(rows)?rows:rows.recordings)?.[0]?.recordingId;if(!recordingId)await new Promise(r=>setTimeout(r,200));}
  assert(recordingId);
  const observe=async()=>{let revision;while(Date.now()<deadline+180_000){const at=Date.now();const response=await fetch(httpOrigin+'/recordings/'+encodeURIComponent(recordingId)+'/status'+(revision?'?after='+revision:''),{headers:{authorization:'Bearer '+bearers[0]},signal:AbortSignal.timeout(30_000)});if(response.status===404){await new Promise(r=>setTimeout(r,100));continue;}assert.equal(response.status,200);const row=await response.json();statusReads.push({...row,waitMs:Date.now()-at});revision=row.revision;if(row.summaryStatus==='ready'||row.summaryStatus==='failed')return; }throw new Error('Status never reached terminal summary');};
  const observing=observe();
  // Parent completion can precede child completion; await every accepted actual
  // executor, including automatically added children, without manual start.
  for(let n=0;n<running.length;n++){const result=await running[n];await write('run-'+accepted[n].pipelineId+'-'+n+'.json',result);assert.equal(result.status,'completed');}
  const ready=await callRecordingRepo.getRecording(callId,recordingId,{consistentRead:true});
  assert.equal(ready.callBindingStatus,'bound');assert.equal(ready.recordingGoverned,true);assert.equal(ready.tracks.length,2);assert.equal(ready.participantCount,2);
  assert.equal(ready.transcriptionRequested,true);assert.equal(ready.transcriptStatus,'ready');assert.equal(ready.summaryStatus,'ready');
  const children=accepted.filter(r=>r.pipelineId===CALL_TRANSCRIPTION_PIPELINE_ID);assert.equal(children.length,1);assert.equal(ready.processingAttemptId,children[0].processingAttemptId);assert.equal(ready.summaryRunId,children[0].runId);
  const transcript=JSON.parse(ready.transcriptJson);assert(transcript.segments.length>=2);assert.equal(transcript.timestampGranularity,'segment');for(const s of transcript.segments){assert.equal(s.timestampSource,'engine');assert.equal(s.speakerId,'call');assert(s.endMs>=s.startMs);}assert.match(ready.summaryMarkdown,/^## Summary\n/);
  await observing;
  assert(statusReads.length>=2); assert.equal(statusReads.at(-1).summaryStatus,'ready');
  assert(statusReads.some(r=>r.summaryStatus!=='ready'));
  const denied=await fetch(httpOrigin+'/recordings/'+encodeURIComponent(recordingId)+'/status',{headers:{authorization:'Bearer '+bearerFor('not-recorded')}});assert.equal(denied.status,404);
  const after=ready;
  const nativeRows=await native('/api/channels/'+encodeURIComponent(arn)+'/recordings',null,'GET');
  const nativeRecord=(Array.isArray(nativeRows)?nativeRows:nativeRows.recordings).find(row=>row.recordingId===recordingId);
  assert(nativeRecord);assert.equal(nativeRecord.tracks.length,2);
  assert.equal(nativeRecord.composeStatus,'ok','Composition must cover the captured media clock without drain-time degradation');
  assert(nativeRecord.composeReport.inputs.every(input=>input.kept));
  assert(nativeRecord.tracks.every(track=>seats.some((seat,n)=>seat.participantId===track.tokenParticipantId&&participants[n]===track.userId)));
  const mediaResponse=await fetch(nativeOrigin+'/api/channels/'+encodeURIComponent(arn)+'/recordings/'+encodeURIComponent(recordingId)+'/segments/composed.mp4',{headers:{authorization:'Bearer '+process.env.VIDEO_ADMIN_TOKEN},signal:AbortSignal.timeout(30_000)});
  assert.equal(mediaResponse.status,200);const mediaBytes=Buffer.from(await mediaResponse.arrayBuffer());assert(mediaBytes.length>1000);
  const mediaFile=path.join(output,'native-composed.mp4');await fsp.writeFile(mediaFile,mediaBytes);
  const probe=JSON.parse(execFileSync('ffprobe',['-v','error','-show_entries','stream=codec_type,codec_name','-show_entries','format=duration,size','-of','json',mediaFile],{encoding:'utf8'}));
  assert(probe.streams.some(stream=>stream.codec_type==='audio'));assert(probe.streams.some(stream=>stream.codec_type==='video'));
  await write('native-media-proof.json',{recordingId,sha256:await hashFile(mediaFile),bytes:mediaBytes.length,probe,composeStatus:nativeRecord.composeStatus,allInputsRetained:true,allSeatsNativeSigned:true});
  const reads=[];for(const user of participants){const result=await callRecordingRepo.resolveForUser(user,recordingId);assert.equal(result.isParticipant,true);assert.equal(result.detail.summaryMarkdown,after.summaryMarkdown);reads.push({userId:user,isParticipant:true,exactSummarySHA256:hashValue(result.detail.summaryMarkdown)});}assert.equal((await callRecordingRepo.resolveForUser('not-recorded',recordingId)).isParticipant,false);
  await write('status-transitions.json',statusReads); await write('native-webhook-receipts.json',webhookReceipts);await write('recording-ready.private.json',after);await write('transcript.private.json',transcript);await write('summary.private.md',after.summaryMarkdown);await write('observed-lifecycle.json',observed);await write('native-deliveries.json',getRecentDeliveries());
  const proof={status:'passed',sourceSHA256:await hashFile(__filename),mediaSHA256:await hashFile(mediaFile),mediaBytes:mediaBytes.length,composeStatus:nativeRecord.composeStatus,actualBrowserRTP:true,actualNativeSignedMediaSeats:true,actualSFUCapture:true,actualNativeCompletionWebhook:true,actualFinalizeExecutor:true,automaticNativeChild:true,actualNativeDynamoTransactions:true,actualWhisper:true,actualCodexSubscription:true,actualNativeEventBusStatusHTTP:true,recordingRowPreSeeded:false,manualTranscriptionHelperCalled:false,
    callId,recordingId,lobbyName,sessionId,parentRunId,childRunId:children[0].runId,processingAttemptId:ready.processingAttemptId,segments:transcript.segments.length,summaryChars:after.summaryMarkdown.length,transcriptSHA256:hashValue(transcript),summarySHA256:hashValue(after.summaryMarkdown),statusResponses:statusReads.length,readResults:reads,nonparticipantDenied:true,
    boundaries:['Browser capture uses prepared speech WAV and animated canvas; no physical devices.','Native JWT seats and media are actual; accepted gateway call provenance and host directory authority are explicitly isolated fixtures.','Actual native PipelineExecutor/EventBus/Redis/WAL and DynamoDB run; production Raft and RunPersistence bootstrap are not exercised.','Chat/work-graph delivery is intercepted; primary application card rendering and search are not exercised in this harness.','Primary application, original media and existing services remain unchanged.']};
  await write('proof.json',proof);console.log(JSON.stringify({status:proof.status,recordingId,segments:proof.segments,statusResponses:proof.statusResponses,output}));

}
const deadline=setTimeout(()=>{console.error('Owned automatic finalizer acceptance deadline reached');process.exit(2);},360_000);
main().catch(async error=>{await fsp.mkdir(output,{recursive:true});await write('failure.json',{status:'failed',name:error.name,error:error.message,modelHTTPRequests:modelRequests.length,speechHTTPRequests:speechRequests.length,ownedTables:tables});console.error(error.name,error.message);process.exitCode=1;}).finally(async()=>{
  clearTimeout(deadline);require('../src/pipeline/bridge-types').setPipelineBridge(null);
  if(bus)await bus.stop();if(pubsub)await pubsub.stop();if(browser)await browser.close();for(const s of [webhookServer,modelProxy,speechProxy])if(s)await new Promise(resolve=>s.close(resolve));
  const {getRedisClient,getRedisCacheClient}=require('../src/lib/redis-client');for(const client of new Set(await Promise.all([getRedisClient(),getRedisCacheClient()])))if(client?.isOpen)await client.quit();
  const cleanup=[];if(ddbClient){const {DeleteTableCommand}=require('@aws-sdk/client-dynamodb');for(const table of tables){await ddbClient.send(new DeleteTableCommand({TableName:table}),{abortSignal:AbortSignal.timeout(10_000)});cleanup.push({table,deleted:true});}ddbClient.destroy();}await write('cleanup.json',{ownedTables:cleanup,sharedResourcesChanged:false});
});
