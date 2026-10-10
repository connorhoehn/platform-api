// Native repository + actual finalize/dispatch handlers over an inert command store.
// No engine, credentials, services, or model inference is started by this suite.
import { GetCommand, PutCommand, UpdateCommand, DeleteCommand, QueryCommand, TransactWriteCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { PipelineNode } from 'distributed-core/applications/pipeline';
import { CallRecordingRepository, type CallRecording } from '../../repositories/CallRecordingRepository';
let mockRepo: CallRecordingRepository;
const mockSfu = jest.fn();
const mockPost = jest.fn().mockResolvedValue(undefined);
const mockMeeting = jest.fn().mockResolvedValue({ published: true });
jest.mock('../../repositories', () => ({ callRecordingRepo: {
  putRecording: (r: never) => mockRepo.putRecording(r),
  getRecording: (c: string, r: string, o: never) => mockRepo.getRecording(c, r, o),
  putFinalizedRecording: (r: never) => mockRepo.putFinalizedRecording(r),
  markFinalizationAnnounced: (c: string, r: string) => mockRepo.markFinalizationAnnounced(c, r),
  updateTranscriptStatus: (c: string, r: string, p: never) => mockRepo.updateTranscriptStatus(c, r, p),
  updateComposeState: (c: string, r: string, p: never) => mockRepo.updateComposeState(c, r, p),
}, videoSessionRepo: {}, callReactionRepo: {} }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));
jest.mock('../../lib/sfu-breaker', () => ({ callSfu: (...a: unknown[]) => mockSfu(...a) }));
jest.mock('../../lib/aws-clients', () => ({ ddbClient: { send: jest.fn() }, docClient: { send: jest.fn() } }));
jest.mock('../../lib/redis-client', () => ({ getRedisClient: jest.fn().mockResolvedValue(null) }));
jest.mock('../../work-graph/sources/recordingLocator', () => ({ recordingLocator: { noteWrite: jest.fn() } }));
jest.mock('../../work-graph/sources/platformPublisher', () => ({ platformWorkGraphSourcePublisher: { meeting: (...a: unknown[]) => mockMeeting(...a) } }));
jest.mock('../../gateway/GatewayServiceClient', () => ({ postChatMessage: (...a: unknown[]) => mockPost(...a), PIPELINE_IDENTITY: { userId: 'pipeline', displayName: 'Pipeline' } }));
import { buildRecordingPipelineHandlers, KINDS, startRecordingTranscription } from '../recordingPipelines';

const PRIMARY = 'test-video-call-recordings', INDEX = 'test-video-call-recording-participants';
const conditional = () => Object.assign(new Error('native condition rejected'), { name: 'ConditionalCheckFailedException' });
function database() {
  const tables = new Map<string, Array<Record<string, any>>>();
  let failIndex = false;
  let hold: { waiting: Promise<void>; arrived: Promise<void>; release: () => void; entered: () => void } | undefined;
  const matches = (r: Record<string, any>, k: Record<string, unknown>) => Object.entries(k).every(([n, v]) => r[n] === v);
  const send = jest.fn(async (command: unknown) => {
    const input = (command as { input: any }).input, rows = tables.get(input.TableName) ?? [];
    if (command instanceof GetCommand) return { Item: structuredClone(rows.find(r => matches(r, input.Key))) };
    if (command instanceof PutCommand) {
      if (input.TableName === INDEX && failIndex) { failIndex = false; throw new Error('initial index unavailable'); }
      const row = structuredClone(input.Item), key = input.TableName === PRIMARY ? { callId: row.callId, recordingId: row.recordingId }
        : { userId: row.userId, startedAtRecordingId: row.startedAtRecordingId };
      if (input.ConditionExpression && rows.some(r => matches(r, key))) throw conditional();
      tables.set(input.TableName, [...rows.filter(r => !matches(r, key)), row]); return {};
    }
    if (command instanceof DeleteCommand) { tables.set(input.TableName, rows.filter(r => !matches(r, input.Key))); return {}; }
    if (command instanceof QueryCommand) return { Items: structuredClone(rows.filter(r => r.recordingId === input.ExpressionAttributeValues[':r'])) };
    if (command instanceof TransactWriteCommand) {
      if (hold) { const gate = hold; hold = undefined; gate.entered(); await gate.waiting; }
      const entries = input.TransactItems as Array<any>, checkpoint = entries[0].Update ?? entries[0].ConditionCheck;
      const primary = tables.get(PRIMARY)?.find(r => matches(r, checkpoint.Key));
      if (!primary || primary.finalizationState !== checkpoint.ExpressionAttributeValues[':pending']) throw Object.assign(new Error('completed checkpoint rejects repair'), { name: 'TransactionCanceledException' });
      if (failIndex) { failIndex = false; throw new Error('initial index unavailable'); }
      for (const entry of entries.slice(1)) {
        const item = structuredClone(entry.Put.Item), prior = tables.get(INDEX) ?? [];
        tables.set(INDEX, [...prior.filter(r => !matches(r, { userId: item.userId, startedAtRecordingId: item.startedAtRecordingId })), item]);
      }
      if (entries[0].Update) primary.finalizationState = checkpoint.ExpressionAttributeValues[':done'];
      return {};
    }
    if (command instanceof UpdateCommand) {
      const row = rows.find(r => matches(r, input.Key)), cond = input.ConditionExpression as string;
      const values = input.ExpressionAttributeValues ?? {}, names = input.ExpressionAttributeNames ?? {};
      if (!row && cond?.includes('attribute_exists(callId)')) throw conditional();
      for (const field of ['processingAttemptId', 'transcriptStatus']) if (cond?.includes(`attribute_not_exists(${field})`) && row?.[field] !== undefined) throw conditional();
      for (const [field, token] of [['processingAttemptId', ':epa'], ['#fs', ':pending'], ['transcriptStatus', ':ets']]) if (cond?.includes(`${field} = ${token}`)
        && row?.[names[field] ?? field] !== values[token]) throw conditional();
      if (!row) throw new Error('test refuses upsert');
      const [set, remove] = input.UpdateExpression.replace(/^SET /, '').split(' REMOVE ');
      for (const assignment of set.split(', ')) { const [field, token] = assignment.split(' = '); row[names[field] ?? field] = structuredClone(values[token]); }
      for (const field of remove?.split(', ') ?? []) delete row[names[field] ?? field];
      return { Attributes: structuredClone(row) };
    }
    throw new Error('unexpected native command');
  });
  return { send, client: { send } as unknown as DynamoDBDocumentClient,
    seed: (r: CallRecording) => tables.set(PRIMARY, [structuredClone(r)]),
    row: () => structuredClone(tables.get(PRIMARY)?.[0]) as CallRecording,
    index: () => structuredClone(tables.get(INDEX) ?? []), failNextIndex: () => { failIndex = true; },
    untag: () => tables.set(INDEX, []),
    holdNextTransaction: () => {
      let release!: () => void, entered!: () => void;
      const waiting = new Promise<void>(resolve => { release = resolve; }), arrived = new Promise<void>(resolve => { entered = resolve; });
      hold = { waiting, arrived, release, entered }; return { arrived, release };
    } };
}
const node = { id: 'test', type: 'test', position: { x: 0, y: 0 }, data: { type: 'test' } } as unknown as PipelineNode;
const context = () => ({ callId: 'canonical-call', recordingId: 'exact-recording', arn: 'native-channel', lobbyName: 'room:meeting',
  organizationId: 'org-one', startedBy: 'one', participants: [{ userId: 'one' }], callBindingStatus: 'legacy',
  detail: { startedAt: '2026-10-10T10:00:00Z', endedAt: '2026-10-10T10:01:00Z', durationMs: 60_000,
    tracks: [{ participantId: 'opaque-camera', tokenParticipantId: 'seat-one', userId: 'one', key: 'camera.mp4', url: '', size: 123 }], transcribe: true } });
const persisted = (): CallRecording => ({ callId: 'canonical-call', recordingId: 'exact-recording', channelArn: 'native-channel', lobbyName: 'room:meeting',
  startedAt: '2026-10-10T10:00:00Z', endedAt: '2026-10-10T10:01:00Z', durationMs: 60_000, segments: [], totalBytes: 123,
  participants: [{ userId: 'one' }], startedBy: 'one', orgId: 'org-one', destination: 'local', ttl: 2_000_000_000 });
let db: ReturnType<typeof database>;
beforeEach(() => { jest.clearAllMocks(); mockSfu.mockReset(); mockPost.mockResolvedValue(undefined); db = database(); mockRepo = new CallRecordingRepository(db.client); });
const execute = (kind: string, ctx: Record<string, unknown>, startRun = jest.fn().mockResolvedValue('child-run')) =>
  buildRecordingPipelineHandlers({ startRun }).get(kind)!.execute(node, ctx, { runId: 'finalize-run' } as never);

it('reproduces duplicate finalize erasing ready artifacts; the finalizer must preserve the saved generation', async () => {
  const ready = { ...persisted(), transcriptStatus: 'ready' as const, summaryStatus: 'ready' as const, transcriptJson: '{"segments":[]}',
    summaryMarkdown: '## Summary\nSaved exact result.', processingAttemptId: 'saved-generation', summaryRunId: 'saved-run', composeStatus: 'ok' as const };
  db.seed(ready);
  await execute(KINDS.persist, context());
  expect(db.row()).toEqual(ready);
  expect(db.index()).toEqual([]); // completed/legacy index removals are not recreated
});

it.each(['ok', 'degraded', 'failed', 'skipped'] as const)('reuses saved settled %s composition without a native media call or metadata rewrite', async composeStatus => {
  const ready = { ...persisted(), composeStatus, composedSegment: { key: 'native-composed.mp4', url: 'exact-saved-url', size: 98765 },
    transcriptStatus: 'ready' as const, summaryStatus: 'ready' as const, transcriptJson: 'saved-transcript', summaryMarkdown: 'saved-summary', processingAttemptId: 'saved-generation' };
  db.seed(ready);
  const stored = await execute(KINDS.persist, context());
  const out = await execute(KINDS.awaitCompose, { ...context(), ...(stored.output as Record<string, unknown>) });
  expect(out.output).toMatchObject({ composeStatus, composedKey: 'native-composed.mp4', composedBytes: 98765, reusedSavedComposition: true });
  expect(mockSfu).not.toHaveBeenCalled(); expect(db.row()).toEqual(ready);
  expect(db.send.mock.calls.filter(([command]) => command instanceof UpdateCommand)).toEqual([]);
});
it('only an unfinished composition polls native GET, then its own retry reuses the saved result', async () => {
  db.seed({ ...persisted(), composeStatus: 'queued' });
  const stored = await execute(KINDS.persist, context()), ctx = { ...context(), ...(stored.output as Record<string, unknown>) };
  mockSfu.mockResolvedValue({ ok: true, json: async () => ({ recordings: [{ recordingId: 'exact-recording', composeStatus: 'ok', composedSegment: { key: 'native-composed.mp4', size: 98765 } }] }) });
  expect((await execute(KINDS.awaitCompose, ctx)).output).toMatchObject({ composeStatus: 'ok', composedKey: 'native-composed.mp4' });
  expect((await execute(KINDS.awaitCompose, ctx)).output).toMatchObject({ reusedSavedComposition: true });
  expect(mockSfu).toHaveBeenCalledTimes(1);
  expect(mockSfu.mock.calls[0][1]).toMatchObject({ method: 'GET' });
  expect(db.row()).toMatchObject({ composeStatus: 'ok', composedSegment: { key: 'native-composed.mp4', size: 98765 } });
});

it('repairs interrupted initial participant indexing without replacing a newer processing generation', async () => {
  db.failNextIndex();
  await expect(execute(KINDS.persist, context())).rejects.toThrow('initial index unavailable');
  await mockRepo.updateTranscriptStatus('canonical-call', 'exact-recording', { status: 'processing', processingAttemptId: 'accepted-generation' });
  const result = await execute(KINDS.persist, context());
  expect(result.output).toMatchObject({ finalizationReplay: true });
  expect(db.row()).toMatchObject({ finalizationState: 'persisted', processingAttemptId: 'accepted-generation', transcriptStatus: 'processing' });
  expect(db.index()).toHaveLength(1);
  db.untag();
  await execute(KINDS.persist, context());
  expect(db.index()).toEqual([]);
});

it('a late overlapping repair cannot recreate a participant index after completed-state untag', async () => {
  const gate = db.holdNextTransaction(), first = execute(KINDS.persist, context());
  await gate.arrived;
  await execute(KINDS.persist, context());
  db.untag(); gate.release(); await first;
  expect(db.row().finalizationState).toBe('persisted'); expect(db.index()).toEqual([]);
});

it('batches a large initial index within Dynamo limits and fences every batch against completion', async () => {
  const people = Array.from({ length: 150 }, (_, n) => ({ userId: `person-${n}` }));
  await execute(KINDS.persist, { ...context(), startedBy: undefined, participants: people, detail: { ...context().detail, tracks: [] } });
  const transactions = db.send.mock.calls.map(([command]) => command).filter(command => command instanceof TransactWriteCommand) as TransactWriteCommand[];
  expect(transactions).toHaveLength(2);
  expect(transactions.map(t => t.input.TransactItems!.length)).toEqual([100, 52]);
  expect(transactions[0].input.TransactItems![0]).toMatchObject({ ConditionCheck: { ConditionExpression: '#fs = :pending' } });
  expect(transactions[1].input.TransactItems![0]).toMatchObject({ Update: { ConditionExpression: '#fs = :pending', UpdateExpression: 'SET #fs = :done' } });
  expect(db.index()).toHaveLength(150);
});

it('recovers an acknowledged transaction whose response is lost, without repeating completed indexing', async () => {
  const original = db.send.getMockImplementation()!; let lost = false;
  db.send.mockImplementation(async command => {
    const result = await original(command);
    if (command instanceof TransactWriteCommand && !lost) { lost = true; throw new Error('response lost after commit'); }
    return result;
  });
  await execute(KINDS.persist, context()); expect(db.row().finalizationState).toBe('persisted');
  db.untag(); await execute(KINDS.persist, context()); expect(db.index()).toEqual([]);
  expect(db.send.mock.calls.filter(([command]) => command instanceof TransactWriteCommand)).toHaveLength(1);
});

it('completes an empty initial participant index atomically without inventing entitlement', async () => {
  await execute(KINDS.persist, { ...context(), startedBy: undefined, participants: [], detail: { ...context().detail, tracks: [] } });
  expect(db.row()).toMatchObject({ finalizationState: 'persisted', participants: [] }); expect(db.index()).toEqual([]);
  const transaction = db.send.mock.calls.map(([command]) => command).find(command => command instanceof TransactWriteCommand) as TransactWriteCommand;
  expect(transaction.input.TransactItems).toHaveLength(1);
});

it('concurrent initial finalizers cannot replace the first durable row', async () => {
  const [a, b] = await Promise.all([execute(KINDS.persist, context()), execute(KINDS.persist, { ...context(), detail: { ...context().detail, totalBytes: 999 } })]);
  expect(db.row().totalBytes).toBe(0); // first webhook wins, not conflicting duplicate metadata
  expect([a.output, b.output].filter(o => (o as any).finalizationReplay === false)).toHaveLength(1);
  expect(db.index()).toHaveLength(1);
});

it('skips a completed recording announcement but retries an interrupted/failed post', async () => {
  const initial = await execute(KINDS.persist, context()), ctx = { ...context(), ...(initial.output as Record<string, unknown>) };
  mockPost.mockRejectedValueOnce(new Error('gateway unavailable'));
  expect((await execute(KINDS.announce, ctx)).output).toMatchObject({ posted: false });
  const retry = await execute(KINDS.persist, context());
  expect((await execute(KINDS.announce, { ...context(), ...(retry.output as Record<string, unknown>) })).output).toMatchObject({ posted: true });
  const replay = await execute(KINDS.persist, context());
  expect((await execute(KINDS.announce, { ...context(), ...(replay.output as Record<string, unknown>) })).output).toMatchObject({ posted: false, reason: 'already announced' });
  expect((await execute(KINDS.announce, ctx)).output).toMatchObject({ posted: false, reason: 'already announced' }); // same-run stale context too
  expect(mockPost).toHaveBeenCalledTimes(2);
  expect(mockMeeting).toHaveBeenCalledTimes(2); // sourceSequence is stable for at-least-once publication
});

const automatic = (startRun: jest.Mock) => execute(KINDS.startTranscription, { ...context(), composeStatus: 'ok' }, startRun);
it('two automatic finalizers claim one generation and enqueue one child', async () => {
  db.seed(persisted()); const startRun = jest.fn().mockResolvedValue('child-run');
  const results = await Promise.all([automatic(startRun), automatic(startRun)]);
  expect(startRun).toHaveBeenCalledTimes(1);
  expect(results.map(result => (result.output as any).started).sort()).toEqual([false, true]);
  expect(db.row().processingAttemptId).toMatch(/^[0-9a-f-]{36}$/);
});
it.each(['pending', 'processing', 'ready', 'failed'] as const)('automatic replay preserves %s attempts without enqueueing', async transcriptStatus => {
  const prior = { ...persisted(), transcriptStatus, processingAttemptId: 'prior-generation', transcriptStatusAt: '2026-10-10T10:01:00Z',
    summaryStatus: 'ready' as const, summaryMarkdown: 'saved-summary' };
  db.seed(prior); const startRun = jest.fn();
  expect((await automatic(startRun)).output).toMatchObject({ started: false, reason: 'transcription already claimed' });
  expect(startRun).not.toHaveBeenCalled(); expect(db.row()).toEqual(prior);
});
it('rejects a missing record or unavailable strong read rather than claiming a replay is complete', async () => {
  const startRun = jest.fn();
  expect((await automatic(startRun)).status).toBe('failed'); expect(startRun).not.toHaveBeenCalled();
  db.seed({ ...persisted(), transcriptStatus: 'ready', processingAttemptId: 'accepted' });
  const original = db.send.getMockImplementation()!;
  db.send.mockImplementation(async command => {
    if (command instanceof GetCommand && command.input.ConsistentRead) throw new Error('primary unavailable');
    return original(command);
  });
  expect((await automatic(startRun)).status).toBe('failed'); expect(startRun).not.toHaveBeenCalled();
});
it('a completed automatic replay does not require a currently available model configuration', async () => {
  db.seed({ ...persisted(), transcriptStatus: 'ready', processingAttemptId: 'saved' });
  const previous = process.env.RECORDING_SUMMARY_PROVIDER, startRun = jest.fn();
  process.env.RECORDING_SUMMARY_PROVIDER = 'unknown-provider';
  try { expect((await automatic(startRun)).output).toMatchObject({ started: false, reason: 'transcription already claimed' }); }
  finally { if (previous === undefined) delete process.env.RECORDING_SUMMARY_PROVIDER; else process.env.RECORDING_SUMMARY_PROVIDER = previous; }
  expect(startRun).not.toHaveBeenCalled();
});
it('known enqueue failure settles its generation and automatic replay does not silently retry inference', async () => {
  db.seed(persisted()); const startRun = jest.fn().mockRejectedValue(new Error('engine unavailable'));
  expect((await automatic(startRun)).status).toBe('failed');
  const failed = db.row(); expect(failed).toMatchObject({ transcriptStatus: 'failed', summaryStatus: 'failed', transcriptError: 'enqueue failed: engine unavailable' });
  await automatic(startRun); expect(startRun).toHaveBeenCalledTimes(1); expect(db.row()).toEqual(failed);
});
it.each(['processing', 'ready'] as const)('an accepted child %s survives a rejected enqueue acknowledgment', async transcriptStatus => {
  db.seed(persisted());
  const startRun = jest.fn(async (_id: string, _definition: unknown, payload: Record<string, unknown>) => {
    await mockRepo.updateTranscriptStatus('canonical-call', 'exact-recording', { status: transcriptStatus,
      expectedProcessingAttemptId: payload.processingAttemptId as string,
      ...(transcriptStatus === 'ready' ? { transcriptJson: 'actual accepted transcript', summaryStatus: 'ready', summaryMarkdown: 'actual accepted summary' } : {}),
    });
    throw new Error('accepted run acknowledgment lost');
  });
  expect((await automatic(startRun)).status).toBe('failed'); // finalize cannot claim enqueue acknowledgment
  const accepted = db.row(); expect(accepted.transcriptStatus).toBe(transcriptStatus);
  expect(accepted).not.toHaveProperty('transcriptError');
  if (transcriptStatus === 'ready') expect(accepted).toMatchObject({ transcriptJson: 'actual accepted transcript', summaryMarkdown: 'actual accepted summary' });
  await automatic(startRun); expect(startRun).toHaveBeenCalledTimes(1); expect(db.row()).toEqual(accepted);
});
it('a lost claim-to-enqueue process stays visibly clocked for bounded stall/manual retry, not duplicate enqueue', async () => {
  db.seed({ ...persisted(), transcriptStatus: 'pending', processingAttemptId: 'crashed-before-start', transcriptStatusAt: '2026-10-10T10:01:00Z' });
  const startRun = jest.fn(); await automatic(startRun); expect(startRun).not.toHaveBeenCalled();
  expect(db.row()).toMatchObject({ processingAttemptId: 'crashed-before-start', transcriptStatusAt: '2026-10-10T10:01:00Z' });
});
it('explicit retranscription retains fresh-generation semantics, including after failed or ready attempts', async () => {
  db.seed({ ...persisted(), transcriptStatus: 'ready', processingAttemptId: 'old', summaryStatus: 'ready', summaryMarkdown: 'old-summary' });
  const startRun = jest.fn().mockResolvedValue('manual-run');
  const first = await startRecordingTranscription(persisted(), 'native-participant', { startRun });
  const second = await startRecordingTranscription(persisted(), 'native-participant', { startRun });
  expect(first.processingAttemptId).not.toBe(second.processingAttemptId); expect(startRun).toHaveBeenCalledTimes(2);
  expect(db.row()).toMatchObject({ processingAttemptId: second.processingAttemptId, transcriptStatus: 'pending', summaryStatus: 'pending' });
  expect(db.row()).not.toHaveProperty('summaryMarkdown');
});
