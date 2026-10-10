const update = jest.fn().mockResolvedValue(undefined);
jest.mock('../../repositories', () => ({ callRecordingRepo: { updateTranscriptStatus: update }, videoSessionRepo: {}, callReactionRepo: {} }));
jest.mock('../../lib/aws-clients', () => ({ ddbClient: { send: jest.fn() }, docClient: { send: jest.fn() } }));
jest.mock('../../lib/redis-client', () => ({ getRedisClient: jest.fn().mockResolvedValue(null) }));
jest.mock('../../gateway/GatewayServiceClient', () => ({ postChatMessage: jest.fn(), PIPELINE_IDENTITY: { userId: 'pipeline', displayName: 'Pipeline' } }));
import { buildCallTranscriptionDefinition, buildRecordingPipelineHandlers, KINDS, recordingSummaryConfiguration, startRecordingTranscription } from '../recordingPipelines';
import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import type { LLMClient, LLMChunk } from '../../pipeline/LLMClient';

const env = { provider: process.env.RECORDING_SUMMARY_PROVIDER, model: process.env.RECORDING_SUMMARY_MODEL, legacy: process.env.DOCUMENT_AGENT_MODEL };
const helpers = { runId: 'native-run-2', abortSignal: new AbortController().signal } as StepHandlerHelpers;
// The native handler registry supports custom kinds beyond the built-in NodeType union.
const summaryNode = { id: 'summarize', type: KINDS.summarize, position: { x: 800, y: 0 }, data: { type: KINDS.summarize, provider: 'bedrock', model: 'amazon.nova-pro-v1:0' } } as unknown as PipelineNode;
const speech = { callId: 'call-1', recordingId: 'recording-1', processingAttemptId: 'generation-2', hasSpeech: true, transcriptText: '[00:04] Call: We will ship the migration on Friday.' };
const stream = jest.fn();
const client: LLMClient = { provider: 'bedrock', stream };
const run = (ctx: Record<string, unknown>, node = summaryNode) => buildRecordingPipelineHandlers({ summaryLlmClient: client }).get(KINDS.summarize)!.execute(node, ctx, helpers);
beforeEach(() => { jest.clearAllMocks(); update.mockResolvedValue(undefined); delete process.env.RECORDING_SUMMARY_PROVIDER; delete process.env.RECORDING_SUMMARY_MODEL; delete process.env.DOCUMENT_AGENT_MODEL; });
afterAll(() => { for (const [key, value] of [['RECORDING_SUMMARY_PROVIDER', env.provider], ['RECORDING_SUMMARY_MODEL', env.model], ['DOCUMENT_AGENT_MODEL', env.legacy]]) { if (value === undefined) delete process.env[key!]; else process.env[key!] = value; } });

describe('recording summary provider selection', () => {
  it('retains the tool-free legacy agent when not explicitly configured', () => {
    const graph = buildCallTranscriptionDefinition() as { nodes: PipelineNode[] };
    expect(graph.nodes.find(node => node.id === 'summarize')).toMatchObject({ type: 'agent', data: { allowedTools: [], maxTurns: 1, model: 'haiku' } });
    expect(graph.nodes.find(node => node.id === 'has-speech')).toMatchObject({ data: { expression: 'context.hasSpeech === true' } });
  });
  it('chooses Nova locally without changing global pipeline models', () => {
    process.env.RECORDING_SUMMARY_PROVIDER = 'bedrock';
    expect(recordingSummaryConfiguration()).toEqual({ provider: 'bedrock', model: 'amazon.nova-pro-v1:0' });
    expect((buildCallTranscriptionDefinition() as { nodes: PipelineNode[] }).nodes.find(node => node.id === 'summarize')).toMatchObject({ type: KINDS.summarize, data: { provider: 'bedrock', model: 'amazon.nova-pro-v1:0' } });
  });
  it('lets Codex use its active subscription default rather than a Claude alias', () => {
    process.env.RECORDING_SUMMARY_PROVIDER = 'codex';
    expect(recordingSummaryConfiguration()).toEqual({ provider: 'codex', model: '' });
    process.env.RECORDING_SUMMARY_MODEL = 'selected-native-model';
    expect(recordingSummaryConfiguration().model).toBe('selected-native-model');
  });
  it('rejects unknown or tool-capable implicit provider selection', () => {
    process.env.RECORDING_SUMMARY_PROVIDER = 'claude-code';
    expect(() => buildCallTranscriptionDefinition()).toThrow('leave unset');
  });
});

describe('native recording inference handler', () => {
  it('uses the stored timestamped Call transcript and records actual provider output', async () => {
    stream.mockReturnValue((async function* (): AsyncGenerator<LLMChunk> { yield { done: false, token: 'partial' }; yield { done: true, response: '## Summary\nThe migration will ship on Friday.', tokensIn: 122, tokensOut: 18 }; })());
    const result = await run(speech);
    expect(result).toMatchObject({ status: 'completed', output: { text: '## Summary\nThe migration will ship on Friday.', provider: 'bedrock', model: 'amazon.nova-pro-v1:0', tokensIn: 122, tokensOut: 18 } });
    expect(stream).toHaveBeenCalledWith('amazon.nova-pro-v1:0', expect.stringContaining('speaker labels verbatim'), expect.stringContaining(speech.transcriptText), expect.objectContaining({ signal: helpers.abortSignal }));
    expect(stream.mock.calls[0][1]).toContain('untrusted meeting content');
    expect(update).toHaveBeenCalledWith('call-1', 'recording-1', expect.objectContaining({ summaryStatus: 'processing', expectedSummaryRunId: 'native-run-2', expectedProcessingAttemptId: 'generation-2' }));
  });
  it('makes no model call and no persistence mutation for silence', async () => {
    await expect(run({ ...speech, hasSpeech: false, transcriptText: '' })).resolves.toMatchObject({ output: { skipped: true, reason: 'no speech' } });
    expect(stream).not.toHaveBeenCalled(); expect(update).not.toHaveBeenCalled();
  });
  it('propagates provider unavailability, without producing a substitute summary', async () => {
    stream.mockReturnValue((async function* () { throw new Error('native provider unavailable'); })());
    await expect(run(speech)).rejects.toThrow('native provider unavailable');
    expect(update).toHaveBeenCalledTimes(1);
  });
  it('rejects an unfinished stream instead of storing its partial text', async () => {
    stream.mockReturnValue((async function* () { yield { done: false, token: 'incomplete' }; })());
    await expect(run(speech)).resolves.toMatchObject({ status: 'failed', error: expect.stringContaining('without a completion') });
  });
  it('does not call the provider after the generation or run was superseded', async () => {
    update.mockRejectedValue(Object.assign(new Error('obsolete generation'), { name: 'ConditionalCheckFailedException' }));
    await expect(run(speech)).rejects.toThrow('obsolete generation');
    expect(stream).not.toHaveBeenCalled();
  });
});

describe('trusted native recording restart', () => {
  const rec = { callId: 'call-1', recordingId: 'recording-1', channelArn: 'arn:native:media', lobbyName: 'orgiq:initiative:p-observability', orgId: 'orgiq', startedBy: 'user-1' };
  it('claims a fresh generation and removes stale summary artifacts before dispatching the exact row', async () => {
    const startRun = jest.fn().mockResolvedValue('native-run-2');
    const out = await startRecordingTranscription(rec, 'requester-2', { startRun });
    expect(out.runId).toBe('native-run-2'); expect(out.processingAttemptId).toMatch(/^[0-9a-f-]{36}$/);
    expect(update).toHaveBeenCalledWith(rec.callId, rec.recordingId, expect.objectContaining({ status: 'pending', processingAttemptId: out.processingAttemptId, summaryStatus: 'pending', summaryRunId: null, summaryMarkdown: null, summaryKey: null, summaryError: null }));
    expect(startRun.mock.calls[0][2]).toMatchObject({ callId: rec.callId, recordingId: rec.recordingId, arn: rec.channelArn, lobbyName: rec.lobbyName, processingAttemptId: out.processingAttemptId, organizationId: rec.orgId, startedBy: rec.startedBy, requestedBy: 'requester-2' });
    expect(update.mock.invocationCallOrder[0]).toBeLessThan(startRun.mock.invocationCallOrder[0]);
  });
  it('settles enqueue failure on only its own generation instead of leaving pending forever', async () => {
    const startRun = jest.fn().mockRejectedValue(new Error('native engine stopped'));
    await expect(startRecordingTranscription(rec, 'user-1', { startRun })).rejects.toThrow('native engine stopped');
    const attempt = update.mock.calls[0][2].processingAttemptId;
    expect(update.mock.calls[1][2]).toMatchObject({ status: 'failed', expectedProcessingAttemptId: attempt, transcriptError: 'enqueue failed: native engine stopped' });
  });
});
