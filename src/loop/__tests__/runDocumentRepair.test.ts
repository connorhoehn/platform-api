// A Done /agent run whose document never reached its conversation is repaired
// by the same function create time uses: the row moved onto the conversation
// (channel + the members as editors), the card turned into the document card.
// Every step is a no-op the second time.
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn(async () => ({})) }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { GetCommand, PutCommand, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import type { PipelineRunItem } from '../../repositories/PipelineRunRepository';
import { announceRunDocument } from '../../documents/chatRunCard';
import type { ChatHistoryMessage } from '../../gateway/GatewayServiceClient';
import { putRunDocumentInConversation } from '../conversationDocument';
import {
  planRunDocumentRepair, repairAllRunDocuments, repairDocumentId, repairRunDocuments,
  resetRunDocumentRepairMemo, scheduleRunDocumentRepair, REPAIR_NOTE,
} from '../runDocumentRepair';

const CHANNEL = 'chat:dm:dev-alice:dev-grace';
const docStep = { id: 's1', title: 'Create pizza oven page', kind: 'document', prompt: "Create a new blank page and title it 'pizza oven'.", dependsOn: [], done: 'x' };

function executor(over: Partial<PipelineRunItem> & { stepsSummary?: Record<string, unknown> } = {}): PipelineRunItem {
  return {
    pipelineId: 'chat-agent-loop', runId: 'exec-1', status: 'completed', triggeredBy: 'dev-alice',
    checkpoint: { context: { chatChannel: CHANNEL, requestedBy: 'dev-alice', requestedByName: 'Alice Chen', instruction: 'Blank page title it pizza oven', plan: { steps: [docStep] } } },
    stepsSummary: { 'agent:s1': { status: 'completed' } },
    ...over,
  } as unknown as PipelineRunItem;
}
const loopRow = { pipelineId: 'chat-agent-loop', runId: 'loop-1', status: 'pending', agentLoopRunId: 'exec-1' } as unknown as PipelineRunItem;

/** A documents table and a chat channel, in memory. */
function world(opts: { rows?: Record<string, Record<string, unknown>>; history?: ChatHistoryMessage[] } = {}) {
  const rows: Record<string, Record<string, unknown>> = { ...(opts.rows ?? {}) };
  const history: ChatHistoryMessage[] = opts.history ?? [{ id: 'm-card', userId: 'dev-alice', message: '/agent Blank page', metadata: { kind: 'agent-run', runId: 'loop-1', pipelineId: 'chat-agent-loop' } }];
  const writes: string[] = [];
  const edits: Array<Record<string, unknown>> = [];
  const posts: Array<Record<string, unknown>> = [];
  const bodies: string[] = [];
  const send = async (command: unknown) => {
    const input = (command as { input: Record<string, any> }).input;
    if (command instanceof GetCommand) return { Item: rows[input.Key.documentId] ? { ...rows[input.Key.documentId] } : undefined };
    if (command instanceof PutCommand) {
      if (rows[input.Item.documentId]) throw Object.assign(new Error('exists'), { name: 'ConditionalCheckFailedException' });
      rows[input.Item.documentId] = { ...input.Item }; writes.push(`put:${input.Item.documentId}`); return {};
    }
    if (command instanceof UpdateCommand) {
      const row = rows[input.Key.documentId];
      rows[input.Key.documentId] = { ...row, channel: input.ExpressionAttributeValues[':channel'], editors: input.ExpressionAttributeValues[':editors'] };
      writes.push(`update:${input.Key.documentId}`); return {};
    }
    throw new Error('unexpected command');
  };
  const announce = (card: Parameters<typeof announceRunDocument>[0]) => announceRunDocument(card, {
    readHistory: async () => history.map((m) => ({ ...m, metadata: { ...m.metadata } })),
    edit: (async (e: Record<string, unknown>) => {
      edits.push(e);
      const m = history.find((h) => h.id === e.messageId)!;
      m.metadata = { ...m.metadata, ...(e.metadata as Record<string, unknown>) };
    }) as never,
    post: (async (p: Record<string, unknown>) => { posts.push(p); }) as never,
  });
  const runs: Record<string, PipelineRunItem> = {};
  return {
    rows, history, writes, edits, posts, bodies, runs,
    deps: {
      documents: { send },
      names: async () => new Map([['dev-grace', 'Grace Wilson']]),
      announce,
      getRun: async (_p: string, r: string) => runs[r] ?? null,
      writeBody: async (documentId: string) => { bodies.push(documentId); return 1; },
      now: () => new Date('2026-09-29T12:00:00.000Z'),
    },
  };
}

describe('putRunDocumentInConversation', () => {
  const input = {
    documentId: 'doc-1', title: 'pizza oven', icon: '📄', channel: CHANNEL, runIds: ['loop-1', 'exec-1'], pipelineId: 'chat-agent-loop',
    requester: { userId: 'dev-alice', displayName: 'Alice Chen' }, instruction: 'Blank page title it pizza oven', postIfNoCard: false,
  };

  it('attaches a produced page to the DM and promotes the run card', async () => {
    const w = world({ rows: { 'doc-1': { documentId: 'doc-1', title: 'pizza oven', createdBy: 'dev-alice', editors: ['dev-alice'] } } });
    const out = await putRunDocumentInConversation(input, w.deps);
    expect(out).toMatchObject({ document: 'attached', editors: ['dev-alice', 'dev-grace'], card: { outcome: 'promoted', messageId: 'm-card' } });
    expect(w.rows['doc-1']).toMatchObject({ channel: CHANNEL, editors: ['dev-alice', 'dev-grace'] });
    expect(w.history[0].metadata).toMatchObject({ kind: 'document', documentId: 'doc-1', run: { runId: 'loop-1', summary: '“Blank page title it pizza oven” · Created by the agent for Alice Chen & Grace Wilson' } });
  });

  it('is a no-op when the page is already attached and the card already is its card', async () => {
    const w = world({ rows: { 'doc-1': { documentId: 'doc-1', channel: CHANNEL, editors: ['dev-alice', 'dev-grace'] } } });
    await putRunDocumentInConversation(input, w.deps);
    expect(w.writes).toEqual([]);
    expect(w.edits).toHaveLength(1);
    const again = await putRunDocumentInConversation(input, w.deps);
    expect(again).toMatchObject({ document: 'already', card: { outcome: 'already', messageId: 'm-card' } });
    expect(w.writes).toEqual([]);
    expect(w.edits).toHaveLength(1);
  });

  it('leaves a page that belongs to another conversation where it is', async () => {
    const w = world({ rows: { 'doc-1': { documentId: 'doc-1', channel: 'chat:dm:dev-dave:dev-eve', editors: ['dev-dave'] } } });
    await expect(putRunDocumentInConversation(input, w.deps)).resolves.toMatchObject({ document: 'elsewhere', card: { outcome: 'skipped' } });
    expect(w.writes).toEqual([]);
    expect(w.edits).toEqual([]);
  });

  it('a repair never posts a card it could not find', async () => {
    const w = world({ rows: { 'doc-1': { documentId: 'doc-1' } }, history: [] });
    await expect(putRunDocumentInConversation(input, w.deps)).resolves.toMatchObject({ document: 'attached', card: { outcome: 'skipped' } });
    expect(w.posts).toEqual([]);
  });
});

describe('repairRunDocuments', () => {
  beforeEach(() => resetRunDocumentRepairMemo());

  it('creates the page a devbox-era document step never made, once, and turns the card into it', async () => {
    const w = world();
    w.runs['loop-1'] = loopRow;
    w.runs['exec-1'] = executor();
    const id = repairDocumentId('exec-1', 's1');
    const first = await repairRunDocuments('chat-agent-loop', 'loop-1', w.deps);
    expect(first).toMatchObject({ loopId: 'loop-1', executorRunId: 'exec-1', channel: CHANNEL, documents: [{ documentId: id, title: 'pizza oven', created: true, card: 'promoted' }] });
    expect(w.rows[id]).toMatchObject({ title: 'pizza oven', type: 'page', createdBy: 'dev-alice', createdByName: 'Alice Chen', channel: CHANNEL, editors: ['dev-alice', 'dev-grace'], createdByRunId: 'exec-1' });
    expect(w.bodies).toEqual([id]);
    expect((w.history[0].metadata as { run: { summary: string } }).run.summary).toContain(REPAIR_NOTE);

    const second = await repairRunDocuments('chat-agent-loop', 'loop-1', w.deps);
    expect(second.documents).toEqual([{ documentId: id, title: 'pizza oven', created: false, document: 'already', card: 'already' }]);
    expect(w.writes).toEqual([`put:${id}`]);
    expect(w.bodies).toEqual([id]);
    expect(w.edits).toHaveLength(1);
  });

  it('attaches a page the run did make, asked about by the executor id', async () => {
    const w = world({ rows: { 'doc-9': { documentId: 'doc-9', editors: ['dev-alice'] } } });
    w.runs['exec-1'] = executor({
      checkpoint: { context: { ...(executor().checkpoint as { context: Record<string, unknown> }).context, loopId: 'loop-1' } },
      stepsSummary: { 'document:create:s1': { status: 'completed', output: { documentId: 'doc-9', title: 'pizza oven' } } },
    } as never);
    const out = await repairRunDocuments('chat-agent-loop', 'exec-1', w.deps);
    expect(out.documents).toEqual([{ documentId: 'doc-9', title: 'pizza oven', created: false, document: 'attached', card: 'promoted' }]);
    expect(w.bodies).toEqual([]);
  });

  it('touches nothing on a dry run, a failed run, or a run with no document step', async () => {
    const w = world();
    w.runs['loop-1'] = loopRow;
    w.runs['exec-1'] = executor();
    await expect(repairRunDocuments('chat-agent-loop', 'loop-1', w.deps, { apply: false })).resolves.toMatchObject({ documents: [{ created: true, card: 'dry-run' }] });
    expect(planRunDocumentRepair('loop-1', executor({ status: 'failed' } as never))).toBeNull();
    expect(planRunDocumentRepair('loop-1', executor({ checkpoint: { context: { chatChannel: CHANNEL, requestedBy: 'dev-alice', plan: { steps: [{ ...docStep, kind: 'research' }] } } } } as never))).toBeNull();
    expect(w.writes).toEqual([]);
    expect(w.edits).toEqual([]);
  });

  it('a read repairs a completed agent loop once per process', async () => {
    const w = world();
    w.runs['loop-1'] = loopRow;
    w.runs['exec-1'] = executor();
    await expect(scheduleRunDocumentRepair('other-pipeline', 'loop-1', 'completed', w.deps)).resolves.toBeNull();
    await expect(scheduleRunDocumentRepair('chat-agent-loop', 'loop-1', 'running', w.deps)).resolves.toBeNull();
    await expect(scheduleRunDocumentRepair('chat-agent-loop', 'loop-1', 'completed', w.deps)).resolves.toMatchObject({ documents: [{ created: true }] });
    await expect(scheduleRunDocumentRepair('chat-agent-loop', 'loop-1', 'completed', w.deps)).resolves.toBeNull();
  });

  it('the backfill walks every loop row and repairs each run', async () => {
    const w = world();
    w.runs['loop-1'] = loopRow;
    w.runs['exec-1'] = executor();
    const query = async (command: unknown) => {
      expect(command).toBeInstanceOf(QueryCommand);
      return { Items: [loopRow, executor(), { pipelineId: 'chat-agent-loop', runId: 'loop-2', agentLoopRunId: 'gone' }] };
    };
    const out = await repairAllRunDocuments({ ...w.deps, query }, { apply: true });
    expect(out.scanned).toBe(2);
    expect(out.repaired.map((r) => r.loopId)).toEqual(['loop-1']);
    expect(out.failed).toEqual([]);
  });
});
