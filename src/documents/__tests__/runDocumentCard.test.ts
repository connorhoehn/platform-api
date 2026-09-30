// A run that made a document leaves ONE message: its card, turned into the
// document card, edited as whoever posted it. No card → one document card.
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() } }));

import { announceRunDocument, findRunCard, type RunDocumentCardInput } from '../chatRunCard';
import type { ChatHistoryMessage } from '../../gateway/GatewayServiceClient';

const input: RunDocumentCardInput = {
  channel: 'chat:dm:dev-dave:dev-eve', runIds: ['loop-1', 'exec-1'], pipelineId: 'chat-agent-loop',
  documentId: 'doc-pizza', title: 'pizza oven', icon: '📄',
  requester: { userId: 'dev-dave', displayName: 'Dave Kim' }, summary: '“Blank page” · Created by the agent for Dave Kim & Eve Park',
};
const runCard = (userId: string): ChatHistoryMessage => ({ id: 'm-card', userId, message: '/agent Blank page', metadata: { kind: 'agent-run', runId: 'loop-1', pipelineId: 'chat-agent-loop' } });

function fakeGateway(history: ChatHistoryMessage[]) {
  const edits: Array<Record<string, unknown>> = [];
  const posts: Array<Record<string, unknown>> = [];
  return {
    edits, posts,
    deps: {
      readHistory: jest.fn(async () => history),
      edit: jest.fn(async (e: Record<string, unknown>) => {
        edits.push(e);
        const m = history.find((h) => h.id === e.messageId)!;
        m.metadata = { ...m.metadata, ...(e.metadata as Record<string, unknown>) };
        m.message = e.message as string;
      }),
      post: jest.fn(async (p: Record<string, unknown>) => { posts.push(p); }),
    } as never,
  };
}

describe('announceRunDocument', () => {
  it('turns the requester\'s run card into the document card, keeping the run', async () => {
    const history = [runCard('dev-dave')];
    const gw = fakeGateway(history);
    await expect(announceRunDocument(input, gw.deps)).resolves.toEqual({ outcome: 'promoted', messageId: 'm-card' });
    expect(gw.posts).toEqual([]);
    expect(gw.edits[0]).toMatchObject({ channel: input.channel, messageId: 'm-card', message: 'Dave Kim created pizza oven', identity: { userId: 'dev-dave', kind: 'person' } });
    expect(history[0].metadata).toMatchObject({
      kind: 'document', documentId: 'doc-pizza', title: 'pizza oven', icon: '📄', createdBy: 'Dave Kim', createdById: 'dev-dave', displayName: 'Dave Kim',
      runId: 'loop-1',
      run: { runId: 'loop-1', pipelineId: 'chat-agent-loop', summary: input.summary },
    });
  });

  it('edits a card the pipeline posted as the pipeline', async () => {
    const gw = fakeGateway([runCard('agent:pipeline')]);
    await expect(announceRunDocument({ ...input, channel: 'room:eng' }, gw.deps)).resolves.toMatchObject({ outcome: 'promoted' });
    expect(gw.edits[0]).toMatchObject({ identity: { userId: 'agent:pipeline' } });
  });

  it('posts one document card when the conversation has no run card', async () => {
    const gw = fakeGateway([]);
    await expect(announceRunDocument(input, gw.deps)).resolves.toEqual({ outcome: 'posted' });
    expect(gw.posts).toEqual([expect.objectContaining({ channel: input.channel, message: 'Dave Kim created pizza oven', identity: expect.objectContaining({ userId: 'dev-dave' }), metadata: expect.objectContaining({ kind: 'document', documentId: 'doc-pizza' }) })]);
  });

  it('does not edit a card that already is this document\'s, and never repoints another document\'s card', async () => {
    const already = fakeGateway([{ id: 'm-card', userId: 'dev-dave', metadata: { kind: 'document', documentId: 'doc-pizza', runId: 'loop-1' } }]);
    await expect(announceRunDocument(input, already.deps)).resolves.toEqual({ outcome: 'already', messageId: 'm-card' });
    expect(already.edits).toEqual([]);
    const other = fakeGateway([{ id: 'm-card', userId: 'dev-dave', metadata: { kind: 'document', documentId: 'doc-first', run: { runId: 'loop-1' } } }]);
    await expect(announceRunDocument(input, other.deps)).resolves.toEqual({ outcome: 'posted' });
    expect(other.edits).toEqual([]);
    await expect(announceRunDocument({ ...input, postIfNoCard: false }, fakeGateway([]).deps)).resolves.toEqual({ outcome: 'skipped' });
  });

  it('never throws', async () => {
    const deps = { readHistory: jest.fn(async () => { throw new Error('gateway down'); }) } as never;
    await expect(announceRunDocument(input, deps)).resolves.toEqual({ outcome: 'failed', error: 'gateway down' });
  });

  it('finds the newest card for either run id', () => {
    const older = { ...runCard('dev-dave'), id: 'm-old' };
    const newer = { ...runCard('dev-dave'), id: 'm-new', metadata: { kind: 'agent-run', runId: 'exec-1' } };
    expect(findRunCard([older, { id: 'x', metadata: { kind: 'chat' } }, newer], ['loop-1', 'exec-1'])?.id).toBe('m-new');
    expect(findRunCard([older], ['other'])).toBeUndefined();
  });
});
