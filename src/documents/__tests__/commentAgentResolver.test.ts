// WS-J lane B: a comment handed to the agent is answered once, from the run's
// terminal event, on whichever replica sees it — and resolved only when the
// edit landed and the asker left "Resolve when applied" on.

import type { EventBus } from 'distributed-core';
import type { PipelineEventMap } from 'distributed-core/applications/pipeline';

jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() };
  return { __esModule: true, default: l, withContext: () => l };
});
jest.mock('../../repositories', () => ({ documentCommentRepo: {} }));
jest.mock('../../lib/event-catalog', () => ({ publishSignal: jest.fn(() => Promise.resolve()) }));
jest.mock('../../deck/deckReviseRequests', () => ({ DynamoClaimStore: class {} }));
jest.mock('../../middleware/idempotency', () => ({ IDEMPOTENCY_CLAIMS_TABLE: 't' }));

import { CommentAgentResolver } from '../commentAgentResolver';
import {
  commentScopedInstruction,
  settleCommentAgentRun,
  settlementReply,
  summarizeApply,
} from '../commentAgentRequests';
import type { DocumentComment } from '../../repositories/DocumentCommentRepository';

type Handler = (event: { payload: unknown }) => void | Promise<void>;
class StubBus {
  private handlers = new Map<string, Handler[]>();
  subscribe(name: string, h: Handler): string { this.handlers.set(name, [...(this.handlers.get(name) ?? []), h]); return name; }
  unsubscribe(): void {}
  async emit(name: keyof PipelineEventMap, payload: unknown): Promise<void> {
    for (const h of this.handlers.get(name) ?? []) await h({ payload });
  }
}

/** One table shared by two "replicas": the CAS is the table's, not a process's. */
function sharedTable(root: DocumentComment) {
  const rows = new Map<string, DocumentComment>([[root.commentId, root]]);
  return {
    rows,
    repo: {
      getComment: jest.fn(async (_d: string, id: string) => (rows.get(id) ? JSON.parse(JSON.stringify(rows.get(id))) : null)),
      settleAgentRequest: jest.fn(async (_d: string, id: string, runId: string, state: string, replyId: string, at: string) => {
        const row = rows.get(id);
        if (!row?.agentRequest || row.agentRequest.runId !== runId || row.agentRequest.state !== 'pending') return false;
        row.agentRequest = { ...row.agentRequest, state: state as never, replyId, settledAt: at };
        return true;
      }),
      createComment: jest.fn(async (c: Partial<DocumentComment>) => { rows.set(c.commentId!, c as DocumentComment); return c as DocumentComment; }),
      resolveThread: jest.fn(async (_d: string, id: string, by: string, at: string) => {
        const row = rows.get(id)!; row.resolved = true; row.resolvedBy = by; row.resolvedAt = at;
      }),
    },
  };
}

const root = (resolveOnApply = true): DocumentComment => ({
  documentId: 'doc-1', commentId: 'c-1', text: 'Rollback procedure needs an owner', userId: 'u-carol',
  displayName: 'carol@local.dev', color: '#000', timestamp: '2026-09-23T00:00:00Z',
  anchor: { v: 1, key: 'body', start: 'a', end: 'b', quote: 'Publish the rollback procedure' },
  agentRequest: { runId: 'run-12345678', pipelineId: 'document-agent-edit', state: 'pending', resolveOnApply, requestedBy: 'u-connor', requestedByName: 'Connor', at: 'x' },
});
const link = { documentId: 'doc-1', commentId: 'c-1', pipelineId: 'document-agent-edit' };
const applyOutput = { applied: 1, skipped: 0, mode: 'suggest', ops: [{ op: 'replaceBlock', index: 7, text: 'Publish the rollback procedure (owner: Carol)' }] };

describe('commentScopedInstruction', () => {
  it('carries the comment and scopes the edit to the anchored text', () => {
    const text = commentScopedInstruction('Address carol’s comment', root());
    expect(text).toContain('a comment by carol@local.dev: “Rollback procedure needs an owner”');
    expect(text).toContain('“Publish the rollback procedure”. Change only the block(s) containing it');
  });
  it('says nothing about a span for an unanchored comment', () => {
    const { anchor: _a, ...plain } = root();
    expect(commentScopedInstruction('x', plain)).not.toContain('about this text');
  });
});

describe('settlementReply', () => {
  it('addressed: links the run and says what changed in one line', () => {
    const r = settlementReply({ kind: 'completed', outcome: applyOutput }, 'document-agent-edit', 'run-12345678');
    expect(r.state).toBe('addressed');
    expect(r.text).toBe('Addressed by [run run-1234](/pipelines/document-agent-edit/runs/run-12345678) — Suggested 1 change: rewrote block 7 to “Publish the rollback procedure (owner: Carol)” — review it on the page.');
  });
  it('a model that skipped is a refusal, with its reason', () => {
    const r = settlementReply({ kind: 'completed', outcome: { applied: 0, reason: 'nothing names an owner' } }, 'p', 'run-1');
    expect(r.state).toBe('refused');
    expect(r.text).toContain('nothing names an owner. Left open.');
  });
  it('rejected at approval and failed runs leave it open and say why', () => {
    expect(settlementReply({ kind: 'rejected', reason: 'too big' }, 'p', 'r').text).toContain('declined at approval: too big');
    expect(settlementReply({ kind: 'failed', reason: 'LLM timeout' }, 'p', 'r')).toMatchObject({ state: 'failed' });
  });
  it('summarizeApply without ops still says something true', () => {
    expect(summarizeApply(null)).toBe('Made 0 changes on the page');
  });
});

describe('settleCommentAgentRun', () => {
  it('replies and resolves once when two replicas settle the same run', async () => {
    const t = sharedTable(root());
    const publish = jest.fn(() => Promise.resolve());
    const settlement = { kind: 'completed' as const, outcome: applyOutput };
    const results = await Promise.all([
      settleCommentAgentRun(link, 'run-12345678', settlement, { repo: t.repo, publish }),
      settleCommentAgentRun(link, 'run-12345678', settlement, { repo: t.repo, publish }),
    ]);
    expect(results.sort()).toEqual(['resolved', 'skipped']);
    expect(t.repo.createComment).toHaveBeenCalledTimes(1);
    expect(t.repo.resolveThread).toHaveBeenCalledTimes(1);
    expect(t.rows.get('c-1')!.agentRequest!.state).toBe('addressed');
    const reply = t.repo.createComment.mock.calls[0][0];
    expect(reply).toMatchObject({ parentCommentId: 'c-1', userId: 'agent:haiku', commentId: t.rows.get('c-1')!.agentRequest!.replyId });
    // A redelivery later changes nothing.
    expect(await settleCommentAgentRun(link, 'run-12345678', settlement, { repo: t.repo, publish })).toBe('skipped');
  });
  it('replies without resolving when "Resolve when applied" was off', async () => {
    const t = sharedTable(root(false));
    expect(await settleCommentAgentRun(link, 'run-12345678', { kind: 'completed', outcome: applyOutput }, { repo: t.repo, publish: jest.fn(() => Promise.resolve()) })).toBe('settled');
    expect(t.repo.resolveThread).not.toHaveBeenCalled();
  });
  it('a failed run replies and leaves the thread open', async () => {
    const t = sharedTable(root());
    await settleCommentAgentRun(link, 'run-12345678', { kind: 'failed', reason: 'boom' }, { repo: t.repo, publish: jest.fn(() => Promise.resolve()) });
    expect(t.repo.resolveThread).not.toHaveBeenCalled();
    expect(t.rows.get('c-1')!.agentRequest!.state).toBe('failed');
  });
  it('ignores a run the comment was not (or is no longer) handed to', async () => {
    const t = sharedTable(root());
    expect(await settleCommentAgentRun(link, 'run-other', { kind: 'completed', outcome: applyOutput }, { repo: t.repo })).toBe('skipped');
    expect(t.repo.createComment).not.toHaveBeenCalled();
  });
});

describe('CommentAgentResolver', () => {
  it('settles from the apply output on the replica that ran it, and looks the link up where it did not', async () => {
    const settle = jest.fn(async () => 'resolved' as const);
    const bus = new StubBus();
    const lookup = jest.fn(async (runId: string) => (runId === 'run-resumed' ? link : null));
    const r = new CommentAgentResolver({ eventBus: bus as unknown as EventBus<PipelineEventMap>, lookup, settle });
    r.start();
    r.remember('run-12345678', link);
    await bus.emit('pipeline:step:completed', { runId: 'run-12345678', stepId: 'apply', output: applyOutput, durationMs: 1, at: 'x' });
    await bus.emit('pipeline:run:completed', { runId: 'run-12345678', durationMs: 1, at: 'x' });
    await bus.emit('pipeline:run:failed', { runId: 'run-resumed', error: { nodeId: 'plan', message: 'timeout' }, at: 'x' });
    await bus.emit('pipeline:run:completed', { runId: 'run-unrelated', durationMs: 1, at: 'x' });
    await r.drain();
    expect(settle).toHaveBeenCalledWith(link, 'run-12345678', { kind: 'completed', outcome: applyOutput });
    expect(settle).toHaveBeenCalledWith(link, 'run-resumed', { kind: 'failed', reason: 'timeout' });
    expect(settle).toHaveBeenCalledTimes(2);
    expect(lookup).not.toHaveBeenCalledWith('run-12345678');
  });
  it('a rejected run is a refusal with the approver’s comment', async () => {
    const settle = jest.fn(async () => 'settled' as const);
    const bus = new StubBus();
    const r = new CommentAgentResolver({ eventBus: bus as unknown as EventBus<PipelineEventMap>, lookup: async () => link, settle });
    r.start();
    await bus.emit('pipeline:run:completed', { runId: 'run-x', durationMs: 1, at: 'x', status: 'rejected', rejection: { stepId: 'approve', userId: 'u', comment: 'too big', at: 'x' } });
    await r.drain();
    expect(settle).toHaveBeenCalledWith(link, 'run-x', { kind: 'rejected', reason: 'too big' });
  });
});
