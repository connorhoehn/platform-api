// A document step has to leave a real page behind, in the conversation the
// /agent command came from, editable by everyone in it — and the run's card
// becomes the page's card.
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn(async () => ({})) }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { PutCommand } from '@aws-sdk/lib-dynamodb';
import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import { createDocumentCreateStepHandler, namesLine, pageOps, validatePage } from '../documentCreateStepHandler';
import type { DocOp } from '../../documents/documentAgentOps';
import type { RunDocumentCardInput } from '../../documents/chatRunCard';
import { conversationMembers } from '../../chat/conversationMembers';

const helpers = { runId: 'exec-1', stepId: 'document:create:s1', emit: async () => undefined } as unknown as StepHandlerHelpers;
const node = (data: Record<string, unknown> = {}): PipelineNode => ({
  id: 'document:create:s1', type: 'document:create', position: { x: 0, y: 0 },
  data: { type: 'document:create', pageFrom: '$.steps.agent:s1.text', title: 'pizza oven', ...data },
} as unknown as PipelineNode);

function deps(roomMembers: string[] = []) {
  const puts: Array<Record<string, unknown>> = [];
  const written: Array<{ documentId: string; ops: DocOp[]; requestedBy: string }> = [];
  const cards: RunDocumentCardInput[] = [];
  return {
    puts, written, cards,
    opts: {
      documents: {
        send: async (command: unknown) => {
          if (command instanceof PutCommand) puts.push((command as unknown as { input: { Item: Record<string, unknown> } }).input.Item);
          return {};
        },
      },
      writeBody: async (documentId: string, _title: string, ops: DocOp[], requestedBy: string) => {
        written.push({ documentId, ops, requestedBy });
        return ops.length;
      },
      // The real reader, over a fake table: a DM never touches it, a room reads chat-members.
      members: (channel: string, requester: string) => conversationMembers(channel, requester, async () => ({
        Items: roomMembers.map((userId) => ({ channel, userId })),
      })),
      names: async () => new Map([['dev-eve', 'Eve Park']]),
      announce: async (input: RunDocumentCardInput) => { cards.push(input); return { outcome: 'promoted' as const, messageId: 'm-1' }; },
      newDocumentId: () => 'doc-pizza',
      now: () => new Date('2026-09-28T09:00:00.000Z'),
    },
  };
}

// What the mock devbox actually answers — not a page.
const MOCK_DRAFT = 'Made progress on the task; more work remains.\n\nTask complete: Create a new blank page.\nDONE';

const dmContext = {
  requestedBy: 'dev-dave', requestedByName: 'Dave Kim', chatChannel: 'chat:dm:dev-dave:dev-eve',
  instruction: 'Blank page title it pizza oven', loopId: 'loop-1',
  steps: { 'agent:s1': { text: MOCK_DRAFT } },
};

describe('the document step', () => {
  it('creates the page in the DM, owned by the requester and shared with both members as editors', async () => {
    const { opts, puts, written, cards } = deps();
    const outcome = await createDocumentCreateStepHandler(opts).execute(node(), dmContext, helpers);

    expect(outcome.status).toBe('completed');
    expect(puts).toHaveLength(1);
    expect(puts[0]).toMatchObject({
      documentId: 'doc-pizza', title: 'pizza oven', type: 'page', status: 'draft',
      createdBy: 'dev-dave', createdByName: 'Dave Kim',
      channel: 'chat:dm:dev-dave:dev-eve',
      editors: ['dev-dave', 'dev-eve'],
      createdByRunId: 'exec-1',
    });
    // A blank page is still a page: its title, no body.
    expect(written[0]).toMatchObject({ documentId: 'doc-pizza', requestedBy: 'dev-dave', ops: [{ op: 'setTitle', text: 'pizza oven' }] });
    expect((outcome as { output: Record<string, unknown> }).output).toMatchObject({
      documentId: 'doc-pizza', channel: 'chat:dm:dev-dave:dev-eve', editors: ['dev-dave', 'dev-eve'], card: 'promoted',
      result: { kind: 'document', documentId: 'doc-pizza' },
      notices: ['The draft was not a page; the page was created with its title and no body.'],
    });
    // The card the conversation already has is the one that turns into the page's.
    expect(cards).toEqual([expect.objectContaining({
      channel: 'chat:dm:dev-dave:dev-eve', runIds: ['loop-1', 'exec-1'], documentId: 'doc-pizza', title: 'pizza oven',
      requester: { userId: 'dev-dave', displayName: 'Dave Kim' },
      summary: '“Blank page title it pizza oven” · Created by the agent for Dave Kim & Eve Park',
    })]);
  });

  it('shares a room\'s page with the room\'s members at creation time', async () => {
    const { opts, puts } = deps(['dev-eve', 'dev-frank']);
    await createDocumentCreateStepHandler(opts).execute(node(), { ...dmContext, chatChannel: 'room:eng' }, helpers);
    expect(puts[0]).toMatchObject({ channel: 'room:eng', editors: ['dev-dave', 'dev-eve', 'dev-frank'] });
  });

  it('writes a drafted body when the draft is a page', async () => {
    const { opts, written } = deps();
    const draft = JSON.stringify({ title: 'Pizza oven build', blocks: [{ kind: 'heading', level: 2, text: 'Materials' }, { kind: 'bulletList', text: 'firebrick\nmortar' }, { kind: 'bogus', text: 'x' }] });
    await createDocumentCreateStepHandler(opts).execute(node(), { ...dmContext, steps: { 'agent:s1': { text: draft } } }, helpers);
    expect(written[0].ops).toEqual([
      { op: 'setTitle', text: 'Pizza oven build' },
      { op: 'appendBlock', kind: 'heading', level: 2, text: 'Materials' },
      { op: 'appendBlock', kind: 'bulletList', text: 'firebrick\nmortar' },
    ]);
  });

  it('keeps a page with no conversation to the requester and announces nothing', async () => {
    const { opts, puts, cards } = deps();
    const { chatChannel: _omit, ...noChannel } = dmContext;
    await createDocumentCreateStepHandler(opts).execute(node(), noChannel, helpers);
    expect(puts[0]).toMatchObject({ editors: ['dev-dave'] });
    expect(puts[0]).not.toHaveProperty('channel');
    expect(cards).toEqual([]);
  });

  it('refuses to create a page for nobody', async () => {
    const { opts, puts } = deps();
    const outcome = await createDocumentCreateStepHandler(opts).execute(node(), { chatChannel: 'room:eng' }, helpers);
    expect(outcome.status).toBe('failed');
    expect(puts).toEqual([]);
  });

  it('validates pages and names people', () => {
    expect(validatePage({ blocks: [] }, 'Fallback')).toEqual({ title: 'Fallback', blocks: [] });
    expect(validatePage('nope', 'x')).toBeNull();
    expect(pageOps({ title: 'T', blocks: [] })).toEqual([{ op: 'setTitle', text: 'T' }]);
    expect(namesLine(['A', 'B'])).toBe('A & B');
    expect(namesLine(['A', 'B', 'C', 'D'])).toBe('A, B & 2 others');
  });
});
