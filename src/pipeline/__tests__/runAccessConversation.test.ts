/**
 * A run started from a conversation (an /agent loop) is readable by the people
 * in that conversation, not only the requester: its card sits in their thread,
 * and a 404 left the card at "Received" for the DM partner. Owner actions stay
 * the owner's, and strangers still learn nothing.
 */
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() } }));

import { assertRunAccess, setRunAccessDeps, type RunAccessDeps } from '../runAccess';
import { dmNamesAdmit } from '../../deck/documentEditAccess';

const req = (sub: string) => ({
  user: { sub, orgId: 'org-1' },
  headers: { authorization: 'Bearer x' },
  header: () => undefined,
  body: {},
  method: 'GET',
  path: '/api/pipelines/chat-agent-loop/runs/loop-1',
}) as any;

function deps(chatChannel: string, roomMembers: string[] = []): RunAccessDeps {
  return {
    liveRun: async () => null,
    storedRun: async (_pid, runId) => (runId === 'loop-1' ? { ownerId: 'dev-dave', pipelineId: 'chat-agent-loop', chatChannel } : null),
    documentRow: async () => null,
    conversationMember: async (channel, ownerId, userId) => dmNamesAdmit(channel, ownerId, userId) || roomMembers.includes(userId),
  };
}

afterEach(() => setRunAccessDeps(null));

describe('a run started from a conversation', () => {
  it('is readable by the other person in the DM', async () => {
    setRunAccessDeps(deps('chat:dm:dev-dave:dev-eve'));
    await expect(assertRunAccess(req('dev-eve'), 'chat-agent-loop', 'loop-1', 'read')).resolves.toMatchObject({ role: 'reader', ownerId: 'dev-dave' });
  });
  it('is readable by a member of the room', async () => {
    setRunAccessDeps(deps('room:eng', ['dev-eve']));
    await expect(assertRunAccess(req('dev-eve'), 'chat-agent-loop', 'loop-1', 'read')).resolves.toMatchObject({ role: 'reader' });
  });
  it('stays hidden from someone outside the conversation', async () => {
    setRunAccessDeps(deps('chat:dm:dev-dave:dev-eve'));
    await expect(assertRunAccess(req('dev-mallory'), 'chat-agent-loop', 'loop-1', 'read')).rejects.toThrow('run not found');
  });
  it('does not make a member the owner', async () => {
    setRunAccessDeps(deps('chat:dm:dev-dave:dev-eve'));
    await expect(assertRunAccess(req('dev-eve'), 'chat-agent-loop', 'loop-1', 'owner')).rejects.toBeTruthy();
  });
});
