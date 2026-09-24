// NFR #41: who may change a document — the rule every deck write path shares.
import { canEditDocument, documentRoleOf, editRefusal } from '../documentEditAccess';

const row = { createdBy: 'dev-connor', editors: ['dev-erin'], viewers: ['dev-frank'] };

describe('document edit access', () => {
  it.each([
    ['dev-connor', 'owner', true],
    ['dev-erin', 'editor', true],
    ['dev-frank', 'viewer', false],
    ['dev-mallory', 'stranger', false],
  ])('%s is %s → allow=%s', async (sub, role, allow) => {
    expect(documentRoleOf(row, sub)).toBe(role);
    const decision = await canEditDocument(row, { sub }, 'doc-1');
    expect(decision.allow).toBe(allow);
    if (!decision.allow) expect(decision.status).toBe(403);
  });

  it('fails closed: no owner on the row, or no caller, is a refusal', async () => {
    expect((await canEditDocument({ editors: ['dev-erin'] }, { sub: 'dev-mallory' }, 'doc-1')).allow).toBe(false);
    expect((await canEditDocument(row, { sub: '' }, 'doc-1')).allow).toBe(false);
    expect((await canEditDocument(row, undefined, 'doc-1')).allow).toBe(false);
  });

  it('a row that does not exist yet is the caller\'s to create', async () => {
    expect((await canEditDocument(null, { sub: 'dev-mallory' }, 'doc-new')).allow).toBe(true);
  });

  it('tells a viewer they can view but not change it', () => {
    expect(editRefusal(row, 'dev-frank')).toMatch(/view this document but not change it/);
    expect(editRefusal(row, 'dev-mallory')).toMatch(/owner or an editor/);
  });

  it('assertCanEditDocument throws a 403 forbidden naming the document, and passes owner/editor/new rows', async () => {
    const { assertCanEditDocument, DocumentEditForbiddenError } = await import('../documentEditAccess');
    const row = { createdBy: 'u-owner', editors: ['u-ed'], viewers: ['u-view'] };
    await expect(assertCanEditDocument(row, { sub: 'u-owner' }, 'd1')).resolves.toBeUndefined();
    await expect(assertCanEditDocument(row, { sub: 'u-ed' }, 'd1')).resolves.toBeUndefined();
    await expect(assertCanEditDocument(null, { sub: 'u-anyone' }, 'd1')).resolves.toBeUndefined();
    for (const [sub, role] of [['u-view', 'viewer'], ['u-x', 'stranger']] as const) {
      const err = await assertCanEditDocument(row, { sub }, 'd1').catch((e: unknown) => e);
      expect(err).toBeInstanceOf(DocumentEditForbiddenError);
      expect((err as InstanceType<typeof DocumentEditForbiddenError>).status).toBe(403);
      expect((err as InstanceType<typeof DocumentEditForbiddenError>).code).toBe('forbidden');
      expect((err as InstanceType<typeof DocumentEditForbiddenError>).documentId).toBe('d1');
      expect((err as InstanceType<typeof DocumentEditForbiddenError>).role).toBe(role);
    }
  });
});

// NFR #151: a deck shared in a DM is readable by the other DM member — from
// the chat-conversations rows the gateway keeps per peer, not the name alone.
describe('channel membership for document:read', () => {
  const dm = 'chat:dm:dev-connor:dev-frank';
  const tables: Record<string, Record<string, Record<string, unknown>>> = {
    'chat-conversations': {
      [`dev-frank|${dm}`]: { userId: 'dev-frank', channel: dm, peers: JSON.stringify(['dev-connor', 'dev-frank']) },
      [`dev-connor|${dm}`]: { userId: 'dev-connor', channel: dm, peers: JSON.stringify(['dev-connor', 'dev-frank']) },
      // a row a stranger somehow holds, but the thread does not list them
      [`dev-mallory|chat:dmg:g1`]: { userId: 'dev-mallory', channel: 'chat:dmg:g1', peers: JSON.stringify(['dev-a', 'dev-b']) },
      [`dev-a|chat:dmg:g1`]: { userId: 'dev-a', channel: 'chat:dmg:g1', peers: JSON.stringify(['dev-a', 'dev-b']) },
    },
    'chat-members': {
      'room:x|dev-erin': { channel: 'room:x', userId: 'dev-erin' },
      'room:x|dev-gone': { channel: 'room:x', userId: 'dev-gone', removedAt: '2026-09-01' },
    },
  };
  const get = jest.fn(async ({ TableName, Key }: { TableName: string; Key: Record<string, string> }) => {
    const table = Object.keys(tables).find((t) => TableName.endsWith(t))!;
    const id = table === 'chat-conversations' ? `${Key.userId}|${Key.channel}` : `${Key.channel}|${Key.userId}`;
    return { Item: tables[table][id] };
  });

  it('DM and group-DM members are read from chat-conversations; a room from chat-members', async () => {
    const { makeChannelMemberLookup } = await import('../documentEditAccess');
    const member = makeChannelMemberLookup(get);
    expect(await member(dm, 'dev-frank')).toBe(true);
    expect(await member(dm, 'dev-connor')).toBe(true);
    expect(await member('chat:dmg:g1', 'dev-a')).toBe(true);
    expect(await member('chat:dmg:g1', 'dev-mallory')).toBe(false); // holds a row, not in peers
    expect(await member(dm, 'dev-mallory')).toBe(false);
    expect(await member('chat:dm:dev-connor:dev-mallory', 'dev-mallory')).toBe(false); // named, but no conversation
    expect(await member('room:x', 'dev-erin')).toBe(true);
    expect(await member('room:x', 'dev-gone')).toBe(false);
    expect(await member('room:x', 'dev-frank')).toBe(false);
  });

  it('a name that does not include the user is refused without a read; a failing table fails closed', async () => {
    const { makeChannelMemberLookup } = await import('../documentEditAccess');
    get.mockClear();
    expect(await makeChannelMemberLookup(get)(dm, 'dev-mallory')).toBe(false);
    expect(get).not.toHaveBeenCalled();
    const failing = makeChannelMemberLookup(async () => { throw new Error('down'); });
    expect(await failing(dm, 'dev-frank')).toBe(false);
  });

  it('canReadDocument: the other DM member reads a deck shared in the DM, a third user does not', async () => {
    const { canReadDocument, makeChannelMemberLookup } = await import('../documentEditAccess');
    const deck = { createdBy: 'dev-connor', channel: dm };
    const member = makeChannelMemberLookup(get);
    expect((await canReadDocument(deck, { sub: 'dev-frank' }, 'doc-dm', member)).allow).toBe(true);
    const stranger = await canReadDocument(deck, { sub: 'dev-mallory' }, 'doc-dm', member);
    expect(stranger.allow).toBe(false);
    if (!stranger.allow) expect(stranger.status).toBe(404);
  });
});
