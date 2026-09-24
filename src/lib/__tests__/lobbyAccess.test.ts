// NFR #175: the call-lobby rule every /api/video route now asks.
import type { Request } from 'express';
import { makeLobbyAccess } from '../lobbyAccess';

const req = (sub: string | undefined, orgId = 'org-1') => ({ user: sub ? { sub, orgId } : undefined, headers: { authorization: 'Bearer t' } }) as unknown as Request;

function access(over: Partial<Parameters<typeof makeLobbyAccess>[0]> = {}) {
  return makeLobbyAccess({
    roomBySlug: async (orgId, slug) => (slug === 'private' ? { roomId: 'r-1', orgId, visibility: 'private', createdBy: 'owner' } : slug === 'open' ? { roomId: 'r-2', orgId, visibility: 'org' } : null),
    isRoomMember: async (roomId, userId) => roomId === 'r-1' && userId === 'member',
    isChannelMember: async (channel, userId) => channel === 'chat:dmg:abc' && userId === 'member',
    documentExists: async (id) => id === 'doc-1',
    assertCanReadDocument: async (r, id) => { if (!(id === 'doc-1' && r.user?.sub === 'reader')) throw new Error('404'); },
    ...over,
  });
}

describe('lobbyAccess', () => {
  it('dm: the named people only; a malformed dm name admits no one', async () => {
    const a = access();
    expect(await a.canUseLobby(req('alice'), 'dm:alice:bob')).toBe(true);
    expect(await a.canUseLobby(req('eve'), 'dm:alice:bob')).toBe(false);
    expect(await a.canUseLobby(req('alice'), 'dm:alice')).toBe(false);
  });
  it('dmg: a member of the group DM channel', async () => {
    const a = access();
    expect(await a.canUseLobby(req('member'), 'dmg:abc')).toBe(true);
    expect(await a.canUseLobby(req('eve'), 'dmg:abc')).toBe(false);
  });
  it('room: org-visible to the org; a private room to its creator and members; unknown room refused', async () => {
    const a = access();
    expect(await a.canUseLobby(req('anyone'), 'room:open')).toBe(true);
    expect(await a.canUseLobby(req('owner'), 'room:private')).toBe(true);
    expect(await a.canUseLobby(req('member'), 'room:private')).toBe(true);
    expect(await a.canUseLobby(req('eve'), 'room:private')).toBe(false);
    expect(await a.canUseLobby(req('owner'), 'room:ghost')).toBe(false);
  });
  it('a document lobby (bare id or doc:) follows document:read; an ad-hoc lobby stays open', async () => {
    const a = access();
    expect(await a.canUseLobby(req('reader'), 'doc-1')).toBe(true);
    expect(await a.canUseLobby(req('reader'), 'doc:doc-1')).toBe(true);
    expect(await a.canUseLobby(req('eve'), 'doc-1')).toBe(false);
    expect(await a.canUseLobby(req('eve'), 'doc:missing')).toBe(false);
    expect(await a.canUseLobby(req('eve'), 'global-hangout')).toBe(true);
  });
  it('no principal is refused, and assertLobbyAccess throws 404', async () => {
    const a = access();
    expect(await a.canUseLobby(req(undefined), 'global-hangout')).toBe(false);
    await expect(a.assertLobbyAccess(req('eve'), 'dm:alice:bob')).rejects.toMatchObject({ status: 404 });
    await expect(a.assertLobbyAccess(req('bob'), 'dm:alice:bob')).resolves.toBeUndefined();
  });
});
