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

  describe('tenant-prefixed lobbies (OrgIQ lobbyGuard parity)', () => {
    const orgiq = (sub: string, orgId = 'orgiq') => ({ user: { sub, orgId, authKind: 'orgiq-hs256' }, headers: {} }) as unknown as Request;
    it('an OrgIQ caller: only its own <org>: prefix; <org>:dm: only for a listed id', async () => {
      const a = access();
      expect(await a.canUseLobby(orgiq('u-vera'), 'orgiq:dm:u-nora:u-vera')).toBe(true);
      expect(await a.canUseLobby(orgiq('u-theo'), 'orgiq:dm:u-nora:u-vera')).toBe(false);
      expect(await a.canUseLobby(orgiq('u-vera'), 'orgiq:initiative:p-auth-migration')).toBe(true);
      expect(await a.canUseLobby(orgiq('u-vera'), 'assessment:dm:u-nora:u-vera')).toBe(false);
      expect(await a.canUseLobby(orgiq('u-vera'), 'assessment:initiative:x')).toBe(false);
      expect(await a.canUseLobby(orgiq('u-vera'), 'orgiq:')).toBe(false);
      expect(await a.canUseLobby(orgiq('u-vera'), 'orgiq:dm')).toBe(false);
    });
    it('an OrgIQ caller gets no unprefixed lobby — not an ad-hoc one, not dm:, not a document', async () => {
      const a = access();
      expect(await a.canUseLobby(orgiq('u-vera'), 'global-hangout')).toBe(false);
      expect(await a.canUseLobby(orgiq('u-vera'), 'dm:u-nora:u-vera')).toBe(false);
      expect(await a.canUseLobby(orgiq('reader'), 'doc-1')).toBe(false);
    });
    it('an OrgIQ token for another tenant does not reach this tenant', async () => {
      const a = access();
      expect(await a.canUseLobby(orgiq('u-vera', 'assessment'), 'orgiq:dm:u-nora:u-vera')).toBe(false);
      expect(await a.canUseLobby(orgiq('u-vera', 'assessment'), 'assessment:dm:u-nora:u-vera')).toBe(true);
    });
    it('any other caller: a <tenant>:dm: lobby needs a listed id AND that org — never an open ad-hoc lobby', async () => {
      const a = access();
      expect(await a.canUseLobby(req('u-vera', 'orgiq'), 'orgiq:dm:u-nora:u-vera')).toBe(true);
      expect(await a.canUseLobby(req('u-vera', 'org-1'), 'orgiq:dm:u-nora:u-vera')).toBe(false);
      expect(await a.canUseLobby(req('eve', 'orgiq'), 'orgiq:dm:u-nora:u-vera')).toBe(false);
      // Unchanged: unprefixed shapes keep their rules.
      expect(await a.canUseLobby(req('alice'), 'dm:alice:bob')).toBe(true);
      expect(await a.canUseLobby(req('anyone'), 'room:open')).toBe(true);
    });
  });

  describe('the tenants claim partitions lobbies (any listed tenant, not only org)', () => {
    const tok = (sub: string, over: Record<string, unknown> = {}) =>
      ({ user: { sub, orgId: 'org-a', authKind: 'orgiq-hs256', ...over }, headers: {} }) as unknown as Request;
    const social = (sub: string) => tok(sub, { tenants: ['org-a', 'social'] });
    it('a token listing social may use a social:dm lobby it is in, and any social lobby', async () => {
      const a = access();
      expect(await a.canUseLobby(social('u-vera'), 'social:dm:u-nora:u-vera')).toBe(true);
      expect(await a.canUseLobby(social('u-theo'), 'social:dm:u-nora:u-vera')).toBe(false);
      expect(await a.canUseLobby(social('u-vera'), 'social:room:lounge')).toBe(true);
      expect(await a.canUseLobby(social('u-vera'), 'org-a:dm:u-nora:u-vera')).toBe(true);
    });
    it('refuses a tenant the token does not list (org-b), with the same 404', async () => {
      const a = access();
      expect(await a.canUseLobby(social('u-vera'), 'org-b:dm:u-nora:u-vera')).toBe(false);
      expect(await a.canUseLobby(social('u-vera'), 'org-b:room:lounge')).toBe(false);
      await expect(a.assertLobbyAccess(social('u-vera'), 'org-b:dm:u-nora:u-vera')).rejects.toMatchObject({ status: 404 });
    });
    it('a token without tenants keeps org-only', async () => {
      const a = access();
      expect(await a.canUseLobby(tok('u-vera'), 'org-a:dm:u-nora:u-vera')).toBe(true);
      expect(await a.canUseLobby(tok('u-vera'), 'social:dm:u-nora:u-vera')).toBe(false);
      expect(await a.canUseLobby(tok('u-vera', { tenants: [] }), 'social:room:x')).toBe(false);
    });
    it('a lobby with no partition keeps today\'s rule (unprefixed: refused to a token; open ad-hoc to others)', async () => {
      const a = access();
      expect(await a.canUseLobby(social('u-vera'), 'global-hangout')).toBe(false);
      expect(await a.canUseLobby(social('u-vera'), 'dm:u-nora:u-vera')).toBe(false);
      expect(await a.canUseLobby(req('eve'), 'global-hangout')).toBe(true);
    });
  });

  describe('a private room admits members only (rooms route or video API)', () => {
    const rooms: Record<string, { roomId: string; orgId: string; visibility: string; createdBy: string }> = {
      'social|den': { roomId: 'r-den', orgId: 'social', visibility: 'private', createdBy: 'u-owner' },
      'social|lounge': { roomId: 'r-lounge', orgId: 'social', visibility: 'org', createdBy: 'u-owner' },
      'org-1|den': { roomId: 'r-den1', orgId: 'org-1', visibility: 'private', createdBy: 'owner' },
    };
    const roomBySlug = jest.fn(async (orgId: string, slug: string) => rooms[`${orgId}|${slug}`] ?? null);
    const isRoomMember = jest.fn(async (roomId: string, userId: string) => userId === 'u-member' && roomId === 'r-den');
    const a = () => access({ roomBySlug, isRoomMember });
    const tok = (sub: string) => ({ user: { sub, orgId: 'org-a', tenants: ['org-a', 'social'], authKind: 'orgiq-hs256' }, headers: {} }) as unknown as Request;
    beforeEach(() => { roomBySlug.mockClear(); isRoomMember.mockClear(); });

    it('private room: a member and the creator are allowed', async () => {
      expect(await a().canUseLobby(tok('u-member'), 'social:room:den')).toBe(true);
      expect(await a().canUseLobby(tok('u-owner'), 'social:room:den')).toBe(true);
    });
    it('private room: a listed-tenant non-member is refused with the rooms route 403', async () => {
      expect(await a().canUseLobby(tok('u-stranger'), 'social:room:den')).toBe(false);
      await expect(a().assertLobbyAccess(tok('u-stranger'), 'social:room:den'))
        .rejects.toMatchObject({ status: 403, message: 'Access denied to room: den' });
    });
    it('public room: any listed tenant is allowed', async () => {
      expect(await a().canUseLobby(tok('u-stranger'), 'social:room:lounge')).toBe(true);
    });
    it('unknown room: today\'s behaviour (open under a listed tenant; refused outside it)', async () => {
      expect(await a().canUseLobby(tok('u-stranger'), 'social:room:nowhere')).toBe(true);
      expect(await a().canUseLobby(tok('u-stranger'), 'org-b:room:den')).toBe(false);
      expect(roomBySlug).not.toHaveBeenCalledWith('org-b', 'den');
    });
    it('a Cognito caller: its own tenant\'s private room is members-only; another tenant\'s room is 404; unknown stays ad-hoc', async () => {
      await expect(a().assertLobbyAccess(req('u-stranger', 'social'), 'social:room:den')).rejects.toMatchObject({ status: 403 });
      expect(await a().canUseLobby(req('u-member', 'social'), 'social:room:den')).toBe(true);
      await expect(a().assertLobbyAccess(req('u-member', 'org-1'), 'social:room:den')).rejects.toMatchObject({ status: 404 });
      expect(await a().canUseLobby(req('u-stranger', 'org-1'), 'social:room:nowhere')).toBe(true);
    });
    it('room:<slug>: a non-member gets 403; an unknown room stays 404', async () => {
      await expect(a().assertLobbyAccess(req('eve'), 'room:den')).rejects.toMatchObject({ status: 403 });
      await expect(a().assertLobbyAccess(req('eve'), 'room:ghost')).rejects.toMatchObject({ status: 404 });
    });
    it('reads the room once per request', async () => {
      const access1 = a();
      const r = tok('u-stranger');
      await access1.canUseLobby(r, 'social:room:den');
      await access1.assertLobbyAccess(r, 'social:room:den').catch(() => undefined);
      expect(roomBySlug).toHaveBeenCalledTimes(1);
      expect(isRoomMember).toHaveBeenCalledTimes(1);
    });
  });
});
