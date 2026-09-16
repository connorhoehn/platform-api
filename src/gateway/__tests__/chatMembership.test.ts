import {
  _resetSyncCache,
  _setClientFactory,
  claimSync,
  ensureRoomChatMembers,
  removeRoomChatMember,
} from '../chatMembership';
import type { GatewayFrame, GatewayServiceClient, ServiceIdentity } from '../GatewayServiceClient';

/** A gateway that answers `members` with a fixed view and `addMembers` with the next roster. */
function fakeGateway(opts: { open: boolean; members: Array<{ userId: string; role: string }>; refuse?: string }) {
  const sent: GatewayFrame[] = [];
  const identities: ServiceIdentity[] = [];
  const make = (identity: ServiceIdentity) => {
    identities.push(identity);
    const listeners = new Set<(f: GatewayFrame) => void>();
    const emit = (f: GatewayFrame) => { for (const l of listeners) l(f); };
    const client = {
      identity,
      open: true,
      connect: async () => undefined,
      close: async () => undefined,
      onFrame: (l: (f: GatewayFrame) => void) => { listeners.add(l); return () => listeners.delete(l); },
      waitFor: (match: (f: GatewayFrame) => boolean, _ms: number) => new Promise<GatewayFrame | null>((resolve) => {
        const off = client.onFrame((f) => { if (match(f)) { off(); resolve(f); } });
      }),
      send: (frame: GatewayFrame) => {
        sent.push(frame);
        const channel = frame.channel as string;
        setImmediate(() => {
          if (frame.action === 'members') {
            emit({ type: 'chat', action: 'members', channel, open: opts.open, members: opts.members });
          } else if (frame.action === 'addMembers' || frame.action === 'removeMember') {
            if (opts.refuse) { emit({ type: 'error', service: 'chat', channel, error: { code: 'forbidden', message: opts.refuse } }); return; }
            emit({ type: 'chat', action: 'membersUpdated', channel, open: false, members: [] });
          }
        });
      },
    };
    return client as unknown as GatewayServiceClient;
  };
  return { sent, identities, make };
}

afterEach(() => { _setClientFactory(null); _resetSyncCache(); });

describe('ensureRoomChatMembers', () => {
  it('closes an open channel as the owner and adds the others with all history', async () => {
    const gw = fakeGateway({ open: true, members: [] });
    _setClientFactory(gw.make);
    const out = await ensureRoomChatMembers({ slug: 'design', ownerId: 'eve', ownerName: 'Eve', members: [{ userId: 'carol', displayName: 'Carol' }, { userId: 'eve' }] });
    expect(out.added).toEqual(['carol']);
    expect(gw.identities[0].userId).toBe('eve');
    const add = gw.sent.find((f) => f.action === 'addMembers')!;
    expect(add).toMatchObject({ channel: 'room:design', userIds: ['carol'], history: { mode: 'all' }, names: { carol: 'Carol', eve: 'Eve' } });
  });

  it('adds only the people the channel is missing', async () => {
    const gw = fakeGateway({ open: false, members: [{ userId: 'eve', role: 'owner' }, { userId: 'carol', role: 'member' }] });
    _setClientFactory(gw.make);
    const out = await ensureRoomChatMembers({ slug: 'design', ownerId: 'eve', members: [{ userId: 'carol' }, { userId: 'bob' }] });
    expect(out.added).toEqual(['bob']);
  });

  it('is a no-op when already in sync', async () => {
    const gw = fakeGateway({ open: false, members: [{ userId: 'eve', role: 'owner' }, { userId: 'carol', role: 'member' }] });
    _setClientFactory(gw.make);
    const out = await ensureRoomChatMembers({ slug: 'design', ownerId: 'eve', members: [{ userId: 'carol' }] });
    expect(out.added).toEqual([]);
    expect(gw.sent.map((f) => f.action)).toEqual(['members']);
  });

  it("throws with the gateway's reason", async () => {
    const gw = fakeGateway({ open: true, members: [], refuse: 'You are not a member of this channel' });
    _setClientFactory(gw.make);
    await expect(ensureRoomChatMembers({ slug: 'design', ownerId: 'eve', members: [{ userId: 'carol' }] })).rejects.toThrow(/not a member/);
  });
});

describe('removeRoomChatMember', () => {
  it('sends removeMember as the owner', async () => {
    const gw = fakeGateway({ open: false, members: [] });
    _setClientFactory(gw.make);
    await removeRoomChatMember({ slug: 'design', userId: 'carol', name: 'Carol', byOwnerId: 'eve' });
    expect(gw.sent[0]).toMatchObject({ service: 'chat', action: 'removeMember', channel: 'room:design', userId: 'carol', name: 'Carol' });
    expect(gw.identities[0].userId).toBe('eve');
  });
});

describe('claimSync', () => {
  it('claims once per slug until the TTL passes', () => {
    expect(claimSync('a', 1000)).toBe(true);
    expect(claimSync('a', 2000)).toBe(false);
    expect(claimSync('a', 1000 + 10 * 60 * 1000 + 1)).toBe(true);
    expect(claimSync('b', 1000)).toBe(true);
  });
});
