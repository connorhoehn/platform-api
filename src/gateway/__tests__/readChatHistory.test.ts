import { ChatRefusedError, _setHistoryClientFactory, readChatHistory, type GatewayFrame, type GatewayServiceClient, type ServiceIdentity } from '../GatewayServiceClient';

/** A gateway socket that answers each frame with `reply(frame)` (or nothing). */
function fakeGateway(reply: (frame: GatewayFrame) => GatewayFrame | null) {
  const sent: GatewayFrame[] = [];
  const identities: ServiceIdentity[] = [];
  const make = (identity: ServiceIdentity) => {
    identities.push(identity);
    const listeners = new Set<(f: GatewayFrame) => void>();
    const client = {
      identity,
      open: true,
      connect: async () => undefined,
      close: async () => undefined,
      onFrame: (l: (f: GatewayFrame) => void) => { listeners.add(l); return () => listeners.delete(l); },
      waitFor: (match: (f: GatewayFrame) => boolean, ms: number) => new Promise<GatewayFrame | null>((resolve) => {
        const timer = setTimeout(() => { off(); resolve(null); }, ms);
        const off = client.onFrame((f) => { if (match(f)) { clearTimeout(timer); off(); resolve(f); } });
      }),
      send: (frame: GatewayFrame) => {
        sent.push(frame);
        const answer = reply(frame);
        if (answer) setImmediate(() => { for (const l of listeners) l(answer); });
      },
    };
    return client as unknown as GatewayServiceClient;
  };
  return { sent, identities, make };
}

afterEach(() => _setHistoryClientFactory(null));

const DM = 'chat:dm:dev-connor:dev-frank';

describe('readChatHistory', () => {
  it('asks for history without joining, as the identity given, and returns it oldest first', async () => {
    const gw = fakeGateway((f) => (f.action === 'history'
      ? { type: 'chat', action: 'history', channel: DM, messages: [{ id: '2', timestamp: '2026-09-22T10:01:00Z' }, { id: '1', timestamp: '2026-09-22T10:00:00Z' }] }
      : null));
    _setHistoryClientFactory(gw.make);
    const out = await readChatHistory({ channel: DM, limit: 50, identity: { userId: 'dev-connor', displayName: 'Connor', kind: 'person' } });
    expect(out.map((m) => m.id)).toEqual(['1', '2']);
    // One frame: no join (no cluster-wide subscribe for a one-shot read), no leave.
    expect(gw.sent).toEqual([{ service: 'chat', action: 'history', channel: DM, limit: 50 }]);
    expect(gw.identities[0]).toMatchObject({ userId: 'dev-connor', kind: 'person' });
  });

  it('fails at once with the gateway’s reason when the read is refused, even with no channel on the error', async () => {
    // The dm-membership gate's real frame: no `channel` field.
    const gw = fakeGateway(() => ({ type: 'error', service: 'chat', error: { code: 'CHAT_DM_FORBIDDEN', message: 'You are not a member of this direct-message channel' } }));
    _setHistoryClientFactory(gw.make);
    const started = Date.now();
    const read = readChatHistory({ channel: DM, timeoutMs: 4000 });
    await expect(read).rejects.toBeInstanceOf(ChatRefusedError);
    await expect(read).rejects.toThrow(/not a member of this direct-message channel \(CHAT_DM_FORBIDDEN\)/);
    expect(Date.now() - started).toBeLessThan(1000);
  });

  it('says "timeout" when the gateway does not answer, so the executor retries it', async () => {
    _setHistoryClientFactory(fakeGateway(() => null).make);
    await expect(readChatHistory({ channel: DM, timeoutMs: 20 })).rejects.toThrow(/timeout after 20ms/);
  });

  it('says "connection" when the socket cannot open, so the executor retries it', async () => {
    const gw = fakeGateway(() => null);
    _setHistoryClientFactory((identity) => {
      const client = gw.make(identity) as unknown as { connect: () => Promise<void> };
      client.connect = async () => { throw new Error('could not connect to the gateway at ws://x: ECONNREFUSED'); };
      return client as unknown as GatewayServiceClient;
    });
    await expect(readChatHistory({ channel: DM })).rejects.toThrow(/^gateway connection failed/);
  });
});
