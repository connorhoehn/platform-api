// A platform-api process acting on the gateway the way a browser does.
//
// Every side effect a pipeline has on a conversation or a document goes
// through the same socket path people use — the chat service persists the
// message, the CRDT service applies the update, presence sees the client —
// so there is exactly one write path into those surfaces. This is the
// connection: identity, frames out, frames in, and the wait for a reply.
import { WebSocket } from 'ws';
import log from '../lib/logger';

export interface ServiceIdentity {
  /** `agent:` prefix so nothing mistakes it for a person. */
  userId: string;
  displayName: string;
  color?: string;
}

const b64url = (s: string) => Buffer.from(s).toString('base64').replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');

/**
 * The gateway's dev auth (`SKIP_AUTH`) reads identity from a decoded — not
 * verified — bearer token in the subprotocol; that is how the browser's dev
 * identities arrive too. In a signed deployment this is where a service
 * token minted for the identity goes instead.
 */
export function serviceToken(identity: ServiceIdentity): string {
  const header = b64url(JSON.stringify({ alg: 'none', typ: 'JWT' }));
  const payload = b64url(JSON.stringify({
    sub: identity.userId,
    given_name: identity.displayName,
    family_name: '(agent)',
    email: `${identity.userId.replace(/[^a-z0-9]+/gi, '-')}@agents.local`,
    iat: Math.floor(Date.now() / 1000),
  }));
  return `${header}.${payload}.`;
}

export type GatewayFrame = Record<string, unknown>;

export class GatewayServiceClient {
  readonly identity: ServiceIdentity;
  private readonly url: string;
  private ws: WebSocket | null = null;
  private readonly listeners = new Set<(frame: GatewayFrame) => void>();

  constructor(identity: ServiceIdentity, gatewayUrl?: string) {
    this.identity = identity;
    this.url = (gatewayUrl ?? process.env.GATEWAY_WS_URL ?? 'ws://localhost:18080').replace(/\/$/, '');
  }

  get open(): boolean { return !!this.ws && this.ws.readyState === WebSocket.OPEN; }

  async connect(timeoutMs = 4000): Promise<void> {
    const url = `${this.url}/?userId=${encodeURIComponent(this.identity.userId)}`;
    const ws = new WebSocket(url, ['bearer-token-v1', serviceToken(this.identity)]);
    this.ws = ws;
    ws.on('message', (raw) => {
      let msg: GatewayFrame;
      try { msg = JSON.parse(String(raw)) as GatewayFrame; } catch { return; }
      for (const l of this.listeners) l(msg);
    });
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`gateway did not open within ${timeoutMs}ms (${this.url})`)), timeoutMs);
      ws.once('open', () => { clearTimeout(timer); resolve(); });
      ws.once('error', (err) => { clearTimeout(timer); reject(err); });
    });
  }

  send(frame: GatewayFrame): void {
    if (this.open) this.ws!.send(JSON.stringify(frame));
    else log.warn({ frame: frame.service, action: frame.action }, 'gateway service client: send on a closed socket');
  }

  onFrame(listener: (frame: GatewayFrame) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  /** Resolve with the first frame `match` accepts, or null after `timeoutMs`. */
  waitFor(match: (frame: GatewayFrame) => boolean, timeoutMs: number): Promise<GatewayFrame | null> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => { off(); resolve(null); }, timeoutMs);
      const off = this.onFrame((f) => { if (match(f)) { clearTimeout(timer); off(); resolve(f); } });
    });
  }

  async close(graceMs = 150): Promise<void> {
    await new Promise((r) => setTimeout(r, graceMs));
    this.ws?.close();
    this.ws = null;
  }
}

export const PIPELINE_IDENTITY: ServiceIdentity = { userId: 'agent:pipeline', displayName: 'Pipeline', color: '#0ea5e9' };

/**
 * Post a message into a chat channel as a service identity, through the chat
 * service (so it is persisted and fanned out like any message). Join first —
 * the service refuses a send from a client that has not joined.
 */
export async function postChatMessage(input: { channel: string; message: string; metadata?: Record<string, unknown>; identity?: ServiceIdentity; gatewayUrl?: string }): Promise<void> {
  const client = new GatewayServiceClient(input.identity ?? PIPELINE_IDENTITY, input.gatewayUrl);
  await client.connect();
  try {
    client.send({ service: 'chat', action: 'join', channel: input.channel });
    // The join reply (history) is the signal the subscription is in place.
    await client.waitFor((f) => f.channel === input.channel && (f.type === 'chat:history' || f.type === 'chat:joined' || f.type === 'history'), 2000);
    client.send({ service: 'chat', action: 'send', channel: input.channel, message: input.message, metadata: input.metadata ?? {} });
    await client.waitFor((f) => f.channel === input.channel && (f.type === 'chat:message' || f.type === 'message') && (f as { message?: unknown }).message === input.message, 2000);
  } finally {
    await client.close();
  }
}
