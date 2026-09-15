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

/** A message for any thrown value, including the ones that have none. */
export function describeError(err: unknown): string {
  if (err instanceof AggregateError) {
    const inner = err.errors.map(describeError).filter(Boolean);
    return inner.length ? [...new Set(inner)].join('; ') : (err.message || err.name);
  }
  if (err instanceof Error) {
    const code = (err as NodeJS.ErrnoException).code;
    return err.message || (code ? String(code) : err.name);
  }
  return String(err);
}

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
      // Named, not passed through: a refused connection on a host with two
      // addresses (`localhost` → ::1 and 127.0.0.1) surfaces as an
      // AggregateError whose own message is EMPTY, and every layer above
      // reported `err.message` — so a pipeline step failed three times with
      // the error "" and its retry history read as three successes.
      ws.once('error', (err) => { clearTimeout(timer); reject(new Error(`could not connect to the gateway at ${this.url}: ${describeError(err)}`)); });
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
 * Does this frame carry the chat service's `action`? The gateway emits NESTED
 * frames — `{ type: 'chat', action: 'joined' | 'history' | 'message' | 'sent' }`
 * — and an older flat spelling (`type: 'chat:history'`) has been seen in
 * fixtures. Accept both so a reply is never waited out on a spelling.
 */
export function isChatFrame(frame: GatewayFrame, action: string, channel?: string): boolean {
  if (channel !== undefined && frame.channel !== channel) return false;
  if (frame.type === 'chat' && frame.action === action) return true;
  return frame.type === `chat:${action}` || frame.type === action;
}

/** The chat message record the gateway persists and replays. */
export interface ChatHistoryMessage {
  id?: string;
  userId?: string;
  message?: string;
  metadata?: Record<string, unknown>;
  timestamp?: string;
}

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
    // The joined ack (or the join's history replay) is the signal the subscription is in place.
    await client.waitFor((f) => isChatFrame(f, 'joined', input.channel) || isChatFrame(f, 'history', input.channel), 2000);
    client.send({ service: 'chat', action: 'send', channel: input.channel, message: input.message, metadata: input.metadata ?? {} });
    // The service acks the sender with `sent`; the fan-out `message` frame nests the record.
    await client.waitFor((f) => isChatFrame(f, 'sent', input.channel)
      || (isChatFrame(f, 'message', input.channel) && (f.message === input.message || (f.message as ChatHistoryMessage | undefined)?.message === input.message)), 2000);
  } finally {
    await client.close();
  }
}

/**
 * Read a channel's history as a service identity: join, ask for `limit`
 * messages, and hand back the records oldest first. The join itself replays a
 * short history; the explicit request may answer with more, so whichever
 * reply carries the most messages wins.
 */
export async function readChatHistory(input: { channel: string; limit?: number; identity?: ServiceIdentity; gatewayUrl?: string; timeoutMs?: number }): Promise<ChatHistoryMessage[]> {
  const client = new GatewayServiceClient(input.identity ?? PIPELINE_IDENTITY, input.gatewayUrl);
  const timeoutMs = input.timeoutMs ?? 4000;
  await client.connect(timeoutMs);
  const seen: ChatHistoryMessage[][] = [];
  const off = client.onFrame((f) => {
    if (isChatFrame(f, 'history', input.channel) && Array.isArray(f.messages)) seen.push(f.messages as ChatHistoryMessage[]);
  });
  try {
    client.send({ service: 'chat', action: 'join', channel: input.channel });
    const joined = await client.waitFor((f) => isChatFrame(f, 'joined', input.channel) || isChatFrame(f, 'history', input.channel), timeoutMs);
    if (!joined) throw new Error(`the chat service did not confirm the join of ${input.channel} within ${timeoutMs}ms`);
    client.send({ service: 'chat', action: 'history', channel: input.channel, limit: input.limit ?? 200 });
    const reply = await client.waitFor((f) => isChatFrame(f, 'history', input.channel), timeoutMs);
    if (!reply && seen.length === 0) throw new Error(`the chat service did not answer the history request for ${input.channel} within ${timeoutMs}ms`);
    const best = seen.reduce<ChatHistoryMessage[]>((acc, m) => (m.length > acc.length ? m : acc), []);
    return [...best].sort((a, b) => String(a.timestamp ?? '').localeCompare(String(b.timestamp ?? '')));
  } finally {
    off();
    client.send({ service: 'chat', action: 'leave', channel: input.channel });
    await client.close();
  }
}
