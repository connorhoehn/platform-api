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
  /**
   * `person`: the service is acting AS a signed-in person (mirroring a room
   * member into its conversation as the room's owner). The token then carries
   * no "(agent)" family name and a person-shaped email, so the gateway names
   * them as themselves rather than "dev-eve (agent)". Default `agent`.
   */
  kind?: 'agent' | 'person';
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
  const person = identity.kind === 'person';
  const payload = b64url(JSON.stringify({
    sub: identity.userId,
    given_name: identity.displayName,
    ...(person ? {} : { family_name: '(agent)' }),
    email: `${identity.userId.replace(/[^a-z0-9]+/gi, '-')}@${person ? 'local.dev' : 'agents.local'}`,
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
 * A chat-service refusal: the gateway answered, and the answer was no (not a
 * member of a direct-message channel, a closed channel, a bad request). It is
 * the gateway's own reason, not a timeout, so a caller can tell "you may not
 * read this" apart from "the gateway did not answer" and must not retry it.
 */
export class ChatRefusedError extends Error {
  constructor(readonly channel: string, readonly code: string | undefined, readonly reason: string, verb = 'read') {
    super(`the chat service refused to ${verb} ${channel}: ${reason}${code ? ` (${code})` : ''}`);
    this.name = 'ChatRefusedError';
  }
}

/**
 * A chat error frame. Only ever read on a socket opened for one request, so
 * ANY chat error on it is the answer to that request: the dm-membership gate
 * sends its refusal without the channel, and matching on the channel is what
 * made a refused read sit out its whole timeout, three times.
 */
function chatErrorOf(frame: GatewayFrame): { code?: string; message: string } | null {
  if (frame.type !== 'error' || (frame.service !== undefined && frame.service !== 'chat')) return null;
  const e = frame.error as { code?: string; message?: string } | string | undefined;
  if (typeof e === 'string') return { message: e };
  return { ...(e?.code ? { code: e.code } : {}), message: e?.message ?? e?.code ?? 'the gateway refused' };
}

type HistoryClientFactory = (identity: ServiceIdentity, gatewayUrl?: string) => GatewayServiceClient;
let historyClientFactory: HistoryClientFactory = (identity, url) => new GatewayServiceClient(identity, url);
/** Tests swap the socket for a fake (history reads and posts share it). */
export function _setHistoryClientFactory(f: HistoryClientFactory | null): void {
  historyClientFactory = f ?? ((identity, url) => new GatewayServiceClient(identity, url));
}
export const _setChatClientFactory = _setHistoryClientFactory;

/**
 * Post a message into a chat channel through the chat service (so it is
 * persisted and fanned out like any message), AS the identity given. Join
 * first — the service refuses a send from a client that has not joined.
 *
 * On a member-addressed direct message (`chat:dm:a:b`) the gateway admits only
 * its two members (`ChatService._checkDmMembership`, no service bypass), so a
 * caller posting there passes the person the post is for (see
 * `documents/chatRunCard.ts`). A refusal is the answer, not a slow reply: the
 * dm gate's error frame has no channel, so ANY chat error on this
 * single-purpose socket throws `ChatRefusedError` with the gateway's reason at
 * once. No join ack or no `sent` ack within the timeout throws too — this
 * used to return normally after two silent 2 s waits, and a refused card
 * read as posted.
 */
export async function postChatMessage(input: { channel: string; message: string; metadata?: Record<string, unknown>; identity?: ServiceIdentity; gatewayUrl?: string; timeoutMs?: number }): Promise<void> {
  const client = historyClientFactory(input.identity ?? PIPELINE_IDENTITY, input.gatewayUrl);
  const timeoutMs = input.timeoutMs ?? 2000;
  try {
    await client.connect();
  } catch (err) {
    throw new Error(`gateway connection failed: ${describeError(err)}`);
  }
  try {
    const refusedOrThrow = (reply: GatewayFrame | null, what: string): void => {
      if (!reply) throw new Error(`the chat service did not answer the ${what} for ${input.channel} (timeout after ${timeoutMs}ms)`);
      const refused = chatErrorOf(reply);
      if (refused) throw new ChatRefusedError(input.channel, refused.code, refused.message, 'post to');
    };
    client.send({ service: 'chat', action: 'join', channel: input.channel });
    // The joined ack (or the join's history replay) is the signal the subscription is in place.
    refusedOrThrow(await client.waitFor((f) => isChatFrame(f, 'joined', input.channel) || isChatFrame(f, 'history', input.channel) || !!chatErrorOf(f), timeoutMs), 'join');
    client.send({ service: 'chat', action: 'send', channel: input.channel, message: input.message, metadata: input.metadata ?? {} });
    // The service acks the sender with `sent`; the fan-out `message` frame nests the record.
    refusedOrThrow(await client.waitFor((f) => isChatFrame(f, 'sent', input.channel)
      || (isChatFrame(f, 'message', input.channel) && (f.message === input.message || (f.message as ChatHistoryMessage | undefined)?.message === input.message))
      || !!chatErrorOf(f), timeoutMs), 'send');
  } finally {
    await client.close();
  }
}

/**
 * Edit a message AS its author (the chat service lets only the author edit):
 * the text is replaced and `metadata` is MERGED into the stored metadata, and
 * everyone else on the channel gets the same `messageUpdated` an edit in the
 * browser produces. The service does not ack the author, so a refusal is the
 * only answer that comes back: a chat error within the wait throws
 * `ChatRefusedError`, silence means the edit was applied. Callers that must
 * know read the history back.
 */
export async function editChatMessage(input: { channel: string; messageId: string; message: string; metadata?: Record<string, unknown>; identity?: ServiceIdentity; gatewayUrl?: string; timeoutMs?: number }): Promise<void> {
  const client = historyClientFactory(input.identity ?? PIPELINE_IDENTITY, input.gatewayUrl);
  const timeoutMs = input.timeoutMs ?? 1500;
  try {
    await client.connect();
  } catch (err) {
    throw new Error(`gateway connection failed: ${describeError(err)}`);
  }
  try {
    client.send({ service: 'chat', action: 'join', channel: input.channel });
    const joined = await client.waitFor((f) => isChatFrame(f, 'joined', input.channel) || isChatFrame(f, 'history', input.channel) || !!chatErrorOf(f), timeoutMs);
    const joinRefused = joined ? chatErrorOf(joined) : null;
    if (joinRefused) throw new ChatRefusedError(input.channel, joinRefused.code, joinRefused.message, 'edit in');
    client.send({ service: 'chat', action: 'edit', channel: input.channel, messageId: input.messageId, message: input.message, metadata: input.metadata ?? {} });
    const reply = await client.waitFor((f) => !!chatErrorOf(f), timeoutMs);
    const refused = reply ? chatErrorOf(reply) : null;
    if (refused) throw new ChatRefusedError(input.channel, refused.code, refused.message, 'edit in');
  } finally {
    await client.close();
  }
}

/**
 * Read a channel's durable history through the chat service, AS the identity
 * given — pass the person who asked, so the gateway's own gates (dm
 * membership, a closed channel's per-member history floor) decide what they
 * may read, exactly as they would for that person's browser.
 *
 * No `join`. A read needs no live subscription: `history` is gated and answered
 * on its own, from the channel's store (DynamoDB, merged with the process
 * cache), so any gateway replica gives the same answer and nothing is
 * subscribed across the cluster for a one-shot read. Joining first cost a
 * distributed channel subscribe per read, and a refused join was indistinguishable
 * from a slow one: the read waited out its timeout on an error it never looked at.
 */
export async function readChatHistory(input: { channel: string; limit?: number; identity?: ServiceIdentity; gatewayUrl?: string; timeoutMs?: number }): Promise<ChatHistoryMessage[]> {
  const client = historyClientFactory(input.identity ?? PIPELINE_IDENTITY, input.gatewayUrl);
  const timeoutMs = input.timeoutMs ?? 4000;
  // Worded so the executor's classifier reads both as transient ("connection",
  // "timeout") and retries them, while a refusal — which is neither — is not.
  try {
    await client.connect(timeoutMs);
  } catch (err) {
    throw new Error(`gateway connection failed: ${describeError(err)}`);
  }
  try {
    client.send({ service: 'chat', action: 'history', channel: input.channel, limit: input.limit ?? 200 });
    const reply = await client.waitFor((f) => isChatFrame(f, 'history', input.channel) || !!chatErrorOf(f), timeoutMs);
    if (!reply) throw new Error(`the chat service did not answer the history request for ${input.channel} (timeout after ${timeoutMs}ms)`);
    const refused = chatErrorOf(reply);
    if (refused) throw new ChatRefusedError(input.channel, refused.code, refused.message);
    const messages = Array.isArray(reply.messages) ? reply.messages as ChatHistoryMessage[] : [];
    return [...messages].sort((a, b) => String(a.timestamp ?? '').localeCompare(String(b.timestamp ?? '')));
  } finally {
    await client.close(0);
  }
}
