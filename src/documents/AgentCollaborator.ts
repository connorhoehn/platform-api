// A pipeline step that edits a document is a COLLABORATOR, not a writer to a
// database: it joins the document's CRDT channel on the gateway the way a
// browser does, announces itself through awareness (so the people in the
// document see "Haiku" arrive, edit, and leave, with a colour and a caret
// like anyone else), applies its change as a Y.js update, and goes.
//
// Nothing here bypasses the collaboration path — there is no second write
// route into a document — which is what makes the edit indistinguishable from
// a person's once it lands: same conflict handling, same undo history, same
// snapshots, same presence.
import { WebSocket } from 'ws';
import type * as YT from 'yjs' with { 'resolution-mode': 'import' };
// yjs ships ESM types but a CJS build; under `module: node16` the value has to
// come in through require while the types come in as type-only.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const Y = require('yjs') as typeof YT;
import type * as AwarenessT from 'y-protocols/awareness' with { 'resolution-mode': 'import' };
// eslint-disable-next-line @typescript-eslint/no-require-imports
const awarenessProtocol = require('y-protocols/awareness') as typeof AwarenessT;
import log from '../lib/logger';

export interface AgentIdentity {
  /** The gateway user id. `agent:` prefix so nothing mistakes it for a person. */
  userId: string;
  displayName: string;
  color: string;
}

export const HAIKU_AGENT: AgentIdentity = { userId: 'agent:haiku', displayName: 'Haiku', color: '#8b5cf6' };

export interface AgentCollaboratorOptions {
  documentId: string;
  identity?: AgentIdentity;
  /** ws://host:port of the gateway. Default `GATEWAY_WS_URL` or ws://localhost:18080. */
  gatewayUrl?: string;
  /** How long to wait for the snapshot before treating the document as empty. */
  snapshotTimeoutMs?: number;
}

const b64url = (s: string) => Buffer.from(s).toString('base64').replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');

/**
 * The gateway's dev auth (`SKIP_AUTH`) reads identity from a decoded — not
 * verified — bearer token in the subprotocol; that is how the browser's dev
 * identities arrive too. In a signed deployment this is where a service
 * token minted for the agent identity goes instead.
 */
export function agentToken(identity: AgentIdentity): string {
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

export class AgentCollaborator {
  readonly doc = new Y.Doc();
  readonly channel: string;
  readonly identity: AgentIdentity;
  private readonly url: string;
  private ws: WebSocket | null = null;
  private awareness: AwarenessT.Awareness | null = null;
  private readonly snapshotTimeoutMs: number;

  constructor(opts: AgentCollaboratorOptions) {
    this.identity = opts.identity ?? HAIKU_AGENT;
    this.channel = `doc:${opts.documentId}`;
    this.url = (opts.gatewayUrl ?? process.env.GATEWAY_WS_URL ?? 'ws://localhost:18080').replace(/\/$/, '');
    this.snapshotTimeoutMs = opts.snapshotTimeoutMs ?? 4000;
  }

  private send(frame: Record<string, unknown>): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(frame));
  }

  /** Connect, subscribe, and resolve once the document's current state is in `doc`. */
  async join(): Promise<void> {
    const url = `${this.url}/?userId=${encodeURIComponent(this.identity.userId)}`;
    const ws = new WebSocket(url, ['bearer-token-v1', agentToken(this.identity)]);
    this.ws = ws;

    // Our own changes go out as updates; theirs come in and are applied with a
    // `remote` origin so they are never echoed back.
    this.doc.on('update', (update: Uint8Array, origin: unknown) => {
      if (origin === 'remote') return;
      this.send({ service: 'crdt', action: 'update', channel: this.channel, update: Buffer.from(update).toString('base64') });
    });

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`gateway did not open within ${this.snapshotTimeoutMs}ms (${this.url})`)), this.snapshotTimeoutMs);
      ws.once('open', () => { clearTimeout(timer); resolve(); });
      ws.once('error', (err) => { clearTimeout(timer); reject(err); });
    });

    const snapshot = new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        log.warn({ channel: this.channel }, 'agent collaborator: no snapshot arrived; treating the document as empty');
        resolve();
      }, this.snapshotTimeoutMs);
      ws.on('message', (raw) => {
        let msg: Record<string, unknown>;
        try { msg = JSON.parse(String(raw)) as Record<string, unknown>; } catch { return; }
        if (msg.channel !== this.channel) return;
        if (msg.type === 'crdt:snapshot' && typeof msg.snapshot === 'string') {
          Y.applyUpdate(this.doc, new Uint8Array(Buffer.from(msg.snapshot, 'base64')), 'remote');
          clearTimeout(timer);
          resolve();
        } else if (msg.type === 'crdt:update' && typeof msg.update === 'string') {
          Y.applyUpdate(this.doc, new Uint8Array(Buffer.from(msg.update, 'base64')), 'remote');
        }
      });
    });
    this.send({ service: 'crdt', action: 'subscribe', channel: this.channel });
    await snapshot;
  }

  /** Show up in the document's presence, as an editor. */
  announce(): void {
    if (!this.awareness) this.awareness = new awarenessProtocol.Awareness(this.doc);
    this.awareness.setLocalState({
      user: {
        userId: this.identity.userId,
        displayName: this.identity.displayName,
        color: this.identity.color,
        mode: 'editor',
        currentSectionId: null,
        agent: true,
      },
    });
    const update = awarenessProtocol.encodeAwarenessUpdate(this.awareness, [this.awareness.clientID]);
    this.send({ service: 'crdt', action: 'awareness', channel: this.channel, update: Buffer.from(update).toString('base64'), mode: 'editor', idle: false });
  }

  /** Leave the way a person does: presence cleared, then the socket closed. */
  async leave(holdMs = 1200): Promise<void> {
    await new Promise((r) => setTimeout(r, holdMs));
    if (this.awareness) {
      awarenessProtocol.removeAwarenessStates(this.awareness, [this.awareness.clientID], 'leave');
      const update = awarenessProtocol.encodeAwarenessUpdate(this.awareness, [this.awareness.clientID]);
      this.send({ service: 'crdt', action: 'awareness', channel: this.channel, update: Buffer.from(update).toString('base64') });
    }
    this.send({ service: 'crdt', action: 'unsubscribe', channel: this.channel });
    await new Promise((r) => setTimeout(r, 150));
    this.ws?.close();
    this.ws = null;
  }
}
