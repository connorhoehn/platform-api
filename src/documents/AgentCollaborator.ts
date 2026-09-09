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
import type * as YT from 'yjs' with { 'resolution-mode': 'import' };
// yjs ships ESM types but a CJS build; under `module: node16` the value has to
// come in through require while the types come in as type-only.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const Y = require('yjs') as typeof YT;
import type * as AwarenessT from 'y-protocols/awareness' with { 'resolution-mode': 'import' };
// eslint-disable-next-line @typescript-eslint/no-require-imports
const awarenessProtocol = require('y-protocols/awareness') as typeof AwarenessT;
import log from '../lib/logger';

import { GatewayServiceClient, type ServiceIdentity } from '../gateway/GatewayServiceClient';

export type AgentIdentity = ServiceIdentity & { color: string };

export const HAIKU_AGENT: AgentIdentity = { userId: 'agent:haiku', displayName: 'Haiku', color: '#8b5cf6' };

export interface AgentCollaboratorOptions {
  documentId: string;
  identity?: AgentIdentity;
  /** ws://host:port of the gateway. Default `GATEWAY_WS_URL` or ws://localhost:18080. */
  gatewayUrl?: string;
  /** How long to wait for the snapshot before treating the document as empty. */
  snapshotTimeoutMs?: number;
}

export class AgentCollaborator {
  readonly doc = new Y.Doc();
  readonly channel: string;
  readonly identity: AgentIdentity;
  private readonly client: GatewayServiceClient;
  private awareness: AwarenessT.Awareness | null = null;
  private readonly snapshotTimeoutMs: number;

  constructor(opts: AgentCollaboratorOptions) {
    this.identity = opts.identity ?? HAIKU_AGENT;
    this.channel = `doc:${opts.documentId}`;
    this.client = new GatewayServiceClient(this.identity, opts.gatewayUrl);
    this.snapshotTimeoutMs = opts.snapshotTimeoutMs ?? 4000;
  }

  private send(frame: Record<string, unknown>): void { this.client.send(frame); }

  /** Connect, subscribe, and resolve once the document's current state is in `doc`. */
  async join(): Promise<void> {
    // Our own changes go out as updates; theirs come in and are applied with a
    // `remote` origin so they are never echoed back.
    this.doc.on('update', (update: Uint8Array, origin: unknown) => {
      if (origin === 'remote') return;
      this.send({ service: 'crdt', action: 'update', channel: this.channel, update: Buffer.from(update).toString('base64') });
    });
    await this.client.connect(this.snapshotTimeoutMs);
    this.client.onFrame((msg) => {
      if (msg.channel !== this.channel) return;
      if (msg.type === 'crdt:update' && typeof msg.update === 'string') {
        Y.applyUpdate(this.doc, new Uint8Array(Buffer.from(msg.update, 'base64')), 'remote');
      }
    });
    const snapshot = this.client.waitFor((f) => f.channel === this.channel && f.type === 'crdt:snapshot' && typeof f.snapshot === 'string', this.snapshotTimeoutMs);
    this.send({ service: 'crdt', action: 'subscribe', channel: this.channel });
    const snap = await snapshot;
    if (snap) Y.applyUpdate(this.doc, new Uint8Array(Buffer.from(snap.snapshot as string, 'base64')), 'remote');
    else log.warn({ channel: this.channel }, 'agent collaborator: no snapshot arrived; treating the document as empty');
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
    await this.client.close(150);
  }
}
