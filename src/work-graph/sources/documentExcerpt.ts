/**
 * A page document's opening words, for the work graph's document pane
 * (realtime-examples NFR #209: the Release plan pane showed only its status
 * line — "Release plan · rev 6 · 09:08" — at every size and theme).
 *
 * A page's words live only in its Y.Doc, which the gateway persists to
 * `crdt-snapshots` (gzipped state update, newest `timestamp` last). A build
 * asks one key-only Query for the newest snapshot's timestamp; the excerpt
 * derived from that snapshot is remembered in process by (document,
 * timestamp), so the bytes are read and decoded once per saved version, not
 * once per build. Anything unreadable — no snapshot, a sectioned document,
 * bytes that do not decode — is simply no excerpt.
 */

import { gunzipSync } from 'node:zlib';
import type * as YT from 'yjs' with { 'resolution-mode': 'import' };
import { GetCommand, QueryCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { tableName } from '../../lib/ddb-table-name';
import { CANVAS_BODY_KEY } from '../../documents/documentAgentOps';

// eslint-disable-next-line @typescript-eslint/no-require-imports
const Y = require('yjs') as typeof YT;

const SNAPSHOTS_TABLE = tableName(process.env.DYNAMODB_CRDT_TABLE || 'crdt-snapshots');
/** The excerpt's length cap, in characters. */
export const EXCERPT_MAX = 160;
/** Snapshots bigger than this are not decoded for an excerpt. */
const MAX_SNAPSHOT_BYTES = 2 * 1024 * 1024;
const REMEMBERED = 500;

const remembered = new Map<string, { timestamp: number; excerpt: string | null }>();

function textOf(node: YT.XmlElement | YT.XmlText): string {
  if (node instanceof Y.XmlText) {
    return (node.toDelta() as Array<{ insert?: unknown }>).map((part) => (typeof part.insert === 'string' ? part.insert : '')).join('');
  }
  if (node instanceof Y.XmlElement) return node.toArray().map((child) => textOf(child as YT.XmlElement | YT.XmlText)).join(' ');
  return '';
}

/**
 * The first block of prose that is not the document's own title: a heading
 * that repeats the title ("Release plan" in "Release plan") says nothing.
 */
export function excerptOfUpdate(update: Uint8Array, title?: string): string | null {
  const doc = new Y.Doc();
  try {
    Y.applyUpdate(doc, update);
    const blocks = doc.getXmlFragment(CANVAS_BODY_KEY).toArray()
      .filter((node): node is YT.XmlElement => node instanceof Y.XmlElement);
    const name = title?.trim().toLowerCase();
    for (const block of blocks) {
      if (block.nodeName === 'heading' || block.nodeName === 'horizontalRule' || block.nodeName === 'macro') continue;
      const text = textOf(block).replace(/\s+/g, ' ').trim();
      if (!text || text.toLowerCase() === name) continue;
      return text.length > EXCERPT_MAX ? `${text.slice(0, EXCERPT_MAX - 1).trimEnd()}…` : text;
    }
    return null;
  } catch {
    return null;
  } finally {
    doc.destroy();
  }
}

function bytesOf(value: unknown): Uint8Array | null {
  if (value instanceof Uint8Array) return value;
  if (typeof value === 'string') return Buffer.from(value, 'base64');
  return null;
}

export async function readDocumentExcerpt(
  client: DynamoDBDocumentClient,
  documentId: string,
  title?: string,
): Promise<string | null> {
  const channel = `doc:${documentId}`;
  const newest = await client.send(new QueryCommand({
    TableName: SNAPSHOTS_TABLE,
    KeyConditionExpression: 'documentId = :d',
    ExpressionAttributeValues: { ':d': channel },
    ProjectionExpression: '#ts',
    ExpressionAttributeNames: { '#ts': 'timestamp' },
    ScanIndexForward: false,
    Limit: 1,
  }));
  const timestamp = Number(newest.Items?.[0]?.timestamp);
  if (!Number.isFinite(timestamp)) return null;
  const key = `${documentId}\u0000${title ?? ''}`;
  const known = remembered.get(key);
  if (known && known.timestamp === timestamp) return known.excerpt;
  const item = (await client.send(new GetCommand({
    TableName: SNAPSHOTS_TABLE,
    Key: { documentId: channel, timestamp },
  }))).Item;
  let bytes = bytesOf(item?.snapshot);
  let excerpt: string | null = null;
  if (bytes && bytes.length <= MAX_SNAPSHOT_BYTES) {
    try {
      if (bytes[0] === 0x1f && bytes[1] === 0x8b) bytes = gunzipSync(bytes);
      excerpt = excerptOfUpdate(bytes, title);
    } catch {
      excerpt = null;
    }
  }
  remembered.delete(key);
  remembered.set(key, { timestamp, excerpt });
  if (remembered.size > REMEMBERED) {
    const oldest = remembered.keys().next().value;
    if (oldest !== undefined) remembered.delete(oldest);
  }
  return excerpt;
}
