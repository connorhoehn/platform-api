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

const remembered = new Map<string, { timestamp: number; excerpt: string | null; text: string }>();

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

/** The searchable text's length cap, in characters (NFR #230: document search). */
export const TEXT_MAX = 20_000;

/**
 * Every block's words, one block per line — what document search matches a
 * body against. Headings included (they are words the author chose); capped
 * at `TEXT_MAX`.
 */
export function textOfUpdate(update: Uint8Array): string {
  const doc = new Y.Doc();
  try {
    Y.applyUpdate(doc, update);
    const lines: string[] = [];
    let size = 0;
    for (const node of doc.getXmlFragment(CANVAS_BODY_KEY).toArray()) {
      if (!(node instanceof Y.XmlElement)) continue;
      const text = textOf(node).replace(/\s+/g, ' ').trim();
      if (!text) continue;
      lines.push(text);
      size += text.length + 1;
      if (size >= TEXT_MAX) break;
    }
    return lines.join('\n').slice(0, TEXT_MAX);
  } catch {
    return '';
  } finally {
    doc.destroy();
  }
}

export interface DocumentText {
  /** The newest saved version's timestamp; null when the document has none. */
  timestamp: number | null;
  excerpt: string | null;
  text: string;
}

export async function readDocumentExcerpt(
  client: DynamoDBDocumentClient,
  documentId: string,
  title?: string,
): Promise<string | null> {
  return (await readDocumentText(client, documentId, title)).excerpt;
}

/**
 * The excerpt and the searchable text of the newest saved version: one
 * key-only Query, then (once per version) a Get + gunzip + decode that both
 * readers share.
 */
export async function readDocumentText(
  client: DynamoDBDocumentClient,
  documentId: string,
  title?: string,
): Promise<DocumentText> {
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
  if (!Number.isFinite(timestamp)) return { timestamp: null, excerpt: null, text: '' };
  const key = `${documentId}\u0000${title ?? ''}`;
  const known = remembered.get(key);
  if (known && known.timestamp === timestamp) return { timestamp, excerpt: known.excerpt, text: known.text };
  const item = (await client.send(new GetCommand({
    TableName: SNAPSHOTS_TABLE,
    Key: { documentId: channel, timestamp },
  }))).Item;
  let bytes = bytesOf(item?.snapshot);
  let excerpt: string | null = null;
  let text = '';
  if (bytes && bytes.length <= MAX_SNAPSHOT_BYTES) {
    try {
      if (bytes[0] === 0x1f && bytes[1] === 0x8b) bytes = gunzipSync(bytes);
      excerpt = excerptOfUpdate(bytes, title);
      text = textOfUpdate(bytes);
    } catch {
      excerpt = null;
    }
  }
  remembered.delete(key);
  remembered.set(key, { timestamp, excerpt, text });
  if (remembered.size > REMEMBERED) {
    const oldest = remembered.keys().next().value;
    if (oldest !== undefined) remembered.delete(oldest);
  }
  return { timestamp, excerpt, text };
}
