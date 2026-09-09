// The edits a small model is allowed to make to a canvas document, and how
// they land in the Y.Doc.
//
// The model never sees Y.js. It sees an OUTLINE — numbered blocks with their
// kind and text — and answers with a short list of operations against those
// numbers. That keeps the contract small enough for a light model to get
// right every time, and keeps every write structural: a heading stays a
// heading, a paragraph stays a paragraph, marks inside untouched blocks are
// never disturbed. Anything the model says that is not one of these
// operations is ignored, not improvised around.
import type * as YT from 'yjs' with { 'resolution-mode': 'import' };
// yjs ships ESM types but a CJS build; under `module: node16` the value has to
// come in through require while the types come in as type-only.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const Y = require('yjs') as typeof YT;

/** The canvas body fragment and the metadata map — the same keys the editor uses. */
export const CANVAS_BODY_KEY = 'body';
export const CANVAS_META_KEY = 'meta';

export type DocOp =
  | { op: 'setTitle'; text: string }
  | { op: 'replaceBlock'; index: number; text: string }
  | { op: 'appendBlock'; kind: 'paragraph' | 'heading'; level?: number; text: string }
  | { op: 'insertBlock'; index: number; kind: 'paragraph' | 'heading'; level?: number; text: string }
  | { op: 'deleteBlock'; index: number };

export interface OutlineBlock { index: number; kind: string; level?: number; text: string }
export interface DocumentOutline { title: string | undefined; blocks: OutlineBlock[]; text: string }

const MAX_TEXT = 4000;
const MAX_OPS = 40;

function plainText(el: YT.XmlElement | YT.XmlText | YT.XmlHook): string {
  if (el instanceof Y.XmlText) {
    return (el.toDelta() as Array<{ insert?: unknown }>)
      .map((d) => (typeof d.insert === 'string' ? d.insert : ''))
      .join('');
  }
  if (el instanceof Y.XmlElement) return el.toArray().map((c) => plainText(c as YT.XmlElement | YT.XmlText)).join('');
  return '';
}

function blocksOf(doc: YT.Doc): YT.XmlElement[] {
  return doc.getXmlFragment(CANVAS_BODY_KEY).toArray().filter((n): n is YT.XmlElement => n instanceof Y.XmlElement);
}

/** What the model is shown: numbered blocks, kind and level, plain text. */
export function outlineOf(doc: YT.Doc): DocumentOutline {
  const meta = doc.getMap<unknown>(CANVAS_META_KEY);
  const title = typeof meta.get('title') === 'string' ? (meta.get('title') as string) : undefined;
  const blocks: OutlineBlock[] = blocksOf(doc).map((el, index) => {
    const level = el.nodeName === 'heading' ? Number(el.getAttribute('level') ?? 1) : undefined;
    return { index, kind: el.nodeName, ...(level ? { level } : {}), text: plainText(el).slice(0, MAX_TEXT) };
  });
  const lines = [
    `title: ${title ?? '(none)'}`,
    ...blocks.map((b) => `#${b.index} ${b.kind}${b.level ? `(h${b.level})` : ''}: ${b.text || '(empty)'}`),
  ];
  return { title, blocks, text: lines.join('\n') };
}

function makeBlock(kind: 'paragraph' | 'heading', text: string, level?: number): YT.XmlElement {
  const el = new Y.XmlElement(kind);
  if (kind === 'heading') el.setAttribute('level', String(level && level >= 1 && level <= 6 ? level : 2));
  const t = new Y.XmlText();
  t.insert(0, text);
  el.insert(0, [t]);
  return el;
}

function setBlockText(el: YT.XmlElement, text: string): void {
  if (el.length > 0) el.delete(0, el.length);
  const t = new Y.XmlText();
  t.insert(0, text);
  el.insert(0, [t]);
}

/**
 * Pull the first JSON array out of whatever the model said. A light model
 * fences, prefaces and apologises; the array is the only part that counts.
 */
export function parseOps(text: string): DocOp[] {
  const start = text.indexOf('[');
  const end = text.lastIndexOf(']');
  if (start < 0 || end <= start) return [];
  let raw: unknown;
  try { raw = JSON.parse(text.slice(start, end + 1)); } catch { return []; }
  if (!Array.isArray(raw)) return [];
  const ops: DocOp[] = [];
  for (const item of raw.slice(0, MAX_OPS)) {
    const o = item as Record<string, unknown>;
    const text = typeof o.text === 'string' ? o.text.slice(0, MAX_TEXT) : undefined;
    const index = typeof o.index === 'number' && Number.isInteger(o.index) && o.index >= 0 ? o.index : undefined;
    const kind = o.kind === 'heading' || o.kind === 'paragraph' ? o.kind : undefined;
    const level = typeof o.level === 'number' ? o.level : undefined;
    switch (o.op) {
      case 'setTitle': if (text) ops.push({ op: 'setTitle', text }); break;
      case 'replaceBlock': if (index !== undefined && text !== undefined) ops.push({ op: 'replaceBlock', index, text }); break;
      case 'appendBlock': if (kind && text) ops.push({ op: 'appendBlock', kind, text, ...(level ? { level } : {}) }); break;
      case 'insertBlock': if (index !== undefined && kind && text) ops.push({ op: 'insertBlock', index, kind, text, ...(level ? { level } : {}) }); break;
      case 'deleteBlock': if (index !== undefined) ops.push({ op: 'deleteBlock', index }); break;
      default: break;
    }
  }
  return ops;
}

export interface ApplyResult { applied: number; skipped: number; title?: string }

/** Apply in ONE transaction under `origin`, so the change is one update on the wire. */
export function applyOps(doc: YT.Doc, ops: DocOp[], origin: unknown = 'agent'): ApplyResult {
  const result: ApplyResult = { applied: 0, skipped: 0 };
  doc.transact(() => {
    const body = doc.getXmlFragment(CANVAS_BODY_KEY);
    for (const op of ops) {
      const blocks = blocksOf(doc);
      switch (op.op) {
        case 'setTitle': {
          doc.getMap<unknown>(CANVAS_META_KEY).set('title', op.text);
          const h1 = blocks.find((b) => b.nodeName === 'heading' && String(b.getAttribute('level') ?? '1') === '1');
          if (h1) setBlockText(h1, op.text);
          else body.insert(0, [makeBlock('heading', op.text, 1)]);
          result.title = op.text;
          result.applied += 1;
          break;
        }
        case 'replaceBlock': {
          const el = blocks[op.index];
          if (!el) { result.skipped += 1; break; }
          setBlockText(el, op.text);
          result.applied += 1;
          break;
        }
        case 'appendBlock':
          body.insert(body.length, [makeBlock(op.kind, op.text, op.level)]);
          result.applied += 1;
          break;
        case 'insertBlock': {
          const el = blocks[op.index];
          if (!el) { body.insert(body.length, [makeBlock(op.kind, op.text, op.level)]); result.applied += 1; break; }
          // Position in the FRAGMENT, which may hold non-element nodes too.
          const at = body.toArray().indexOf(el);
          body.insert(at < 0 ? body.length : at, [makeBlock(op.kind, op.text, op.level)]);
          result.applied += 1;
          break;
        }
        case 'deleteBlock': {
          const el = blocks[op.index];
          if (!el) { result.skipped += 1; break; }
          const at = body.toArray().indexOf(el);
          if (at >= 0) { body.delete(at, 1); result.applied += 1; } else result.skipped += 1;
          break;
        }
        default: result.skipped += 1;
      }
    }
  }, origin);
  return result;
}
