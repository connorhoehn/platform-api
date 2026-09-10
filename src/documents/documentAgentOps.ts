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
import {
  acceptSuggestions, createSuggestionIdentity, readSuggestions, rejectSuggestions,
  suggestDeleteRange, suggestInsertText, suggestReplaceText, suggestionKey,
  SUGGESTION_DELETE_MARK, SUGGESTION_INSERT_MARK,
  type SuggestionIdentity, type SuggestionRun,
} from 'distributed-core/applications/document';

export { createSuggestionIdentity, readSuggestions, suggestionKey, type SuggestionIdentity, type SuggestionRun };

/** The canvas body fragment and the metadata map — the same keys the editor uses. */
export const CANVAS_BODY_KEY = 'body';
export const CANVAS_META_KEY = 'meta';

export type DocOp =
  | { op: 'setTitle'; text: string }
  /** Stamp the document's type (meta) — a transform's first act. */
  | { op: 'setDocumentType'; typeId?: string; typeName: string; icon?: string }
  /** Carry a block from the SOURCE outline verbatim — how macros cross a transform without the model retyping them. */
  | { op: 'copyBlock'; index: number }
  | { op: 'appendMacro'; macroName: string; yaml: string }
  | { op: 'skip'; reason: string }
  | { op: 'replaceBlock'; index: number; text: string }
  | { op: 'appendBlock'; kind: 'paragraph' | 'heading'; level?: number; text: string }
  | { op: 'insertBlock'; index: number; kind: 'paragraph' | 'heading'; level?: number; text: string }
  | { op: 'deleteBlock'; index: number }
  /** Resolve every suggestion under one key (`${user}@${date}`) across the whole body — a review landing server-side. */
  | { op: 'acceptSuggestion'; key: string }
  | { op: 'rejectSuggestion'; key: string };

export interface OutlineBlock { index: number; kind: string; level?: number; text: string; macroName?: string }
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
    const macroName = el.nodeName === 'macro' ? el.getAttribute('macroName') ?? undefined : undefined;
    return { index, kind: el.nodeName, ...(level ? { level } : {}), ...(macroName ? { macroName } : {}), text: plainText(el).slice(0, MAX_TEXT) };
  });
  const lines = [
    `title: ${title ?? '(none)'}`,
    ...blocks.map((b) => `#${b.index} ${b.kind}${b.level ? `(h${b.level})` : ''}${b.macroName ? `(${b.macroName})` : ''}: ${b.text || '(empty)'}`),
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

function makeMacro(macroName: string, yaml: string): YT.XmlElement {
  const el = new Y.XmlElement('macro');
  el.setAttribute('macroName', macroName);
  const t = new Y.XmlText();
  t.insert(0, yaml);
  el.insert(0, [t]);
  return el;
}

/** Rebuild a block from an outline entry — headings/paragraphs by text, macros by name + YAML body. */
function materialize(b: OutlineBlock): YT.XmlElement {
  if (b.kind === 'macro' && b.macroName) return makeMacro(b.macroName, b.text);
  if (b.kind === 'heading') return makeBlock('heading', b.text, b.level);
  return makeBlock('paragraph', b.text);
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
      case 'setDocumentType': if (typeof o.typeName === 'string' && o.typeName) ops.push({ op: 'setDocumentType', typeName: o.typeName, ...(typeof o.typeId === 'string' ? { typeId: o.typeId } : {}), ...(typeof o.icon === 'string' ? { icon: o.icon } : {}) }); break;
      case 'copyBlock': if (index !== undefined) ops.push({ op: 'copyBlock', index }); break;
      case 'skip': ops.push({ op: 'skip', reason: typeof o.reason === 'string' ? o.reason.slice(0, 300) : '' }); break;
      case 'appendMacro': if (typeof o.macroName === 'string' && o.macroName && typeof o.yaml === 'string') ops.push({ op: 'appendMacro', macroName: o.macroName, yaml: o.yaml.slice(0, MAX_TEXT) }); break;
      case 'replaceBlock': if (index !== undefined && text !== undefined) ops.push({ op: 'replaceBlock', index, text }); break;
      case 'appendBlock': if (kind && text) ops.push({ op: 'appendBlock', kind, text, ...(level ? { level } : {}) }); break;
      case 'insertBlock': if (index !== undefined && kind && text) ops.push({ op: 'insertBlock', index, kind, text, ...(level ? { level } : {}) }); break;
      case 'deleteBlock': if (index !== undefined) ops.push({ op: 'deleteBlock', index }); break;
      case 'acceptSuggestion': if (typeof o.key === 'string' && o.key) ops.push({ op: 'acceptSuggestion', key: o.key.slice(0, 200) }); break;
      case 'rejectSuggestion': if (typeof o.key === 'string' && o.key) ops.push({ op: 'rejectSuggestion', key: o.key.slice(0, 200) }); break;
      default: break;
    }
  }
  return ops;
}

export interface ApplyResult {
  applied: number;
  skipped: number;
  title?: string;
  /** Set in suggest mode: the `${user}@${date}` every mark this call wrote carries — what a review accepts or rejects. */
  suggestionKey?: string;
}

/**
 * How an edit lands. `direct` writes the text the way a person typing would;
 * `suggest` writes TRACKED CHANGES — the editor's suggestion marks, attributed
 * to `identity` — so nothing the agent proposes is final until a person
 * accepts it. Block-level marks do not survive Yjs, so every suggestion sits on
 * inline text: a new block is a block whose text is all insert-marked, a
 * deleted block is one whose text is all delete-marked (and stays until
 * accepted).
 */
export type ApplyMode = 'direct' | 'suggest';

export interface ApplyOptions {
  /** Transaction origin (default `'agent'`). */
  origin?: unknown;
  /** The SOURCE outline `copyBlock` carries from (a transform). */
  sourceBlocks?: readonly OutlineBlock[];
  /** Default `direct`. */
  mode?: ApplyMode;
  /** Who the suggestions are attributed to. Required for `suggest`; falls back to direct without one. */
  identity?: SuggestionIdentity;
}

/** The XmlText children of a block, in order. */
function textsOf(el: YT.XmlElement): YT.XmlText[] {
  return el.toArray().filter((c): c is YT.XmlText => c instanceof Y.XmlText);
}

/** The block's text to hang a mark on — created (empty, appended) when the block has none. */
function primaryText(el: YT.XmlElement): YT.XmlText {
  const texts = textsOf(el);
  if (texts.length > 0) return texts[texts.length - 1]!;
  const t = new Y.XmlText();
  el.insert(el.length, [t]);
  return t;
}

/** An empty block of `kind`, to be filled by a suggested insertion once it is in the document. */
function makeEmptyBlock(kind: 'paragraph' | 'heading' | 'macro', level?: number, macroName?: string): YT.XmlElement {
  const el = new Y.XmlElement(kind);
  if (kind === 'heading') el.setAttribute('level', String(level && level >= 1 && level <= 6 ? level : 2));
  if (kind === 'macro' && macroName) el.setAttribute('macroName', macroName);
  el.insert(0, [new Y.XmlText()]);
  return el;
}

/** Insert a block whose whole text is a suggested insertion. The block is placed FIRST so its text is integrated when the mark is written. */
function suggestNewBlock(body: YT.XmlFragment, at: number, kind: 'paragraph' | 'heading' | 'macro', text: string, id: SuggestionIdentity, level?: number, macroName?: string): void {
  const el = makeEmptyBlock(kind, level, macroName);
  body.insert(at, [el]);
  suggestInsertText(primaryText(el), 0, text, id);
}

/** Suggest replacing a block's text: the last text run is rewritten, any others are delete-marked. */
function suggestBlockText(el: YT.XmlElement, text: string, id: SuggestionIdentity): void {
  const texts = textsOf(el);
  for (const t of texts.slice(0, -1)) if (t.length > 0) suggestDeleteRange(t, 0, t.length, id);
  suggestReplaceText(primaryText(el), text, id);
}

/** Suggest deleting a block: every text run delete-marked; a ZWS anchor when there is no text at all. */
function suggestBlockDelete(el: YT.XmlElement, id: SuggestionIdentity): void {
  const texts = textsOf(el);
  const total = texts.reduce((n, t) => n + t.length, 0);
  if (total === 0) { suggestDeleteRange(primaryText(el), 0, 0, id); return; }
  for (const t of texts) if (t.length > 0) suggestDeleteRange(t, 0, t.length, id);
}

/** Runs of `key` in a block: does it carry any, and are they all one mark? */
function keyRuns(el: YT.XmlElement, key: string): SuggestionRun[] {
  return textsOf(el).flatMap((t) => readSuggestions(t)).filter((r) => r.key === key);
}

/**
 * Resolve suggestion `key` across the whole body. A block that was ENTIRELY a
 * suggestion — a proposed new paragraph on reject, a proposed deletion on
 * accept — is left with no text by the helpers; that block is removed, since
 * the editor's own resolve leaves no empty shell either. A block that had text
 * of its own before the suggestion keeps it.
 */
function resolveSuggestion(body: YT.XmlFragment, key: string, decision: 'accept' | 'reject'): number {
  let touched = 0;
  const blocks = body.toArray().filter((n): n is YT.XmlElement => n instanceof Y.XmlElement);
  for (const el of blocks) {
    const runs = keyRuns(el, key);
    if (runs.length === 0) continue;
    touched += 1;
    const removable = decision === 'accept'
      ? runs.every((r) => r.markName === SUGGESTION_DELETE_MARK)
      : runs.every((r) => r.markName === SUGGESTION_INSERT_MARK);
    for (const t of textsOf(el)) (decision === 'accept' ? acceptSuggestions : rejectSuggestions)(t, key);
    if (removable && textsOf(el).every((t) => t.length === 0)) {
      const at = body.toArray().indexOf(el);
      if (at >= 0) body.delete(at, 1);
    }
  }
  return touched;
}

/** What a canvas document carries in `meta` once it has been seeded (the client's CanvasSeedHost writes the same keys). */
export const CANVAS_SCHEMA_VERSION = 2;

export interface CanvasMetaSeed { id: string; title: string; createdBy: string; type?: string; createdAt?: string }

/**
 * A document created outside the client's seed (an API smoke test, an
 * external caller, a page a pipeline makes for itself) has no `schemaVersion`
 * and so opens as a SECTION document — the body a pipeline writes is there but
 * invisible. Stamp the same metadata the client seed writes, once. Never
 * touches a document that already has a schemaVersion.
 */
export function ensureCanvasMeta(doc: YT.Doc, seed: CanvasMetaSeed, origin: unknown = 'agent'): boolean {
  const meta = doc.getMap<unknown>(CANVAS_META_KEY);
  if (meta.get('schemaVersion') !== undefined) return false;
  doc.transact(() => {
    if (typeof meta.get('id') !== 'string') meta.set('id', seed.id);
    if (typeof meta.get('title') !== 'string') meta.set('title', seed.title);
    meta.set('sourceType', 'notes');
    meta.set('sourceId', '');
    meta.set('createdBy', seed.createdBy);
    meta.set('createdAt', seed.createdAt ?? new Date().toISOString());
    meta.set('aiModel', '');
    if (typeof meta.get('status') !== 'string') meta.set('status', 'draft');
    if (seed.type && typeof meta.get('documentType') !== 'string') meta.set('documentType', seed.type);
    meta.set('templateApplied', true);
    meta.set('schemaVersion', CANVAS_SCHEMA_VERSION);
  }, origin);
  return true;
}

/**
 * Apply in ONE transaction under `origin`, so the change is one update on the
 * wire. The old positional form `(doc, ops, origin, sourceBlocks)` still works;
 * the options form adds `mode` + `identity` for tracked changes.
 *
 * In suggest mode `setTitle` splits: the h1 mirror is a suggested replace, but
 * the META title is written directly — a Y.Map scalar has no suggestion
 * representation (marks live on inline text only), and the header reads
 * `meta.title`. `setDocumentType` stays direct for the same reason.
 */
export function applyOps(doc: YT.Doc, ops: DocOp[], origin?: unknown, sourceBlocks?: readonly OutlineBlock[]): ApplyResult;
export function applyOps(doc: YT.Doc, ops: DocOp[], options: ApplyOptions): ApplyResult;
export function applyOps(doc: YT.Doc, ops: DocOp[], originOrOptions: unknown = 'agent', sourceBlocksArg: readonly OutlineBlock[] = []): ApplyResult {
  const opts: ApplyOptions = isApplyOptions(originOrOptions) ? originOrOptions : { origin: originOrOptions, sourceBlocks: sourceBlocksArg };
  const origin = opts.origin ?? 'agent';
  const sourceBlocks = opts.sourceBlocks ?? [];
  const id = opts.mode === 'suggest' && opts.identity ? opts.identity : null;
  const result: ApplyResult = { applied: 0, skipped: 0, ...(id ? { suggestionKey: suggestionKey(id) } : {}) };
  doc.transact(() => {
    const body = doc.getXmlFragment(CANVAS_BODY_KEY);
    for (const op of ops) {
      const blocks = blocksOf(doc);
      switch (op.op) {
        case 'setTitle': {
          doc.getMap<unknown>(CANVAS_META_KEY).set('title', op.text);
          const h1 = blocks.find((b) => b.nodeName === 'heading' && String(b.getAttribute('level') ?? '1') === '1');
          if (h1) { if (id) suggestBlockText(h1, op.text, id); else setBlockText(h1, op.text); }
          else if (id) suggestNewBlock(body, 0, 'heading', op.text, id, 1);
          else body.insert(0, [makeBlock('heading', op.text, 1)]);
          result.title = op.text;
          result.applied += 1;
          break;
        }
        case 'replaceBlock': {
          const el = blocks[op.index];
          if (!el) { result.skipped += 1; break; }
          if (id) suggestBlockText(el, op.text, id); else setBlockText(el, op.text);
          result.applied += 1;
          break;
        }
        case 'appendBlock':
          if (id) suggestNewBlock(body, body.length, op.kind, op.text, id, op.level);
          else body.insert(body.length, [makeBlock(op.kind, op.text, op.level)]);
          result.applied += 1;
          break;
        case 'appendMacro':
          // The YAML body is the macro's inline text, so it carries the mark like a paragraph would.
          if (id) suggestNewBlock(body, body.length, 'macro', op.yaml, id, undefined, op.macroName);
          else body.insert(body.length, [makeMacro(op.macroName, op.yaml)]);
          result.applied += 1;
          break;
        case 'copyBlock': {
          const src = sourceBlocks[op.index];
          if (!src) { result.skipped += 1; break; }
          if (id) {
            if (src.kind === 'macro' && src.macroName) suggestNewBlock(body, body.length, 'macro', src.text, id, undefined, src.macroName);
            else suggestNewBlock(body, body.length, src.kind === 'heading' ? 'heading' : 'paragraph', src.text, id, src.level);
          } else body.insert(body.length, [materialize(src)]);
          result.applied += 1;
          break;
        }
        case 'setDocumentType': {
          const meta = doc.getMap<unknown>(CANVAS_META_KEY);
          meta.set('documentType', op.typeName);
          if (op.typeId) meta.set('documentTypeId', op.typeId);
          if (op.icon) meta.set('icon', op.icon);
          result.applied += 1;
          break;
        }
        case 'insertBlock': {
          const el = blocks[op.index];
          // Position in the FRAGMENT, which may hold non-element nodes too.
          const at = el ? body.toArray().indexOf(el) : -1;
          const pos = at < 0 ? body.length : at;
          if (id) suggestNewBlock(body, pos, op.kind, op.text, id, op.level);
          else body.insert(pos, [makeBlock(op.kind, op.text, op.level)]);
          result.applied += 1;
          break;
        }
        case 'deleteBlock': {
          const el = blocks[op.index];
          if (!el) { result.skipped += 1; break; }
          if (id) { suggestBlockDelete(el, id); result.applied += 1; break; }
          const at = body.toArray().indexOf(el);
          if (at >= 0) { body.delete(at, 1); result.applied += 1; } else result.skipped += 1;
          break;
        }
        case 'acceptSuggestion':
        case 'rejectSuggestion': {
          // Always direct — a review is the person's decision landing, not a new proposal.
          const touched = resolveSuggestion(body, op.key, op.op === 'acceptSuggestion' ? 'accept' : 'reject');
          if (touched > 0) result.applied += 1; else result.skipped += 1;
          break;
        }
        default: result.skipped += 1;
      }
    }
  }, origin);
  return result;
}

function isApplyOptions(v: unknown): v is ApplyOptions {
  if (!v || typeof v !== 'object') return false;
  const o = v as Record<string, unknown>;
  return 'mode' in o || 'identity' in o || 'sourceBlocks' in o || 'origin' in o;
}
