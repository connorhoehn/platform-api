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

/**
 * NFR #213 — a run's suggestions carry the RUN in their identity.
 *
 * The chassis identity is `user@date` with `date` floored to ten minutes, which
 * is right for a person typing and wrong for agent runs: every run in the same
 * bucket wrote `agent:haiku@<bucket>`, so accepting one run's suggestion (by
 * key, in the margin or server-side) resolved every other run's marks under
 * that key — and their linked comments. The mark schema has exactly three
 * attributes (`user`, `username`, `date`) shared by the editor, this writer and
 * every stored document, so the run goes into `user` (`username` is what is
 * displayed; `user` is only ever a key) and `date` is the apply time, unbucketed:
 *
 *     agent:haiku~<runId>@<epochMs>            one apply step
 *     agent:haiku~<runId>.<n>@<epochMs>        the n-th arm of an apply step in a loop
 *
 * Still `user@date`, still starts with `agent:`, parsed back by `suggestionKeyRunId`.
 * A key without `~` is LEGACY (shared across runs) and proves no run.
 */
export const SUGGESTION_RUN_SEPARATOR = '~';

export function runSuggestionIdentity(
  agentUserId: string,
  username: string,
  runId: string,
  opts: { change?: number; nowMs?: number } = {},
): SuggestionIdentity {
  const change = opts.change && opts.change > 1 ? `.${opts.change}` : '';
  return { user: `${agentUserId}${SUGGESTION_RUN_SEPARATOR}${runId}${change}`, username, date: opts.nowMs ?? Date.now() };
}

/** The run a suggestion key belongs to, or null for a legacy (shared, bucketed) key. */
export function suggestionKeyRunId(key: string | undefined | null): string | null {
  if (!key) return null;
  const at = key.lastIndexOf('@');
  const user = at >= 0 ? key.slice(0, at) : key;
  const sep = user.lastIndexOf(SUGGESTION_RUN_SEPARATOR);
  if (sep < 0) return null;
  const run = user.slice(sep + 1).replace(/\.\d+$/, '');
  return run || null;
}

/** The arm index of an apply step id (`apply:2` → 2), else undefined. */
export function applyChangeIndex(stepId: string | undefined): number | undefined {
  const m = stepId ? /:(\d+)$/.exec(stepId) : null;
  return m ? Number(m[1]) : undefined;
}

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
  | { op: 'replaceBlock'; index: number; text: string; rows?: string[][] }
  | { op: 'appendBlock'; kind: BlockKind; level?: number; text: string; rows?: string[][] }
  | { op: 'insertBlock'; index: number; kind: BlockKind; level?: number; text: string; rows?: string[][] }
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
/** The items of a list block as plain strings — the inverse of `makeList`. */
export function listItemsOf(list: YT.XmlElement): string[] {
  return list.toArray()
    .filter((li): li is YT.XmlElement => li instanceof Y.XmlElement)
    .map((li) => plainText(li).trim())
    .filter((t) => t.length > 0);
}

export function outlineOf(doc: YT.Doc): DocumentOutline {
  const meta = doc.getMap<unknown>(CANVAS_META_KEY);
  const title = typeof meta.get('title') === 'string' ? (meta.get('title') as string) : undefined;
  const blocks: OutlineBlock[] = blocksOf(doc).map((el, index) => {
    const level = el.nodeName === 'heading' ? Number(el.getAttribute('level') ?? 1) : undefined;
    const macroName = el.nodeName === 'macro' ? el.getAttribute('macroName') ?? undefined : undefined;
    // A table reads as rows — cells joined by " | ", rows by newline — so the
    // model sees the shape, and copyBlock can rebuild it (see `tableRowsOf`).
    // A table reads as rows; a list reads as ONE ITEM PER LINE, which is both
    // the shape the model is told to write and what `makeList` reads back —
    // so copyBlock and replaceBlock round-trip. Plain text would run every
    // item together into a single unreadable line.
    const text = el.nodeName === 'table'
      ? tableRowsOf(el).map((r) => r.join(' | ')).join('\n').slice(0, MAX_TEXT)
      : (LIST_KINDS as readonly string[]).includes(el.nodeName)
        ? listItemsOf(el).join('\n').slice(0, MAX_TEXT)
        : plainText(el).slice(0, MAX_TEXT);
    return { index, kind: el.nodeName, ...(level ? { level } : {}), ...(macroName ? { macroName } : {}), text };
  });
  const lines = [
    `title: ${title ?? '(none)'}`,
    ...blocks.map((b) => `#${b.index} ${b.kind}${b.level ? `(h${b.level})` : ''}${b.macroName ? `(${b.macroName})` : ''}: ${b.text || '(empty)'}`),
  ];
  return { title, blocks, text: lines.join('\n') };
}

/** Block kinds the agent may write. A horizontal rule is a divider with no text; a table carries `rows`. */
export type BlockKind = 'paragraph' | 'heading' | 'horizontalRule' | 'table' | 'bulletList' | 'orderedList';
export const BLOCK_KINDS: readonly BlockKind[] = ['paragraph', 'heading', 'horizontalRule', 'table', 'bulletList', 'orderedList'];
/** List kinds carry their items as newline-separated `text`, one item per line. */
const LIST_KINDS: readonly BlockKind[] = ['bulletList', 'orderedList'];

/** A table is capped so a light model cannot hand back a spreadsheet. */
export const TABLE_MAX_ROWS = 20;
export const TABLE_MAX_COLS = 8;
const TABLE_MAX_CELL = 200;

/**
 * The shape the editor's table extension (tiptap TableKit) reads back
 * through y-prosemirror: `table` → `tableRow` → `tableHeader` | `tableCell`
 * → `paragraph` → text. The first row is the header. Cells have no attrs
 * here; the extension's defaults (colspan 1, rowspan 1) apply. Every cell
 * gets a paragraph with a text node even when empty, so a suggestion mark
 * always has somewhere to sit.
 */
function makeTable(rows: string[][]): YT.XmlElement {
  // Children are built as arrays and inserted once: a preliminary Yjs type
  // (one not yet in a document) has no readable `length` to append at.
  const table = new Y.XmlElement('table');
  table.insert(0, rows.map((cells, r) => {
    const row = new Y.XmlElement('tableRow');
    row.insert(0, cells.map((cell) => {
      const td = new Y.XmlElement(r === 0 ? 'tableHeader' : 'tableCell');
      const para = new Y.XmlElement('paragraph');
      const t = new Y.XmlText();
      if (cell) t.insert(0, cell);
      para.insert(0, [t]);
      td.insert(0, [para]);
      return td;
    }));
    return row;
  }));
  return table;
}

/**
 * A real list: `bulletList > listItem > paragraph > text`, one item per line
 * of `text`. Built the same way as `makeTable` — children assembled as arrays
 * and inserted once, because a preliminary Yjs type has no readable `length`.
 */
function makeList(kind: 'bulletList' | 'orderedList', text: string): YT.XmlElement {
  const items = text.split(/\r?\n/).map((l) => stripListMarker(l.trim())).filter((l) => l.length > 0);
  const list = new Y.XmlElement(kind);
  list.insert(0, (items.length > 0 ? items : ['']).map((item) => {
    const li = new Y.XmlElement('listItem');
    const para = new Y.XmlElement('paragraph');
    const t = new Y.XmlText();
    if (item) t.insert(0, item);
    para.insert(0, [t]);
    li.insert(0, [para]);
    return li;
  }));
  return list;
}

const BULLET_MARKER = /^\s*([•‣▪*+-]|\d+[.)])\s+/;

function stripListMarker(line: string): string {
  return line.replace(BULLET_MARKER, '');
}

/**
 * A paragraph whose every line opens with a bullet or a number IS a list —
 * same judgement as `rowsFromPipeText` makes about pipes, and for the same
 * reason: the model is told to emit `bulletList` and writes "• item" into a
 * paragraph anyway, so the coercion belongs here and not in the prompt alone.
 * Left as a paragraph, it rendered as one run-on line with literal • in it.
 *
 * Returns the marker family so an enumerated list does not come back bulleted.
 */
export function listFromMarkerText(text: string): { kind: 'bulletList' | 'orderedList'; text: string } | undefined {
  const lines = text.split(/\r?\n/).map((l) => l.trim()).filter((l) => l.length > 0);
  // One line is a sentence that happens to start with a dash, not a list.
  if (lines.length < 2 || !lines.every((l) => BULLET_MARKER.test(l))) return undefined;
  const ordered = lines.every((l) => /^\s*\d+[.)]\s+/.test(l));
  const items = lines.map(stripListMarker).filter((l) => l.length > 0);
  if (items.length < 2) return undefined;
  return { kind: ordered ? 'orderedList' : 'bulletList', text: items.join('\n') };
}

/** The rows of a table block as plain strings — the inverse of `makeTable`. */
export function tableRowsOf(table: YT.XmlElement): string[][] {
  return table.toArray()
    .filter((n): n is YT.XmlElement => n instanceof Y.XmlElement && n.nodeName === 'tableRow')
    .map((row) => row.toArray()
      .filter((c): c is YT.XmlElement => c instanceof Y.XmlElement)
      .map((cell) => plainText(cell)));
}

/**
 * A table the model drew as text — Markdown pipes, or the outline's own
 * "cell | cell" echo — read back as rows. The prompt says not to; a light
 * model does it anyway (measured: "| Column 1 | Column 2 |" as a paragraph,
 * and a table's header cells replaced with "| Name | Owner |"). Undefined
 * when the text is not a table: fewer than one row with a cell, or no pipes.
 */
export function rowsFromPipeText(text: string): string[][] | undefined {
  const lines = text.split(/\r?\n/).map((l) => l.trim()).filter((l) => l.length > 0);
  if (lines.length === 0 || !lines.every((l) => l.includes('|'))) return undefined;
  const rows: string[][] = [];
  for (const line of lines) {
    const inner = line.replace(/^\|/, '').replace(/\|$/, '');
    const cells = inner.split('|').map((c) => c.trim());
    // The Markdown separator row: every cell dashes, maybe with alignment colons.
    if (cells.every((c) => /^:?-{2,}:?$/.test(c) || c === '')) {
      if (cells.some((c) => /-{2,}/.test(c))) continue;
    }
    rows.push(cells);
  }
  if (rows.length === 0 || rows.every((r) => r.every((c) => c === ''))) {
    return rows.length > 0 && rows[0]!.length > 1 ? normalizeTableRows(rows) : undefined;
  }
  return normalizeTableRows(rows);
}

/** Square, capped, trimmed: every row as wide as the widest, within the caps. */
export function normalizeTableRows(raw: unknown): string[][] | undefined {
  if (!Array.isArray(raw)) return undefined;
  const rows = raw.slice(0, TABLE_MAX_ROWS)
    .filter((r): r is unknown[] => Array.isArray(r))
    .map((r) => r.slice(0, TABLE_MAX_COLS).map((c) => (typeof c === 'string' ? c.slice(0, TABLE_MAX_CELL) : typeof c === 'number' ? String(c) : '')));
  if (rows.length === 0) return undefined;
  const width = Math.max(...rows.map((r) => r.length));
  if (width === 0) return undefined;
  return rows.map((r) => [...r, ...Array.from({ length: width - r.length }, () => '')]);
}

/**
 * The heading level as the editor stores it: a NUMBER 1–6. Tiptap's heading
 * renders `h${levels.includes(level) ? level : levels[0]}`, so a string "2"
 * (what a stringified write left in the Y.Doc) fails `includes` and every
 * agent heading showed as h1. A missing or out-of-range level is 2 — the
 * page's h1 is its title (setTitle), a body heading sits under it. (yjs types
 * XmlElement attrs as strings by default; the value itself may be a number,
 * which is what y-prosemirror writes for a heading typed in the editor.)
 */
export function headingLevel(level?: number): number {
  return typeof level === 'number' && Number.isInteger(level) && level >= 1 && level <= 6 ? level : 2;
}

function makeBlock(kind: BlockKind, text: string, level?: number, rows?: string[][]): YT.XmlElement {
  if (kind === 'table') return makeTable(rows && rows.length > 0 ? rows : [['']]);
  if (kind === 'bulletList' || kind === 'orderedList') return makeList(kind, text);
  const el = new Y.XmlElement(kind);
  if (kind === 'horizontalRule') return el;
  if (kind === 'heading') (el as unknown as YT.XmlElement<{ level: number }>).setAttribute('level', headingLevel(level));
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
  if (b.kind === 'horizontalRule') return makeBlock('horizontalRule', '');
  if (b.kind === 'table') return makeBlock('table', '', undefined, normalizeTableRows(b.text.split('\n').map((line) => line.split(' | '))));
  if (b.kind === 'bulletList' || b.kind === 'orderedList') return makeBlock(b.kind, b.text);
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
    // A paragraph that is a table drawn in pipes IS a table: the model was
    // told, and does it anyway, so the coercion lives here rather than in
    // the prompt alone.
    const pipeRows = typeof text === 'string' && String(o.kind ?? 'paragraph') === 'paragraph' ? rowsFromPipeText(text) : undefined;
    // Same coercion, one step later: a paragraph written as "• a\n• b" is a
    // list. Only when it is not already a table, since a pipe row can open
    // with a dash.
    const markerList = !pipeRows && typeof text === 'string' && String(o.kind ?? 'paragraph') === 'paragraph'
      ? listFromMarkerText(text)
      : undefined;
    const kind = pipeRows && (o.op === 'appendBlock' || o.op === 'insertBlock')
      ? 'table'
      : markerList && (o.op === 'appendBlock' || o.op === 'insertBlock')
        ? markerList.kind
        : (BLOCK_KINDS as readonly string[]).includes(String(o.kind)) ? (o.kind as BlockKind) : undefined;
    // A divider carries no text, a table carries rows; every other block needs text.
    const rows = kind === 'table' ? (normalizeTableRows(o.rows) ?? pipeRows) : undefined;
    const blockText = kind === 'horizontalRule' || kind === 'table'
      ? ''
      : markerList && kind === markerList.kind ? markerList.text : text;
    // The model sometimes writes the level as a string ("2", "h2"); read the digit.
    const rawLevel = typeof o.level === 'number' ? o.level : typeof o.level === 'string' ? Number(o.level.replace(/^h/i, '')) : NaN;
    const level = Number.isInteger(rawLevel) && rawLevel >= 1 && rawLevel <= 6 ? rawLevel : undefined;
    const blockOk = kind === 'table' ? rows !== undefined : blockText !== undefined;
    switch (o.op) {
      case 'setTitle': if (text) ops.push({ op: 'setTitle', text }); break;
      case 'setDocumentType': if (typeof o.typeName === 'string' && o.typeName) ops.push({ op: 'setDocumentType', typeName: o.typeName, ...(typeof o.typeId === 'string' ? { typeId: o.typeId } : {}), ...(typeof o.icon === 'string' ? { icon: o.icon } : {}) }); break;
      case 'copyBlock': if (index !== undefined) ops.push({ op: 'copyBlock', index }); break;
      case 'skip': ops.push({ op: 'skip', reason: typeof o.reason === 'string' ? o.reason.slice(0, 300) : '' }); break;
      case 'appendMacro': if (typeof o.macroName === 'string' && o.macroName && typeof o.yaml === 'string') ops.push({ op: 'appendMacro', macroName: o.macroName, yaml: o.yaml.slice(0, MAX_TEXT) }); break;
      case 'replaceBlock': {
        const replaceRows = normalizeTableRows(o.rows) ?? pipeRows;
        if (index !== undefined && (text !== undefined || replaceRows)) ops.push({ op: 'replaceBlock', index, text: text ?? '', ...(replaceRows ? { rows: replaceRows } : {}) });
        break;
      }
      case 'appendBlock': if (kind && blockOk) ops.push({ op: 'appendBlock', kind, text: blockText ?? '', ...(level ? { level } : {}), ...(rows ? { rows } : {}) }); break;
      case 'insertBlock': if (index !== undefined && kind && blockOk) ops.push({ op: 'insertBlock', index, kind, text: blockText ?? '', ...(level ? { level } : {}), ...(rows ? { rows } : {}) }); break;
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

/** The XmlText runs of a block, in order — nested for a table, whose text sits in its cells. */
function textsOf(el: YT.XmlElement): YT.XmlText[] {
  const out: YT.XmlText[] = [];
  for (const c of el.toArray()) {
    if (c instanceof Y.XmlText) out.push(c);
    else if (c instanceof Y.XmlElement) out.push(...textsOf(c));
  }
  return out;
}

/** A zero-width anchor that carries a mark where a cell has no text of its own. */
const CELL_ANCHOR = '\u200B';

/**
 * A table proposed as a suggestion: every cell's text is insert-marked under
 * the one key, and an empty cell gets a marked zero-width anchor so the whole
 * table still reads — and resolves — as one insertion. Accepting strips the
 * anchors (see `resolveSuggestion`); rejecting empties every run and the
 * table goes with them.
 */
function suggestNewTable(body: YT.XmlFragment, at: number, rows: string[][], id: SuggestionIdentity): void {
  const table = makeTable(rows.map((r) => r.map(() => '')));
  body.insert(at, [table]);
  const texts = textsOf(table);
  let i = 0;
  for (const r of rows) for (const cell of r) {
    const t = texts[i++];
    if (t) suggestInsertText(t, 0, cell || CELL_ANCHOR, id);
  }
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
  if (kind === 'heading') (el as unknown as YT.XmlElement<{ level: number }>).setAttribute('level', headingLevel(level));
  if (kind === 'macro' && macroName) el.setAttribute('macroName', macroName);
  el.insert(0, [new Y.XmlText()]);
  return el;
}

/** Insert a block whose whole text is a suggested insertion. The block is placed FIRST so its text is integrated when the mark is written. */
function suggestNewBlock(body: YT.XmlFragment, at: number, kind: 'paragraph' | 'heading' | 'macro', text: string, id: SuggestionIdentity, level?: number, macroName?: string): void {
  const el = makeEmptyBlock(kind, level, macroName);
  body.insert(at, [el]);
  // An empty proposed block still needs a mark, or it lands bare and outlives a Reject.
  suggestInsertText(primaryText(el), 0, text || CELL_ANCHOR, id);
}

/**
 * A list proposed as a suggestion: every item's text is insert-marked under the
 * one key, the way `suggestNewTable` marks every cell. Before this a list went
 * in through `makeBlock` — bare content in suggest mode — so rejecting a run
 * that wrote bullets left the bullets in the page (an empty page asked for "a
 * basic recipe style" kept all eight items after Reject). Rejecting empties
 * every item and the list goes with them; the editor's reject removes the
 * wholly inserted blocks the same way.
 */
function suggestNewList(body: YT.XmlFragment, at: number, kind: 'bulletList' | 'orderedList', text: string, id: SuggestionIdentity): void {
  const items = text.split(/\r?\n/).map((l) => stripListMarker(l.trim())).filter((l) => l.length > 0);
  // One empty item per line, filled once the list is in the document (a
  // preliminary Yjs type cannot carry a formatted insert).
  const skeleton = new Y.XmlElement(kind);
  skeleton.insert(0, (items.length > 0 ? items : ['']).map(() => {
    const li = new Y.XmlElement('listItem');
    const para = new Y.XmlElement('paragraph');
    para.insert(0, [new Y.XmlText()]);
    li.insert(0, [para]);
    return li;
  }));
  body.insert(at, [skeleton]);
  const texts = textsOf(skeleton);
  (items.length > 0 ? items : [CELL_ANCHOR]).forEach((item, i) => { const t = texts[i]; if (t) suggestInsertText(t, 0, item, id); });
}

/**
 * A new block of any kind the agent may write, as a suggestion. Every kind that
 * lands must land MARKED — a bare block is one Reject cannot take back. A
 * divider has no text to carry a mark, so in suggest mode it is not written at
 * all (false: the caller counts a skip) rather than written for good.
 */
function suggestNewKind(body: YT.XmlFragment, at: number, op: { kind: BlockKind; text: string; level?: number; rows?: string[][] }, id: SuggestionIdentity): boolean {
  switch (op.kind) {
    case 'table': suggestNewTable(body, at, op.rows ?? [['']], id); return true;
    case 'bulletList': case 'orderedList': suggestNewList(body, at, op.kind, op.text, id); return true;
    case 'paragraph': case 'heading': suggestNewBlock(body, at, op.kind, op.text, id, op.level); return true;
    case 'horizontalRule': return false;
  }
}

/**
 * Suggest replacing a block's text: the last text run is rewritten, any others are delete-marked.
 *
 * A block with NO text (an empty title, an empty paragraph) gets a delete-marked
 * zero-width anchor first, so the suggestion is a replace and not a wholly
 * inserted block: rejecting it must leave the block where it was, and a block
 * whose every run is an insertion is one the reject removes (here in
 * `resolveSuggestion`, and in the editor).
 */
function suggestBlockText(el: YT.XmlElement, text: string, id: SuggestionIdentity): void {
  const texts = textsOf(el);
  if (texts.every((t) => t.length === 0)) suggestDeleteRange(primaryText(el), 0, 0, id);
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
    for (const t of textsOf(el)) {
      (decision === 'accept' ? acceptSuggestions : rejectSuggestions)(t, key);
      // An accepted anchor is content now; it was only ever there to carry the mark.
      if (decision === 'accept' && plainText(t) === CELL_ANCHOR) t.delete(0, t.length);
    }
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
          // Rows given, or the target is a table: the table is rebuilt whole.
          // Writing text INTO a table element was how header cells became
          // "| Name | Owner |" — a table's content is rows, not a string.
          const rows = op.rows ?? (el.nodeName === 'table' ? (rowsFromPipeText(op.text) ?? [[op.text]]) : undefined);
          if (rows) {
            const at = body.toArray().indexOf(el);
            if (at < 0) { result.skipped += 1; break; }
            if (id) { suggestBlockDelete(el, id); suggestNewTable(body, at + 1, rows, id); }
            else { body.delete(at, 1); body.insert(at, [makeTable(rows)]); }
            result.applied += 1;
            break;
          }
          if (id) suggestBlockText(el, op.text, id); else setBlockText(el, op.text);
          result.applied += 1;
          break;
        }
        case 'appendBlock':
          if (id && !suggestNewKind(body, body.length, op, id)) { result.skipped += 1; break; }
          if (!id) body.insert(body.length, [makeBlock(op.kind, op.text, op.level, op.rows)]);
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
            else if (src.kind === 'table') suggestNewTable(body, body.length, normalizeTableRows(src.text.split('\n').map((line) => line.split(' | '))) ?? [['']], id);
            else if (src.kind === 'bulletList' || src.kind === 'orderedList') suggestNewList(body, body.length, src.kind, src.text, id);
            else if (src.kind === 'horizontalRule') { result.skipped += 1; break; }
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
          if (id && !suggestNewKind(body, pos, op, id)) { result.skipped += 1; break; }
          if (!id) body.insert(pos, [makeBlock(op.kind, op.text, op.level, op.rows)]);
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
