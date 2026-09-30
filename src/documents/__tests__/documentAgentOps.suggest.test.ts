// Suggest mode: every operation lands as tracked changes the editor's review
// chrome can see under ONE key, nothing is final until a person resolves it,
// and the accept/reject ops resolve it through the same write path.
import type * as YT from 'yjs' with { 'resolution-mode': 'import' };
// eslint-disable-next-line @typescript-eslint/no-require-imports
const Y = require('yjs') as typeof YT;
import { SUGGESTION_DELETE_MARK, SUGGESTION_INSERT_MARK, ZERO_WIDTH_SPACE, overlappingMarkAttrKey } from 'distributed-core/applications/document';
import {
  applyOps, createSuggestionIdentity, ensureCanvasMeta, outlineOf, parseOps, readSuggestions, suggestionKey,
  CANVAS_BODY_KEY, CANVAS_META_KEY, CANVAS_SCHEMA_VERSION, type DocOp,
} from '../documentAgentOps';

const NOW = Date.UTC(2026, 8, 9, 12, 34, 56);
const grace = createSuggestionIdentity('agent:haiku', 'Haiku', NOW);
const KEY = suggestionKey(grace);

function seed(): YT.Doc {
  const doc = new Y.Doc();
  doc.getMap(CANVAS_META_KEY).set('title', 'Audit decide');
  const body = doc.getXmlFragment(CANVAS_BODY_KEY);
  const h = new Y.XmlElement('heading'); h.setAttribute('level', '1'); const ht = new Y.XmlText(); ht.insert(0, 'Audit decide'); h.insert(0, [ht]);
  const p = new Y.XmlElement('paragraph'); const pt = new Y.XmlText(); pt.insert(0, 'Staffing needs a second reader.'); pt.format(0, 8, { bold: true }); p.insert(0, [pt]);
  const empty = new Y.XmlElement('paragraph'); empty.insert(0, [new Y.XmlText()]);
  body.insert(0, [h, p, empty]);
  return doc;
}

function blocks(doc: YT.Doc): YT.XmlElement[] {
  return doc.getXmlFragment(CANVAS_BODY_KEY).toArray().filter((n): n is YT.XmlElement => n instanceof Y.XmlElement);
}
function texts(el: YT.XmlElement): YT.XmlText[] { return el.toArray().filter((c): c is YT.XmlText => c instanceof Y.XmlText); }
/** Suggestion runs per block. `readSuggestions` splits at ordinary formatting boundaries (bold/plain); adjacent runs of one mark are joined here. */
function runsOf(doc: YT.Doc) {
  const out: Array<{ block: number } & ReturnType<typeof readSuggestions>[number]> = [];
  blocks(doc).forEach((b, i) => texts(b).forEach((t) => readSuggestions(t).forEach((r) => {
    const last = out[out.length - 1];
    if (last && last.block === i && last.key === r.key && last.markName === r.markName) last.text += r.text;
    else out.push({ block: i, ...r });
  })));
  return out;
}
function plain(el: YT.XmlElement): string {
  return texts(el).flatMap((t) => (t.toDelta() as Array<{ insert: unknown }>).map((d) => (typeof d.insert === 'string' ? d.insert : ''))).join('');
}

const suggest = (doc: YT.Doc, ops: DocOp[]) => applyOps(doc, ops, { origin: 'agent', mode: 'suggest', identity: grace });

describe('applyOps in suggest mode', () => {
  it('reports the one key every mark carries, and the identity is bucketed to ten minutes', () => {
    expect(grace.date % (10 * 60 * 1000)).toBe(0);
    expect(KEY).toBe(`agent:haiku@${grace.date}`);
    const r = suggest(seed(), [{ op: 'appendBlock', kind: 'paragraph', text: 'Thanks.' }]);
    expect(r).toEqual({ applied: 1, skipped: 0, suggestionKey: KEY });
  });

  it('replaceBlock: the old text is delete-marked and stays, the new text is insert-marked, existing marks survive', () => {
    const doc = seed();
    suggest(doc, [{ op: 'replaceBlock', index: 1, text: 'Staffing needs two readers.' }]);
    const runs = runsOf(doc);
    expect(runs).toEqual([
      expect.objectContaining({ block: 1, key: KEY, markName: SUGGESTION_DELETE_MARK, text: 'Staffing needs a second reader.', user: 'agent:haiku', username: 'Haiku', date: grace.date }),
      expect.objectContaining({ block: 1, key: KEY, markName: SUGGESTION_INSERT_MARK, text: 'Staffing needs two readers.' }),
    ]);
    // Bold on the first word is still there under the delete mark — the editor's attribute key, byte for byte.
    const delta = texts(blocks(doc)[1]!)[0]!.toDelta() as Array<{ insert: string; attributes?: Record<string, unknown> }>;
    expect(delta[0]).toEqual({ insert: 'Staffing', attributes: { bold: true, [overlappingMarkAttrKey(SUGGESTION_DELETE_MARK, grace)]: { user: 'agent:haiku', username: 'Haiku', date: grace.date } } });
    // The outline the NEXT model call sees still shows both, so nothing is hidden from it.
    expect(outlineOf(doc).blocks[1]!.text).toBe('Staffing needs a second reader.Staffing needs two readers.');
  });

  it('appendBlock / insertBlock / copyBlock / appendMacro: the whole new block reads as one insertion', () => {
    const doc = seed();
    const source = [{ index: 0, kind: 'macro', macroName: 'decision', text: 'title: Ship it\nstatus: decided' }];
    applyOps(doc, [
      { op: 'appendBlock', kind: 'paragraph', text: 'Thanks to the reviewers.' },
      { op: 'insertBlock', index: 1, kind: 'heading', level: 2, text: 'Context' },
      { op: 'copyBlock', index: 0 },
      { op: 'appendMacro', macroName: 'field', yaml: 'label: Owner\ntype: text' },
    ], { mode: 'suggest', identity: grace, sourceBlocks: source });
    const b = blocks(doc);
    expect(b.map((el) => `${el.nodeName}${el.getAttribute('macroName') ? `(${el.getAttribute('macroName')})` : ''}:${plain(el)}`)).toEqual([
      'heading:Audit decide', 'heading:Context', 'paragraph:Staffing needs a second reader.', 'paragraph:',
      'paragraph:Thanks to the reviewers.', 'macro(decision):title: Ship it\nstatus: decided', 'macro(field):label: Owner\ntype: text',
    ]);
    expect(b[1]!.getAttribute('level')).toBe('2');
    const runs = runsOf(doc);
    expect(runs.map((r) => [r.block, r.markName, r.text])).toEqual([
      [1, SUGGESTION_INSERT_MARK, 'Context'],
      [4, SUGGESTION_INSERT_MARK, 'Thanks to the reviewers.'],
      [5, SUGGESTION_INSERT_MARK, 'title: Ship it\nstatus: decided'],
      [6, SUGGESTION_INSERT_MARK, 'label: Owner\ntype: text'],
    ]);
    expect(new Set(runs.map((r) => r.key))).toEqual(new Set([KEY]));
  });

  it('deleteBlock keeps the block, delete-marks all of its text, and anchors an empty block with a ZWS', () => {
    const doc = seed();
    const r = suggest(doc, [{ op: 'deleteBlock', index: 1 }, { op: 'deleteBlock', index: 2 }]);
    expect(r).toEqual({ applied: 2, skipped: 0, suggestionKey: KEY });
    expect(blocks(doc)).toHaveLength(3);
    expect(runsOf(doc).map((x) => [x.block, x.markName, x.text])).toEqual([
      [1, SUGGESTION_DELETE_MARK, 'Staffing needs a second reader.'],
      [2, SUGGESTION_DELETE_MARK, ZERO_WIDTH_SPACE],
    ]);
  });

  it('setTitle: the meta title is written directly (a Y.Map scalar has no suggestion form); the h1 mirror is a suggested replace', () => {
    const doc = seed();
    const r = suggest(doc, [{ op: 'setTitle', text: 'Audit: decide' }]);
    expect(r).toEqual({ applied: 1, skipped: 0, title: 'Audit: decide', suggestionKey: KEY });
    expect(doc.getMap(CANVAS_META_KEY).get('title')).toBe('Audit: decide');
    expect(runsOf(doc).map((x) => [x.block, x.markName, x.text])).toEqual([
      [0, SUGGESTION_DELETE_MARK, 'Audit decide'],
      [0, SUGGESTION_INSERT_MARK, 'Audit: decide'],
    ]);
    // No h1 yet → the new h1 is itself a suggested insertion at the top.
    const bare = new Y.Doc();
    bare.getXmlFragment(CANVAS_BODY_KEY).insert(0, [new Y.XmlElement('paragraph')]);
    suggest(bare, [{ op: 'setTitle', text: 'Fresh' }]);
    expect(blocks(bare)[0]!.nodeName).toBe('heading');
    expect(runsOf(bare).map((x) => [x.block, x.markName, x.text])).toEqual([[0, SUGGESTION_INSERT_MARK, 'Fresh']]);
  });

  it('setDocumentType stays a direct write, and without an identity suggest mode falls back to direct', () => {
    const doc = seed();
    const r = suggest(doc, [{ op: 'setDocumentType', typeName: 'Runbook', typeId: 't1' }]);
    expect(r).toEqual({ applied: 1, skipped: 0, suggestionKey: KEY });
    expect(doc.getMap(CANVAS_META_KEY).get('documentType')).toBe('Runbook');
    expect(runsOf(doc)).toEqual([]);
    const plainDoc = seed();
    const d = applyOps(plainDoc, [{ op: 'appendBlock', kind: 'paragraph', text: 'x' }], { mode: 'suggest' });
    expect(d).toEqual({ applied: 1, skipped: 0 });
    expect(runsOf(plainDoc)).toEqual([]);
  });

  it('the old positional signature still writes directly', () => {
    const doc = seed();
    expect(applyOps(doc, [{ op: 'replaceBlock', index: 1, text: 'Direct.' }], 'agent', [])).toEqual({ applied: 1, skipped: 0 });
    expect(plain(blocks(doc)[1]!)).toBe('Direct.');
    expect(runsOf(doc)).toEqual([]);
  });

  it('lands as ONE update under the origin, like direct mode', () => {
    const doc = seed();
    let updates = 0;
    doc.on('update', (_u: Uint8Array, origin: unknown) => { if (origin === 'agent') updates += 1; });
    suggest(doc, [{ op: 'replaceBlock', index: 1, text: 'a' }, { op: 'appendBlock', kind: 'paragraph', text: 'b' }, { op: 'deleteBlock', index: 0 }]);
    expect(updates).toBe(1);
  });
});

describe('resolving a suggestion through applyOps', () => {
  const proposal: DocOp[] = [
    { op: 'replaceBlock', index: 1, text: 'Staffing needs two readers.' },
    { op: 'appendBlock', kind: 'paragraph', text: 'Thanks to the reviewers.' },
    { op: 'deleteBlock', index: 2 },
  ];

  it('parseOps admits acceptSuggestion / rejectSuggestion with a key', () => {
    expect(parseOps('[{"op":"acceptSuggestion","key":"a@1"},{"op":"rejectSuggestion","key":"b@2"},{"op":"rejectSuggestion"}]'))
      .toEqual([{ op: 'acceptSuggestion', key: 'a@1' }, { op: 'rejectSuggestion', key: 'b@2' }]);
  });

  it('rejectSuggestion restores the text, removes the proposed block, keeps the block that was to be deleted', () => {
    const doc = seed();
    suggest(doc, proposal);
    expect(blocks(doc)).toHaveLength(4);
    const r = applyOps(doc, [{ op: 'rejectSuggestion', key: KEY }], { origin: 'agent' });
    expect(r).toEqual({ applied: 1, skipped: 0 });
    expect(blocks(doc).map(plain)).toEqual(['Audit decide', 'Staffing needs a second reader.', '']);
    expect(runsOf(doc)).toEqual([]);
    // No ZWS anchor survives.
    expect(blocks(doc).some((b) => plain(b).includes(ZERO_WIDTH_SPACE))).toBe(false);
    // Bold came through untouched.
    expect((texts(blocks(doc)[1]!)[0]!.toDelta() as Array<{ insert: string; attributes?: unknown }>)[0]).toEqual({ insert: 'Staffing', attributes: { bold: true } });
  });

  it('acceptSuggestion commits the text, keeps the proposed block, removes the block that was to be deleted', () => {
    const doc = seed();
    suggest(doc, proposal);
    const r = applyOps(doc, [{ op: 'acceptSuggestion', key: KEY }], { origin: 'agent' });
    expect(r).toEqual({ applied: 1, skipped: 0 });
    expect(blocks(doc).map(plain)).toEqual(['Audit decide', 'Staffing needs two readers.', 'Thanks to the reviewers.']);
    expect(runsOf(doc)).toEqual([]);
  });

  it('only the named key is resolved; another author\'s suggestions stay, and an unknown key is a skip', () => {
    const doc = seed();
    suggest(doc, [{ op: 'appendBlock', kind: 'paragraph', text: 'Haiku says hi.' }]);
    const bob = createSuggestionIdentity('dev-bob', 'Bob', NOW);
    applyOps(doc, [{ op: 'appendBlock', kind: 'paragraph', text: 'Bob says hi.' }], { mode: 'suggest', identity: bob });
    expect(applyOps(doc, [{ op: 'rejectSuggestion', key: 'nobody@0' }], { origin: 'agent' })).toEqual({ applied: 0, skipped: 1 });
    applyOps(doc, [{ op: 'rejectSuggestion', key: KEY }], { origin: 'agent' });
    expect(blocks(doc).map(plain)).toEqual(['Audit decide', 'Staffing needs a second reader.', '', 'Bob says hi.']);
    expect(runsOf(doc).map((x) => x.key)).toEqual([suggestionKey(bob)]);
  });
});

describe('ensureCanvasMeta', () => {
  it('seeds a section-shaped document with the client\'s canvas metadata, once', () => {
    const doc = new Y.Doc();
    const meta = doc.getMap<unknown>(CANVAS_META_KEY);
    expect(ensureCanvasMeta(doc, { id: 'doc-1', title: 'Smoke', createdBy: 'dev-grace', type: 'Runbook', createdAt: '2026-09-09T00:00:00.000Z' })).toBe(true);
    expect(meta.toJSON()).toEqual({
      id: 'doc-1', title: 'Smoke', sourceType: 'notes', sourceId: '', createdBy: 'dev-grace', createdAt: '2026-09-09T00:00:00.000Z',
      aiModel: '', status: 'draft', documentType: 'Runbook', templateApplied: true, schemaVersion: CANVAS_SCHEMA_VERSION,
    });
    // Idempotent: a second call changes nothing and says so.
    expect(ensureCanvasMeta(doc, { id: 'other', title: 'Other', createdBy: 'x' })).toBe(false);
    expect(meta.get('id')).toBe('doc-1');
  });

  it('never overwrites an existing schemaVersion or an existing title', () => {
    const doc = new Y.Doc();
    const meta = doc.getMap<unknown>(CANVAS_META_KEY);
    meta.set('schemaVersion', 1);
    expect(ensureCanvasMeta(doc, { id: 'd', title: 't', createdBy: 'u' })).toBe(false);
    expect(meta.toJSON()).toEqual({ schemaVersion: 1 });
    const titled = seed();
    ensureCanvasMeta(titled, { id: 'd', title: 'Seed title', createdBy: 'u' });
    expect(titled.getMap(CANVAS_META_KEY).get('title')).toBe('Audit decide');
    expect(titled.getMap(CANVAS_META_KEY).get('schemaVersion')).toBe(CANVAS_SCHEMA_VERSION);
  });
});

describe('a table as a suggestion', () => {
  function deepTexts(el: YT.XmlElement): YT.XmlText[] {
    const out: YT.XmlText[] = [];
    for (const c of el.toArray()) {
      if (c instanceof Y.XmlText) out.push(c);
      else if (c instanceof Y.XmlElement) out.push(...deepTexts(c));
    }
    return out;
  }

  it('every cell — an empty one through a zero-width anchor — is insert-marked under the one key', () => {
    const doc = seed();
    const ops = parseOps('[{"op":"appendBlock","kind":"table","rows":[["Column 1","Column 2"],["",""]]}]');
    const result = applyOps(doc, ops, { mode: 'suggest', identity: grace });
    expect(result).toMatchObject({ applied: 1, suggestionKey: KEY });
    const table = blocks(doc)[blocks(doc).length - 1]!;
    expect(table.nodeName).toBe('table');
    const cellTexts = deepTexts(table);
    expect(cellTexts).toHaveLength(4);
    for (const t of cellTexts) {
      const runs = readSuggestions(t);
      expect(runs).toHaveLength(1);
      expect(runs[0]).toMatchObject({ key: KEY, markName: SUGGESTION_INSERT_MARK });
    }
    expect(cellTexts[2]!.toDelta().map((d: { insert?: unknown }) => d.insert).join('')).toBe(ZERO_WIDTH_SPACE);
  });

  it('accepting keeps the table and strips the anchors; rejecting removes the table', () => {
    const accepted = seed();
    applyOps(accepted, parseOps('[{"op":"appendBlock","kind":"table","rows":[["H"],[""]]}]'), { mode: 'suggest', identity: grace });
    applyOps(accepted, [{ op: 'acceptSuggestion', key: KEY }], 'test');
    const table = blocks(accepted)[blocks(accepted).length - 1]!;
    expect(table.nodeName).toBe('table');
    expect(deepTexts(table).map((t) => t.toDelta().map((d: { insert?: unknown }) => d.insert).join(''))).toEqual(['H', '']);
    expect(deepTexts(table).flatMap((t) => readSuggestions(t))).toHaveLength(0);

    const rejected = seed();
    const before = blocks(rejected).length;
    applyOps(rejected, parseOps('[{"op":"appendBlock","kind":"table","rows":[["H"],[""]]}]'), { mode: 'suggest', identity: grace });
    expect(blocks(rejected)).toHaveLength(before + 1);
    applyOps(rejected, [{ op: 'rejectSuggestion', key: KEY }], 'test');
    expect(blocks(rejected)).toHaveLength(before);
    expect(blocks(rejected).some((b) => b.nodeName === 'table')).toBe(false);
  });
});

// The live bug: a DM `/doc make sure to add your basic recipe style` against an
// EMPTY page. The run's plan (captured from run c844cc8e) wrote a title, a
// divider, two headings and two bullet lists; the lists and the divider went in
// bare, so Reject took the headings back and left every bullet in the page.
describe('an empty page: everything a run writes is taken back by Reject', () => {
  const recipeRun = parseOps(JSON.stringify([
    { op: 'setTitle', text: 'Basic Recipe Style Guide' },
    { op: 'insertBlock', index: 1, kind: 'horizontalRule', text: '' },
    { op: 'appendBlock', kind: 'heading', level: 2, text: 'Recipe Format' },
    { op: 'appendBlock', kind: 'bulletList', text: 'Ingredients (with quantities and units)\nInstructions (numbered steps)' },
    { op: 'appendBlock', kind: 'heading', level: 2, text: 'Writing Style' },
    { op: 'appendBlock', kind: 'orderedList', text: 'Use imperative voice\nNumber steps clearly' },
  ]));
  const empty = () => { const d = new Y.Doc(); d.getXmlFragment(CANVAS_BODY_KEY); return d; };
  const allTexts = (el: YT.XmlElement): YT.XmlText[] => el.toArray().flatMap((c) => (c instanceof Y.XmlText ? [c] : c instanceof Y.XmlElement ? allTexts(c) : []));
  const bareText = (doc: YT.Doc) => blocks(doc).flatMap(allTexts).flatMap((t) => (t.toDelta() as Array<{ insert: unknown; attributes?: Record<string, unknown> }>)
    .filter((d) => typeof d.insert === 'string' && !Object.keys(d.attributes ?? {}).some((k) => k.startsWith(SUGGESTION_INSERT_MARK)))
    .map((d) => d.insert as string));

  it('lists are written as marked insertions, every item, and a divider is not written at all', () => {
    const doc = empty();
    const r = suggest(doc, recipeRun);
    expect(r).toEqual({ applied: 5, skipped: 1, title: 'Basic Recipe Style Guide', suggestionKey: KEY });
    expect(blocks(doc).map((b) => b.nodeName)).toEqual(['heading', 'heading', 'bulletList', 'heading', 'orderedList']);
    // Nothing in the body is unmarked text.
    expect(bareText(doc)).toEqual([]);
    const list = blocks(doc)[2]!;
    expect(list.toArray().map((li) => (li as YT.XmlElement).nodeName)).toEqual(['listItem', 'listItem']);
    expect(allTexts(list).flatMap((t) => readSuggestions(t)).map((x) => [x.markName, x.text])).toEqual([
      [SUGGESTION_INSERT_MARK, 'Ingredients (with quantities and units)'],
      [SUGGESTION_INSERT_MARK, 'Instructions (numbered steps)'],
    ]);
  });

  it('rejectSuggestion leaves the page as empty as it was', () => {
    const doc = empty();
    suggest(doc, recipeRun);
    applyOps(doc, [{ op: 'rejectSuggestion', key: KEY }], { origin: 'agent' });
    expect(blocks(doc)).toEqual([]);
  });

  it('acceptSuggestion keeps it all, unmarked', () => {
    const doc = empty();
    suggest(doc, recipeRun);
    applyOps(doc, [{ op: 'acceptSuggestion', key: KEY }], { origin: 'agent' });
    expect(blocks(doc).map((b) => [b.nodeName, allTexts(b).map((t) => t.toString().replace(/<[^>]+>/g, '')).join('|')])).toEqual([
      ['heading', 'Basic Recipe Style Guide'],
      ['heading', 'Recipe Format'],
      ['bulletList', 'Ingredients (with quantities and units)|Instructions (numbered steps)'],
      ['heading', 'Writing Style'],
      ['orderedList', 'Use imperative voice|Number steps clearly'],
    ]);
    expect(blocks(doc).flatMap(allTexts).flatMap((t) => readSuggestions(t))).toEqual([]);
  });

  it('a replace into an EMPTY block is a replace: Reject keeps the block, Accept fills it, no anchor survives', () => {
    const doc = seed(); // block 2 is an empty paragraph
    suggest(doc, [{ op: 'replaceBlock', index: 2, text: 'Filled in.' }]);
    expect(runsOf(doc).map((x) => [x.block, x.markName, x.text])).toEqual([
      [2, SUGGESTION_DELETE_MARK, ZERO_WIDTH_SPACE],
      [2, SUGGESTION_INSERT_MARK, 'Filled in.'],
    ]);
    const rejected = Y.encodeStateAsUpdate(doc);
    applyOps(doc, [{ op: 'rejectSuggestion', key: KEY }], { origin: 'agent' });
    expect(blocks(doc).map(plain)).toEqual(['Audit decide', 'Staffing needs a second reader.', '']);
    const again = new Y.Doc(); Y.applyUpdate(again, rejected);
    applyOps(again, [{ op: 'acceptSuggestion', key: KEY }], { origin: 'agent' });
    expect(blocks(again).map(plain)).toEqual(['Audit decide', 'Staffing needs a second reader.', 'Filled in.']);
  });

  it('copyBlock of a list is a marked list; of a divider, a skip', () => {
    const doc = empty();
    const r = applyOps(doc, [{ op: 'copyBlock', index: 0 }, { op: 'copyBlock', index: 1 }], {
      mode: 'suggest', identity: grace,
      sourceBlocks: [{ index: 0, kind: 'bulletList', text: 'a\nb' }, { index: 1, kind: 'horizontalRule', text: '' }],
    });
    expect(r).toMatchObject({ applied: 1, skipped: 1 });
    expect(blocks(doc).map((b) => b.nodeName)).toEqual(['bulletList']);
    expect(bareText(doc)).toEqual([]);
    applyOps(doc, [{ op: 'rejectSuggestion', key: KEY }], { origin: 'agent' });
    expect(blocks(doc)).toEqual([]);
  });
});
