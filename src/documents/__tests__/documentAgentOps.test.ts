import type * as YT from 'yjs' with { 'resolution-mode': 'import' };
// yjs ships ESM types but a CJS build; under `module: node16` the value has to
// come in through require while the types come in as type-only.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const Y = require('yjs') as typeof YT;
import { applyOps, outlineOf, parseOps, CANVAS_BODY_KEY, CANVAS_META_KEY } from '../documentAgentOps';

function seed(): YT.Doc {
  const doc = new Y.Doc();
  doc.getMap(CANVAS_META_KEY).set('title', 'Audit decide');
  const body = doc.getXmlFragment(CANVAS_BODY_KEY);
  const h = new Y.XmlElement('heading'); h.setAttribute('level', '1'); const ht = new Y.XmlText(); ht.insert(0, 'Audit decide'); h.insert(0, [ht]);
  const p = new Y.XmlElement('paragraph'); const pt = new Y.XmlText(); pt.insert(0, 'Staffing needs a second reader.'); pt.format(0, 8, { bold: true }); p.insert(0, [pt]);
  body.insert(0, [h, p]);
  return doc;
}

describe('the outline a light model is shown', () => {
  it('numbers blocks, names their kind and level, and strips marks to plain text', () => {
    const o = outlineOf(seed());
    expect(o.title).toBe('Audit decide');
    expect(o.text).toBe('title: Audit decide\n#0 heading(h1): Audit decide\n#1 paragraph: Staffing needs a second reader.');
  });
});

describe('parsing what the model said', () => {
  it('finds the array through fences and prose, drops unknown or malformed ops', () => {
    const ops = parseOps('Sure! ```json\n[{"op":"setTitle","text":"Audit: decide"},{"op":"nope"},{"op":"replaceBlock","index":"1","text":"x"},{"op":"deleteBlock","index":1}]\n```');
    expect(ops).toEqual([{ op: 'setTitle', text: 'Audit: decide' }, { op: 'deleteBlock', index: 1 }]);
    expect(parseOps('I cannot help with that.')).toEqual([]);
  });
});

describe('applying operations', () => {
  it('renames through meta AND the first h1, edits by index, and lands as one update', () => {
    const doc = seed();
    let updates = 0;
    doc.on('update', (_u: Uint8Array, origin: unknown) => { if (origin === 'agent') updates += 1; });
    const r = applyOps(doc, [
      { op: 'setTitle', text: 'Audit: decide' },
      { op: 'replaceBlock', index: 1, text: 'Staffing needs two readers.' },
      { op: 'appendBlock', kind: 'heading', level: 2, text: 'Next steps' },
      { op: 'deleteBlock', index: 99 },
    ], 'agent');
    expect(r).toEqual({ applied: 3, skipped: 1, title: 'Audit: decide' });
    expect(updates).toBe(1);
    const o = outlineOf(doc);
    expect(o.title).toBe('Audit: decide');
    expect(o.blocks.map((b) => b.text)).toEqual(['Audit: decide', 'Staffing needs two readers.', 'Next steps']);
    expect(o.blocks[2]).toMatchObject({ kind: 'heading', level: 2 });
  });

  it('adds a title heading when the document has none', () => {
    const doc = new Y.Doc();
    applyOps(doc, [{ op: 'setTitle', text: 'Fresh' }]);
    expect(outlineOf(doc).blocks[0]).toMatchObject({ kind: 'heading', level: 1, text: 'Fresh' });
  });
});

describe('transform operations', () => {
  it('copies a source macro verbatim, stamps the type, and appends a new typed field', () => {
    const source = new Y.Doc();
    const body = source.getXmlFragment(CANVAS_BODY_KEY);
    const m = new Y.XmlElement('macro'); m.setAttribute('macroName', 'field'); const mt = new Y.XmlText(); mt.insert(0, 'label: Severity\ntype: text'); m.insert(0, [mt]);
    body.insert(0, [m]);
    const src = outlineOf(source);
    expect(src.blocks[0]).toMatchObject({ kind: 'macro', macroName: 'field', text: 'label: Severity\ntype: text' });
    expect(src.text).toContain('#0 macro(field): label: Severity');

    const target = new Y.Doc();
    const r = applyOps(target, [
      { op: 'setDocumentType', typeName: 'Incident', typeId: 't-inc', icon: '🚨' },
      { op: 'setTitle', text: 'Q3 incident' },
      { op: 'appendBlock', kind: 'heading', level: 2, text: 'Severity' },
      { op: 'copyBlock', index: 0 },
      { op: 'appendMacro', macroName: 'field', yaml: 'label: Owner\ntype: text' },
      { op: 'copyBlock', index: 9 },
    ], 'agent', src.blocks);
    expect(r).toMatchObject({ applied: 5, skipped: 1, title: 'Q3 incident' });
    const meta = target.getMap(CANVAS_META_KEY);
    expect(meta.get('documentType')).toBe('Incident');
    expect(meta.get('documentTypeId')).toBe('t-inc');
    const out = outlineOf(target);
    expect(out.blocks.map((b) => `${b.kind}:${b.macroName ?? ''}:${b.text}`)).toEqual([
      'heading::Q3 incident', 'heading::Severity', 'macro:field:label: Severity\ntype: text', 'macro:field:label: Owner\ntype: text',
    ]);
  });
});

describe('horizontal rules', () => {
  it('parses and writes a divider with no text, after the title when asked', async () => {
    const { parseOps, applyOps, outlineOf } = await import('../documentAgentOps.js');
    const doc = new Y.Doc();
    const body = doc.getXmlFragment('body');
    const h = new Y.XmlElement('heading'); h.setAttribute('level', '1'); const t = new Y.XmlText(); t.insert(0, 'Title'); h.insert(0, [t]);
    const p = new Y.XmlElement('paragraph'); const pt = new Y.XmlText(); pt.insert(0, 'Body'); p.insert(0, [pt]);
    body.insert(0, [h, p]);
    const ops = parseOps('[{"op":"insertBlock","index":1,"kind":"horizontalRule","text":""},{"op":"appendBlock","kind":"horizontalRule"}]');
    expect(ops).toHaveLength(2);
    const result = applyOps(doc, ops, 'test');
    expect(result.applied).toBe(2);
    const kinds = outlineOf(doc).blocks.map((b) => b.kind);
    expect(kinds).toEqual(['heading', 'horizontalRule', 'paragraph', 'horizontalRule']);
  });
});

describe('tables', () => {
  it('parses rows, caps and squares them, and drops a table with no rows', () => {
    const ops = parseOps('[{"op":"appendBlock","kind":"table","rows":[["Column 1","Column 2"],["only one"]]},{"op":"appendBlock","kind":"table","rows":[]},{"op":"appendBlock","kind":"table"}]');
    expect(ops).toHaveLength(1);
    expect(ops[0]).toEqual({ op: 'appendBlock', kind: 'table', text: '', rows: [['Column 1', 'Column 2'], ['only one', '']] });
    const wide = parseOps(`[{"op":"appendBlock","kind":"table","rows":${JSON.stringify(Array.from({ length: 30 }, () => Array.from({ length: 12 }, () => 'x')))}}]`);
    const rows = (wide[0] as { rows: string[][] }).rows;
    expect(rows).toHaveLength(20);
    expect(rows[0]).toHaveLength(8);
  });

  it('writes the shape the editor\'s table extension reads: table → tableRow → tableHeader|tableCell → paragraph → text', () => {
    const doc = seed();
    const result = applyOps(doc, parseOps('[{"op":"appendBlock","kind":"table","rows":[["Name","Owner"],["Audit","Carol"]]}]'), 'test');
    expect(result.applied).toBe(1);
    const body = doc.getXmlFragment(CANVAS_BODY_KEY);
    const table = body.get(body.length - 1) as YT.XmlElement;
    expect(table.nodeName).toBe('table');
    const rows = table.toArray() as YT.XmlElement[];
    expect(rows.map((r) => r.nodeName)).toEqual(['tableRow', 'tableRow']);
    expect((rows[0]!.toArray() as YT.XmlElement[]).map((c) => c.nodeName)).toEqual(['tableHeader', 'tableHeader']);
    expect((rows[1]!.toArray() as YT.XmlElement[]).map((c) => c.nodeName)).toEqual(['tableCell', 'tableCell']);
    const cell = (rows[1]!.toArray() as YT.XmlElement[])[1]!;
    expect((cell.toArray()[0] as YT.XmlElement).nodeName).toBe('paragraph');
    expect(((cell.toArray()[0] as YT.XmlElement).toArray()[0] as YT.XmlText).toString()).toBe('Carol');
    // The outline shows the model the rows, and a copy rebuilds them.
    const outline = outlineOf(doc);
    expect(outline.blocks[outline.blocks.length - 1]).toMatchObject({ kind: 'table', text: 'Name | Owner\nAudit | Carol' });
  });

  it('inserts a table before a block by index', () => {
    const doc = seed();
    applyOps(doc, parseOps('[{"op":"insertBlock","index":1,"kind":"table","rows":[["A"]]}]'), 'test');
    expect(outlineOf(doc).blocks.map((b) => b.kind)).toEqual(['heading', 'table', 'paragraph']);
  });
});

// Tiptap renders `h${levels.includes(level) ? level : levels[0]}`: a string
// "2" in the Y.Doc fails includes() and every agent heading showed as h1.
describe('heading levels are written as numbers', () => {
  const headings = (doc: YT.Doc) => doc.getXmlFragment(CANVAS_BODY_KEY).toArray()
    .filter((n): n is YT.XmlElement => n instanceof Y.XmlElement && n.nodeName === 'heading');

  it('appendBlock / insertBlock write level 2 and 3 as numbers', () => {
    const doc = seed();
    applyOps(doc, [
      { op: 'appendBlock', kind: 'heading', level: 2, text: 'Two' },
      { op: 'insertBlock', index: 1, kind: 'heading', level: 3, text: 'Three' },
    ]);
    const [title, three, two] = headings(doc);
    expect(title!.getAttribute('level')).toBe('1'); // the seed's own title, untouched
    expect(three!.getAttribute('level')).toBe(3);
    expect(two!.getAttribute('level')).toBe(2);
    expect(outlineOf(doc).blocks.filter((b) => b.kind === 'heading').map((b) => b.level)).toEqual([1, 3, 2]);
  });

  it('a missing level is 2 (the h1 is the title); setTitle on an empty page writes 1', () => {
    const doc = new Y.Doc();
    applyOps(doc, [{ op: 'setTitle', text: 'T' }, { op: 'appendBlock', kind: 'heading', text: 'Body' }]);
    expect(headings(doc).map((h) => h.getAttribute('level'))).toEqual([1, 2]);
  });

  it('parseOps reads a string level from the model ("2", "h3") and drops nonsense', () => {
    const ops = parseOps(JSON.stringify({ ops: [
      { op: 'appendBlock', kind: 'heading', level: '2', text: 'a' },
      { op: 'appendBlock', kind: 'heading', level: 'h3', text: 'b' },
      { op: 'appendBlock', kind: 'heading', level: 9, text: 'c' },
    ] }));
    expect(ops.map((o) => (o as { level?: number }).level)).toEqual([2, 3, undefined]);
  });
});
