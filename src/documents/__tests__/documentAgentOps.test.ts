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
