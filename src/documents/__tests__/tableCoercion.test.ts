import * as Y from 'yjs';
import { applyOps, parseOps, rowsFromPipeText, tableRowsOf, CANVAS_BODY_KEY } from '../documentAgentOps';

describe('rowsFromPipeText', () => {
  it('reads a Markdown table, dropping the separator row', () => {
    expect(rowsFromPipeText('| Column 1 | Column 2 |\n|----------|----------|\n| | |')).toEqual([['Column 1', 'Column 2'], ['', '']]);
  });
  it("reads the outline's own echo", () => {
    expect(rowsFromPipeText('Name | Owner\nData access | Sarah Chen')).toEqual([['Name', 'Owner'], ['Data access', 'Sarah Chen']]);
  });
  it('leaves prose alone', () => {
    expect(rowsFromPipeText('Please review by Friday.')).toBeUndefined();
    expect(rowsFromPipeText('a | b\nplain line')).toBeUndefined();
  });
});

describe('parseOps coerces pipe tables', () => {
  it('turns an appended pipe paragraph into a table block', () => {
    const ops = parseOps(JSON.stringify([{ op: 'appendBlock', kind: 'paragraph', text: '| Column 1 | Column 2 |\n|---|---|\n| a | b |' }]));
    expect(ops).toEqual([{ op: 'appendBlock', kind: 'table', text: '', rows: [['Column 1', 'Column 2'], ['a', 'b']] }]);
  });
  it('keeps rows on a replaceBlock', () => {
    const ops = parseOps(JSON.stringify([{ op: 'replaceBlock', index: 1, rows: [['Name', 'Owner'], ['x', 'y']] }]));
    expect(ops).toEqual([{ op: 'replaceBlock', index: 1, text: '', rows: [['Name', 'Owner'], ['x', 'y']] }]);
  });
});

describe('replaceBlock on a table rebuilds the rows', () => {
  it('direct mode', () => {
    const doc = new Y.Doc();
    const body = doc.getXmlFragment(CANVAS_BODY_KEY);
    const first = parseOps(JSON.stringify([{ op: 'appendBlock', kind: 'table', rows: [['Name', 'Owner'], ['', '']] }]));
    applyOps(doc, first);
    const second = parseOps(JSON.stringify([{ op: 'replaceBlock', index: 0, text: '| Name | Owner |\n|---|---|\n| Data access | Sarah Chen |' }]));
    const res = applyOps(doc, second);
    expect(res.applied).toBe(1);
    const table = body.toArray().find((n) => n instanceof Y.XmlElement && n.nodeName === 'table') as Y.XmlElement;
    expect(tableRowsOf(table)).toEqual([['Name', 'Owner'], ['Data access', 'Sarah Chen']]);
  });
});
