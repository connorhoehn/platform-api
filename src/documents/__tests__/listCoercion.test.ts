// Lists the agent writes, and lists it writes badly.
//
// `BlockKind` had no list at all, so `/doc add a short Risks section with two
// bullets` produced ONE paragraph containing literal • characters — a run-on
// line, not a list. Two halves to the fix: the kinds exist now, and a
// paragraph that is plainly a list is coerced into one, the same judgement
// `rowsFromPipeText` already makes about pipes.

import * as Y from 'yjs';
import { parseOps, applyOps, outlineOf, listFromMarkerText, listItemsOf, CANVAS_BODY_KEY } from '../documentAgentOps';

function blocks(doc: Y.Doc): Y.XmlElement[] {
  return doc.getXmlFragment(CANVAS_BODY_KEY).toArray().filter((n): n is Y.XmlElement => n instanceof Y.XmlElement);
}

describe('listFromMarkerText', () => {
  it('reads a bulleted paragraph as a bullet list', () => {
    expect(listFromMarkerText('• one\n• two')).toEqual({ kind: 'bulletList', text: 'one\ntwo' });
  });

  it('handles the markers a model actually reaches for', () => {
    expect(listFromMarkerText('- one\n- two')?.kind).toBe('bulletList');
    expect(listFromMarkerText('* one\n* two')?.kind).toBe('bulletList');
    expect(listFromMarkerText('‣ one\n▪ two')?.kind).toBe('bulletList');
  });

  it('keeps an enumerated list enumerated', () => {
    expect(listFromMarkerText('1. first\n2. second')).toEqual({ kind: 'orderedList', text: 'first\nsecond' });
    expect(listFromMarkerText('1) first\n2) second')?.kind).toBe('orderedList');
  });

  it('is a bullet list when the markers are mixed', () => {
    // Only an all-numbered run is ordered; one stray number does not make a
    // numbered list out of prose bullets.
    expect(listFromMarkerText('1. first\n- second')?.kind).toBe('bulletList');
  });

  it('leaves a single dashed line alone — that is a sentence, not a list', () => {
    expect(listFromMarkerText('- just the one thing')).toBeUndefined();
  });

  it('leaves ordinary prose alone', () => {
    expect(listFromMarkerText('We will ship the rail changes first.\nThen the call surface.')).toBeUndefined();
  });
});

describe('list blocks', () => {
  it('builds real list items, not one run-on paragraph', () => {
    const doc = new Y.Doc();
    applyOps(doc, parseOps('[{"op":"appendBlock","kind":"bulletList","text":"one\\ntwo"}]'));
    const [list] = blocks(doc);
    expect(list!.nodeName).toBe('bulletList');
    expect(list!.length).toBe(2);
    expect(listItemsOf(list!)).toEqual(['one', 'two']);
    // listItem > paragraph > text, which is what the editor's schema reads.
    const first = list!.toArray()[0] as Y.XmlElement;
    expect(first.nodeName).toBe('listItem');
    expect((first.toArray()[0] as Y.XmlElement).nodeName).toBe('paragraph');
  });

  it('coerces the paragraph-with-bullets the model actually sends', () => {
    const doc = new Y.Doc();
    applyOps(doc, parseOps('[{"op":"appendBlock","kind":"paragraph","text":"• Migration ownership unclear\\n• Freeze timing unknown"}]'));
    const [list] = blocks(doc);
    expect(list!.nodeName).toBe('bulletList');
    // The marker itself is gone — it is the list that draws the bullet now.
    expect(listItemsOf(list!)).toEqual(['Migration ownership unclear', 'Freeze timing unknown']);
  });

  it('strips a marker the model typed into a real list block too', () => {
    const doc = new Y.Doc();
    applyOps(doc, parseOps('[{"op":"appendBlock","kind":"bulletList","text":"- one\\n- two"}]'));
    expect(listItemsOf(blocks(doc)[0]!)).toEqual(['one', 'two']);
  });

  it('shows the model one item per line, so a copy round-trips', () => {
    const doc = new Y.Doc();
    applyOps(doc, parseOps('[{"op":"appendBlock","kind":"bulletList","text":"one\\ntwo"}]'));
    const outline = outlineOf(doc);
    expect(outline.blocks[0]).toMatchObject({ kind: 'bulletList', text: 'one\ntwo' });
  });

  it('does not mistake a pipe table for a list', () => {
    const doc = new Y.Doc();
    applyOps(doc, parseOps('[{"op":"appendBlock","kind":"paragraph","text":"| Name | Owner |\\n| - | - |\\n| Rail | Ada |"}]'));
    expect(blocks(doc)[0]!.nodeName).toBe('table');
  });
});
