// NFR #213 — every agent run's suggestions carry the run, so a verdict on one
// run's key resolves that run's marks and nothing else on the page.
import type * as YT from 'yjs' with { 'resolution-mode': 'import' };
// eslint-disable-next-line @typescript-eslint/no-require-imports
const Y = require('yjs') as typeof YT;
import {
  applyChangeIndex, applyOps, createSuggestionIdentity, readSuggestions, runSuggestionIdentity, suggestionKey, suggestionKeyRunId,
  CANVAS_BODY_KEY, CANVAS_META_KEY, type DocOp,
} from '../documentAgentOps';

describe('run suggestion identity', () => {
  it('derives the key from the run id (and the apply arm), unbucketed', () => {
    const a = runSuggestionIdentity('agent:haiku', 'Haiku', 'run-a', { nowMs: 1790232612345 });
    expect(suggestionKey(a)).toBe('agent:haiku~run-a@1790232612345');
    const arm = runSuggestionIdentity('agent:haiku', 'Haiku', 'run-a', { change: 2, nowMs: 1790232612345 });
    expect(suggestionKey(arm)).toBe('agent:haiku~run-a.2@1790232612345');
    expect(suggestionKey(runSuggestionIdentity('agent:haiku', 'Haiku', 'run-a', { change: 1, nowMs: 5 }))).toBe('agent:haiku~run-a@5');
  });

  it('parses the run back; a legacy bucket key proves no run', () => {
    expect(suggestionKeyRunId('agent:haiku~0f3c9a1e-77aa-4c1b-9d8e-1234567890ab@1790232612345')).toBe('0f3c9a1e-77aa-4c1b-9d8e-1234567890ab');
    expect(suggestionKeyRunId('agent:haiku~run-a.3@1')).toBe('run-a');
    expect(suggestionKeyRunId('agent:haiku@1790232600000')).toBeNull();
    expect(suggestionKeyRunId('')).toBeNull();
    expect(suggestionKeyRunId(undefined)).toBeNull();
  });

  it('reads the arm index off a step id', () => {
    expect(applyChangeIndex('apply')).toBeUndefined();
    expect(applyChangeIndex('apply:2')).toBe(2);
  });
});

describe('two runs in the same minute on one page', () => {
  function seed(): YT.Doc {
    const doc = new Y.Doc();
    doc.getMap(CANVAS_META_KEY).set('title', 'Audit');
    const body = doc.getXmlFragment(CANVAS_BODY_KEY);
    const para = (text: string) => { const p = new Y.XmlElement('paragraph'); const t = new Y.XmlText(); t.insert(0, text); p.insert(0, [t]); return p; };
    body.insert(0, [para('Alpha paragraph.'), para('Beta paragraph.')]);
    return doc;
  }
  const texts = (doc: YT.Doc) => doc.getXmlFragment(CANVAS_BODY_KEY).toArray()
    .flatMap((b) => (b as YT.XmlElement).toArray().filter((c): c is YT.XmlText => c instanceof Y.XmlText));
  const keysOn = (doc: YT.Doc) => new Set(texts(doc).flatMap((t) => readSuggestions(t).map((r) => r.key)));
  const body = (doc: YT.Doc) => texts(doc).map((t) => (t.toDelta() as Array<{ insert: unknown }>).map((d) => (typeof d.insert === 'string' ? d.insert : '')).join('')).join('\n');

  it('accepting run A leaves run B\'s marks; rejecting run B leaves A\'s accepted text', () => {
    const doc = seed();
    const now = 1790232612345;
    const a = applyOps(doc, [{ op: 'replaceBlock', index: 0, text: 'Alpha, revised by A.' }], {
      origin: 'agent', mode: 'suggest', identity: runSuggestionIdentity('agent:haiku', 'Haiku', 'run-a', { nowMs: now }),
    });
    const b = applyOps(doc, [{ op: 'replaceBlock', index: 1, text: 'Beta, revised by B.' }], {
      origin: 'agent', mode: 'suggest', identity: runSuggestionIdentity('agent:haiku', 'Haiku', 'run-b', { nowMs: now + 20_000 }),
    });
    expect(a.suggestionKey).not.toBe(b.suggestionKey);
    expect(keysOn(doc)).toEqual(new Set([a.suggestionKey, b.suggestionKey]));

    applyOps(doc, [{ op: 'acceptSuggestion', key: a.suggestionKey! }] as DocOp[], 'review');
    expect(keysOn(doc)).toEqual(new Set([b.suggestionKey]));

    applyOps(doc, [{ op: 'rejectSuggestion', key: b.suggestionKey! }] as DocOp[], 'review');
    expect(keysOn(doc).size).toBe(0);
    expect(body(doc)).toContain('Alpha, revised by A.');
    expect(body(doc)).not.toContain('Alpha paragraph.');
    expect(body(doc)).toContain('Beta paragraph.');
    expect(body(doc)).not.toContain('revised by B');
  });

  it('the shared ten-minute bucket (legacy) is why it leaked: same key for both runs', () => {
    const a = createSuggestionIdentity('agent:haiku', 'Haiku', 1790232612345);
    const b = createSuggestionIdentity('agent:haiku', 'Haiku', 1790232632345);
    expect(suggestionKey(a)).toBe(suggestionKey(b));
    expect(suggestionKeyRunId(suggestionKey(a))).toBeNull();
  });
});
