import { completeStream, extractMarkdown, liftMacros, looksLikeDocument, restoreMacros, transformPrompts } from '../documentTransform';
import type { LLMClient, LLMChunk } from 'distributed-core/applications/pipeline';

describe('document transform prompts', () => {
  it('lists the target fields in order and forbids touching macro fences', () => {
    const { system, user } = transformPrompts({
      markdown: '# Standup\n\n```macro:action-items\nitems: []\n```',
      sourceType: 'Meeting Notes',
      target: { name: 'Bug Report', fields: [{ name: 'Severity', sectionType: 'text' }, { name: 'Steps' }] },
    });
    expect(user).toContain('- Severity (text)\n- Steps');
    expect(user).toContain('Target type: Bug Report');
    // The fence itself never reaches the model in the real path; see liftMacros.
    expect(system).toMatch(/\[\[MACRO_n\]\]/);
  });
});

describe('extractMarkdown', () => {
  it('unwraps a whole-document fence and leaves inner macro fences alone', () => {
    const inner = '# T\n\n```macro:decision\nid: d1\ndecision: yes\n```';
    expect(extractMarkdown('```markdown\n' + inner + '\n```')).toBe(inner);
    expect(extractMarkdown(inner)).toBe(inner);
  });
});

describe('completeStream', () => {
  it('prefers the done chunk\'s full response, falling back to the tokens', async () => {
    const chunks = (list: LLMChunk[]): LLMClient => ({
      provider: 'fake',
      stream: () => ({ [Symbol.asyncIterator]: async function* () { for (const c of list) yield c; } }),
    });
    expect(await completeStream(chunks([{ done: false, token: 'a' }, { done: true, response: 'FULL', tokensIn: 1, tokensOut: 1 }]), 'm', 's', 'u')).toBe('FULL');
    expect(await completeStream(chunks([{ done: false, token: 'a' }, { done: false, token: 'b' }]), 'm', 's', 'u')).toBe('ab');
  });
});

describe('looksLikeDocument', () => {
  it('accepts a heading or a macro fence, and rejects a question dressed as an answer', () => {
    expect(looksLikeDocument('# Bug Report\n\n## Summary\nx')).toBe(true);
    expect(looksLikeDocument('```macro:action-items\nitems: []\n```')).toBe(true);
    expect(looksLikeDocument('I need to see the actual source document content to transform it.')).toBe(false);
  });
  it('is told never to ask or refuse', () => {
    expect(transformPrompts({ markdown: 'x', target: { name: 'T', fields: [] } }).system).toMatch(/never ask/i);
  });
});

describe('macro fences never depend on the model', () => {
  const links = '```macro:links\nlinks:\n  - label: Runbook\n    url: https://r.local\n```';
  const items = '```macro:action-items\nitems: []\n```';
  const source = `# T\n\nintro\n\n${links}\n\nmore\n\n${items}`;

  it('lifts every fence to a placeholder and puts it back where the model left the token', () => {
    const lifted = liftMacros(source);
    expect(lifted.fences).toEqual([links, items]);
    expect(lifted.markdown).not.toContain('```macro');
    expect(lifted.markdown).toContain('[[MACRO_1]]');
    const answer = '# T\n\n## A\nintro\n\n[[MACRO_2]]\n\n## B\n[[MACRO_1]]';
    expect(restoreMacros(answer, lifted.fences)).toBe(`# T\n\n## A\nintro\n\n${items}\n\n## B\n${links}`);
  });

  it('re-attaches a fence the model dropped under Notes rather than losing it', () => {
    const lifted = liftMacros(source);
    const answer = '# T\n\n## A\nintro [[MACRO_1]]';
    const out = restoreMacros(answer, lifted.fences);
    expect(out).toContain(links);
    expect(out).toMatch(/## Notes\n\n```macro:action-items/);
  });
});
