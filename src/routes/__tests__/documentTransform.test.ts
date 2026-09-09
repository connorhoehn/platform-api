import { completeStream, extractMarkdown, looksLikeDocument, transformPrompts } from '../documentTransform';
import type { LLMClient, LLMChunk } from 'distributed-core/applications/pipeline';

describe('document transform prompts', () => {
  it('lists the target fields in order and forbids touching macro fences', () => {
    const { system, user } = transformPrompts({
      markdown: '# Standup\n\n```macro:action-items\nitems: []\n```',
      sourceType: 'Meeting Notes',
      target: { name: 'Bug Report', fields: [{ name: 'Severity', sectionType: 'text' }, { name: 'Steps' }] },
    });
    expect(system).toMatch(/VERBATIM/);
    expect(user).toContain('- Severity (text)\n- Steps');
    expect(user).toContain('Target type: Bug Report');
    expect(user).toContain('```macro:action-items');
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
