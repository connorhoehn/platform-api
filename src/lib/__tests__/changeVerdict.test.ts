// platform-api/src/lib/__tests__/changeVerdict.test.ts
//
// Two assertions here are load-bearing and the rest are support:
//
//   1. An uncertain model cannot produce `pass`. `pass` is the chip people skim
//      past, so a reviewer that hedges must land on `concern`.
//   2. Editing a change changes its revision. That is the entire cache
//      invalidation story — get it wrong and the feature confidently attaches
//      an old verdict to new text, which is worse than shipping no verdict.

import type { LLMClient, LLMChunk } from '../../pipeline/LLMClient';
import type { Proposal } from '../../types/workItem';
import {
  buildVerdictContext,
  computeChangeRevision,
  describeError,
  judgeChange,
  parseVerdict,
  VerdictParseError,
  VerdictProviderError,
  VerdictTimeoutError,
} from '../changeVerdict';

// --- fixtures ---------------------------------------------------------------

const snapshot = {
  meta: { title: 'Retry Policy' },
  sections: [
    { id: 'sec-a', title: 'Limits', text: 'The client retries at most 3 times before failing.' },
    { id: 'sec-b', title: 'Appendix', text: 'Unrelated prose that nobody is editing.' },
  ],
};

function prose(over: Partial<{ sectionId: string; anchorQuote: string; replacement: string }> = {}): Proposal {
  return {
    kind: 'prose',
    proposalId: 'p-1',
    sectionId: over.sectionId ?? 'sec-a',
    edits: [
      {
        anchorQuote: over.anchorQuote ?? 'at most 3 times',
        replacement: over.replacement ?? 'at most 5 times',
      },
    ],
  };
}

function field(to: unknown = 'Retry Policy v2'): Proposal {
  return {
    kind: 'field',
    proposalId: 'p-2',
    change: { path: 'meta.title', from: 'Retry Policy', to },
  };
}

/**
 * An LLMClient that yields one final chunk, throws, or hangs until aborted.
 *
 * The hang case honours the AbortSignal the way ClaudeCodeLLMClient does —
 * killing the subprocess makes the stream throw — because a fake that ignored
 * the signal would let the timeout assertion pass for the wrong reason.
 */
function fakeClient(behaviour: string | (() => never) | { hang: true }): LLMClient {
  return {
    provider: 'claude-code',
    stream(
      _model: string,
      _system: string,
      _user: string,
      opts: { signal?: AbortSignal } = {},
    ): AsyncIterable<LLMChunk> {
      return {
        async *[Symbol.asyncIterator]() {
          if (typeof behaviour === 'function') behaviour();
          if (typeof behaviour === 'object' && 'hang' in behaviour) {
            await new Promise((_resolve, reject) => {
              opts.signal?.addEventListener('abort', () =>
                reject(new DOMException('Aborted', 'AbortError')),
              );
            });
          }
          yield { done: true, response: behaviour as string } as unknown as LLMChunk;
        },
      };
    },
  } as unknown as LLMClient;
}

// ---------------------------------------------------------------------------
// forRevision — the invalidation
// ---------------------------------------------------------------------------

describe('computeChangeRevision', () => {
  it('is stable for the same content', () => {
    expect(computeChangeRevision(prose())).toBe(computeChangeRevision(prose()));
  });

  it('CHANGES when the replacement text is edited', () => {
    expect(computeChangeRevision(prose({ replacement: 'at most 9 times' }))).not.toBe(
      computeChangeRevision(prose()),
    );
  });

  it('CHANGES when the anchor quote is edited', () => {
    expect(computeChangeRevision(prose({ anchorQuote: 'retries at most 3 times' }))).not.toBe(
      computeChangeRevision(prose()),
    );
  });

  it('CHANGES when the change moves to another section', () => {
    expect(computeChangeRevision(prose({ sectionId: 'sec-b' }))).not.toBe(
      computeChangeRevision(prose()),
    );
  });

  it('ignores proposalId — a re-draft of identical edits is the same change', () => {
    const a = prose();
    const b = { ...prose(), proposalId: 'a-completely-different-uuid' } as Proposal;
    expect(computeChangeRevision(b)).toBe(computeChangeRevision(a));
  });

  it('hashes a field change by its value, not by key insertion order', () => {
    const one = computeChangeRevision(field({ label: 'x', weight: 2 }));
    const two = computeChangeRevision(field({ weight: 2, label: 'x' }));
    expect(one).toBe(two);
    expect(computeChangeRevision(field({ label: 'x', weight: 3 }))).not.toBe(one);
  });

  it('distinguishes a prose change from a field change', () => {
    expect(computeChangeRevision(prose())).not.toBe(computeChangeRevision(field()));
  });
});

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

describe('buildVerdictContext', () => {
  it('shows the target section verbatim, so contradictions are checkable', () => {
    const { block } = buildVerdictContext({ proposal: prose(), snapshot });
    expect(block).toContain('The client retries at most 3 times before failing.');
    expect(block).toContain('at most 5 times');
  });

  it('lists the other pending changes, which nothing else compares side by side', () => {
    const { block } = buildVerdictContext({
      proposal: prose(),
      snapshot,
      peers: [{ itemId: 'item-9', summary: 'raise the ceiling', proposal: field('Retry Policy v3') }],
    });
    expect(block).toContain('item-9');
    expect(block).toContain('Retry Policy v3');
  });

  it('says out loud when the section is missing rather than reviewing against nothing', () => {
    const { block } = buildVerdictContext({ proposal: prose({ sectionId: 'gone' }), snapshot });
    expect(block).toContain('IS NOT IN THE DOCUMENT');
  });

  it('marks an empty replacement as a deletion', () => {
    const { block } = buildVerdictContext({ proposal: prose({ replacement: '' }), snapshot });
    expect(block).toContain('DELETES');
  });

  it('does not churn on an edit to an unrelated section body', () => {
    // Only the target section's body is shipped. Otherwise a typo three
    // sections away would invalidate every pending verdict in the document.
    const other = {
      ...snapshot,
      sections: [snapshot.sections[0]!, { id: 'sec-b', title: 'Appendix', text: 'Rewritten entirely.' }],
    };
    expect(buildVerdictContext({ proposal: prose(), snapshot: other }).digest).toBe(
      buildVerdictContext({ proposal: prose(), snapshot }).digest,
    );
  });

  it('DOES change when the target section text changes', () => {
    const moved = {
      ...snapshot,
      sections: [
        { id: 'sec-a', title: 'Limits', text: 'The client retries at most 4 times before failing.' },
        snapshot.sections[1]!,
      ],
    };
    expect(buildVerdictContext({ proposal: prose(), snapshot: moved }).digest).not.toBe(
      buildVerdictContext({ proposal: prose(), snapshot }).digest,
    );
  });
});

// ---------------------------------------------------------------------------
// parseVerdict — the safety rule
// ---------------------------------------------------------------------------

describe('parseVerdict', () => {
  it('passes a clean pass through', () => {
    const v = parseVerdict({ verdict: 'PASS ', headline: 'Consistent with §Limits.' }, 'rev-1');
    expect(v.verdict).toBe('pass');
    expect(v.headline).toBe('Consistent with §Limits.');
    expect(v.forRevision).toBe('rev-1');
  });

  it.each([
    ['a missing verdict', {}],
    ['a hedge', { verdict: 'unsure' }],
    ['a qualified pass', { verdict: 'pass?' }],
    ['pass with commentary', { verdict: 'pass, probably' }],
    ['a non-string', { verdict: 3 }],
    ['a nested object', { verdict: { value: 'pass' } }],
    ['an empty string', { verdict: '' }],
  ])('turns %s into concern, never pass', (_label, parsed) => {
    expect(parseVerdict(parsed as Record<string, unknown>, 'rev-1').verdict).toBe('concern');
  });

  it('says so in the headline when the model would not commit', () => {
    expect(parseVerdict({ reasoning: 'It depends.' }, 'rev-1').headline).toMatch(/would not commit/i);
  });

  it('keeps fail as fail', () => {
    expect(parseVerdict({ verdict: 'fail', headline: 'Deletes the whole limits table.' }, 'r').verdict)
      .toBe('fail');
  });

  it('falls back to the reasoning when no headline was given', () => {
    const v = parseVerdict({ verdict: 'concern', reasoning: 'The number 5 is not in the document.' }, 'r');
    expect(v.headline).toBe('The number 5 is not in the document.');
  });

  it('flattens and truncates a runaway headline to one line', () => {
    const v = parseVerdict({ verdict: 'pass', headline: `a\nb${'x'.repeat(400)}` }, 'r');
    expect(v.headline).not.toContain('\n');
    expect(v.headline.length).toBeLessThanOrEqual(160);
  });
});

// ---------------------------------------------------------------------------
// judgeChange
// ---------------------------------------------------------------------------

describe('judgeChange', () => {
  const base = { proposal: prose(), snapshot, forRevision: 'rev-1' };

  it('produces a verdict from a well-formed reply', async () => {
    const client = fakeClient(
      '{"verdict":"fail","headline":"Says 5 while §Limits still says 3.","reasoning":"The section text says 3."}',
    );
    const out = await judgeChange(base, client);
    expect(out.verdict.verdict).toBe('fail');
    expect(out.verdict.forRevision).toBe('rev-1');
    expect(out.verdict.reasoning).toContain('says 3');
  });

  it('tolerates a fenced reply', async () => {
    const client = fakeClient('```json\n{"verdict":"pass","headline":"Fine."}\n```');
    expect((await judgeChange(base, client)).verdict.verdict).toBe('pass');
  });

  it('does NOT invent a pass when the model hedges', async () => {
    const client = fakeClient('{"verdict":"maybe","headline":"Hard to say."}');
    expect((await judgeChange(base, client)).verdict.verdict).toBe('concern');
  });

  it('raises a provider error rather than a verdict when the call throws', async () => {
    const client = fakeClient(() => {
      throw new Error('claude exited 1: Not logged in');
    });
    await expect(judgeChange(base, client)).rejects.toBeInstanceOf(VerdictProviderError);
  });

  it('names a non-Error rejection instead of logging an empty message', async () => {
    // The bug this guards: `err?.message ?? String(err)` renders '' for these,
    // so the one line saying it failed says nothing about why.
    const client = fakeClient(() => {
      // eslint-disable-next-line no-throw-literal
      throw { code: 'ENOENT', path: 'claude' } as never;
    });
    await expect(judgeChange(base, client)).rejects.toThrow(/ENOENT/);
  });

  it('raises a provider error on an empty reply', async () => {
    await expect(judgeChange(base, fakeClient('   '))).rejects.toBeInstanceOf(VerdictProviderError);
  });

  it('raises a parse error when the reply holds no JSON', async () => {
    await expect(judgeChange(base, fakeClient('I would rather not.'))).rejects.toBeInstanceOf(
      VerdictParseError,
    );
  });

  it('times out rather than hanging the request forever', async () => {
    const client = fakeClient({ hang: true });
    await expect(judgeChange({ ...base, timeoutMs: 20 }, client)).rejects.toBeInstanceOf(
      VerdictTimeoutError,
    );
  }, 5000);
});

describe('describeError', () => {
  it('keeps an Error message', () => {
    expect(describeError(new TypeError('bad'))).toBe('TypeError: bad');
  });

  it('says something useful for an Error with no message', () => {
    expect(describeError(new Error(''))).toContain('Error');
    expect(describeError(new Error('')).length).toBeGreaterThan('Error: '.length);
  });

  it('says something useful for a thrown object', () => {
    expect(describeError({ code: 'EPIPE' })).toContain('EPIPE');
  });

  it('says something useful for undefined', () => {
    expect(describeError(undefined)).toContain('undefined');
  });
});
