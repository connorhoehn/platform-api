// The transcript crosses into the document as operations for the shared
// document:apply step — the same step /doc and Transform write through.
import { transcriptOps, buildCallTranscriptionDefinition, KINDS } from '../recordingPipelines';
import { DOCUMENT_APPLY_KIND, opsFromContext } from '../../documents/documentAgentPipeline';

describe('transcriptOps', () => {
  it('is a heading and one paragraph per spoken segment', () => {
    expect(transcriptOps([{ text: ' hello ' }, { text: '' }, { text: 'world' }])).toEqual([
      { op: 'appendBlock', kind: 'heading', level: 2, text: 'Call transcript' },
      { op: 'appendBlock', kind: 'paragraph', text: 'hello' },
      { op: 'appendBlock', kind: 'paragraph', text: 'world' },
    ]);
  });
  it('is nothing at all for a silent recording', () => {
    expect(transcriptOps([{ text: '' }])).toEqual([]);
  });
});

describe('the transcription pipeline writes through document:apply', () => {
  const def = buildCallTranscriptionDefinition() as { nodes: Array<{ id: string; type: string; data: Record<string, unknown> }>; edges: Array<{ source: string; target: string }> };
  const apply = def.nodes.find((n) => n.type === DOCUMENT_APPLY_KIND)!;

  it('has the shared apply node after the summary and before publish, optional, reading transcriptOps', () => {
    expect(apply).toBeDefined();
    expect(apply.data).toMatchObject({ opsFrom: 'transcriptOps', optional: true });
    expect(def.edges.map((e) => `${e.source}->${e.target}`)).toEqual([
      'trigger->transcribe', 'transcribe->merge-reactions', 'merge-reactions->store',
      'store->summarize', 'summarize->store-summary', `store-summary->${apply.id}`, `${apply.id}->publish`,
    ]);
  });

  it('summarises with a light agent step that retries, and stores the answer with a step of its own', () => {
    const summarize = def.nodes.find((n) => n.id === 'summarize')!;
    expect(summarize.type).toBe('agent');
    expect(summarize.data).toMatchObject({ type: 'agent', allowedTools: [], maxTurns: 1, retryPolicy: { maxAttempts: 3, retryUnknown: true } });
    expect(String(summarize.data.prompt)).toContain('{{ context.transcriptText }}');
    expect(String(summarize.data.prompt)).toContain('(no speech)');
    const storeSummary = def.nodes.find((n) => n.id === 'store-summary')!;
    expect(storeSummary.type).toBe(KINDS.storeSummary);
    expect(storeSummary.data).toMatchObject({ retryPolicy: { maxAttempts: 3 } });
  });

  it('resolves the ops the store step produced', () => {
    const { ops } = opsFromContext({ transcriptOps: transcriptOps([{ text: 'hi' }]) }, apply.data as never);
    expect(ops).toHaveLength(2);
    expect(ops[0]).toMatchObject({ op: 'appendBlock', kind: 'heading' });
  });

  it('falls back to the planning agent when no path is named', () => {
    const { ops, source } = opsFromContext({ steps: { plan: { text: '[{"op":"setTitle","text":"x"}]' } } }, {});
    expect(source).toBe('steps.plan.text');
    expect(ops).toEqual([{ op: 'setTitle', text: 'x' }]);
  });
});

describe('looksLikeSilence', () => {
  const { looksLikeSilence } = jest.requireActual<typeof import('../recordingPipelines')>('../recordingPipelines');
  it('treats the engine’s stock phrases as no speech', () => {
    expect(looksLikeSilence('you', 27_000)).toBe(true);
    expect(looksLikeSilence('Thank you.', 5_000)).toBe(true);
    expect(looksLikeSilence('', 5_000)).toBe(true);
  });
  it('treats one or two words over ten seconds as no speech, but not a short clip', () => {
    expect(looksLikeSilence('hello there', 30_000)).toBe(true);
    expect(looksLikeSilence('hello there', 3_000)).toBe(false);
  });
  it('keeps real speech', () => {
    expect(looksLikeSilence('the compose step is still queued after twelve attempts', 27_000)).toBe(false);
  });
});

describe('looksLikeSilence — repeated tokens', () => {
  const { looksLikeSilence } = jest.requireActual<typeof import('../recordingPipelines')>('../recordingPipelines');
  it('treats a token looping on noise as no speech', () => {
    expect(looksLikeSilence('you you you you you you you I I I I I', 360_000)).toBe(true);
    expect(looksLikeSilence('the the the', 4_000)).toBe(true);
  });
  it('keeps a real sentence with a common repeated word', () => {
    expect(looksLikeSilence('we ship the checkout on Friday and the runbook covers the rollback', 30_000)).toBe(false);
  });
});
