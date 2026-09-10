// The transcript crosses into the document as operations for the shared
// document:apply step — the same step /doc and Transform write through.
import { transcriptOps, buildCallTranscriptionDefinition } from '../recordingPipelines';
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

  it('has the shared apply node between store and publish, optional, reading transcriptOps', () => {
    expect(apply).toBeDefined();
    expect(apply.data).toMatchObject({ opsFrom: 'transcriptOps', optional: true });
    expect(def.edges).toEqual(expect.arrayContaining([
      expect.objectContaining({ source: 'store', target: apply.id }),
      expect.objectContaining({ source: apply.id, target: 'publish' }),
    ]));
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
