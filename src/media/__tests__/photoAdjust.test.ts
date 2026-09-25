import type { LLMChunk, LLMClient } from '../../pipeline/LLMClient';
import { ModelUnavailableError } from '../../lib/modelHealth';
import { createPhotoProposeAdjustHandler, buildPhotoAdjustDefinition } from '../photoAdjustPipeline';
import { validateAssistantOps, mergeOps, PhotoOpRefusal } from '../photoAdjustOps';
import { assertSystemPipelinesLint } from '../../pipeline/systemPipelineLint';

const FILE = 'a'.repeat(64);
const MASKS = [
  { maskId: 'people', label: 'People', kind: 'people', subjects: 2 },
  { maskId: 'left', label: 'Person on left', kind: 'subject' },
];

function llmAnswering(answer: string | Error): LLMClient & { prompts: string[] } {
  const prompts: string[] = [];
  return {
    provider: 'fake',
    prompts,
    async *stream(_model: string, _system: string, user: string) {
      prompts.push(user);
      if (answer instanceof Error) throw answer;
      yield { done: true, response: answer } as unknown as LLMChunk;
    },
  } as LLMClient & { prompts: string[] };
}

const ctx = (over: Record<string, unknown> = {}) => ({
  requestedBy: 'dev-connor',
  fileId: FILE,
  command: 'adjust',
  instruction: 'Lift the shadows on the walkers. Keep the afternoon contrast.',
  masks: MASKS,
  referencedMaskIds: ['people'],
  currentOps: [{ kind: 'exposure', stops: 0.15 }, { kind: 'highlights', amount: -12 }],
  ...over,
});

const node = { id: 'propose', type: 'photo:propose-adjust', data: {} } as never;

describe('validateAssistantOps', () => {
  it('keeps allowed ops in bounds, rounded, with only their field and a known mask', () => {
    expect(validateAssistantOps([{ kind: 'shadows', amount: 18.123, maskId: 'people' }, { kind: 'exposure', stops: 0.2 }], MASKS))
      .toEqual([{ kind: 'shadows', amount: 18.12, maskId: 'people' }, { kind: 'exposure', stops: 0.2 }]);
  });
  it.each([
    [[{ kind: 'blur', radiusPx: 4 }], /may not propose/],
    [[{ kind: 'shadows', amount: 400 }], /outside -100..100/],
    [[{ kind: 'shadows', amount: 'lots' }], /needs a number/],
    [[{ kind: 'shadows', amount: 10, maskId: 'sky' }], /not one of this photo's masks/],
    [[{ kind: 'shadows', amount: 10, curve: 1 }], /unknown fields/],
    [[{ kind: 'shadows', amount: 10 }, { kind: 'shadows', amount: 12 }], /repeats/],
    ['nope', /no ops list/],
  ])('refuses the whole answer: %j', (ops, message) => {
    expect(() => validateAssistantOps(ops, MASKS)).toThrow(PhotoOpRefusal);
    expect(() => validateAssistantOps(ops, MASKS)).toThrow(message as RegExp);
  });
  it('mergeOps replaces by kind + mask, else appends', () => {
    expect(mergeOps([{ kind: 'shadows', amount: 5 }], [{ kind: 'shadows', amount: 9 }, { kind: 'shadows', amount: 20, maskId: 'people' }]))
      .toEqual([{ kind: 'shadows', amount: 9 }, { kind: 'shadows', amount: 20, maskId: 'people' }]);
  });
});

describe('photo:propose-adjust', () => {
  const photoExists = async () => ({ name: 'L1040606.DNG' });

  it('turns the instruction into proposed ops scoped to the referenced mask', async () => {
    const llm = llmAnswering('Here: {"ops":[{"kind":"shadows","amount":22,"maskId":"people"},{"kind":"exposure","stops":0.2,"maskId":"people"},{"kind":"whites","amount":-5}],"summary":"Lifted the walkers"}');
    const out = await createPhotoProposeAdjustHandler({ llm, photoExists }).execute(node, ctx(), {} as never) as { status: string; output: Record<string, unknown> };
    expect(out.status).toBe('completed');
    expect(out.output.changedKinds).toEqual(['shadows', 'exposure', 'whites']);
    expect(out.output.proposedOps).toEqual([
      { kind: 'exposure', stops: 0.15 }, { kind: 'highlights', amount: -12 },
      { kind: 'shadows', amount: 22, maskId: 'people' }, { kind: 'exposure', stops: 0.2, maskId: 'people' }, { kind: 'whites', amount: -5 },
    ]);
    expect(out.output.summary).toBe('Lifted the walkers');
    expect(llm.prompts[0]).toContain('Referenced masks: people ("People")');
  });

  it('refuses an out-of-bounds answer instead of clamping it', async () => {
    const llm = llmAnswering('{"ops":[{"kind":"shadows","amount":180}]}');
    const out = await createPhotoProposeAdjustHandler({ llm, photoExists }).execute(node, ctx(), {} as never) as { status: string; error: string };
    expect(out.status).toBe('failed');
    expect(out.error).toMatch(/refused the model's answer — .*outside -100..100/);
  });

  it('says "Model unavailable: <reason>" when the gate refuses', async () => {
    const llm = llmAnswering(new ModelUnavailableError('unreachable' as never, 'breaker open after 3 failures'));
    const out = await createPhotoProposeAdjustHandler({ llm, photoExists }).execute(node, ctx(), {} as never) as { status: string; error: string };
    expect(out.status).toBe('failed');
    expect(out.error).toBe('Model unavailable: breaker open after 3 failures');
  });

  it('refuses a photo not in the catalog, an unknown referenced mask, and other commands before any model call', async () => {
    const llm = llmAnswering('{"ops":[]}');
    const h = createPhotoProposeAdjustHandler({ llm, photoExists: async () => null });
    expect((await h.execute(node, ctx(), {} as never) as { error: string }).error).toMatch(/no such photo/);
    const h2 = createPhotoProposeAdjustHandler({ llm, photoExists });
    expect((await h2.execute(node, ctx({ referencedMaskIds: ['sky'] }), {} as never) as { error: string }).error).toMatch(/not one of this photo's/);
    expect((await h2.execute(node, ctx({ command: 'heal' }), {} as never) as { error: string }).error).toMatch(/only \/adjust/);
    expect(llm.prompts).toHaveLength(0);
  });

  it('the definition passes the system-pipeline lint', () => {
    expect(() => assertSystemPipelinesLint([buildPhotoAdjustDefinition()], { metadata: true })).not.toThrow();
  });
});
