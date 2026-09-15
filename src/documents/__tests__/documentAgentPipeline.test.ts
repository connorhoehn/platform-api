// The document pipelines: their shape (read → plan → review → approve → apply),
// their retries, and the review step that decides when a person has to see the
// plan before it lands.
import {
  buildDocumentAgentEditDefinition, buildDocumentTransformDefinition, createDocumentReviewHandler, reviewPlan,
  DOCUMENT_READ_KIND, DOCUMENT_REVIEW_KIND, DOCUMENT_APPLY_KIND, DOCUMENT_IO_RETRY, DOCUMENT_PLAN_RETRY, APPROVAL_TIMEOUT_MS,
} from '../documentAgentPipeline';
import { parseOps } from '../documentAgentOps';
import { lintSystemPipeline } from '../../pipeline/systemPipelineLint';

type Def = { id: string; nodes: Array<{ id: string; type: string; data: Record<string, unknown> }>; edges: Array<{ source: string; sourceHandle?: string; target: string }> };
const byId = (def: Def, id: string) => def.nodes.find((n) => n.id === id)!;

describe('the document pipeline definitions', () => {
  it.each([
    ['edit', () => buildDocumentAgentEditDefinition('haiku') as Def],
    ['transform', () => buildDocumentTransformDefinition('haiku') as Def],
  ])('%s: read → plan → review → approve(approved) → apply, and lints clean', (_name, build) => {
    const def = build();
    expect(def.edges.map((e) => `${e.source}${e.sourceHandle && e.sourceHandle !== 'out' ? `[${e.sourceHandle}]` : ''}->${e.target}`))
      .toEqual(['trigger->read', 'read->plan', 'plan->review', 'review->approve', 'approve[approved]->apply']);
    // No `rejected` edge on purpose: the executor gives it no fallback, so a reject ends the run as `rejected`.
    expect(def.edges.some((e) => e.sourceHandle === 'rejected')).toBe(false);
    expect(byId(def, 'read')).toMatchObject({ type: DOCUMENT_READ_KIND, data: { retryPolicy: DOCUMENT_IO_RETRY } });
    // ONE model call at temperature 0 with a short bound — not an agent session.
    expect(byId(def, 'plan')).toMatchObject({ type: 'document:plan', data: { model: 'haiku', temperature: 0, timeoutMs: 30_000, retryPolicy: DOCUMENT_PLAN_RETRY } });
    expect(byId(def, 'review')).toMatchObject({ type: DOCUMENT_REVIEW_KIND, data: { retryPolicy: DOCUMENT_IO_RETRY } });
    expect(byId(def, 'apply')).toMatchObject({ type: DOCUMENT_APPLY_KIND, data: { retryPolicy: DOCUMENT_IO_RETRY } });
    expect(byId(def, 'approve')).toMatchObject({ type: 'approval', data: {
      condition: 'context.needsApproval === true', requiredCount: 1, timeoutMs: APPROVAL_TIMEOUT_MS, timeoutAction: 'reject',
      approvers: [{ type: 'role', value: 'requester' }],
    } });
    expect(lintSystemPipeline(def)).toEqual([]);
  });

  it('names the requester as the approver when the route builds it', () => {
    const def = buildDocumentAgentEditDefinition(undefined, { approverUserId: 'dev-grace' }) as Def;
    expect(byId(def, 'approve').data.approvers).toEqual([{ type: 'user', value: 'dev-grace', userId: 'dev-grace' }]);
    const t = buildDocumentTransformDefinition(undefined, { approverUserId: 'dev-grace' }) as Def;
    expect(byId(t, 'approve').data.approvers).toEqual([{ type: 'user', value: 'dev-grace', userId: 'dev-grace' }]);
  });

  it('a transform is allowed to change the type without asking; an edit is not', () => {
    expect(byId(buildDocumentTransformDefinition() as Def, 'review').data.allowTypeChange).toBe(true);
    expect(byId(buildDocumentAgentEditDefinition() as Def, 'review').data.allowTypeChange).toBeUndefined();
  });
});

describe('reviewPlan', () => {
  const ops = (arr: unknown[]) => parseOps(JSON.stringify(arr));
  const small = [{ op: 'setTitle', text: 'Q3 audit' }, { op: 'replaceBlock', index: 1, text: 'hello' }];

  it('lets a small edit through', () => {
    expect(reviewPlan(ops(small))).toEqual({ needsApproval: false, reason: '2 small changes; no approval needed.', opCount: 2 });
    expect(reviewPlan(ops([{ op: 'skip', reason: 'nothing to do' }]))).toMatchObject({ needsApproval: false, opCount: 0 });
  });

  it('pauses for a deletion, for a type change, and for more operations than the limit', () => {
    expect(reviewPlan(ops([...small, { op: 'deleteBlock', index: 2 }]))).toEqual({ needsApproval: true, reason: 'The plan deletes 1 block.', opCount: 3 });
    expect(reviewPlan(ops([{ op: 'setDocumentType', typeName: 'Runbook' }]))).toMatchObject({ needsApproval: true, reason: 'The plan changes the document type.' });
    const many = Array.from({ length: 9 }, (_, i) => ({ op: 'appendBlock', kind: 'paragraph', text: `p${i}` }));
    expect(reviewPlan(ops(many))).toMatchObject({ needsApproval: true, reason: 'The plan 9 changes (more than 8).', opCount: 9 });
    expect(reviewPlan(ops(many), { maxOpsWithoutApproval: 20 })).toMatchObject({ needsApproval: false });
  });

  it('a transform may stamp the type', () => {
    expect(reviewPlan(ops([{ op: 'setDocumentType', typeName: 'Runbook' }, { op: 'setTitle', text: 'x' }]), { allowTypeChange: true })).toMatchObject({ needsApproval: false, opCount: 2 });
  });

  it('honours DOCUMENT_AGENT_APPROVAL_OPS', () => {
    const prev = process.env.DOCUMENT_AGENT_APPROVAL_OPS;
    process.env.DOCUMENT_AGENT_APPROVAL_OPS = '1';
    try { expect(reviewPlan(ops(small))).toMatchObject({ needsApproval: true, reason: 'The plan 2 changes (more than 1).' }); }
    finally { if (prev === undefined) delete process.env.DOCUMENT_AGENT_APPROVAL_OPS; else process.env.DOCUMENT_AGENT_APPROVAL_OPS = prev; }
  });
});

describe('the review step', () => {
  const node = (data: Record<string, unknown> = {}) => ({ id: 'review', type: DOCUMENT_REVIEW_KIND, position: { x: 0, y: 0 }, data: { type: DOCUMENT_REVIEW_KIND, ...data } }) as never;
  const helpers = {} as never;

  it('reads the plan where the apply step will and reports what the gate needs', async () => {
    const h = createDocumentReviewHandler();
    const out = await h.execute(node(), { documentId: 'd', steps: { plan: { text: 'Sure! [{"op":"deleteBlock","index":1}]' } } }, helpers);
    expect(out).toEqual({ status: 'completed', output: { needsApproval: true, reason: 'The plan deletes 1 block.', opCount: 1 } });
    const fine = await h.execute(node(), { documentId: 'd', text: '[{"op":"setTitle","text":"x"}]' }, helpers);
    expect(fine.output).toMatchObject({ needsApproval: false, opCount: 1 });
  });

  it('follows opsFrom like the apply step', async () => {
    const out = await createDocumentReviewHandler().execute(node({ opsFrom: 'transcriptOps' }), { transcriptOps: [{ op: 'deleteBlock', index: 0 }] }, helpers);
    expect(out.output).toMatchObject({ needsApproval: true });
  });
});
