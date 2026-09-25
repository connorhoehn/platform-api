// document-summary / document-action-list: draftable system pipelines that
// write a NEW page from one source document, and the check step that
// authorizes the requester (read on the source, edit on the target) first.

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn(async () => ({})) }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import type { PipelineNode } from 'distributed-core/applications/pipeline';
import { createDocumentCheckSourcesHandler, DOCUMENT_CHECK_SOURCES_KIND } from '../DocumentCheckSourcesStep';
import {
  buildDocumentActionListDefinition, buildDocumentSummaryDefinition,
  DOCUMENT_ACTION_LIST_PIPELINE_ID, DOCUMENT_ACTION_LIST_PROMPT, DOCUMENT_SUMMARY_PIPELINE_ID, DOCUMENT_SUMMARY_PROMPT,
} from '../documentFromSourcePipelines';
import { buildDocumentAgentHandlers, reviewPlan } from '../documentAgentPipeline';
import { parseOps } from '../documentAgentOps';
import { interpolateContext } from '../../pipeline/agent/AgentStepHandler';
import { lintSystemPipeline } from '../../pipeline/systemPipelineLint';
import { getDraftableSystemPipeline, listSystemPipelines } from '../../pipeline/systemPipelines';

const ROWS: Record<string, { createdBy: string; viewers?: string[]; editors?: string[] }> = {
  mine: { createdBy: 'dev-connor' },
  shared: { createdBy: 'dev-frank', viewers: ['dev-connor'] },
  secret: { createdBy: 'dev-frank' },
  page: { createdBy: 'dev-connor' },
  'their-page': { createdBy: 'dev-frank', viewers: ['dev-connor'] },
  'co-page': { createdBy: 'dev-frank', editors: ['dev-connor'] },
};
const node = { id: 'check', type: DOCUMENT_CHECK_SOURCES_KIND, data: {} } as unknown as PipelineNode;
const check = createDocumentCheckSourcesHandler({
  access: { getDocumentRow: async (id) => ROWS[id] ?? null, getLifecycleOwner: async (id) => (id === 'lifecycle-only' ? 'dev-frank' : undefined) },
});
const run = (ctx: Record<string, unknown>) => check.execute(node, { requestedBy: 'dev-connor', organizationId: 'dev-org', ...ctx }, {} as never);
const src = (...ids: string[]) => ids.map((id) => ({ kind: 'document', id }));

describe('document:check-sources', () => {
  it('passes a readable source and an editable target, handing the read step the first source', async () => {
    const out = await run({ documentId: 'page', sources: src('shared', 'mine') });
    expect(out.status).toBe('completed');
    expect(out.output).toEqual({ sourceDocumentId: 'shared', sourceIds: ['shared', 'mine'], targetDocumentId: 'page' });
  });

  it('allows a target the requester co-edits, and a brand-new target with no row yet', async () => {
    expect((await run({ documentId: 'co-page', sources: src('mine') })).status).toBe('completed');
    expect((await run({ documentId: 'fresh', sources: src('mine') })).status).toBe('completed');
  });

  it('reads the target from the trigger output on a replay', async () => {
    const out = await run({ steps: { trigger: { output: { documentId: 'page' } } }, sources: src('mine') });
    expect(out.status).toBe('completed');
  });

  it('refuses unreadable sources, naming every one, without retry', async () => {
    const out = await run({ documentId: 'page', sources: src('mine', 'secret', 'missing') });
    expect(out.status).toBe('failed');
    expect((out as { retry?: boolean }).retry).toBe(false);
    expect(out.error).toContain('secret, missing');
  });

  it('refuses a target the requester can only view, or that someone else owns via lifecycle', async () => {
    for (const target of ['their-page', 'lifecycle-only']) {
      const out = await run({ documentId: target, sources: src('mine') });
      expect(out.status).toBe('failed');
      expect((out as { retry?: boolean }).retry).toBe(false);
      expect(out.error).toContain(`cannot edit the target document ${target}`);
    }
  });

  it('refuses a target that is also a source — the source is never modified', async () => {
    const out = await run({ documentId: 'mine', sources: src('mine') });
    expect(out.status).toBe('failed');
    expect(out.error).toContain('also a source');
  });

  it('refuses no sources, no target, or no requester', async () => {
    expect((await run({ documentId: 'page', sources: [] })).error).toContain('no source document');
    expect((await run({ sources: src('mine') })).error).toContain('no target document');
    expect((await run({ documentId: 'page', sources: src('mine'), requestedBy: undefined })).error).toContain('no requester');
  });

  it('is registered with the document handlers', () => {
    expect(buildDocumentAgentHandlers({ llm: {} as never }).has(DOCUMENT_CHECK_SOURCES_KIND)).toBe(true);
  });
});

describe('document-summary / document-action-list definitions', () => {
  const defs = [buildDocumentSummaryDefinition(), buildDocumentActionListDefinition()];

  it('lint clean with the page metadata', () => {
    for (const def of defs) expect(lintSystemPipeline(def, { metadata: true })).toEqual([]);
  });

  it('check → read → plan → review → approve → apply, writing directly into the new page', () => {
    for (const def of defs) {
      const nodes = def.nodes as Array<{ id: string; type: string; data: Record<string, unknown> }>;
      expect(nodes.map((n) => n.type)).toEqual(['trigger', 'document:check-sources', 'document:read', 'document:plan', 'document:review-plan', 'approval', 'document:apply']);
      expect(nodes.find((n) => n.id === 'apply')!.data.mode).toBe('direct');
      expect(nodes.find((n) => n.id === 'trigger')!.data.triggerType).toBe('manual');
      expect(def).toMatchObject({ workType: 'documents', kind: 'document', origin: { kind: 'system' } });
    }
  });

  it('are draftable and listed in the catalog', () => {
    expect(getDraftableSystemPipeline(DOCUMENT_SUMMARY_PIPELINE_ID)).toMatchObject({ name: 'Summary from a document', system: true });
    expect(getDraftableSystemPipeline(DOCUMENT_ACTION_LIST_PIPELINE_ID)).toMatchObject({ name: 'Action list from a document', system: true });
    const ids = listSystemPipelines().map((d) => d.id);
    expect(ids).toEqual(expect.arrayContaining([DOCUMENT_SUMMARY_PIPELINE_ID, DOCUMENT_ACTION_LIST_PIPELINE_ID]));
  });

  it('a whole summary page does not stop for approval; a delete would', () => {
    const page = parseOps(JSON.stringify([
      { op: 'setTitle', text: 'Launch plan — Summary' },
      ...Array.from({ length: 12 }, (_, i) => ({ op: 'appendBlock', kind: 'paragraph', text: `p${i}` })),
    ]));
    const reviewData = (defs[0]!.nodes as Array<{ id: string; data: Record<string, unknown> }>).find((n) => n.id === 'review')!.data;
    expect(reviewPlan(page, reviewData).needsApproval).toBe(false);
    expect(reviewPlan(parseOps('[{"op":"deleteBlock","index":1}]'), reviewData).needsApproval).toBe(true);
  });
});

describe('prompts', () => {
  const ctx = { documentOutline: 'title: Launch plan\n#1 Ship Thursday', instruction: 'Keep it brief' };

  it('interpolate the source outline and the instruction', () => {
    for (const prompt of [DOCUMENT_SUMMARY_PROMPT, DOCUMENT_ACTION_LIST_PROMPT]) {
      const text = interpolateContext(prompt, ctx);
      expect(text).toContain('#1 Ship Thursday');
      expect(text).toContain('Keep it brief');
      expect(text).not.toContain('{{');
      expect(text).toContain('Never invent facts');
    }
  });

  it('summary: composed title, Summary / Key points, conditional Decisions / Open questions', () => {
    expect(DOCUMENT_SUMMARY_PROMPT).toContain('<source title> — Summary');
    expect(DOCUMENT_SUMMARY_PROMPT).toMatch(/"Key points"/);
    expect(DOCUMENT_SUMMARY_PROMPT).toMatch(/"Decisions" and "Open questions".*ONLY when/);
  });

  it('action list: composed title, one action-items macro, skip when none', () => {
    expect(DOCUMENT_ACTION_LIST_PROMPT).toContain('<source title> — Action items');
    expect(DOCUMENT_ACTION_LIST_PROMPT).toContain('"macroName":"action-items"');
    expect(DOCUMENT_ACTION_LIST_PROMPT).toMatch(/skip.*no commitments/);
  });

  it('the example action-items op parses as the apply step will read it', () => {
    const ops = parseOps(JSON.stringify([
      { op: 'setTitle', text: 'Launch plan — Action items' },
      { op: 'appendMacro', macroName: 'action-items', yaml: 'items:\n  - assignee: Sarah\n    status: pending\n    text: Ship Thursday' },
    ]));
    expect(ops.map((o) => o.op)).toEqual(['setTitle', 'appendMacro']);
  });
});
