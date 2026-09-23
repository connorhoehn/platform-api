// Inference for legacy definitions, the route summary, and the enum contract
// the realtime-modules catalog mirrors (PLAN §2.1/2.3/2.5).

import {
  decorateDefinition,
  inferKind,
  inferWorkType,
  routeSummary,
  topologicalNodeIds,
  KIND_WORK_TYPE,
  PIPELINE_KINDS,
  WORK_TYPE_ORDER,
  WORK_TYPE_LABEL,
  ORIGIN_KINDS,
  MAX_ROUTE_ENTRIES,
} from '../definition-metadata';
import { listSystemPipelines } from '../systemPipelines';
import { lintSystemPipeline } from '../systemPipelineLint';

const node = (id: string, type: string, data: Record<string, unknown> = {}) => ({ id, type, position: { x: 0, y: 0 }, data: { type, ...data } });
const chain = (...ids: string[]) => ids.slice(0, -1).map((source, i) => ({ id: `e${i}`, source, target: ids[i + 1] }));

describe('inferKind — legacy shapes (no workType / kind stored)', () => {
  const cases: Array<[string, Record<string, unknown>, string]> = [
    ['deck steps → presentation', { nodes: [node('t', 'trigger'), node('a', 'deck:revise-load'), node('b', 'deck:revise')] }, 'presentation'],
    ['diagram step → diagram', { nodes: [node('t', 'trigger'), node('a', 'agent'), node('b', 'diagram:apply')] }, 'diagram'],
    ['document steps → document', { nodes: [node('t', 'trigger'), node('a', 'document:read'), node('b', 'document:apply')] }, 'document'],
    ['transcription → recording (beats the chat publish step)', { nodes: [node('t', 'trigger'), node('a', 'transcription:transcribe'), node('b', 'transcript:publish'), node('c', 'chat:post')] }, 'recording'],
    ['captions → recording', { nodes: [node('t', 'trigger'), node('a', 'captions:arm'), node('b', 'captions:stream')] }, 'recording'],
    ['call steps → call', { nodes: [node('t', 'trigger'), node('a', 'call:find'), node('b', 'call:notify')] }, 'call'],
    ['chat history + summarise → conversation (beats the document write)', { nodes: [node('t', 'trigger'), node('a', 'chat:read-history'), node('b', 'agent'), node('c', 'document:apply')] }, 'conversation'],
    ['media ingest → media', { nodes: [node('t', 'trigger'), node('a', 'media:ingest'), node('b', 'catalog:upsert')] }, 'media'],
    ['image step → media', { nodes: [node('t', 'trigger'), node('a', 'image:generate')] }, 'media'],
    ['bare agent node → agent', { nodes: [node('t', 'trigger'), node('a', 'agent', { prompt: 'do it' })] }, 'agent'],
    ['devbox runtime → agent', { nodes: [node('t', 'trigger'), node('a', 'custom', { runtime: 'devbox' })] }, 'agent'],
    ['loop marker → agent', { nodes: [node('t', 'trigger'), node('a', 'custom', { loop: { kind: 'research', stepId: 's1' } })] }, 'agent'],
    ['origin agent, no nodes → agent', { origin: { kind: 'agent' }, nodes: [] }, 'agent'],
    ['llm-only graph named "Weekly deck" → presentation (by name)', { name: 'Weekly deck', nodes: [node('t', 'trigger'), node('a', 'llm')] }, 'presentation'],
    ['llm-only graph tagged summary → conversation (by tags)', { name: 'Thread digest', tags: ['summary'], nodes: [node('t', 'trigger'), node('a', 'llm')] }, 'conversation'],
    ['llm-only graph named "Standup notes" → document', { name: 'Standup notes', nodes: [node('t', 'trigger'), node('a', 'llm')] }, 'document'],
    ['experiment in the name → experiment', { name: 'Prompt experiment 3', nodes: [node('t', 'trigger'), node('a', 'llm')] }, 'experiment'],
    ['nothing to go on → workflow', { name: 'Untitled', nodes: [node('t', 'trigger'), node('a', 'condition'), node('b', 'llm')] }, 'workflow'],
    ['no nodes at all, no name → workflow', {}, 'workflow'],
    ['nodes not an array → workflow', { nodes: 'nope' }, 'workflow'],
    ['null node entries are skipped', { nodes: [null, node('a', 'document:read'), undefined] }, 'document'],
    ['type on data only (older editor shape) → document', { nodes: [{ id: 'a', data: { type: 'document:plan' } }] }, 'document'],
  ];
  it.each(cases)('%s', (_name, def, expected) => {
    expect(inferKind(def)).toBe(expected);
  });

  it('an explicit kind wins over every heuristic', () => {
    expect(inferKind({ kind: 'call', nodes: [node('a', 'deck:revise')] })).toBe('call');
  });
  it('an invalid explicit kind is ignored', () => {
    expect(inferKind({ kind: 'spreadsheet', nodes: [node('a', 'deck:revise')] })).toBe('presentation');
  });
});

describe('inferWorkType', () => {
  it('follows the kind', () => {
    expect(inferWorkType({ nodes: [node('a', 'deck:revise')] })).toBe('documents');
    expect(inferWorkType({ nodes: [node('a', 'agent')] })).toBe('agents');
    expect(inferWorkType({ nodes: [node('a', 'media:ingest')] })).toBe('conversations');
    expect(inferWorkType({})).toBe('other');
  });
  it('an explicit workType wins even when it disagrees with the kind', () => {
    expect(inferWorkType({ workType: 'other', kind: 'document' })).toBe('other');
  });
});

describe('routeSummary', () => {
  const def = {
    nodes: [
      node('trigger', 'trigger', { triggerType: 'platform', label: '/doc command' }),
      node('read', 'document:read', { label: 'Read the document' }),
      node('plan', 'document:plan', { label: 'Plan the edit (Haiku)' }),
      node('approve', 'approval', { label: 'Approve a big edit', approvers: ['x'] }),
      node('apply', 'document:apply', { label: 'Suggest as a collaborator', shortLabel: 'Suggestions' }),
    ],
    edges: chain('trigger', 'read', 'plan', 'approve', 'apply'),
  };
  it('walks the graph in order: command form, labels, Approval, shortLabel', () => {
    expect(routeSummary(def)).toEqual(['/doc', 'Read the document', 'Plan the edit', 'Approval', 'Suggestions']);
  });
  it('follows edges, not node array order', () => {
    const shuffled = { ...def, nodes: [...def.nodes].reverse() };
    expect(routeSummary(shuffled)).toEqual(routeSummary(def));
  });
  it('caps at MAX_ROUTE_ENTRIES with "… +N"', () => {
    const ids = Array.from({ length: 9 }, (_, i) => `n${i}`);
    const long = { nodes: ids.map((id, i) => node(id, i === 0 ? 'trigger' : 'llm', { label: `Step ${i}` })), edges: chain(...ids) };
    const route = routeSummary(long);
    expect(route).toHaveLength(MAX_ROUTE_ENTRIES);
    expect(route[MAX_ROUTE_ENTRIES - 1]).toBe('… +4');
  });
  it('a trigger with no label shows its trigger type; a step with no label shows its type tail', () => {
    expect(routeSummary({ nodes: [node('t', 'trigger', { triggerType: 'webhook' }), node('a', 'document:read')], edges: chain('t', 'a') }))
      .toEqual(['webhook', 'read']);
  });
  it('a cycle remainder is appended, never dropped', () => {
    const cyc = { nodes: [node('t', 'trigger'), node('a', 'llm', { label: 'A' }), node('b', 'llm', { label: 'B' })], edges: [...chain('t', 'a'), { id: 'x', source: 'a', target: 'b' }, { id: 'y', source: 'b', target: 'a' }] };
    expect(topologicalNodeIds(cyc)).toEqual(['t', 'a', 'b']);
  });
  it('is empty for a definition with no nodes', () => {
    expect(routeSummary({})).toEqual([]);
  });
});

describe('decorateDefinition', () => {
  it('fills the three fields, reports which were inferred, and leaves the input untouched', () => {
    const def = { id: 'p1', name: 'Legacy', nodes: [node('t', 'trigger', { label: 'Manual' }), node('a', 'document:read', { label: 'Read' })], edges: chain('t', 'a') };
    const before = JSON.stringify(def);
    const out = decorateDefinition(def);
    expect(out).toMatchObject({ id: 'p1', workType: 'documents', kind: 'document', route: ['Manual', 'Read'], inferred: ['kind', 'workType', 'route'] });
    expect(out.origin).toBeUndefined();
    expect(JSON.stringify(def)).toBe(before);
  });
  it('a fully stored definition is returned as is, with no `inferred`', () => {
    const out = decorateDefinition({ id: 'p2', workType: 'agents', kind: 'agent', route: ['/agent', 'Research'], origin: { kind: 'agent', instruction: 'x', plannerSource: 'model' } });
    expect(out.inferred).toBeUndefined();
    expect(out.origin).toEqual({ kind: 'agent', instruction: 'x', plannerSource: 'model' });
  });
  it('a system seed gets origin.kind system', () => {
    expect(decorateDefinition({ id: 's', system: true }).origin).toEqual({ kind: 'system' });
  });
  it('an unknown origin kind is dropped rather than passed through', () => {
    expect(decorateDefinition({ id: 's', origin: { kind: 'martian' } }).origin).toBeUndefined();
  });
});

describe('the enum contract the catalog hook mirrors', () => {
  it('pins the values', () => {
    expect(WORK_TYPE_ORDER).toEqual(['documents', 'agents', 'conversations', 'other']);
    expect(WORK_TYPE_LABEL).toEqual({ documents: 'Documents & presentations', agents: 'Agents & experiments', conversations: 'Conversations & media', other: 'Other' });
    expect(PIPELINE_KINDS).toEqual(['document', 'presentation', 'diagram', 'agent', 'experiment', 'recording', 'call', 'conversation', 'media', 'workflow']);
    expect(KIND_WORK_TYPE).toEqual({
      document: 'documents', presentation: 'documents', diagram: 'documents',
      agent: 'agents', experiment: 'agents',
      recording: 'conversations', call: 'conversations', conversation: 'conversations', media: 'conversations',
      workflow: 'other',
    });
    expect(ORIGIN_KINDS).toEqual(['system', 'template', 'agent', 'blank', 'import']);
  });
});

describe('system pipelines', () => {
  it('every seed states workType, kind and route explicitly (nothing inferred) and passes the lint', () => {
    const defs = listSystemPipelines();
    expect(defs.length).toBeGreaterThanOrEqual(10);
    for (const def of defs) {
      const out = decorateDefinition(def);
      expect({ id: def.id, inferred: out.inferred }).toEqual({ id: def.id, inferred: undefined });
      expect(out.origin).toEqual({ kind: 'system' });
      expect(lintSystemPipeline(def, { metadata: true })).toEqual([]);
    }
  });
  it('the lint names a seed that forgets the fields', () => {
    const rules = lintSystemPipeline({ id: 'x', nodes: [] }, { metadata: true }).map((v) => v.rule).sort();
    expect(lintSystemPipeline({ id: 'x', nodes: [] })).toEqual([]);
    expect(rules).toEqual(['kind', 'route', 'work-type']);
  });
  it('the mockup route for /doc is exact', () => {
    const edit = listSystemPipelines().find((d) => d.id === 'document-agent-edit')!;
    expect(edit.route).toEqual(['/doc', 'Read', 'Plan', 'Approval', 'Suggestions']);
  });
});
