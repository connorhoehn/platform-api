// /diagram: the model's sketch becomes Excalidraw records in the document's CRDT.
import type * as YT from 'yjs' with { 'resolution-mode': 'import' };
// eslint-disable-next-line @typescript-eslint/no-require-imports
const Y = require('yjs') as typeof YT;
import {
  parseSketch, normaliseSketch, fractionalIndex, writeDiagram, sketchFromContext, buildDiagramGenerateDefinition, createDiagramPublishHandler,
  DIAGRAM_APPLY_KIND, DIAGRAM_PUBLISH_KIND, DIAGRAM_GENERATE_PIPELINE_ID, DIAGRAM_ROOT_KEY, DIAGRAM_ELEMENTS_KEY,
} from '../diagramGeneratePipeline';
import { CANVAS_META_KEY } from '../documentAgentOps';

const SKETCH = '```json\n[{"id":"api","type":"rectangle","x":40,"y":40,"width":200,"height":80,"text":"API"},{"id":"db","type":"rectangle","x":400,"y":40,"width":200,"height":80,"text":"Database"},{"id":"a1","type":"arrow","startId":"api","endId":"db","text":"reads"},{"id":"bogus","type":"cloud"}]\n```';

describe('parseSketch', () => {
  it('finds the array through fences, drops unknown kinds, keeps ids unique', () => {
    const s = parseSketch(SKETCH);
    expect(s.map((e) => `${e.type}:${e.id}`)).toEqual(['rectangle:api', 'rectangle:db', 'arrow:a1']);
    expect(parseSketch('[{"type":"rectangle"},{"type":"rectangle"}]').map((e) => e.id)).toEqual(['rectangle-1', 'rectangle-2']);
    expect(parseSketch('I cannot draw that.')).toEqual([]);
  });
  it('caps at 14 elements', () => {
    const many = JSON.stringify(Array.from({ length: 30 }, (_, i) => ({ id: `b${i}`, type: 'rectangle' })));
    expect(parseSketch(many)).toHaveLength(14);
  });
});

describe('normaliseSketch', () => {
  const { elements, boxes, arrows, labels } = normaliseSketch(parseSketch(SKETCH));
  const byId = Object.fromEntries(elements.map((e) => [e.id, e]));

  it('gives every box a label text element bound into it', () => {
    expect(boxes).toBe(2);
    expect(labels).toBe(3); // two box labels and the arrow's
    const api = byId.api!; const label = byId['api-label']!;
    expect(label).toMatchObject({ type: 'text', containerId: 'api', text: 'API', originalText: 'API', fontSize: 20, fontFamily: 1, textAlign: 'center', verticalAlign: 'middle', lineHeight: 1.25, autoResize: true });
    expect(api.boundElements).toEqual(expect.arrayContaining([{ type: 'text', id: 'api-label' }]));
    // Fitted inside the box.
    expect(label.x as number).toBeGreaterThanOrEqual(api.x as number);
    expect((label.x as number) + (label.width as number)).toBeLessThanOrEqual((api.x as number) + (api.width as number));
    expect(api.roundness).toEqual({ type: 3 });
  });

  it('anchors an arrow on the source’s right edge and the target’s left edge, bound at both ends', () => {
    expect(arrows).toBe(1);
    const a = byId.a1!;
    expect(a).toMatchObject({ type: 'arrow', x: 240, y: 80, points: [[0, 0], [160, 0]], startBinding: { elementId: 'api', focus: 0, gap: 1 }, endBinding: { elementId: 'db', focus: 0, gap: 1 }, endArrowhead: 'arrow', startArrowhead: null, elbowed: false, lastCommittedPoint: null, roundness: null });
    expect(byId.api!.boundElements).toEqual(expect.arrayContaining([{ type: 'arrow', id: 'a1' }]));
    expect(byId.db!.boundElements).toEqual(expect.arrayContaining([{ type: 'arrow', id: 'a1' }]));
    // The arrow's own label is bound to the arrow.
    expect(a.boundElements).toEqual([{ type: 'text', id: 'a1-label' }]);
    expect(byId['a1-label']).toMatchObject({ containerId: 'a1', text: 'reads' });
  });

  it('carries Excalidraw defaults on every record and ascending fractional indices', () => {
    for (const e of elements) {
      expect(e).toMatchObject({ version: 1, isDeleted: false, angle: 0, strokeColor: '#1e1e1e', backgroundColor: 'transparent', fillStyle: 'solid', strokeWidth: 2, strokeStyle: 'solid', roughness: 1, opacity: 100, groupIds: [], frameId: null, link: null, locked: false });
      expect(typeof e.versionNonce).toBe('number'); expect(typeof e.seed).toBe('number'); expect(typeof e.updated).toBe('number');
    }
    const indices = elements.map((e) => e.index as string);
    expect(indices).toEqual([...indices].sort());
    expect(new Set(indices).size).toBe(indices.length);
    expect(indices[0]).toBe('a0');
  });

  it('snaps unplaced boxes onto the grid and leaves an unbound arrow with the model’s geometry', () => {
    const { elements: els } = normaliseSketch([
      { id: 'x', type: 'ellipse', text: 'X' },
      { id: 'free', type: 'arrow', x: 10, y: 20, points: [[0, 0], [100, 40]] },
    ]);
    const x = els.find((e) => e.id === 'x')!;
    expect((x.x as number) % 40).toBe(0); expect((x.y as number) % 40).toBe(0);
    expect(x).toMatchObject({ width: 200, height: 80, roundness: null });
    expect(els.find((e) => e.id === 'free')).toMatchObject({ x: 10, y: 20, points: [[0, 0], [100, 40]], startBinding: null, endBinding: null });
  });
});

describe('fractionalIndex', () => {
  it('is lexicographically ascending across the a- and b- ranges', () => {
    const keys = Array.from({ length: 130 }, (_, i) => fractionalIndex(i));
    expect(keys.slice(0, 3)).toEqual(['a0', 'a1', 'a2']);
    expect(keys[61]).toBe('az'); expect(keys[62]).toBe('b00');
    expect(keys).toEqual([...keys].sort());
    expect(new Set(keys).size).toBe(keys.length);
  });
});

describe('writeDiagram', () => {
  it('writes meta for a diagram-shaped canvas and one Y.Map per element under excalidraw → elements, as one update', () => {
    const doc = new Y.Doc();
    doc.getMap(CANVAS_META_KEY).set('title', 'Client title');
    let updates = 0;
    doc.on('update', (_u: Uint8Array, origin: unknown) => { if (origin === 'agent') updates += 1; });
    const { elements } = normaliseSketch(parseSketch(SKETCH));
    const { written } = writeDiagram(doc, elements, { documentId: 'doc-7', title: 'Ignored — the client already named it' });
    expect(written).toBe(elements.length);
    expect(updates).toBe(1);
    const meta = doc.getMap(CANVAS_META_KEY).toJSON();
    expect(meta).toMatchObject({ id: 'doc-7', title: 'Client title', docShape: 'diagram', schemaVersion: 2, templateApplied: true, sourceType: 'notes', status: 'draft' });
    const container = doc.getMap(DIAGRAM_ROOT_KEY).get(DIAGRAM_ELEMENTS_KEY) as YT.Map<YT.Map<unknown>>;
    expect(container.size).toBe(elements.length);
    expect((container.get('api')!.toJSON() as Record<string, unknown>).boundElements).toEqual(expect.arrayContaining([{ type: 'arrow', id: 'a1' }]));
    expect(Object.values(container.get('a1')!.toJSON() as Record<string, unknown>)).not.toContain(undefined);
  });
  it('reuses an elements container that is already there', () => {
    const doc = new Y.Doc();
    const existing = new Y.Map<unknown>();
    doc.getMap(DIAGRAM_ROOT_KEY).set(DIAGRAM_ELEMENTS_KEY, existing);
    writeDiagram(doc, normaliseSketch([{ id: 'b', type: 'rectangle', text: 'B' }]).elements, { documentId: 'd', title: 'T' });
    expect(doc.getMap(DIAGRAM_ROOT_KEY).get(DIAGRAM_ELEMENTS_KEY)).toBe(existing);
    expect(existing.size).toBe(2);
    expect(doc.getMap(CANVAS_META_KEY).get('title')).toBe('T');
  });
});

describe('the diagram-generate definition', () => {
  const def = buildDiagramGenerateDefinition('haiku') as { id: string; nodes: Array<{ id: string; type: string; data: Record<string, unknown> }>; edges: Array<{ source: string; target: string }> };
  it('is plan → apply → publish with a retrying light agent step', () => {
    expect(def.id).toBe(DIAGRAM_GENERATE_PIPELINE_ID);
    expect(def.edges.map((e) => `${e.source}->${e.target}`)).toEqual(['trigger->plan', 'plan->apply', 'apply->publish']);
    const plan = def.nodes.find((n) => n.id === 'plan')!;
    expect(plan.type).toBe('agent');
    expect(plan.data).toMatchObject({ model: 'haiku', allowedTools: [], maxTurns: 1, retryPolicy: { maxAttempts: 3, backoffMs: 2000, backoffMultiplier: 2 } });
    expect(String(plan.data.prompt)).toContain('{{ context.prompt }}');
    expect(def.nodes.find((n) => n.id === 'apply')).toMatchObject({ type: DIAGRAM_APPLY_KIND, data: { sketchFrom: 'steps.plan.text' } });
    expect(def.nodes.find((n) => n.id === 'publish')!.type).toBe(DIAGRAM_PUBLISH_KIND);
  });
  it('the apply step reads the sketch from the planning agent’s text', () => {
    expect(sketchFromContext({ steps: { plan: { text: SKETCH } } }, { sketchFrom: 'steps.plan.text' })).toHaveLength(3);
    expect(sketchFromContext({}, {})).toEqual([]);
  });
});

describe('the publish step', () => {
  it('posts "/diagram <title>" with the document id on the card', async () => {
    const postCard = jest.fn(async () => true);
    const out = await createDiagramPublishHandler({ postCard }).execute({ id: 'publish', type: DIAGRAM_PUBLISH_KIND, data: {} } as never,
      { chatChannel: 'room:x', documentId: 'doc-2', title: 'Checkout flow', steps: { apply: { elements: 7 } } }, { runId: 'run-3', emit: async () => undefined } as never);
    expect(postCard).toHaveBeenCalledWith(expect.objectContaining({ channel: 'room:x', runId: 'run-3', pipelineId: DIAGRAM_GENERATE_PIPELINE_ID, command: '/diagram', instruction: 'Checkout flow', extra: expect.objectContaining({ documentId: 'doc-2' }) }));
    expect(out).toEqual({ status: 'completed', output: { posted: true, title: 'Checkout flow', elements: 7 } });
  });
});
