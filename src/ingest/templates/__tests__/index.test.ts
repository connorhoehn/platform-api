// social-api/src/ingest/templates/__tests__/index.test.ts
//
// Structural tests for ingestPipelineTemplates: registry shape, definition
// validity, node/edge graph integrity.

import { ingestPipelineTemplates } from '../index';

// ─── Registry ─────────────────────────────────────────────────────────────

describe('ingestPipelineTemplates — registry', () => {
    test('exports exactly 3 templates', () => {
        expect(ingestPipelineTemplates).toHaveLength(3);
    });

    test('each template has required top-level fields', () => {
        for (const t of ingestPipelineTemplates) {
            expect(typeof t.id).toBe('string');
            expect(t.id.length).toBeGreaterThan(0);
            expect(typeof t.name).toBe('string');
            expect(typeof t.description).toBe('string');
            expect(['knowledge-base', 'review', 'transform']).toContain(t.category);
            expect(t.definition).toBeDefined();
        }
    });

    test('template ids are unique', () => {
        const ids = ingestPipelineTemplates.map((t) => t.id);
        expect(new Set(ids).size).toBe(ids.length);
    });

    test('expected template ids are present', () => {
        const ids = ingestPipelineTemplates.map((t) => t.id);
        expect(ids).toContain('transcript-to-stakeholder-registry');
        expect(ids).toContain('transcript-to-coe');
        expect(ids).toContain('agentic-document-review');
    });
});

// ─── Definition shape ─────────────────────────────────────────────────────

describe('ingestPipelineTemplates — definition shape', () => {
    test('each definition has required fields', () => {
        for (const t of ingestPipelineTemplates) {
            const d = t.definition;
            expect(typeof d.id).toBe('string');
            expect(typeof d.name).toBe('string');
            expect(typeof d.version).toBe('number');
            expect(d.status).toBe('published');
            expect(Array.isArray(d.nodes)).toBe(true);
            expect(Array.isArray(d.edges)).toBe(true);
            expect(typeof d.createdAt).toBe('string');
            expect(typeof d.updatedAt).toBe('string');
            expect(d.createdBy).toBe('system');
        }
    });

    test('each definition has at least one node', () => {
        for (const t of ingestPipelineTemplates) {
            expect(t.definition.nodes.length).toBeGreaterThan(0);
        }
    });

    test('each definition has at least one edge', () => {
        for (const t of ingestPipelineTemplates) {
            expect(t.definition.edges.length).toBeGreaterThan(0);
        }
    });

    test('each node has id, type, position, and data', () => {
        for (const t of ingestPipelineTemplates) {
            for (const n of t.definition.nodes) {
                expect(typeof n.id).toBe('string');
                expect(typeof n.type).toBe('string');
                expect(typeof n.position.x).toBe('number');
                expect(typeof n.position.y).toBe('number');
                expect(n.data).toBeDefined();
            }
        }
    });

    test('node ids are unique within each definition', () => {
        for (const t of ingestPipelineTemplates) {
            const ids = t.definition.nodes.map((n) => n.id);
            expect(new Set(ids).size).toBe(ids.length);
        }
    });

    test('each edge references valid node ids', () => {
        for (const t of ingestPipelineTemplates) {
            const nodeIds = new Set(t.definition.nodes.map((n) => n.id));
            for (const e of t.definition.edges) {
                expect(nodeIds.has(e.source)).toBe(true);
                expect(nodeIds.has(e.target)).toBe(true);
            }
        }
    });

    test('edge ids are unique within each definition', () => {
        for (const t of ingestPipelineTemplates) {
            const ids = t.definition.edges.map((e) => e.id);
            expect(new Set(ids).size).toBe(ids.length);
        }
    });
});

// ─── Trigger nodes ────────────────────────────────────────────────────────

describe('ingestPipelineTemplates — trigger nodes', () => {
    test('each definition has exactly one trigger node', () => {
        for (const t of ingestPipelineTemplates) {
            const triggers = t.definition.nodes.filter((n) => n.type === 'trigger');
            expect(triggers).toHaveLength(1);
        }
    });

    test('trigger nodes have document.finalize trigger type', () => {
        for (const t of ingestPipelineTemplates) {
            const trigger = t.definition.nodes.find((n) => n.type === 'trigger')!;
            expect((trigger.data as { triggerType: string }).triggerType).toBe('document.finalize');
        }
    });

    test('each definition has a matching triggerBinding', () => {
        for (const t of ingestPipelineTemplates) {
            expect(t.definition.triggerBinding).toBeDefined();
            expect(t.definition.triggerBinding!.event).toBe('document.finalize');
        }
    });
});

// ─── Specific templates ────────────────────────────────────────────────────

describe('ingestPipelineTemplates — stakeholder-registry template', () => {
    const tpl = () => ingestPipelineTemplates.find((t) => t.id === 'transcript-to-stakeholder-registry')!;

    test('has category knowledge-base', () => {
        expect(tpl().category).toBe('knowledge-base');
    });

    test('definition has 4 nodes (trigger, llm, transform, action)', () => {
        expect(tpl().definition.nodes).toHaveLength(4);
    });

    test('includes an llm node', () => {
        const types = tpl().definition.nodes.map((n) => n.type);
        expect(types).toContain('llm');
    });
});

describe('ingestPipelineTemplates — agentic-document-review template', () => {
    const tpl = () => ingestPipelineTemplates.find((t) => t.id === 'agentic-document-review')!;

    test('has category review', () => {
        expect(tpl().category).toBe('review');
    });

    test('includes fork and join nodes', () => {
        const types = tpl().definition.nodes.map((n) => n.type);
        expect(types).toContain('fork');
        expect(types).toContain('join');
    });

    test('includes a condition node', () => {
        const types = tpl().definition.nodes.map((n) => n.type);
        expect(types).toContain('condition');
    });
});
