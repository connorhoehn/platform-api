// Tests for the server-side structural gate.
//
// These matter more than most: this is what stands between an agent-authored
// definition and `published`, and it is the only check that runs when nothing
// opens the editor.

import { validateShape, isPublishable } from '../validate-shape';

type Node = { id: string; type?: string };
type Edge = { id: string; source: string; target: string };

function def(nodes: Node[], edges: Edge[] = []): unknown {
  return { id: 'p1', name: 'P', nodes, edges };
}

const trigger: Node = { id: 't', type: 'trigger' };

function codes(result: { errors: { code: string }[]; warnings: { code: string }[] }) {
  return {
    errors: result.errors.map((e) => e.code),
    warnings: result.warnings.map((w) => w.code),
  };
}

describe('validateShape — a sound pipeline', () => {
  it('accepts a wired trigger-to-step chain', () => {
    const result = validateShape(
      def([trigger, { id: 'a' }, { id: 'b' }], [
        { id: 'e1', source: 't', target: 'a' },
        { id: 'e2', source: 'a', target: 'b' },
      ]),
    );
    expect(codes(result)).toEqual({ errors: [], warnings: [] });
    expect(isPublishable(result)).toBe(true);
  });

  it('accepts a diamond', () => {
    const result = validateShape(
      def([trigger, { id: 'a' }, { id: 'b' }, { id: 'c' }], [
        { id: 'e1', source: 't', target: 'a' },
        { id: 'e2', source: 't', target: 'b' },
        { id: 'e3', source: 'a', target: 'c' },
        { id: 'e4', source: 'b', target: 'c' },
      ]),
    );
    expect(isPublishable(result)).toBe(true);
  });
});

describe('validateShape — malformed bodies', () => {
  it('rejects a body with no nodes array', () => {
    expect(codes(validateShape({ id: 'p' })).errors).toEqual(['NODES_MISSING']);
  });

  it('rejects an empty node list', () => {
    expect(codes(validateShape(def([]))).errors).toEqual(['NODES_EMPTY']);
  });

  it('rejects a body with no edges array', () => {
    expect(codes(validateShape({ nodes: [trigger] })).errors).toEqual(['EDGES_MISSING']);
  });

  it('does not throw on null, a string, or a number', () => {
    for (const input of [null, undefined, 'nope', 42]) {
      expect(() => validateShape(input)).not.toThrow();
      expect(isPublishable(validateShape(input))).toBe(false);
    }
  });

  it('rejects a node with no id', () => {
    expect(codes(validateShape(def([trigger, {} as Node]))).errors).toContain('NODE_ID_MISSING');
  });

  it('rejects duplicate node ids', () => {
    const result = validateShape(def([trigger, { id: 'a' }, { id: 'a' }]));
    expect(codes(result).errors).toContain('NODE_ID_DUPLICATE');
  });
});

describe('validateShape — the ways agent-authored graphs break', () => {
  it('rejects an edge pointing at a node that does not exist', () => {
    // The common one: a node gets renamed and its edge is left behind.
    const result = validateShape(
      def([trigger, { id: 'a' }], [{ id: 'e1', source: 't', target: 'gone' }]),
    );
    expect(codes(result).errors).toContain('EDGE_ENDPOINT_UNKNOWN');
    expect(result.errors[0].edgeId).toBe('e1');
  });

  it('names both endpoints so the writer can find the edge', () => {
    const result = validateShape(
      def([trigger], [{ id: 'e1', source: 't', target: 'gone' }]),
    );
    expect(result.errors[0].message).toContain('gone');
  });

  it('rejects a self-loop', () => {
    const result = validateShape(
      def([trigger, { id: 'a' }], [
        { id: 'e1', source: 't', target: 'a' },
        { id: 'e2', source: 'a', target: 'a' },
      ]),
    );
    expect(codes(result).errors).toContain('EDGE_SELF_LOOP');
  });

  it('rejects a pipeline with no trigger', () => {
    const result = validateShape(def([{ id: 'a' }, { id: 'b' }], [
      { id: 'e1', source: 'a', target: 'b' },
    ]));
    expect(codes(result).errors).toContain('TRIGGER_MISSING');
  });

  it('rejects a pipeline with two triggers', () => {
    const result = validateShape(
      def([trigger, { id: 't2', type: 'trigger' }, { id: 'a' }], [
        { id: 'e1', source: 't', target: 'a' },
        { id: 'e2', source: 't2', target: 'a' },
      ]),
    );
    expect(codes(result).errors).toContain('TRIGGER_MULTIPLE');
  });

  it('rejects a cycle', () => {
    const result = validateShape(
      def([trigger, { id: 'a' }, { id: 'b' }], [
        { id: 'e1', source: 't', target: 'a' },
        { id: 'e2', source: 'a', target: 'b' },
        { id: 'e3', source: 'b', target: 'a' },
      ]),
    );
    expect(codes(result).errors).toContain('CYCLE_DETECTED');
  });

  it('names a node on the cycle, deterministically', () => {
    const build = () =>
      validateShape(
        def([trigger, { id: 'a' }, { id: 'b' }], [
          { id: 'e1', source: 'a', target: 'b' },
          { id: 'e2', source: 'b', target: 'a' },
        ]),
      );
    const first = build().errors.find((e) => e.code === 'CYCLE_DETECTED');
    expect(first?.nodeId).toBe('a');
    expect(build().errors.find((e) => e.code === 'CYCLE_DETECTED')?.nodeId).toBe('a');
  });
});

describe('validateShape — advisory only', () => {
  it('warns about an unconnected node but still allows publish', () => {
    const result = validateShape(
      def([trigger, { id: 'a' }, { id: 'lonely' }], [{ id: 'e1', source: 't', target: 'a' }]),
    );
    expect(codes(result).warnings).toEqual(['NODE_ORPHANED']);
    expect(result.warnings[0].nodeId).toBe('lonely');
    expect(isPublishable(result)).toBe(true);
  });

  it('does not call a lone trigger an orphan', () => {
    expect(codes(validateShape(def([trigger]))).warnings).toEqual([]);
  });

  it('reports every orphan, not just the first', () => {
    const result = validateShape(
      def([trigger, { id: 'a' }, { id: 'x' }, { id: 'y' }], [
        { id: 'e1', source: 't', target: 'a' },
      ]),
    );
    expect(result.warnings.map((w) => w.nodeId).sort()).toEqual(['x', 'y']);
  });
});
