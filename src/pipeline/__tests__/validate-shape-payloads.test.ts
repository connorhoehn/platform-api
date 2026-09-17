// Payload checks: what travels down an edge, vs what the far end takes.
//
// Handles say a wire exists. `image:vision-tag.out → image:resize.in` joins two
// real handles and hands a resizer a list of tags — it published, and failed at
// run time holding the wrong thing.

import { validateShape } from '../validate-shape';

const PAYLOADS: Record<string, { consumes?: string; produces?: string }> = {
  'image-source': { produces: 'image' },
  'image:generate': { produces: 'image' },
  'image:resize': { consumes: 'image', produces: 'image' },
  'image:vision-tag': { consumes: 'image', produces: 'data' },
  'image:save': { consumes: 'image', produces: 'data' },
};

const KNOWN = ['trigger', 'action', 'condition', 'join', 'llm', ...Object.keys(PAYLOADS)];

function shape(nodes: unknown[], edges: unknown[]) {
  return validateShape({ nodes, edges }, {
    knownKinds: KNOWN,
    payloadFor: (k) => PAYLOADS[k],
  });
}

const t = { id: 't', type: 'trigger' };

describe('validateShape — edge payloads', () => {
  it('rejects tags fed into a resizer', () => {
    const r = shape(
      [t, { id: 'src', type: 'image-source' }, { id: 'tag', type: 'image:vision-tag' }, { id: 'rz', type: 'image:resize' }],
      [
        { id: 'e1', source: 't', sourceHandle: 'out', target: 'src', targetHandle: 'in' },
        { id: 'e2', source: 'src', sourceHandle: 'out', target: 'tag', targetHandle: 'in' },
        { id: 'e3', source: 'tag', sourceHandle: 'out', target: 'rz', targetHandle: 'in' },
      ],
    );
    const issue = r.errors.find((e) => e.code === 'EDGE_PAYLOAD_MISMATCH');
    expect(issue).toBeDefined();
    expect(issue!.message).toContain('needs an image');
    expect(issue!.message).toContain('with a data object');
    expect(issue!.edgeId).toBe('e3');
  });

  it('accepts an image handed straight to a resizer', () => {
    const r = shape(
      [t, { id: 'src', type: 'image-source' }, { id: 'rz', type: 'image:resize' }],
      [
        { id: 'e1', source: 't', sourceHandle: 'out', target: 'src', targetHandle: 'in' },
        { id: 'e2', source: 'src', sourceHandle: 'out', target: 'rz', targetHandle: 'in' },
      ],
    );
    expect(r.errors.filter((e) => e.code === 'EDGE_PAYLOAD_MISMATCH')).toHaveLength(0);
  });

  it('carries an image through a condition', () => {
    const r = shape(
      [t, { id: 'src', type: 'image-source' }, { id: 'c', type: 'condition' }, { id: 'rz', type: 'image:resize' }],
      [
        { id: 'e1', source: 't', sourceHandle: 'out', target: 'src', targetHandle: 'in' },
        { id: 'e2', source: 'src', sourceHandle: 'out', target: 'c', targetHandle: 'in' },
        { id: 'e3', source: 'c', sourceHandle: 'true', target: 'rz', targetHandle: 'in' },
      ],
    );
    expect(r.errors.filter((e) => e.code === 'EDGE_PAYLOAD_MISMATCH')).toHaveLength(0);
  });

  it('still catches data routed through a condition', () => {
    const r = shape(
      [t, { id: 'src', type: 'image-source' }, { id: 'tag', type: 'image:vision-tag' }, { id: 'c', type: 'condition' }, { id: 'rz', type: 'image:resize' }],
      [
        { id: 'e1', source: 't', sourceHandle: 'out', target: 'src', targetHandle: 'in' },
        { id: 'e2', source: 'src', sourceHandle: 'out', target: 'tag', targetHandle: 'in' },
        { id: 'e3', source: 'tag', sourceHandle: 'out', target: 'c', targetHandle: 'in' },
        { id: 'e4', source: 'c', sourceHandle: 'true', target: 'rz', targetHandle: 'in' },
      ],
    );
    expect(r.errors.some((e) => e.code === 'EDGE_PAYLOAD_MISMATCH' && e.edgeId === 'e4')).toBe(true);
  });

  it('says nothing when a join is fed two different classes', () => {
    const r = shape(
      [t, { id: 'src', type: 'image-source' }, { id: 'tag', type: 'image:vision-tag' }, { id: 'j', type: 'join' }, { id: 'rz', type: 'image:resize' }],
      [
        { id: 'e1', source: 't', sourceHandle: 'out', target: 'src', targetHandle: 'in' },
        { id: 'e2', source: 'src', sourceHandle: 'out', target: 'tag', targetHandle: 'in' },
        { id: 'e3', source: 'src', sourceHandle: 'out', target: 'j', targetHandle: 'in-0' },
        { id: 'e4', source: 'tag', sourceHandle: 'out', target: 'j', targetHandle: 'in-1' },
        { id: 'e5', source: 'j', sourceHandle: 'out', target: 'rz', targetHandle: 'in' },
      ],
    );
    expect(r.errors.filter((e) => e.code === 'EDGE_PAYLOAD_MISMATCH')).toHaveLength(0);
  });

  it('lets an LLM prompt drive image:generate, which makes pixels from text', () => {
    const r = shape(
      [t, { id: 'l', type: 'llm' }, { id: 'g', type: 'image:generate' }],
      [
        { id: 'e1', source: 't', sourceHandle: 'out', target: 'l', targetHandle: 'in' },
        { id: 'e2', source: 'l', sourceHandle: 'out', target: 'g', targetHandle: 'in' },
      ],
    );
    expect(r.errors.filter((e) => e.code === 'EDGE_PAYLOAD_MISMATCH')).toHaveLength(0);
  });

  it('does not guess about a kind that declares nothing', () => {
    const r = shape(
      [t, { id: 'x', type: 'catalog:upsert' }, { id: 'rz', type: 'image:resize' }],
      [
        { id: 'e1', source: 't', sourceHandle: 'out', target: 'x', targetHandle: 'in' },
        { id: 'e2', source: 'x', sourceHandle: 'out', target: 'rz', targetHandle: 'in' },
      ],
    );
    expect(r.errors.filter((e) => e.code === 'EDGE_PAYLOAD_MISMATCH')).toHaveLength(0);
  });
});
