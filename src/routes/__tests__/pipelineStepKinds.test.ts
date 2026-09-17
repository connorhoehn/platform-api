// Tests for GET /api/pipelines/step-kinds.
//
// The catalog is what an agent reads to find out which strings are legal in
// `node.type`, so the shape of this response is a contract.

import { pipelineStepKindsRouter } from '../pipelineStepKinds';
import {
  clearCustomStepHandlers,
  registerCustomStepHandler,
  listStepKinds,
} from '../../pipeline/customStepHandlers';
import type { Request, Response } from 'express';

interface Body {
  kinds: Array<{ kind: string; source: string }>;
  builtin: string[];
  registered: string[];
}

function get(): Body {
  let captured: Body | undefined;
  const res = { json: (b: unknown) => { captured = b as Body; return res; } } as unknown as Response;
  const layer = (pipelineStepKindsRouter as unknown as {
    stack: Array<{ route?: { stack: Array<{ handle: (req: Request, res: Response) => void }> } }>;
  }).stack[0];
  layer.route!.stack[0].handle({} as Request, res);
  return captured as Body;
}

const stub = (kind: string) => ({ kind, execute: async () => ({}) }) as never;

describe('GET /api/pipelines/step-kinds', () => {
  afterEach(() => clearCustomStepHandlers());

  it('always lists the canonical node types as builtin', () => {
    const body = get();
    expect(body.builtin).toEqual(
      expect.arrayContaining(['trigger', 'agent', 'llm', 'condition', 'approval']),
    );
  });

  it('lists a registered handler as registered', () => {
    registerCustomStepHandler(stub('people:search'));
    const body = get();
    expect(body.registered).toContain('people:search');
    expect(body.builtin).not.toContain('people:search');
  });

  it('returns kinds sorted, so the response is diffable', () => {
    registerCustomStepHandler(stub('zzz:last'));
    registerCustomStepHandler(stub('aaa:first'));
    const kinds = get().kinds.map((k) => k.kind);
    expect(kinds).toEqual([...kinds].sort());
  });

  it('does not list an overridable canonical type twice', () => {
    // `agent` is the one canonical type a custom handler may replace.
    registerCustomStepHandler(stub('agent'));
    const kinds = listStepKinds().filter((k) => k.kind === 'agent');
    expect(kinds).toHaveLength(1);
    expect(kinds[0].source).toBe('builtin');
  });

  it('splits the two lists to cover every kind exactly once', () => {
    registerCustomStepHandler(stub('people:search'));
    const body = get();
    expect(body.builtin.length + body.registered.length).toBe(body.kinds.length);
  });
});
