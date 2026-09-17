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
  kinds: Array<{ kind: string; source: string; description?: string; reads?: string; emits?: string }>;
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
const described = (kind: string, extra: Record<string, string>) =>
  ({ kind, execute: async () => ({}), ...extra }) as never;

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

  // A registered kind used to render the same generic paragraph as every other
  // one, in the editor and to any agent reading the catalog. The handler now
  // carries its own description, next to the code that implements it.
  it('carries a handler description through to the response', () => {
    registerCustomStepHandler(
      described('document:review-plan', {
        description: 'Decides whether a person has to see the edit first.',
        reads: 'The planned operations.',
        emits: 'needsApproval.',
      }),
    );
    const entry = get().kinds.find((k) => k.kind === 'document:review-plan');
    expect(entry?.description).toBe('Decides whether a person has to see the edit first.');
    expect(entry?.reads).toBe('The planned operations.');
    expect(entry?.emits).toBe('needsApproval.');
  });

  it('omits the fields entirely for a handler that does not describe itself', () => {
    registerCustomStepHandler(stub('people:search'));
    const entry = get().kinds.find((k) => k.kind === 'people:search');
    expect(entry).toBeDefined();
    expect(entry).not.toHaveProperty('description');
    expect(entry).not.toHaveProperty('reads');
    expect(entry).not.toHaveProperty('emits');
  });
});