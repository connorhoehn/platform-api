// POST /api/v2/pipelines/:pipelineId/runs ran the body's definition with no
// tool check, so a signed-in user could post a devbox agent step requesting
// `Bash` and get a shell in a devbox microVM (security verification, item 3).
// The v2 route now refuses it exactly as the v1 inline trigger and the save
// path do; a stored, already-validated definition still runs.
import express from 'express';
import type { AddressInfo } from 'net';
import type { Server } from 'http';
import { createPipelineRouter, type PipelineRouterDeps } from '../createPipelineRouter';
import type { InlinePipelineDefinition } from '../contracts';

const devboxBashStep = {
  id: 'agent-1',
  type: 'agent',
  data: { runtime: 'devbox', prompt: 'do it', allowedTools: ['Bash'] },
};

const storedGenerated: InlinePipelineDefinition = {
  id: 'p-1',
  nodes: [{ id: 'gen-1', type: 'agent', data: { runtime: 'devbox', allowedTools: ['Read', 'Bash'] } }],
  edges: [],
};

let server: Server;
let base: string;
const trigger = jest.fn<ReturnType<PipelineRouterDeps['trigger']>, Parameters<PipelineRouterDeps['trigger']>>();

beforeAll(async () => {
  const app = express();
  app.use(express.json());
  app.use(
    '/api/v2',
    createPipelineRouter({
      getAuthContext: () => ({ userId: 'alice' }),
      getDefinition: async (_userId, pipelineId) => (pipelineId === 'p-1' ? storedGenerated : null),
      trigger,
    }),
  );
  server = await new Promise<Server>((resolve) => {
    const s = app.listen(0, '127.0.0.1', () => resolve(s));
  });
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterAll(async () => {
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

beforeEach(() => {
  trigger.mockReset();
  trigger.mockResolvedValue({ runId: 'run-1' });
});

async function post(pipelineId: string, body: unknown) {
  const res = await fetch(`${base}/api/v2/pipelines/${pipelineId}/runs`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: (await res.json()) as Record<string, unknown> };
}

describe('POST /api/v2/pipelines/:id/runs — no tools in a caller\'s definition', () => {
  it('refuses a body definition with a devbox agent step requesting Bash, before the trigger', async () => {
    const res = await post('p-1', { definition: { id: 'p-1', nodes: [devboxBashStep], edges: [] } });
    expect(res.status).toBe(400);
    expect(res.body).toEqual({
      action: 'trigger',
      error: 'agent steps in a pipeline you write may not request tools: agent-1 (Bash)',
    });
    expect(trigger).not.toHaveBeenCalled();
  });

  it('refuses the CLI-named `tools` field too', async () => {
    const step = { id: 'x', type: 'llm', data: { tools: ['Bash'] } };
    const res = await post('p-9', { definition: { id: 'p-9', nodes: [step] } });
    expect(res.status).toBe(400);
    expect(trigger).not.toHaveBeenCalled();
  });

  it('runs a body definition with no tool requests', async () => {
    const def = { id: 'p-2', nodes: [{ id: 'a', type: 'agent', data: { prompt: 'hi' } }], edges: [] };
    const res = await post('p-2', { definition: def });
    expect(res.status).toBe(202);
    expect(trigger).toHaveBeenCalledWith(expect.objectContaining({ pipelineId: 'p-2', definition: def }));
  });

  it('still runs a stored, already-validated definition when the body has none', async () => {
    const res = await post('p-1', {});
    expect(res.status).toBe(202);
    expect(res.body).toMatchObject({ runId: 'run-1', pipelineId: 'p-1' });
    expect(trigger).toHaveBeenCalledWith(expect.objectContaining({ pipelineId: 'p-1', definition: storedGenerated }));
  });
});
