/**
 * realtime-examples NFR #172: a run its owner's work graph shows you is a run
 * you may READ over REST too — the run page "Open run" leads to — as it is
 * one you may watch live. Owner actions stay the owner's; a run no graph
 * shows stays hidden.
 */
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() } }));

import { assertRunAccess, setRunAccessDeps, type RunAccessDeps } from '../runAccess';

const req = (sub: string) => ({
  user: { sub, orgId: 'org-1' },
  headers: { authorization: 'Bearer x' },
  header: () => undefined,
  body: {},
  method: 'GET',
  path: '/api/pipelines/p/runs/run-1',
}) as any;

function deps(shown: Set<string>, asked: string[] = []): RunAccessDeps {
  return {
    liveRun: async () => null,
    storedRun: async (_pid, runId) => (runId === 'run-1' ? { ownerId: 'dev-connor', pipelineId: 'p' } : null),
    documentRow: async () => null,
    sharedThroughGraph: async (user, runId) => { asked.push(`${user.sub}:${runId}`); return shown.has(`${user.sub}:${runId}`); },
  };
}

afterEach(() => setRunAccessDeps(null));

describe('run read through a shared work graph (NFR #172)', () => {
  it('a run the graph shows is readable', async () => {
    setRunAccessDeps(deps(new Set(['dev-frank:run-1'])));
    await expect(assertRunAccess(req('dev-frank'), 'p', 'run-1', 'read')).resolves.toMatchObject({ role: 'reader', ownerId: 'dev-connor' });
  });
  it('a run no graph shows stays hidden', async () => {
    setRunAccessDeps(deps(new Set()));
    await expect(assertRunAccess(req('dev-mallory'), 'p', 'run-1', 'read')).rejects.toThrow('run not found');
  });
  it('owner actions do not ask the graph and stay refused', async () => {
    const asked: string[] = [];
    setRunAccessDeps(deps(new Set(['dev-frank:run-1']), asked));
    await expect(assertRunAccess(req('dev-frank'), 'p', 'run-1', 'owner')).rejects.toBeTruthy();
    expect(asked).toEqual([]);
  });
});
