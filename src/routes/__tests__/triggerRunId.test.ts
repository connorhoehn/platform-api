// A server-side caller may choose the run's id (run-draft dispatch records it
// before triggering, so a takeover can adopt the run by id). The id is
// validated, forwarded to the engine, and never read from the HTTP body.

jest.mock('../../pipeline/audit-repository', () => ({ auditRepo: { record: jest.fn(async () => undefined) } }));
jest.mock('../../pipeline/definitions-repository', () => ({ definitionsRepo: { get: jest.fn(async () => null) } }));

import { triggerPipelineRun, CALLER_RUN_ID, type BridgeTriggerArgs } from '../pipelineTriggers';
import { setPipelineBridge, type PipelineBridge } from '../../pipeline/bridge-types';
import { withContext } from '../../lib/logger';

const definition = { id: 'p1', name: 'p1', nodes: [], edges: [] } as never;
const reqLog = withContext({ test: 'triggerRunId' });

describe('triggerPipelineRun — caller-chosen runId', () => {
  const seen: BridgeTriggerArgs[] = [];
  beforeEach(() => {
    seen.length = 0;
    setPipelineBridge({
      trigger: async (args: BridgeTriggerArgs) => { seen.push(args); return { runId: args.runId ?? 'engine-minted' }; },
    } as unknown as PipelineBridge);
  });
  afterAll(() => setPipelineBridge(null));

  it('forwards a valid id to the engine and answers with it', async () => {
    const out = await triggerPipelineRun({ pipelineId: 'p1', userId: 'u1', body: { definition }, reqLog, runId: '5b0f6c1e-3d8a-4c1b-9d2e-7a6b5c4d3e2f' });
    expect(out.runId).toBe('5b0f6c1e-3d8a-4c1b-9d2e-7a6b5c4d3e2f');
    expect(seen[0]!.runId).toBe('5b0f6c1e-3d8a-4c1b-9d2e-7a6b5c4d3e2f');
  });

  it('refuses a malformed id before touching the engine', async () => {
    for (const bad of ['short', 'has space inside', 'a:b:c:d:e', 'x'.repeat(129)]) {
      await expect(triggerPipelineRun({ pipelineId: 'p1', userId: 'u1', body: { definition }, reqLog, runId: bad })).rejects.toThrow('runId must be');
    }
    expect(seen).toHaveLength(0);
    expect(CALLER_RUN_ID.test('run-draft-123')).toBe(true);
  });

  it('a runId inside the request body is not an id request: the engine mints one', async () => {
    const out = await triggerPipelineRun({ pipelineId: 'p1', userId: 'u1', body: { definition, runId: 'client-picked-123' } as never, reqLog });
    expect(out.runId).toBe('engine-minted');
    expect(seen[0]!.runId).toBeUndefined();
  });
});
