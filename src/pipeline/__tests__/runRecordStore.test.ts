// platform-api/src/pipeline/__tests__/runRecordStore.test.ts
//
// The adapter from run-table rows onto pipeline-runtime's `/durable` shapes:
// the resume state it builds must be the one the boot resume always built,
// and the recovery store's claim must be the run table's conditional claim.

import { describe, it, expect, jest } from '@jest/globals';
import type { PipelineDefinition } from 'distributed-core/applications/pipeline';
import type { PipelineRunItem } from '../../repositories/PipelineRunRepository';
import {
  RunRecordRecoveryStore,
  claimedRowOf,
  resumeStateForRow,
  rowOf,
  toDurableRunRecord,
} from '../runRecordStore';
import { resumeInterruptedRuns } from '../resumeInterruptedRuns';

const NOW = '2026-11-04T10:00:00.000Z';
const DEF = { id: 'pipe-1', nodes: [{ id: 'trigger', type: 'trigger' }, { id: 't1', type: 'transform' }], edges: [{ id: 'e', source: 'trigger', target: 't1' }] } as unknown as PipelineDefinition;

const row = (over: Partial<PipelineRunItem> = {}): PipelineRunItem => ({
  pipelineId: 'pipe-1',
  runId: 'run-1',
  status: 'interrupted',
  triggeredBy: 'u1',
  triggerType: 'schedule',
  startedAt: NOW,
  interruptedAt: NOW,
  definitionSnapshot: DEF,
  resumeCount: 2,
  checkpoint: {
    sequenceNumber: 7,
    at: NOW,
    context: { steps: { trigger: { a: 1 } }, a: 1 },
    currentStepIds: ['t1'],
    steps: {
      trigger: { nodeId: 'trigger', status: 'completed', completedAt: NOW },
      t1: { nodeId: 't1', status: 'awaiting_approval', approvals: [{ userId: 'u2', decision: 'approve', at: NOW }] },
    },
    joinStates: { j: [{ sourceNodeId: 'x', output: 1 }] },
    firedJoins: ['k'],
    completedStepCount: 1,
  },
  stepsSummary: { trigger: { status: 'completed', output: { shown: true } } },
  ...over,
});

/** The resume state resumeInterruptedRuns built by hand before it moved onto pipeline-runtime. */
function legacyResumeState(item: PipelineRunItem, replayAttempts: number | undefined) {
  const cp = item.checkpoint!;
  const steps: Record<string, unknown> = {};
  for (const [stepId, raw] of Object.entries(cp.steps ?? {})) {
    const summary = item.stepsSummary?.[stepId];
    const rec = { ...(raw as Record<string, unknown>) };
    if (rec.output === undefined && summary?.output !== undefined) rec.output = summary.output;
    steps[stepId] = rec;
  }
  return {
    runId: item.runId,
    context: cp.context,
    currentStepIds: cp.currentStepIds,
    steps,
    joinStates: cp.joinStates,
    firedJoins: cp.firedJoins,
    completedStepCount: cp.completedStepCount,
    sequenceNumber: cp.sequenceNumber,
    startedAt: item.startedAt,
    triggeredBy: {
      ...(item.triggeredBy && item.triggeredBy !== 'system' ? { userId: item.triggeredBy } : {}),
      triggerType: item.triggerType ?? 'manual',
      payload: {},
    },
    replayAttempts,
  };
}

describe('resumeStateForRow', () => {
  it('is the resume state the boot resume always built', () => {
    const item = row();
    expect(resumeStateForRow(item, DEF)).toEqual(legacyResumeState(item, 2));
  });

  it('a system-triggered run carries no userId; replays can be overridden', () => {
    const item = row({ triggeredBy: 'system' });
    const state = resumeStateForRow(item, DEF, { replays: 5 });
    expect(state.triggeredBy).toEqual({ triggerType: 'schedule', payload: {} });
    expect(state.replayAttempts).toBe(5);
  });

  it('fills absent join bookkeeping with empty values (the executor reads them as empty)', () => {
    const item = row();
    delete item.checkpoint!.joinStates;
    delete item.checkpoint!.firedJoins;
    delete item.checkpoint!.completedStepCount;
    const state = resumeStateForRow(item, DEF);
    expect(state.joinStates).toEqual({});
    expect(state.firedJoins).toEqual([]);
    expect(state.completedStepCount).toBe(0);
  });

  it('does not alias the row: editing the state leaves the checkpoint alone', () => {
    const item = row();
    const state = resumeStateForRow(item, DEF);
    (state.context as Record<string, unknown>)['a'] = 99;
    expect(item.checkpoint!.context['a']).toBe(1);
  });
});

describe('toDurableRunRecord', () => {
  it('maps status, ids, owner and keeps the row in meta', () => {
    const item = row({ status: 'succeeded', resumeClaimedBy: 'node-a' });
    const record = toDurableRunRecord(item, DEF);
    expect(record).toMatchObject({ id: 'run-1', pipelineId: 'pipe-1', status: 'completed', owner: 'node-a', replays: 2, epoch: 2, runSeq: 7 });
    expect(rowOf(record)).toBe(item);
    expect(toDurableRunRecord(row({ status: 'stuck' }), DEF).status).toBe('running');
    expect(toDurableRunRecord(row({ status: 'interrupted' }), DEF).status).toBe('interrupted');
  });
});

describe('RunRecordRecoveryStore', () => {
  const at = Date.parse(NOW) + 1000;

  it('claim is the conditional claimForResume; the claimed row rides along', async () => {
    const item = row();
    const claimed = { ...item, status: 'running' as const, resumeClaimedBy: 'node-b', resumeCount: 3 };
    const claimForResume = jest.fn(async (..._args: unknown[]) => claimed);
    const store = new RunRecordRecoveryStore({ claimForResume }, [toDurableRunRecord(item, DEF)], () => at);
    const record = await store.claim('run-1', 'node-b');
    expect(claimForResume).toHaveBeenCalledWith('pipe-1', 'run-1', 'node-b', new Date(at).toISOString());
    expect(record).toMatchObject({ status: 'running', owner: 'node-b', replays: 3 });
    expect(claimedRowOf(record!)).toBe(claimed);
    expect(rowOf(record!)).toBe(item);
  });

  it('a lost claim is null; a throwing claim is remembered and rethrown', async () => {
    const lost = new RunRecordRecoveryStore({ claimForResume: async () => null }, [toDurableRunRecord(row(), DEF)]);
    expect(await lost.claim('run-1', 'n')).toBeNull();
    expect(lost.claimErrors.size).toBe(0);

    const broken = new RunRecordRecoveryStore({ claimForResume: async () => { throw new Error('ddb down'); } }, [toDurableRunRecord(row(), DEF)]);
    await expect(broken.claim('run-1', 'n')).rejects.toThrow('ddb down');
    expect(broken.claimErrors.get('run-1')).toContain('ddb down');
  });

  it('lists the vetted rows by status; an unknown run is not claimed', async () => {
    const claimForResume = jest.fn(async (..._args: unknown[]) => null);
    const store = new RunRecordRecoveryStore({ claimForResume }, [toDurableRunRecord(row(), DEF), toDurableRunRecord(row({ runId: 'run-2', status: 'failed' }), DEF)]);
    expect((await store.listByStatus(['interrupted'])).map((r) => r.id)).toEqual(['run-1']);
    expect(await store.claim('nope', 'n')).toBeNull();
    expect(claimForResume).not.toHaveBeenCalled();
  });

  it('refuses RunStore transactions — rows are written by RunPersistenceService', async () => {
    const store = new RunRecordRecoveryStore({ claimForResume: async () => null }, []);
    await expect(store.transact()).rejects.toThrow(/RunPersistenceService/);
  });
});

describe('resumeInterruptedRuns on recoverInterruptedRuns', () => {
  const deps = (claimForResume: (...a: unknown[]) => Promise<unknown>, resumeRun: (...a: unknown[]) => Promise<unknown>) => ({
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    repository: { listByStatus: jest.fn(async () => [row()]), claimForResume: jest.fn(claimForResume), put: jest.fn(async () => undefined) } as any,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    module: { resumeRun: jest.fn(resumeRun) } as any,
    runPersistence: null,
    definitionsRepo: { get: async () => null },
    nodeId: 'node-b',
    now: () => Date.parse(NOW) + 60_000,
  });

  it('a claim write that throws counts as failed, not as a lost race', async () => {
    const d = deps(async () => { throw new Error('throttled'); }, async () => ({}));
    const result = await resumeInterruptedRuns(d);
    expect(result).toMatchObject({ scanned: 1, resumed: 0, claimLost: 0, failed: 1 });
    expect(d.module.resumeRun).not.toHaveBeenCalled();
  });

  it('a resume that throws counts as failed and leaves the claimed row alone', async () => {
    const d = deps(async () => ({ ...row(), status: 'running', resumeCount: 3 }), async () => { throw new Error('boom'); });
    const result = await resumeInterruptedRuns(d);
    expect(result).toMatchObject({ resumed: 0, claimLost: 0, failed: 1 });
    expect(d.repository.put).not.toHaveBeenCalled();
  });

  it('resumes with the shared resume state and the claim count as replayAttempts', async () => {
    const d = deps(async () => ({ ...row(), status: 'running', resumeCount: 3 }), async () => ({}));
    const result = await resumeInterruptedRuns(d);
    expect(result).toMatchObject({ resumed: 1, failed: 0 });
    const args = d.module.resumeRun.mock.calls[0]![0] as { definition: unknown; resumeState: unknown };
    expect(args.definition).toBe(DEF);
    expect(args.resumeState).toEqual(legacyResumeState(row(), 3));
  });
});
