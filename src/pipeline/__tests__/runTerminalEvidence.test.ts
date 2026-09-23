// NFR #101: the orphan and resume scans never re-run a run that already
// finished somewhere — deck-revise 15655b83's row said `running` after it
// completed, and the next restart resumed it.

import { describe, it, expect, jest } from '@jest/globals';
import {
  auditEvidence,
  checkpointEvidence,
  composeTerminalEvidence,
  deckReviseEvidence,
} from '../runTerminalEvidence';
import { scanOrphanedRuns } from '../scanOrphanedRuns';
import { resumeInterruptedRuns } from '../resumeInterruptedRuns';
import { readRunRequest } from '../../deck/deckRevisePipeline';
import type { PipelineRunItem } from '../../repositories/PipelineRunRepository';

const T = '2026-09-23T11:32:00.000Z';
// trigger -> load -> propose -> write (write is the sink).
const DEF = {
  id: 'deck-revise',
  nodes: [],
  edges: [
    { id: 'e1', source: 'trigger', sourceHandle: 'out', target: 'load', targetHandle: 'in' },
    { id: 'e2', source: 'load', sourceHandle: 'out', target: 'propose', targetHandle: 'in' },
    { id: 'e3', source: 'propose', sourceHandle: 'out', target: 'write', targetHandle: 'in' },
  ],
};

function run(over: Partial<PipelineRunItem> = {}, frontier = 'write', frontierStatus = 'completed'): PipelineRunItem {
  return {
    pipelineId: 'deck-revise',
    runId: '15655b83',
    status: 'running',
    triggeredBy: 'u1',
    triggerType: 'manual',
    startedAt: T,
    lastEventAt: '2026-09-23T11:33:10.543Z',
    definitionSnapshot: DEF,
    checkpoint: {
      sequenceNumber: 4,
      at: '2026-09-23T11:33:10.543Z',
      context: { trigger: { documentId: 'audit-doc-1', requestId: 'req-1', instruction: 'x' } },
      currentStepIds: [frontier],
      steps: { [frontier]: { status: frontierStatus } },
      completedStepCount: 4,
    },
    ...over,
  };
}

describe('checkpointEvidence', () => {
  it('a completed sink on the frontier means the run finished', () => {
    expect(checkpointEvidence(run())).toMatchObject({ status: 'completed', source: 'checkpoint', at: '2026-09-23T11:33:10.543Z' });
  });
  it('a completed step with successors is mid-run (dc checkpoints before advancing)', () => {
    expect(checkpointEvidence(run({}, 'propose'))).toBeNull();
  });
  it('a sink still running is not finished', () => {
    expect(checkpointEvidence(run({}, 'write', 'running'))).toBeNull();
  });
  it('no definition snapshot or empty frontier is no evidence', () => {
    expect(checkpointEvidence(run({ definitionSnapshot: undefined }))).toBeNull();
    const r = run();
    r.checkpoint!.currentStepIds = [];
    expect(checkpointEvidence(r)).toBeNull();
  });
});

describe('deckReviseEvidence (idempotency by requestId)', () => {
  const find = jest.fn(async (documentId: string, requestId: string) =>
    documentId === 'audit-doc-1' && requestId === 'req-1' ? { id: 'rev-9', createdAt: '2026-09-23T11:33:10.500Z' } : null);
  const check = deckReviseEvidence({ pipelineId: 'deck-revise', readRequest: readRunRequest, findRevisionForRequest: find });

  it('a revision produced for the request finishes the run, even mid-frontier', async () => {
    await expect(check(run({}, 'propose'))).resolves.toMatchObject({ status: 'completed', source: 'idempotency', at: '2026-09-23T11:33:10.500Z' });
  });
  it('no revision for the request: no evidence', async () => {
    const r = run({}, 'propose');
    r.checkpoint!.context = { trigger: { documentId: 'audit-doc-1', requestId: 'req-2' } };
    await expect(check(r)).resolves.toBeNull();
  });
  it('other pipelines are not looked up', async () => {
    find.mockClear();
    await expect(check(run({ pipelineId: 'other' }))).resolves.toBeNull();
    expect(find).not.toHaveBeenCalled();
  });
});

describe('auditEvidence', () => {
  it('a run.completed at or after the row\'s last event counts', async () => {
    const check = auditEvidence(async () => [{ action: 'pipeline.run.completed', timestamp: '2026-09-23T11:33:10.549Z' }]);
    await expect(check(run({}, 'propose'))).resolves.toMatchObject({ status: 'completed', source: 'audit', at: '2026-09-23T11:33:10.549Z' });
  });
  it('a failure older than the row\'s last event (resumed-from-step since) does not', async () => {
    const check = auditEvidence(async () => [{ action: 'pipeline.run.failed', timestamp: '2026-09-23T11:00:00.000Z' }]);
    await expect(check(run({}, 'propose'))).resolves.toBeNull();
  });
});

describe('composeTerminalEvidence', () => {
  it('a check that throws is "unknown", not finished', async () => {
    const check = composeTerminalEvidence(async () => { throw new Error('ddb down'); });
    await expect(check(run({}, 'propose'))).resolves.toBeNull();
  });
});

describe('scanOrphanedRuns with terminal evidence', () => {
  it('writes a finished run terminal instead of interrupted', async () => {
    const put = jest.fn(async (_item: PipelineRunItem) => undefined);
    const repo = { put, listByStatus: jest.fn(async () => [run(), run({ runId: 'live-mid' }, 'propose')]) };
    const result = await scanOrphanedRuns({
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      repository: repo as any,
      now: () => Date.parse(T) + 60_000,
      terminalEvidence: composeTerminalEvidence(),
    });
    expect(result).toMatchObject({ scanned: 2, transitioned: 1, finalized: 1 });
    const written = put.mock.calls.map((c) => c[0]);
    expect(written.find((i) => i.runId === '15655b83')).toMatchObject({
      status: 'completed',
      completedAt: '2026-09-23T11:33:10.543Z',
      finalizedFromEvidence: { source: 'checkpoint' },
    });
    expect(written.find((i) => i.runId === 'live-mid')).toMatchObject({ status: 'interrupted' });
  });
});

describe('resumeInterruptedRuns with terminal evidence', () => {
  it('never claims or resumes a run whose deck revision already exists', async () => {
    const item = run({ status: 'interrupted', interruptedAt: T }, 'propose');
    const put = jest.fn(async (_item: PipelineRunItem) => undefined);
    const claimForResume = jest.fn();
    const resumeRun = jest.fn();
    const result = await resumeInterruptedRuns({
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      repository: { listByStatus: jest.fn(async () => [item]), claimForResume, put } as any,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      module: { resumeRun } as any,
      runPersistence: null,
      definitionsRepo: { get: async () => null },
      nodeId: 'node-b',
      now: () => Date.parse(T) + 60_000,
      terminalEvidence: composeTerminalEvidence(deckReviseEvidence({
        pipelineId: 'deck-revise',
        readRequest: readRunRequest,
        findRevisionForRequest: async () => ({ id: 'rev-9', createdAt: '2026-09-23T11:33:10.500Z' }),
      })),
    });
    expect(result).toMatchObject({ scanned: 1, resumed: 0, skippedFinished: 1 });
    expect(claimForResume).not.toHaveBeenCalled();
    expect(resumeRun).not.toHaveBeenCalled();
    expect(put.mock.calls[0]![0]).toMatchObject({ status: 'completed', finalizedFromEvidence: { source: 'idempotency' } });
  });
});
