import { describe, it, expect } from '@jest/globals';
import { resolveStoredRun } from '../storedRunLookup';
import type { PipelineRunItem } from '../../repositories/PipelineRunRepository';

function repo(rows: Array<Partial<PipelineRunItem>>) {
  const reads: string[] = [];
  return {
    reads,
    get: async (pipelineId: string, runId: string) => {
      reads.push(`${pipelineId}/${runId}`);
      return (rows.find((r) => r.pipelineId === pipelineId && r.runId === runId) as PipelineRunItem | undefined) ?? null;
    },
  };
}

describe('resolveStoredRun (NFR #136)', () => {
  it('answers a run with one keyed read', async () => {
    const r = repo([{ pipelineId: 'p', runId: 'r1', status: 'completed', startedAt: '2026-09-23T10:00:00Z' } as PipelineRunItem]);
    const snap = await resolveStoredRun('p', 'r1', r);
    expect(snap).toMatchObject({ runId: 'r1', status: 'completed' });
    expect(r.reads).toEqual(['p/r1']);
  });

  it('is null for a run that is not there', async () => {
    expect(await resolveStoredRun('p', 'nope', repo([]))).toBeNull();
  });

  it("follows an /agent loop's accept-time row to the executor's settled run, under the asked id", async () => {
    const r = repo([
      { pipelineId: 'chat-agent-loop', runId: 'loop-1', status: 'pending', agentLoopRunId: 'exec-1' },
      { pipelineId: 'chat-agent-loop', runId: 'exec-1', status: 'completed', completedAt: '2026-09-23T10:05:00Z' },
    ] as PipelineRunItem[]);
    const snap = await resolveStoredRun('chat-agent-loop', 'loop-1', r);
    expect(snap).toMatchObject({ runId: 'loop-1', executorRunId: 'exec-1', status: 'completed' });
  });

  it("prefers the executor's live snapshot while the bridge holds it", async () => {
    const r = repo([{ pipelineId: 'chat-agent-loop', runId: 'loop-1', status: 'pending', agentLoopRunId: 'exec-1' }] as PipelineRunItem[]);
    const snap = await resolveStoredRun('chat-agent-loop', 'loop-1', {
      ...r,
      live: async (id) => (id === 'exec-1' ? { runId: 'exec-1', status: 'running' } : null),
    });
    expect(snap).toEqual({ runId: 'loop-1', status: 'running', executorRunId: 'exec-1' });
  });

  it('a loop still waiting on its planner (no executor yet) answers its own row', async () => {
    const r = repo([{ pipelineId: 'chat-agent-loop', runId: 'loop-2', status: 'pending' }] as PipelineRunItem[]);
    expect(await resolveStoredRun('chat-agent-loop', 'loop-2', r)).toMatchObject({ runId: 'loop-2', status: 'pending' });
  });
});
