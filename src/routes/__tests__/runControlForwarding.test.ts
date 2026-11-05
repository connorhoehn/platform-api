// platform-api/src/routes/__tests__/runControlForwarding.test.ts
//
// forwardRunControl for a resume-from-step no replica acknowledges: with
// `fallBackLocally` (the row has a checkpoint) the control is released and the
// caller rewinds the run here; without it the answer stays 409 run-not-resumable.

jest.mock('../../lib/ddb-client', () => ({ getDynamoClient: jest.fn().mockReturnValue({}) }));

import { describe, it, expect, jest, afterEach } from '@jest/globals';
import { forwardRunControl, setRunControlRunRepo } from '../runControlForwarding';
import type { RunControlForwarder } from '../../pipeline/run-control-forwarder';
import type { PipelineRunRepository } from '../../repositories/PipelineRunRepository';

const row = { pipelineId: 'p1', runId: 'r1', status: 'failed', triggeredBy: 'u1', triggerType: 'manual', startedAt: '2026-11-04T00:00:00.000Z', ownerNodeId: 'gone' };

type Reply = { applied?: unknown; accepted?: unknown };

function harness(reply: Reply = {}, claim: { won: boolean; holder: string | null } | Error = { won: true, holder: 'node-here' }) {
  const release = jest.fn(async (..._args: unknown[]) => undefined);
  const claimRerun = jest.fn(async (..._args: unknown[]) => { if (claim instanceof Error) throw claim; return claim; });
  const forwarder = {
    canApply: () => false,
    forward: jest.fn(async (control: unknown) => ({ message: control, applied: reply.applied ?? null, accepted: reply.accepted ?? null, appliedInMs: null, recorded: true })),
    release,
    claimRerun,
  } as unknown as RunControlForwarder;
  const res = { statusCode: 0, body: undefined as unknown, status(code: number) { this.statusCode = code; return this; }, json(body: unknown) { this.body = body; return this; } };
  const log = { info: jest.fn(), warn: jest.fn(), error: jest.fn() };
  setRunControlRunRepo({ get: async () => row } as unknown as PipelineRunRepository);
  const params = (fallBackLocally?: boolean) => ({
    req: { get: () => undefined }, res, reqLog: log, forwarder, runId: 'r1', userId: 'u1', pipelineId: 'p1',
    allowTerminal: true, awaitAppliedMs: 1,
    ...(fallBackLocally !== undefined ? { fallBackLocally } : {}),
    control: ({ controlId, at }: { controlId: string; at: string }) => ({ kind: 'resume-from-step' as const, controlId, runId: 'r1', userId: 'u1', at, fromNodeId: 'node-B' }),
  });
  return { params, res, release, claimRerun };
}

afterEach(() => setRunControlRunRepo(null));

describe('forwardRunControl — resume-from-step nobody holds', () => {
  it('fallBackLocally: releases the control and hands the request back to the caller', async () => {
    const h = harness();
    expect(await forwardRunControl(h.params(true))).toBe(false);
    expect(h.release).toHaveBeenCalledWith('p1', 'r1', expect.any(String));
    expect(h.res.statusCode).toBe(0);
  });

  it('without it: 409 run-not-resumable, as before', async () => {
    const h = harness();
    expect(await forwardRunControl(h.params())).toBe(true);
    expect(h.release).toHaveBeenCalled();
    expect(h.res.statusCode).toBe(409);
    expect(h.res.body).toMatchObject({ error: 'run-not-resumable' });
  });
});

describe('forwardRunControl — resume-from-step the holder is slow to finish', () => {
  it('accepted by the holder: 202 applied:false with acceptedBy, no claim, no local run', async () => {
    const h = harness({ accepted: { v: 1, type: 'accepted', controlId: 'k', runId: 'r1', nodeId: 'node-a' } });
    expect(await forwardRunControl(h.params(true))).toBe(true);
    expect(h.res.statusCode).toBe(202);
    expect(h.res.body).toMatchObject({ applied: false, acceptedBy: 'node-a' });
    expect(h.claimRerun).not.toHaveBeenCalled();
    expect(h.release).not.toHaveBeenCalled();
  });

  it('no answer in time and another replica holds the claim: 202, no local run', async () => {
    const h = harness({}, { won: false, holder: 'node-a' });
    expect(await forwardRunControl(h.params(true))).toBe(true);
    expect(h.claimRerun).toHaveBeenCalledWith('r1', expect.any(String));
    expect(h.res.statusCode).toBe(202);
    expect(h.res.body).toMatchObject({ applied: false, acceptedBy: 'node-a', duplicate: true });
  });

  it('no answer in time and the claim is won: the caller runs it locally', async () => {
    const h = harness({}, { won: true, holder: 'node-here' });
    expect(await forwardRunControl(h.params(true))).toBe(false);
    expect(h.claimRerun).toHaveBeenCalledTimes(1);
    expect(h.res.statusCode).toBe(0);
  });

  it('no answer in time and the claims table is down: 503, no local run', async () => {
    const h = harness({}, new Error('ddb down'));
    expect(await forwardRunControl(h.params(true))).toBe(true);
    expect(h.res.statusCode).toBe(503);
  });

  it('the holder lost the claim (applied:false, duplicate): 202, no local run', async () => {
    const h = harness({ applied: { v: 1, type: 'applied', controlId: 'k', runId: 'r1', kind: 'resume-from-step', nodeId: 'node-a', ok: true, duplicate: true, applied: false, at: 'x' } });
    expect(await forwardRunControl(h.params(true))).toBe(true);
    expect(h.res.statusCode).toBe(202);
    expect(h.res.body).toMatchObject({ applied: false, acceptedBy: 'node-a' });
    expect(h.claimRerun).not.toHaveBeenCalled();
  });
});
