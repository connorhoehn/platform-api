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

function harness() {
  const release = jest.fn(async (..._args: unknown[]) => undefined);
  const forwarder = {
    canApply: () => false,
    forward: jest.fn(async (control: unknown) => ({ message: control, applied: null, appliedInMs: null, recorded: true })),
    release,
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
  return { params, res, release };
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
