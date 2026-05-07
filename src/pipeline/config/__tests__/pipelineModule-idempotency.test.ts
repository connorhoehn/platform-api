// social-api/src/pipeline/config/__tests__/pipelineModule-idempotency.test.ts
//
// Verifies the T2 action-idempotency hook: an action that fires a side-effect
// (simulated by a counter) should not double-execute on retry when the
// idempotency hook returns 'skip' for a dedup-hit.

import { describe, it, expect, beforeEach } from '@jest/globals';

// We import the exported hook and test it directly — no need to boot the
// full PipelineModule stack.
// eslint-disable-next-line import/no-relative-packages
import { onBeforeActionHook } from '../pipelineModule';
import type { OnBeforeActionContext } from 'distributed-core';

function ctx(overrides?: Partial<OnBeforeActionContext>): OnBeforeActionContext {
  return {
    runId: 'run-1',
    nodeId: 'action-node-1',
    attempt: 1,
    actionType: 'webhook',
    idempotencyKey: 'run-1:action-node-1',
    ...overrides,
  };
}

// The module exports a singleton store — reset between tests by patching.
// We can't easily reach the private store, but we CAN observe the hook's
// behaviour directly: first call with a given key → 'proceed', second call
// with the same key → 'skip'.

describe('onBeforeActionHook — per-step dedup', () => {
  it('returns proceed on first call for a key', async () => {
    const result = await onBeforeActionHook(ctx({ idempotencyKey: `rA:nA` }));
    expect(result).toBe('proceed');
  });

  it('returns skip on second call for the same key (dedup hit)', async () => {
    const key = `run-dedup:node-dedup`;
    await onBeforeActionHook(ctx({ idempotencyKey: key }));
    const second = await onBeforeActionHook(ctx({ idempotencyKey: key }));
    expect(second).toBe('skip');
  });

  it('separate keys are independent', async () => {
    const r1 = await onBeforeActionHook(ctx({ idempotencyKey: 'runX:nodeA' }));
    const r2 = await onBeforeActionHook(ctx({ idempotencyKey: 'runX:nodeB' }));
    expect(r1).toBe('proceed');
    expect(r2).toBe('proceed');
  });
});

describe('per-step vs per-attempt key semantics', () => {
  it('per-step key ({runId}:{nodeId}) causes retry to skip', async () => {
    // Simulate: first attempt succeeded (action ran), second attempt is a retry.
    const perStepKey = 'run-ps:action-ps';
    // First attempt fires — proceed.
    await onBeforeActionHook(ctx({ idempotencyKey: perStepKey }));
    // Second attempt (retry) sees same key → skip.
    const retryDecision = await onBeforeActionHook(ctx({
      idempotencyKey: perStepKey,
      attempt: 2,
    }));
    expect(retryDecision).toBe('skip');
  });

  it('per-attempt key ({runId}:{nodeId}:{attempt}) allows retry to proceed', async () => {
    // Each attempt has a unique key, so retries always proceed.
    const attempt1Key = 'run-pa:action-pa:1';
    const attempt2Key = 'run-pa:action-pa:2';
    const r1 = await onBeforeActionHook(ctx({ idempotencyKey: attempt1Key, attempt: 1 }));
    const r2 = await onBeforeActionHook(ctx({ idempotencyKey: attempt2Key, attempt: 2 }));
    expect(r1).toBe('proceed');
    expect(r2).toBe('proceed');
  });
});

describe('side-effect counter — action does not double-execute on retry (integration)', () => {
  it('action fired once even with 2 attempts using per-step key', async () => {
    let callCount = 0;

    // Simulate the action dispatch: first attempt fires side-effect then
    // "succeeds" from the dedup store's perspective.
    const key = 'run-counter:node-counter';

    for (let attempt = 1; attempt <= 3; attempt++) {
      const decision = await onBeforeActionHook(ctx({ idempotencyKey: key, attempt }));
      if (decision === 'proceed') {
        callCount++;
        // Side-effect: webhook fired, counter incremented.
      }
    }

    // Only the first attempt should have proceeded.
    expect(callCount).toBe(1);
  });
});
