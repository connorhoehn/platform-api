// social-api/src/pipeline/__tests__/hintedHandoffQueue.test.ts
//
// Stream 10 — verifies setupHintedHandoffQueue() honours the
// PIPELINE_HINTED_HANDOFF_ENABLED opt-in, constructs/starts the queue with
// the configured dataDir when enabled, returns null when disabled, and that
// bootstrapPipeline() integrates cleanly in both modes.

import { FixtureLLMClient } from 'distributed-core/testing';
import { HintedHandoffQueue } from 'distributed-core';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { bootstrapPipeline } from '../bootstrap';
import { setupHintedHandoffQueue } from '../config/hintedHandoffQueue';

jest.setTimeout(20_000);

/**
 * The `setup*(cluster, opts?)` API takes a live Cluster, but the current
 * implementation only references the cluster as a placeholder for the
 * upstream wiring hook (see file header). For unit-level tests we can pass a
 * minimal stub and exercise the env / opts plumbing directly without booting
 * a full cluster.
 */
function fakeCluster(): import('distributed-core').Cluster {
  return {} as unknown as import('distributed-core').Cluster;
}

function withEnv<T>(
  patch: Record<string, string | undefined>,
  fn: () => Promise<T> | T,
): Promise<T> {
  const saved: Record<string, string | undefined> = {};
  for (const k of Object.keys(patch)) saved[k] = process.env[k];
  for (const [k, v] of Object.entries(patch)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
  const restore = () => {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  };
  return Promise.resolve()
    .then(fn)
    .then(
      (val) => { restore(); return val; },
      (err) => { restore(); throw err; },
    );
}

describe('setupHintedHandoffQueue — opt-in cross-node delivery primitive', () => {
  test('returns null when PIPELINE_HINTED_HANDOFF_ENABLED is unset', async () => {
    const handle = await withEnv(
      { PIPELINE_HINTED_HANDOFF_ENABLED: undefined },
      () => setupHintedHandoffQueue(fakeCluster()),
    );
    expect(handle).toBeNull();
  });

  test('returns null when PIPELINE_HINTED_HANDOFF_ENABLED=false', async () => {
    const handle = await withEnv(
      { PIPELINE_HINTED_HANDOFF_ENABLED: 'false' },
      () => setupHintedHandoffQueue(fakeCluster()),
    );
    expect(handle).toBeNull();
  });

  test('constructs and starts a HintedHandoffQueue when enabled with a tmp path', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pipeline-hh-'));
    try {
      const handle = await withEnv(
        {
          PIPELINE_HINTED_HANDOFF_ENABLED: 'true',
          PIPELINE_HINTED_HANDOFF_PATH: tmpDir,
        },
        () => setupHintedHandoffQueue(fakeCluster()),
      );
      expect(handle).not.toBeNull();
      expect(handle!.queue).toBeInstanceOf(HintedHandoffQueue);
      expect(typeof handle!.stop).toBe('function');
      // Queue should be in a started state — exercise enqueue/size to prove it.
      const id = await handle!.queue.enqueue('offline-target', Buffer.from('hello'));
      expect(typeof id).toBe('string');
      expect(handle!.queue.sizeFor('offline-target')).toBe(1);
      await handle!.stop();
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  test('rejects PIPELINE_HINTED_HANDOFF_MAX_BYTES with a non-positive value', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pipeline-hh-'));
    try {
      await expect(
        withEnv(
          {
            PIPELINE_HINTED_HANDOFF_ENABLED: 'true',
            PIPELINE_HINTED_HANDOFF_PATH: tmpDir,
            PIPELINE_HINTED_HANDOFF_MAX_BYTES: '0',
          },
          () => setupHintedHandoffQueue(fakeCluster()),
        ),
      ).rejects.toThrow(/PIPELINE_HINTED_HANDOFF_MAX_BYTES/);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  test('explicit opts override env (enabled=false even when env says true)', async () => {
    const handle = await withEnv(
      {
        PIPELINE_HINTED_HANDOFF_ENABLED: 'true',
        PIPELINE_HINTED_HANDOFF_PATH: '/tmp/should-not-be-used',
      },
      () => setupHintedHandoffQueue(fakeCluster(), { enabled: false }),
    );
    expect(handle).toBeNull();
  });
});

describe('bootstrapPipeline — HintedHandoffQueue integration', () => {
  test('bootstrap succeeds and shuts down cleanly with the queue enabled', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pipeline-hh-boot-'));
    try {
      const fixture = new FixtureLLMClient(['ok']);
      const { module, nodeId, shutdown } = await withEnv(
        {
          PIPELINE_HINTED_HANDOFF_ENABLED: 'true',
          PIPELINE_HINTED_HANDOFF_PATH: tmpDir,
        },
        () => bootstrapPipeline({ llmClient: fixture }),
      );
      expect(typeof nodeId).toBe('string');
      expect(module.getEventBus()).toBeDefined();
      await expect(shutdown()).resolves.toBeUndefined();
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  test('bootstrap behavior is identical when the queue is disabled', async () => {
    const fixture = new FixtureLLMClient(['ok']);
    const { module, nodeId, shutdown } = await withEnv(
      { PIPELINE_HINTED_HANDOFF_ENABLED: undefined },
      () => bootstrapPipeline({ llmClient: fixture }),
    );
    expect(typeof nodeId).toBe('string');
    expect(module.getEventBus()).toBeDefined();
    await expect(shutdown()).resolves.toBeUndefined();
  });

  // TODO multi-node drain test: simulating an offline target → recovery →
  // automatic drain requires either (a) two real Cluster instances over a
  // network adapter or (b) the upstream DC `attachHintedHandoff(queue)` hook
  // described in config/hintedHandoffQueue.ts. Until one of those exists, a
  // drain-loop test here would be testing the queue itself (already covered
  // by distributed-core), not gateway-side wiring. Skipped intentionally.
});
