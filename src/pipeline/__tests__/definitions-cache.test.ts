// social-api/src/pipeline/__tests__/definitions-cache.test.ts
//
// Unit tests for PipelineDefinitionsCache (hub#1820).

import { describe, it, expect, jest, beforeEach, afterEach } from '@jest/globals';

jest.mock('../../observability/metrics', () => ({
  recordCacheAccess: jest.fn(),
  recordPipelineDefinitionsRefresh: jest.fn(),
}));

jest.mock('../../lib/logger', () => ({
  withContext: () => ({
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  }),
}));

// Mock the definitions-repository so the singleton cache doesn't try DDB.
jest.mock('../definitions-repository', () => ({
  definitionsRepo: {
    listAll: jest.fn(async () => []),
  },
}));

import { PipelineDefinitionsCache } from '../definitions-cache';
import { recordPipelineDefinitionsRefresh } from '../../observability/metrics';
import type { PipelineDefinition } from '../definitions-repository';

function makeWebhookDef(
  id: string,
  webhookPath: string,
  webhookSecret?: string,
): PipelineDefinition {
  return {
    id,
    userId: 'u1',
    name: `Pipeline ${id}`,
    nodes: [],
    edges: [],
    triggerBinding: {
      event: 'webhook',
      webhookPath,
      ...(webhookSecret ? { webhookSecret } : {}),
    },
  } as unknown as PipelineDefinition;
}

function makeManualDef(id: string): PipelineDefinition {
  return {
    id,
    userId: 'u1',
    name: `Pipeline ${id}`,
    nodes: [],
    edges: [],
    triggerBinding: { event: 'manual' },
  } as unknown as PipelineDefinition;
}

describe('PipelineDefinitionsCache', () => {
  let cache: PipelineDefinitionsCache;

  afterEach(() => {
    cache?.stop();
  });

  describe('lifecycle', () => {
    it('isPrimed() is false before any refresh', () => {
      cache = new PipelineDefinitionsCache({
        loader: jest.fn(async () => [] as PipelineDefinition[]),
        intervalMs: 60_000,
      });
      expect(cache.isPrimed()).toBe(false);
    });

    it('start() triggers a refresh and resolves when it completes', async () => {
      const defs = [makeManualDef('p1')];
      cache = new PipelineDefinitionsCache({
        loader: jest.fn(async () => defs),
        intervalMs: 60_000,
      });
      await cache.start();
      expect(cache.isPrimed()).toBe(true);
      expect(cache.all()).toHaveLength(1);
    });

    it('start() is idempotent — second call returns a refresh promise, not a second timer', async () => {
      const loader = jest.fn(async () => [] as PipelineDefinition[]);
      cache = new PipelineDefinitionsCache({ loader, intervalMs: 60_000 });
      await cache.start();
      await cache.start(); // second call: no-op + re-triggers refresh
      expect(loader).toHaveBeenCalledTimes(2); // 1st start + 2nd start re-refreshes
    });

    it('stop() clears the timer safely before start()', () => {
      cache = new PipelineDefinitionsCache({ loader: jest.fn(async () => []), intervalMs: 60_000 });
      expect(() => cache.stop()).not.toThrow();
    });
  });

  describe('refresh()', () => {
    it('populates all() with loaded definitions', async () => {
      const defs = [makeManualDef('p1'), makeManualDef('p2')];
      cache = new PipelineDefinitionsCache({ loader: jest.fn(async () => defs), intervalMs: 60_000 });
      await cache.refresh();
      expect(cache.all()).toHaveLength(2);
    });

    it('advances pipeline_definitions_last_refresh_timestamp gauge after each successful refresh (hub#1008)', async () => {
      const stamp = recordPipelineDefinitionsRefresh as unknown as jest.Mock;
      stamp.mockClear();
      cache = new PipelineDefinitionsCache({
        loader: jest.fn(async () => [makeManualDef('p1')]),
        intervalMs: 60_000,
      });
      await cache.refresh();
      expect(stamp).toHaveBeenCalledTimes(1);
      await cache.refresh();
      expect(stamp).toHaveBeenCalledTimes(2);
    });

    it('does NOT advance pipeline_definitions_last_refresh_timestamp gauge when the loader throws (hub#1008)', async () => {
      const stamp = recordPipelineDefinitionsRefresh as unknown as jest.Mock;
      stamp.mockClear();
      cache = new PipelineDefinitionsCache({
        loader: jest.fn(async () => { throw new Error('boom'); }),
        intervalMs: 60_000,
      });
      await expect(cache.refresh()).rejects.toThrow('boom');
      expect(stamp).not.toHaveBeenCalled();
    });

    it('coalesces concurrent callers onto one in-flight fetch', async () => {
      const loader = jest.fn(async () => [] as PipelineDefinition[]);
      cache = new PipelineDefinitionsCache({ loader, intervalMs: 60_000 });
      await Promise.all([cache.refresh(), cache.refresh(), cache.refresh()]);
      expect(loader).toHaveBeenCalledTimes(1);
    });

    it('keeps previous snapshot when loader throws', async () => {
      const defs = [makeManualDef('p1')];
      let calls = 0;
      cache = new PipelineDefinitionsCache({
        loader: jest.fn(async () => {
          if (++calls === 1) return defs;
          throw new Error('DDB down');
        }),
        intervalMs: 60_000,
      });
      await cache.refresh(); // succeeds, loads p1
      await expect(cache.refresh()).rejects.toThrow('DDB down');
      // Stale snapshot preserved.
      expect(cache.all()).toHaveLength(1);
      // primed is still true (set even on failure).
      expect(cache.isPrimed()).toBe(true);
    });
  });

  describe('getWebhookEntry()', () => {
    beforeEach(() => {
      cache = new PipelineDefinitionsCache({ loader: jest.fn(async () => []), intervalMs: 60_000 });
    });

    it('returns entry for a registered webhook path', () => {
      cache.__setSnapshotForTests([
        makeWebhookDef('p1', '/hooks/my-pipeline', 'secret-abc'),
      ]);
      const entry = cache.getWebhookEntry('/hooks/my-pipeline');
      expect(entry).toBeDefined();
      expect(entry!.pipelineId).toBe('p1');
      expect(entry!.secret).toBe('secret-abc');
    });

    it('returns undefined for unregistered path', () => {
      cache.__setSnapshotForTests([makeWebhookDef('p1', '/hooks/p1')]);
      expect(cache.getWebhookEntry('/hooks/other')).toBeUndefined();
    });

    it('excludes non-webhook trigger bindings from the index', () => {
      cache.__setSnapshotForTests([makeManualDef('p1')]);
      expect(cache.getWebhookEntry('/hooks/p1')).toBeUndefined();
    });

    it('omits secret field when no secret configured', () => {
      cache.__setSnapshotForTests([makeWebhookDef('p1', '/hooks/p1')]);
      const entry = cache.getWebhookEntry('/hooks/p1');
      expect(entry).toBeDefined();
      expect(entry!.secret).toBeUndefined();
    });
  });

  describe('__setSnapshotForTests()', () => {
    it('sets all() and marks isPrimed()', () => {
      cache = new PipelineDefinitionsCache({ loader: jest.fn(async () => []), intervalMs: 60_000 });
      expect(cache.isPrimed()).toBe(false);
      cache.__setSnapshotForTests([makeManualDef('p1')]);
      expect(cache.all()).toHaveLength(1);
      expect(cache.isPrimed()).toBe(true);
    });
  });
});
