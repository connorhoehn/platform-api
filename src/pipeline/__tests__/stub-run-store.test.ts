// social-api/src/pipeline/__tests__/stub-run-store.test.ts
//
// Tests for stubRunStore: put/get, listForPipeline pagination + cursor,
// listActive status filter, cancel auth + state guard, and eviction.

import {
  stubRunStore,
  ACTIVE_STATUSES,
  type StubRunRecord,
  type StubRunStatus,
} from '../stub-run-store';

function record(overrides: Partial<StubRunRecord> = {}): StubRunRecord {
  return {
    runId: 'run-1',
    pipelineId: 'pipe-1',
    userId: 'user-1',
    status: 'running',
    triggeredAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    ...overrides,
  };
}

beforeEach(() => {
  stubRunStore.__resetForTests();
});

// ---------------------------------------------------------------------------
// put / get
// ---------------------------------------------------------------------------

describe('stubRunStore — put and get', () => {
  it('returns undefined for a missing runId', () => {
    expect(stubRunStore.get('nonexistent')).toBeUndefined();
  });

  it('retrieves a record immediately after put', () => {
    const r = record({ runId: 'r1' });
    stubRunStore.put(r);
    expect(stubRunStore.get('r1')).toEqual(r);
  });

  it('overwrites an existing record on re-put', () => {
    stubRunStore.put(record({ runId: 'r1', status: 'running' }));
    stubRunStore.put(record({ runId: 'r1', status: 'completed' }));
    expect(stubRunStore.get('r1')?.status).toBe('completed');
  });
});

// ---------------------------------------------------------------------------
// listForPipeline
// ---------------------------------------------------------------------------

describe('stubRunStore — listForPipeline', () => {
  it('returns only runs for the matching pipelineId + userId', () => {
    stubRunStore.put(record({ runId: 'r1', pipelineId: 'p1', userId: 'u1' }));
    stubRunStore.put(record({ runId: 'r2', pipelineId: 'p2', userId: 'u1' })); // different pipeline
    stubRunStore.put(record({ runId: 'r3', pipelineId: 'p1', userId: 'u2' })); // different user

    const { runs } = stubRunStore.listForPipeline('p1', 'u1');
    expect(runs).toHaveLength(1);
    expect(runs[0]!.runId).toBe('r1');
  });

  it('defaults limit to 20', () => {
    for (let i = 0; i < 25; i++) {
      stubRunStore.put(record({ runId: `r${i}`, pipelineId: 'p1', userId: 'u1', triggeredAt: new Date(i * 1000).toISOString() }));
    }
    const { runs } = stubRunStore.listForPipeline('p1', 'u1');
    expect(runs).toHaveLength(20);
  });

  it('accepts custom limit', () => {
    for (let i = 0; i < 10; i++) {
      stubRunStore.put(record({ runId: `r${i}`, pipelineId: 'p1', userId: 'u1', triggeredAt: new Date(i * 1000).toISOString() }));
    }
    const { runs } = stubRunStore.listForPipeline('p1', 'u1', { limit: 5 });
    expect(runs).toHaveLength(5);
  });

  it('clamps limit at 100', () => {
    for (let i = 0; i < 110; i++) {
      stubRunStore.put(record({ runId: `r${i}`, pipelineId: 'p1', userId: 'u1', triggeredAt: new Date(i * 1000).toISOString() }));
    }
    const { runs } = stubRunStore.listForPipeline('p1', 'u1', { limit: 999 });
    expect(runs).toHaveLength(100);
  });

  it('returns runs newest-first', () => {
    const t = Date.now();
    stubRunStore.put(record({ runId: 'old', pipelineId: 'p1', userId: 'u1', triggeredAt: new Date(t - 10000).toISOString() }));
    stubRunStore.put(record({ runId: 'new', pipelineId: 'p1', userId: 'u1', triggeredAt: new Date(t).toISOString() }));

    const { runs } = stubRunStore.listForPipeline('p1', 'u1');
    expect(runs[0]!.runId).toBe('new');
    expect(runs[1]!.runId).toBe('old');
  });

  it('returns nextCursor when there are more records', () => {
    for (let i = 0; i < 5; i++) {
      stubRunStore.put(record({ runId: `r${i}`, pipelineId: 'p1', userId: 'u1', triggeredAt: new Date(i * 1000).toISOString() }));
    }
    const { runs, nextCursor } = stubRunStore.listForPipeline('p1', 'u1', { limit: 3 });
    expect(runs).toHaveLength(3);
    expect(nextCursor).toBeDefined();
  });

  it('returns undefined nextCursor on the last page', () => {
    for (let i = 0; i < 3; i++) {
      stubRunStore.put(record({ runId: `r${i}`, pipelineId: 'p1', userId: 'u1', triggeredAt: new Date(i * 1000).toISOString() }));
    }
    const { nextCursor } = stubRunStore.listForPipeline('p1', 'u1', { limit: 10 });
    expect(nextCursor).toBeUndefined();
  });

  it('cursor advances to the next page', () => {
    const t = Date.now();
    for (let i = 0; i < 4; i++) {
      stubRunStore.put(record({ runId: `r${i}`, pipelineId: 'p1', userId: 'u1', triggeredAt: new Date(t + i * 1000).toISOString() }));
    }
    const page1 = stubRunStore.listForPipeline('p1', 'u1', { limit: 2 });
    expect(page1.runs).toHaveLength(2);
    expect(page1.nextCursor).toBeDefined();

    const page2 = stubRunStore.listForPipeline('p1', 'u1', { limit: 2, cursor: page1.nextCursor });
    expect(page2.runs).toHaveLength(2);
    // Confirm no overlap
    const ids1 = new Set(page1.runs.map((r) => r.runId));
    for (const r of page2.runs) {
      expect(ids1.has(r.runId)).toBe(false);
    }
  });
});

// ---------------------------------------------------------------------------
// listActive
// ---------------------------------------------------------------------------

describe('stubRunStore — listActive', () => {
  it('returns only runs with active statuses for the given user', () => {
    const activeStatuses = Array.from(ACTIVE_STATUSES);
    const inactiveStatuses: StubRunStatus[] = ['completed', 'failed', 'cancelled'];

    for (const status of activeStatuses) {
      stubRunStore.put(record({ runId: `active-${status}`, userId: 'u1', status }));
    }
    for (const status of inactiveStatuses) {
      stubRunStore.put(record({ runId: `inactive-${status}`, userId: 'u1', status }));
    }
    stubRunStore.put(record({ runId: 'other-user', userId: 'u2', status: 'running' }));

    const result = stubRunStore.listActive('u1');
    expect(result).toHaveLength(activeStatuses.length);
    for (const r of result) {
      expect(ACTIVE_STATUSES.has(r.status)).toBe(true);
    }
  });

  it('returns empty array when no active runs for user', () => {
    stubRunStore.put(record({ userId: 'u1', status: 'completed' }));
    expect(stubRunStore.listActive('u1')).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// cancel
// ---------------------------------------------------------------------------

describe('stubRunStore — cancel', () => {
  it('transitions an active run to cancelled', () => {
    stubRunStore.put(record({ runId: 'r1', userId: 'u1', status: 'running' }));
    const result = stubRunStore.cancel('r1', 'u1');
    expect(result?.status).toBe('cancelled');
  });

  it('stores the reason when provided', () => {
    stubRunStore.put(record({ runId: 'r1', userId: 'u1', status: 'running' }));
    stubRunStore.cancel('r1', 'u1', 'operator intervention');
    expect(stubRunStore.get('r1')?.reason).toBe('operator intervention');
  });

  it('returns null for a non-existent runId', () => {
    expect(stubRunStore.cancel('nope', 'u1')).toBeNull();
  });

  it('returns null when userId does not match (authz guard)', () => {
    stubRunStore.put(record({ runId: 'r1', userId: 'u1', status: 'running' }));
    expect(stubRunStore.cancel('r1', 'u2')).toBeNull();
    expect(stubRunStore.get('r1')?.status).toBe('running'); // unchanged
  });

  it('returns null when the run is already in a terminal status', () => {
    stubRunStore.put(record({ runId: 'r1', userId: 'u1', status: 'completed' }));
    expect(stubRunStore.cancel('r1', 'u1')).toBeNull();
  });

  it('can cancel runs with all active statuses', () => {
    for (const status of ACTIVE_STATUSES) {
      stubRunStore.put(record({ runId: `r-${status}`, userId: 'u1', status }));
      expect(stubRunStore.cancel(`r-${status}`, 'u1')?.status).toBe('cancelled');
    }
  });
});

// ---------------------------------------------------------------------------
// eviction (MAX_STUB_RUNS = 1000)
// ---------------------------------------------------------------------------

describe('stubRunStore — eviction', () => {
  it('evicts the oldest run when MAX_STUB_RUNS (1000) is exceeded', () => {
    const t = Date.now();
    // Fill to max
    for (let i = 0; i < 1000; i++) {
      stubRunStore.put(record({
        runId: `r${i}`,
        triggeredAt: new Date(t + i * 1000).toISOString(),
      }));
    }
    // The oldest one (r0) should still exist before overflow
    expect(stubRunStore.get('r0')).toBeDefined();

    // Adding one more should evict the oldest
    stubRunStore.put(record({
      runId: 'r-new',
      triggeredAt: new Date(t + 1_000_000).toISOString(),
    }));

    expect(stubRunStore.get('r0')).toBeUndefined(); // evicted
    expect(stubRunStore.get('r-new')).toBeDefined(); // kept
  });
});
