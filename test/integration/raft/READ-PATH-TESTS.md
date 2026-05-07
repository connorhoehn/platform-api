# Raft Read Path Integration Tests (Task #961)

**Status:** Test specifications complete, implementation pending  
**Priority:** High — required before production Raft deployment

## Overview

These tests validate distributed-core's Raft read paths (lease reads + ReadIndex protocol) work correctly when wired into social-api. Tests must run against a real 3-node Raft cluster (not mocks).

## Test Environment Setup

```javascript
// test/integration/raft/helpers/raftClusterHarness.js

class RaftClusterHarness {
  constructor() {
    this.nodes = [];  // Array of { cluster, module, nodeId, port }
  }

  async start(numNodes = 3) {
    // Bootstrap N social-api instances with:
    //   PIPELINE_REGISTRY_MODE=raft
    //   PIPELINE_CLUSTER_TRANSPORT=in-memory (fast for tests)
    //   PIPELINE_RAFT_DATA_DIR=/tmp/raft-test-${nodeId}
    //
    // Each node gets a unique HTTP port (3001, 3002, 3003)
    // Store { cluster, module, nodeId, port } in this.nodes[]
  }

  async stop() {
    // Call cluster.stop() on all nodes
    // Clean up /tmp/raft-test-* directories
  }

  getLeader() {
    // Poll cluster.registry.raftNode.role across all nodes
    // Return the node where role === 'leader'
  }

  getFollower() {
    // Return a node where role === 'follower'
  }

  async waitForElection() {
    // Poll until one node has role === 'leader'
    // Timeout after 5 seconds
  }

  async waitForLogSync(maxLagEntries = 10) {
    // Poll until all followers' lastApplied is within maxLagEntries of leader's commitIndex
    // Timeout after 10 seconds
  }

  async killNode(nodeId) {
    // Call cluster.stop() on the specified node
    // Remove from this.nodes[]
  }

  async partitionNode(nodeId) {
    // Simulate network partition: drop all inbound/outbound messages to/from nodeId
    // (In-memory transport: intercept sendMessage calls in PeerMessaging)
  }

  async healPartition(nodeId) {
    // Restore message delivery to/from nodeId
  }
}
```

## Test 1: Lease Reads (leaseReadEnabled=true)

**File:** `test/integration/raft/lease-reads.test.ts`

**Purpose:** Verify that reads on the leader don't require quorum round-trip when leaseReadEnabled=true.

```typescript
describe('Raft lease reads', () => {
  let harness: RaftClusterHarness;

  beforeAll(async () => {
    harness = new RaftClusterHarness();
    await harness.start(3);  // 3-node cluster
    await harness.waitForElection();
  });

  afterAll(async () => {
    await harness.stop();
  });

  it('should serve reads from leader without quorum when lease is valid', async () => {
    const leader = harness.getLeader();
    
    // Write: create a pipeline (goes through Raft consensus)
    const writeResponse = await fetch(`http://localhost:${leader.port}/api/pipelines`, {
      method: 'POST',
      body: JSON.stringify({ name: 'test-pipeline', trigger: { event: 'manual' } }),
      headers: { 'Content-Type': 'application/json' }
    });
    expect(writeResponse.status).toBe(201);
    const { id: pipelineId } = await writeResponse.json();

    await harness.waitForLogSync();  // Ensure followers caught up

    // Read: query pipeline status from leader
    const startMs = Date.now();
    const readResponse = await fetch(`http://localhost:${leader.port}/api/pipelines/${pipelineId}`);
    const latencyMs = Date.now() - startMs;

    expect(readResponse.status).toBe(200);
    const pipeline = await readResponse.json();
    expect(pipeline.id).toBe(pipelineId);

    // Lease reads should be fast (<10ms) because no quorum round-trip
    expect(latencyMs).toBeLessThan(50);

    // TODO: Verify that no AppendEntries RPCs were sent during the read
    // (check cluster.registry.raftNode metrics or mock PeerMessaging)
  });

  it('should allow ~50ms staleness on follower lease reads', async () => {
    const leader = harness.getLeader();
    const follower = harness.getFollower();

    // Write on leader
    const writeResponse = await fetch(`http://localhost:${leader.port}/api/pipelines`, {
      method: 'POST',
      body: JSON.stringify({ name: 'test-pipeline-2', trigger: { event: 'manual' } }),
      headers: { 'Content-Type': 'application/json' }
    });
    const { id: pipelineId } = await writeResponse.json();

    // Immediately read from follower (before log syncs)
    const readResponse = await fetch(`http://localhost:${follower.port}/api/pipelines/${pipelineId}`);

    // With leaseReadEnabled, follower MAY serve stale read (up to electionTimeout - maxClockDriftMs)
    // If read returns 404, that's acceptable staleness
    // If read returns 200, verify data is eventually consistent within 50ms

    if (readResponse.status === 404) {
      // Stale read — acceptable
      expect(true).toBe(true);
    } else {
      expect(readResponse.status).toBe(200);
    }

    // After log sync, follower MUST return the pipeline
    await harness.waitForLogSync();
    const syncedReadResponse = await fetch(`http://localhost:${follower.port}/api/pipelines/${pipelineId}`);
    expect(syncedReadResponse.status).toBe(200);
  });
});
```

## Test 2: Linearizability (Concurrent Writes)

**File:** `test/integration/raft/linearizability.test.ts`

**Purpose:** Verify that concurrent approve/cancel requests on different nodes produce a serializable history (no split-brain).

```typescript
describe('Raft linearizability', () => {
  let harness: RaftClusterHarness;

  beforeAll(async () => {
    harness = new RaftClusterHarness();
    await harness.start(3);
    await harness.waitForElection();
  });

  afterAll(async () => {
    await harness.stop();
  });

  it('should serialize concurrent approvals on different nodes', async () => {
    // Create a pipeline that requires approval
    const leader = harness.getLeader();
    const createResponse = await fetch(`http://localhost:${leader.port}/api/pipelines`, {
      method: 'POST',
      body: JSON.stringify({
        name: 'approval-test',
        trigger: { event: 'manual' },
        steps: [{ type: 'approval', config: {} }]
      }),
      headers: { 'Content-Type': 'application/json' }
    });
    const { id: pipelineId } = await createResponse.json();

    // Trigger the pipeline (creates a run awaiting approval)
    const runResponse = await fetch(`http://localhost:${leader.port}/api/pipelines/${pipelineId}/runs`, {
      method: 'POST',
      body: JSON.stringify({ triggeredBy: { triggerType: 'manual' } }),
      headers: { 'Content-Type': 'application/json' }
    });
    const { runId } = await runResponse.json();

    await harness.waitForLogSync();

    // Concurrent approvals from 3 different nodes
    const nodes = harness.nodes;
    const approvalPromises = nodes.map(async (node) => {
      return fetch(`http://localhost:${node.port}/api/pipelines/${pipelineId}/runs/${runId}/approve`, {
        method: 'POST',
        body: JSON.stringify({ decision: 'approve' }),
        headers: { 'Content-Type': 'application/json' }
      });
    });

    const responses = await Promise.all(approvalPromises);

    // Exactly ONE approval should succeed (status 200)
    // The other two should fail with 409 Conflict (already approved)
    const successCount = responses.filter(r => r.status === 200).length;
    const conflictCount = responses.filter(r => r.status === 409).length;

    expect(successCount).toBe(1);
    expect(conflictCount).toBe(2);

    // Verify final state: run is approved (not in a split-brain state)
    await harness.waitForLogSync();
    const finalStateResponse = await fetch(`http://localhost:${leader.port}/api/pipelines/${pipelineId}/runs/${runId}`);
    const finalState = await finalStateResponse.json();
    expect(finalState.status).toBe('approved');  // or 'running' if approval triggered next step
  });
});
```

## Test 3: Clock Drift Handling (maxClockDriftMs)

**File:** `test/integration/raft/clock-drift.test.ts`

**Purpose:** Verify that lease reads automatically disable when clock drift exceeds maxClockDriftMs (50ms).

```typescript
describe('Raft clock drift handling', () => {
  let harness: RaftClusterHarness;

  beforeAll(async () => {
    harness = new RaftClusterHarness();
    await harness.start(3);
    await harness.waitForElection();
  });

  afterAll(async () => {
    await harness.stop();
  });

  it('should disable lease reads when clock drift > maxClockDriftMs', async () => {
    // This test requires mocking Date.now() or SystemClock in distributed-core
    // to simulate clock skew between leader and followers.

    // Steps:
    // 1. Leader's clock advances by 100ms (> maxClockDriftMs = 50ms)
    // 2. Follower receives heartbeat with leader's timestamp
    // 3. Follower detects clock skew, disables lease reads
    // 4. Verify that subsequent reads on follower use ReadIndex protocol (quorum confirmation)

    // TODO: Implement clock mocking in distributed-core or skip this test
    // until distributed-core exposes a clock injection point (SystemClock interface).

    console.warn('[SKIP] Clock drift test requires SystemClock mocking in distributed-core');
  });
});
```

## Test 4: Reads During Election

**File:** `test/integration/raft/election-reads.test.ts`

**Purpose:** Verify that reads during leader election return appropriate errors (not stale data).

```typescript
describe('Raft reads during election', () => {
  let harness: RaftClusterHarness;

  beforeAll(async () => {
    harness = new RaftClusterHarness();
    await harness.start(3);
    await harness.waitForElection();
  });

  afterAll(async () => {
    await harness.stop();
  });

  it('should reject reads during election (no leader)', async () => {
    const leader = harness.getLeader();

    // Create a pipeline
    const createResponse = await fetch(`http://localhost:${leader.port}/api/pipelines`, {
      method: 'POST',
      body: JSON.stringify({ name: 'election-test', trigger: { event: 'manual' } }),
      headers: { 'Content-Type': 'application/json' }
    });
    const { id: pipelineId } = await createResponse.json();

    await harness.waitForLogSync();

    // Kill the leader to trigger election
    await harness.killNode(leader.nodeId);

    // Attempt read from a follower BEFORE new leader is elected
    // Expected: 503 Service Unavailable or 500 (no leader available)
    const follower = harness.getFollower();
    const readResponse = await fetch(`http://localhost:${follower.port}/api/pipelines/${pipelineId}`);

    // During election, reads MUST NOT return stale data
    // Acceptable responses: 503 (no leader), 500 (election in progress)
    expect([500, 503]).toContain(readResponse.status);

    // After election completes, reads should succeed
    await harness.waitForElection();
    const postElectionResponse = await fetch(`http://localhost:${follower.port}/api/pipelines/${pipelineId}`);
    expect(postElectionResponse.status).toBe(200);
  });
});
```

## Test 5: Performance Baseline (p95 Latency)

**File:** `test/integration/raft/performance.test.ts`

**Purpose:** Validate no-go criterion: p95 approval-write latency must be <250ms with all 3 replicas healthy.

```typescript
describe('Raft performance baseline', () => {
  let harness: RaftClusterHarness;

  beforeAll(async () => {
    harness = new RaftClusterHarness();
    await harness.start(3);
    await harness.waitForElection();
  });

  afterAll(async () => {
    await harness.stop();
  });

  it('should maintain p95 write latency <250ms', async () => {
    const leader = harness.getLeader();

    // Create a pipeline
    const createResponse = await fetch(`http://localhost:${leader.port}/api/pipelines`, {
      method: 'POST',
      body: JSON.stringify({ name: 'perf-test', trigger: { event: 'manual' } }),
      headers: { 'Content-Type': 'application/json' }
    });
    const { id: pipelineId } = await createResponse.json();

    // Issue 100 write requests (pipeline runs)
    const latencies: number[] = [];
    for (let i = 0; i < 100; i++) {
      const startMs = Date.now();
      const runResponse = await fetch(`http://localhost:${leader.port}/api/pipelines/${pipelineId}/runs`, {
        method: 'POST',
        body: JSON.stringify({ triggeredBy: { triggerType: 'manual' } }),
        headers: { 'Content-Type': 'application/json' }
      });
      const latencyMs = Date.now() - startMs;

      expect(runResponse.status).toBe(201);
      latencies.push(latencyMs);
    }

    // Calculate p95
    latencies.sort((a, b) => a - b);
    const p95Index = Math.floor(latencies.length * 0.95);
    const p95LatencyMs = latencies[p95Index];

    console.log(`p95 write latency: ${p95LatencyMs}ms`);

    // No-go criterion from .planning/PHASE-4-MULTI-NODE-RAFT.md section 7
    expect(p95LatencyMs).toBeLessThan(250);
  });
});
```

## Test Execution

```bash
# Run all Raft integration tests
cd social-api
npm test -- test/integration/raft

# Run specific test
npm test -- test/integration/raft/linearizability.test.ts

# Run with coverage
npm test -- --coverage test/integration/raft
```

## Blockers

1. **RaftClusterHarness implementation** — requires multi-node test harness (not yet written)
2. **SystemClock mocking** for clock-drift test (may require distributed-core changes)
3. **Metrics/observability** in tests — verify no quorum RPCs during lease reads

## Next Steps (Operator)

1. Implement `RaftClusterHarness` helper in `test/integration/raft/helpers/`
2. Write tests 1, 2, 4, 5 (skip clock-drift test until mocking available)
3. Run against local 3-node cluster (in-memory transport)
4. Validate p95 latency < 250ms (no-go criterion)
5. If tests pass, proceed to chaos testing (task #962)
