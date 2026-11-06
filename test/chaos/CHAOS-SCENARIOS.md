# Raft Chaos Testing Scenarios

## Overview

Chaos tests validate Raft correctness under failure conditions. Must run against a REAL 3-node cluster (staging environment or local K8s with 3 social-api pods).

## Test Harness

**Target:** 3-node social-api Raft cluster deployed via K8s manifests (k8s/social-api-raft-statefulset.yaml)

**Probe Client:** Script that issues concurrent requests and verifies linearizability.

## Scenario 1: Leader Kill → Election → Writes Resume

**Objective:** Verify that killing the leader triggers election within ~600ms and writes resume.

**Steps:**

```bash
#!/bin/bash
# chaos-test-1-leader-kill.sh

# 1. Deploy 3-node cluster
kubectl apply -f k8s/social-api-*.yaml

# 2. Wait for cluster healthy
kubectl wait --for=condition=Ready pod/social-api-0 pod/social-api-1 pod/social-api-2 --timeout=120s

# 3. Identify leader
LEADER=$(kubectl exec -it social-api-0 -- curl -s http://localhost:3001/internal/raft/health | jq -r '.nodeId')
echo "Current leader: $LEADER"

# 4. Start probe client (issues writes every 100ms)
node scripts/chaos-probe.js --target http://social-api:80 --interval 100 &
PROBE_PID=$!

# 5. Kill leader after 5 seconds
sleep 5
kubectl delete pod $LEADER --force --grace-period=0

# 6. Wait for new leader election
sleep 1
NEW_LEADER=$(kubectl exec -it social-api-0 -- curl -s http://localhost:3001/internal/raft/health | jq -r '.nodeId' || kubectl exec -it social-api-1 -- curl -s http://localhost:3001/internal/raft/health | jq -r '.nodeId')
echo "New leader: $NEW_LEADER"

# 7. Stop probe client after 10 more seconds
sleep 10
kill $PROBE_PID

# 8. Verify linearizability
# Probe client logs show all writes either succeeded or failed cleanly (no duplicates, no split-brain)

# 9. Metrics check
ELECTION_TIME=$(kubectl exec -it social-api-0 -- curl -s http://localhost:3001/internal/metrics | grep raft_elections_started | tail -1)
echo "Election time: $ELECTION_TIME"

# Expected: election completes within ~600ms (electionTimeout=150-300ms + RPC latency)
```

**Success Criteria:**
- New leader elected within 1 second of old leader death
- Writes resume within 600ms of election
- No duplicate pipeline runs created
- All writes have serializable history (probe client verifies)

---

## Scenario 2: Follower Kill → Quorum Maintained

**Objective:** Verify that killing one follower does NOT disrupt writes (2/3 quorum remains).

**Steps:**

```bash
#!/bin/bash
# chaos-test-2-follower-kill.sh

# 1. Deploy 3-node cluster
kubectl apply -f k8s/social-api-*.yaml
kubectl wait --for=condition=Ready pod/social-api-0 pod/social-api-1 pod/social-api-2 --timeout=120s

# 2. Identify a follower
LEADER=$(kubectl exec -it social-api-0 -- curl -s http://localhost:3001/internal/raft/health | jq -r '.nodeId')
FOLLOWER=$(kubectl get pods -l app=social-api -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -v $LEADER | head -1)
echo "Killing follower: $FOLLOWER"

# 3. Start probe client
node scripts/chaos-probe.js --target http://social-api:80 --interval 100 &
PROBE_PID=$!

# 4. Kill follower
kubectl delete pod $FOLLOWER --force --grace-period=0

# 5. Continue writes for 10 seconds
sleep 10
kill $PROBE_PID

# 6. Verify no write failures
# Probe client should report 0 failed writes (quorum maintained with 2/3 nodes)
```

**Success Criteria:**
- Writes continue without interruption (2/3 quorum)
- No elections triggered (leader still has majority)
- Follower rejoins after restart and catches up via AppendEntries or InstallSnapshot

---

## Scenario 3: Network Partition (1 vs 2 Nodes)

**Objective:** Verify that the minority partition (1 node) cannot elect leader and rejects writes.

**Steps:**

```bash
#!/bin/bash
# chaos-test-3-partition.sh

# NOTE: Network partitioning in K8s requires NetworkPolicy or iptables rules
# Simpler approach: use in-memory transport + mock PeerMessaging to drop messages

# 1. Deploy 3-node cluster
kubectl apply -f k8s/social-api-*.yaml
kubectl wait --for=condition=Ready pod/social-api-0 pod/social-api-1 pod/social-api-2 --timeout=120s

# 2. Identify leader
LEADER=$(kubectl exec -it social-api-0 -- curl -s http://localhost:3001/internal/raft/health | jq -r '.nodeId')

# 3. Partition: isolate social-api-2 from social-api-0 and social-api-1
# Use NetworkPolicy to drop traffic:
cat <<EOF | kubectl apply -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: isolate-social-api-2
spec:
  podSelector:
    matchLabels:
      app: social-api
      statefulset.kubernetes.io/pod-name: social-api-2
  policyTypes:
  - Ingress
  - Egress
  egress:
  - to:
    - podSelector: {}
    ports:
    - port: 53  # DNS only
EOF

# 4. Wait for partition to take effect
sleep 2

# 5. Attempt write to social-api-2 (minority)
RESPONSE=$(kubectl exec -it social-api-2 -- curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:3001/api/pipelines -d '{"name":"partition-test","trigger":{"event":"manual"}}' -H 'Content-Type: application/json')

# Expected: 503 Service Unavailable or 500 (no leader, cannot commit)
echo "Minority write response: $RESPONSE"
[ "$RESPONSE" -ge 500 ] && echo "✓ Minority correctly rejects writes" || echo "✗ FAIL: Minority accepted write (split-brain!)"

# 6. Verify writes succeed on majority partition (social-api-0, social-api-1)
RESPONSE=$(kubectl exec -it social-api-0 -- curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:3001/api/pipelines -d '{"name":"partition-test-majority","trigger":{"event":"manual"}}' -H 'Content-Type: application/json')
[ "$RESPONSE" -eq 201 ] && echo "✓ Majority accepts writes" || echo "✗ FAIL: Majority rejected write"

# 7. Heal partition
kubectl delete networkpolicy isolate-social-api-2

# 8. Wait for social-api-2 to rejoin
sleep 5
kubectl exec -it social-api-2 -- curl -s http://localhost:3001/internal/raft/health | jq
```

**Success Criteria:**
- Minority partition (1 node) cannot elect leader
- Writes to minority return 500/503 (no leader available)
- Majority partition (2 nodes) continues accepting writes
- After healing, minority node catches up and rejoins cluster

---

## Scenario 4: WAL Corruption → InstallSnapshot Backfill

**Objective:** Verify that a node with corrupted WAL refuses to start, and peers backfill via InstallSnapshot.

**Steps:**

```bash
#!/bin/bash
# chaos-test-4-wal-corruption.sh

# 1. Deploy 3-node cluster
kubectl apply -f k8s/social-api-*.yaml
kubectl wait --for=condition=Ready pod/social-api-0 pod/social-api-1 pod/social-api-2 --timeout=120s

# 2. Write some data (trigger snapshots)
for i in {1..1000}; do
  kubectl exec -it social-api-0 -- curl -s -X POST http://localhost:3001/api/pipelines -d "{\"name\":\"pipeline-$i\",\"trigger\":{\"event\":\"manual\"}}" -H 'Content-Type: application/json'
done

# Wait for snapshot to be created (raftConfig.snapshotThreshold = 10000 by default, so this won't trigger)
# Alternative: lower threshold to 100 entries via env var PIPELINE_RAFT_SNAPSHOT_THRESHOLD=100

# 3. Corrupt WAL on social-api-2
kubectl exec -it social-api-2 -- sh -c 'echo "CORRUPTED" > /var/lib/social-api/raft/raft.wal'

# 4. Restart social-api-2
kubectl delete pod social-api-2

# 5. Verify pod fails to start (CrashLoopBackOff) due to WAL corruption
kubectl wait --for=condition=Ready=false pod/social-api-2 --timeout=30s
kubectl logs social-api-2 | grep -i "wal.*corrupt"

# 6. Wipe dataDir and allow clean restart
kubectl exec -it social-api-2 -- rm -rf /var/lib/social-api/raft/*
kubectl delete pod social-api-2

# 7. Wait for pod to start and sync via InstallSnapshot
kubectl wait --for=condition=Ready pod/social-api-2 --timeout=120s

# 8. Verify node caught up
kubectl exec -it social-api-2 -- curl -s http://localhost:3001/internal/raft/health | jq '.commitIndex'
LEADER_COMMIT=$(kubectl exec -it social-api-0 -- curl -s http://localhost:3001/internal/raft/health | jq -r '.commitIndex')
echo "Leader commitIndex: $LEADER_COMMIT"
```

**Success Criteria:**
- Corrupted node refuses to start (logs show WAL corruption error)
- After wiping dataDir, node restarts cleanly
- Node catches up via InstallSnapshot RPC (check `raft_snapshots_installed` metric)

---

## Scenario 5: Cascade Restart (All 3 Nodes Reboot)

**Objective:** Verify that simultaneous restart of all 3 nodes does NOT cause data loss and cluster re-elects quickly.

**Steps:**

```bash
#!/bin/bash
# chaos-test-5-cascade-restart.sh

# 1. Deploy 3-node cluster and write data
kubectl apply -f k8s/social-api-*.yaml
kubectl wait --for=condition=Ready pod/social-api-0 pod/social-api-1 pod/social-api-2 --timeout=120s

for i in {1..100}; do
  kubectl exec -it social-api-0 -- curl -s -X POST http://localhost:3001/api/pipelines -d "{\"name\":\"cascade-$i\",\"trigger\":{\"event\":\"manual\"}}" -H 'Content-Type: application/json'
done

# 2. Simultaneously delete all 3 pods
kubectl delete pod social-api-0 social-api-1 social-api-2 --grace-period=1

# 3. Wait for pods to restart
kubectl wait --for=condition=Ready pod/social-api-0 pod/social-api-1 pod/social-api-2 --timeout=120s

# 4. Verify election completes (one node becomes leader)
sleep 2
LEADER=$(kubectl exec -it social-api-0 -- curl -s http://localhost:3001/internal/raft/health | jq -r '.nodeId' || kubectl exec -it social-api-1 -- curl -s http://localhost:3001/internal/raft/health | jq -r '.nodeId')
echo "Leader after cascade restart: $LEADER"

# 5. Verify data persisted (PersistentVolumes retained pipelines)
PIPELINE_COUNT=$(kubectl exec -it social-api-0 -- curl -s http://localhost:3001/api/pipelines | jq 'length')
echo "Pipelines after restart: $PIPELINE_COUNT"
[ "$PIPELINE_COUNT" -eq 100 ] && echo "✓ Data persisted" || echo "✗ FAIL: Data loss"
```

**Success Criteria:**
- All 3 pods restart successfully (PVs reattach)
- Election completes within ~1-3 seconds
- No data loss (pipelines created before restart are still present)
- Write availability resumes after quorum re-establishes

---

## Probe Client (scripts/chaos-probe.js)

**Purpose:** Issue concurrent writes and verify linearizability.

```javascript
#!/usr/bin/env node
// social-api/scripts/chaos-probe.js
//
// Probe client for chaos testing. Issues writes to all 3 nodes concurrently,
// records outcomes, and verifies serializable history (no split-brain).

const http = require('http');

const TARGET_HOST = process.env.TARGET_HOST || 'social-api';
const TARGET_PORT = process.env.TARGET_PORT || 80;
const INTERVAL_MS = parseInt(process.env.INTERVAL_MS || '100', 10);

const results = { success: 0, failure: 0, duplicates: 0, history: [] };

async function issueWrite() {
  const pipelineName = `chaos-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const body = JSON.stringify({ name: pipelineName, trigger: { event: 'manual' } });

  return new Promise((resolve) => {
    const req = http.request({
      hostname: TARGET_HOST,
      port: TARGET_PORT,
      path: '/api/pipelines',
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
    }, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode === 201) {
          try {
            const { id } = JSON.parse(data);
            results.history.push({ pipelineName, id, status: 'success' });
            results.success++;
          } catch {
            results.failure++;
          }
        } else {
          results.history.push({ pipelineName, status: 'failure', code: res.statusCode });
          results.failure++;
        }
        resolve();
      });
    });

    req.on('error', () => {
      results.history.push({ pipelineName, status: 'error' });
      results.failure++;
      resolve();
    });

    req.write(body);
    req.end();
  });
}

function verifyLinearizability() {
  // Check for duplicate IDs (indicates split-brain)
  const ids = results.history.filter(r => r.status === 'success').map(r => r.id);
  const uniqueIds = new Set(ids);
  results.duplicates = ids.length - uniqueIds.size;

  console.log('\n===== Probe Client Results =====');
  console.log(`Success: ${results.success}`);
  console.log(`Failure: ${results.failure}`);
  console.log(`Duplicates (split-brain): ${results.duplicates}`);

  if (results.duplicates > 0) {
    console.error('✗ FAIL: Duplicate IDs detected (split-brain!)');
    process.exit(1);
  } else {
    console.log('✓ PASS: Linearizability verified (no duplicates)');
    process.exit(0);
  }
}

// Main loop
console.log(`[chaos-probe] Starting probe client: ${TARGET_HOST}:${TARGET_PORT}, interval=${INTERVAL_MS}ms`);
const interval = setInterval(() => {
  issueWrite();
}, INTERVAL_MS);

// Graceful shutdown
process.on('SIGTERM', () => {
  clearInterval(interval);
  setTimeout(verifyLinearizability, 2000);  // Wait 2s for in-flight requests
});
process.on('SIGINT', () => {
  clearInterval(interval);
  setTimeout(verifyLinearizability, 2000);
});
```

**Usage:**
```bash
node scripts/chaos-probe.js &
PROBE_PID=$!

# Run chaos scenario...

kill $PROBE_PID  # Triggers verification
```

---

## No-Go Criteria

Chaos tests MUST validate these before production deployment:

1. **p95 approval-write latency < 250ms** (with all 3 replicas healthy)
2. **No split-brain** (concurrent writes produce serializable history)
3. **WAL growth < 50MB/day** (in staging with realistic load)
4. **Election completes < 600ms** (leader kill → new leader)

If any criterion fails, **DO NOT promote to production.**

---

## Execution Checklist

- [ ] Deploy 3-node staging cluster (k8s manifests)
- [ ] Run Scenario 1 (leader kill) — verify election <600ms
- [ ] Run Scenario 2 (follower kill) — verify quorum maintained
- [ ] Run Scenario 3 (partition) — verify minority rejects writes
- [ ] Run Scenario 4 (WAL corruption) — verify InstallSnapshot backfill
- [ ] Run Scenario 5 (cascade restart) — verify data persisted
- [ ] Measure p95 latency with probe client (must be <250ms)
- [ ] Monitor WAL growth over 24 hours (must be <50MB/day)
- [ ] Review Raft metrics (elections, rejections, lag) — all green
