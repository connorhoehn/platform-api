// social-api/src/pipeline/config/__tests__/cluster.test.ts
//
// Unit tests for cluster.ts config builders — TCP transport + Redis pubsub
// (Phase 4.1). Tests validate that the new env-configurable paths produce
// correct ClusterConfig literals without actually starting a cluster.

import {
  buildTransportConfig,
  buildPubSubConfig,
  resolvePubSubMode,
  resolveRaftConfigOverrides,
  type PipelinePubSubMode,
} from '../cluster';

describe('buildTransportConfig', () => {
  it('returns in-memory transport when type is in-memory', () => {
    const result = buildTransportConfig('in-memory', 7100, []);
    expect(result).toEqual({ type: 'in-memory' });
  });

  it('returns TCP transport with seedNodes when provided', () => {
    const seeds = ['social-api-0:7100', 'social-api-1:7100', 'social-api-2:7100'];
    const result = buildTransportConfig('tcp', 7100, seeds);
    expect(result).toEqual({
      type: 'tcp',
      port: 7100,
      seedNodes: seeds,
    });
  });

  it('returns TCP transport without seedNodes when empty array', () => {
    const result = buildTransportConfig('tcp', 7100, []);
    expect(result).toEqual({
      type: 'tcp',
      port: 7100,
    });
  });

  it('returns websocket transport with port', () => {
    const result = buildTransportConfig('websocket', 8080, []);
    expect(result).toEqual({
      type: 'websocket',
      port: 8080,
    });
  });
});

describe('buildPubSubConfig', () => {
  it('returns memory pubsub when mode is memory', () => {
    const result = buildPubSubConfig({ mode: 'memory' });
    expect(result).toEqual({ type: 'memory' });
  });

  it('returns redis pubsub when mode is redis and redisUrl is provided', () => {
    const result = buildPubSubConfig({
      mode: 'redis',
      redisUrl: 'redis://localhost:6379',
    });
    expect(result).toEqual({
      type: 'redis',
      url: 'redis://localhost:6379',
    });
  });

  it('throws when redis mode is specified but redisUrl is missing', () => {
    expect(() => buildPubSubConfig({ mode: 'redis' })).toThrow(
      /buildPubSubConfig: 'redis' mode requires a redisUrl/,
    );
  });

  it('throws when redis mode is specified but redisUrl is empty string', () => {
    expect(() => buildPubSubConfig({ mode: 'redis', redisUrl: '' })).toThrow(
      /buildPubSubConfig: 'redis' mode requires a redisUrl/,
    );
  });

  it('throws when redis mode is specified but redisUrl is whitespace', () => {
    expect(() => buildPubSubConfig({ mode: 'redis', redisUrl: '   ' })).toThrow(
      /buildPubSubConfig: 'redis' mode requires a redisUrl/,
    );
  });
});

describe('resolvePubSubMode', () => {
  const originalEnv = process.env.PIPELINE_PUBSUB_MODE;

  afterEach(() => {
    if (originalEnv !== undefined) {
      process.env.PIPELINE_PUBSUB_MODE = originalEnv;
    } else {
      delete process.env.PIPELINE_PUBSUB_MODE;
    }
  });

  it('returns "memory" when PIPELINE_PUBSUB_MODE is unset', () => {
    delete process.env.PIPELINE_PUBSUB_MODE;
    expect(resolvePubSubMode()).toBe('memory');
  });

  it('returns "memory" when PIPELINE_PUBSUB_MODE is "memory"', () => {
    process.env.PIPELINE_PUBSUB_MODE = 'memory';
    expect(resolvePubSubMode()).toBe('memory');
  });

  it('returns "redis" when PIPELINE_PUBSUB_MODE is "redis"', () => {
    process.env.PIPELINE_PUBSUB_MODE = 'redis';
    expect(resolvePubSubMode()).toBe('redis');
  });

  it('throws when PIPELINE_PUBSUB_MODE is invalid', () => {
    process.env.PIPELINE_PUBSUB_MODE = 'kafka';
    expect(() => resolvePubSubMode()).toThrow(
      /PIPELINE_PUBSUB_MODE must be one of 'memory' \| 'redis'/,
    );
  });

  it('throws when PIPELINE_PUBSUB_MODE is empty string', () => {
    process.env.PIPELINE_PUBSUB_MODE = '';
    expect(resolvePubSubMode()).toBe('memory');
  });
});

describe('resolveRaftConfigOverrides', () => {
  const originalEnv = { ...process.env };

  afterEach(() => {
    // Restore original env
    Object.keys(process.env).forEach((key) => {
      if (key.startsWith('RAFT_')) {
        delete process.env[key];
      }
    });
    Object.keys(originalEnv).forEach((key) => {
      if (key.startsWith('RAFT_')) {
        process.env[key] = originalEnv[key];
      }
    });
  });

  it('returns Phase 4.2 fixed overrides when no env vars are set', () => {
    const result = resolveRaftConfigOverrides();
    expect(result.staleReadsAllowed).toBe(false);
    expect(result.leaseReadEnabled).toBe(true);
  });

  it('parses RAFT_SNAPSHOT_THRESHOLD', () => {
    process.env.RAFT_SNAPSHOT_THRESHOLD = '5000';
    const result = resolveRaftConfigOverrides();
    expect(result.snapshotThreshold).toBe(5000);
  });

  it('throws on invalid RAFT_SNAPSHOT_THRESHOLD', () => {
    process.env.RAFT_SNAPSHOT_THRESHOLD = 'not-a-number';
    expect(() => resolveRaftConfigOverrides()).toThrow(/RAFT_SNAPSHOT_THRESHOLD must be a positive integer/);
  });

  it('parses RAFT_ELECTION_TIMEOUT_MIN', () => {
    process.env.RAFT_ELECTION_TIMEOUT_MIN = '200';
    const result = resolveRaftConfigOverrides();
    expect(result.timerConfig?.electionTimeoutMinMs).toBe(200);
  });

  it('parses RAFT_ELECTION_TIMEOUT_MAX', () => {
    process.env.RAFT_ELECTION_TIMEOUT_MAX = '400';
    const result = resolveRaftConfigOverrides();
    expect(result.timerConfig?.electionTimeoutMaxMs).toBe(400);
  });

  it('parses RAFT_HEARTBEAT_INTERVAL', () => {
    process.env.RAFT_HEARTBEAT_INTERVAL = '75';
    const result = resolveRaftConfigOverrides();
    expect(result.timerConfig?.heartbeatIntervalMs).toBe(75);
  });

  it('parses RAFT_MAX_BATCH_SIZE', () => {
    process.env.RAFT_MAX_BATCH_SIZE = '200';
    const result = resolveRaftConfigOverrides();
    expect(result.maxBatchSize).toBe(200);
  });

  it('parses RAFT_PRE_VOTE_ENABLED true', () => {
    process.env.RAFT_PRE_VOTE_ENABLED = 'true';
    const result = resolveRaftConfigOverrides();
    expect(result.preVoteEnabled).toBe(true);
  });

  it('parses RAFT_PRE_VOTE_ENABLED false', () => {
    process.env.RAFT_PRE_VOTE_ENABLED = 'false';
    const result = resolveRaftConfigOverrides();
    expect(result.preVoteEnabled).toBe(false);
  });

  it('throws on invalid RAFT_PRE_VOTE_ENABLED', () => {
    process.env.RAFT_PRE_VOTE_ENABLED = 'maybe';
    expect(() => resolveRaftConfigOverrides()).toThrow(/RAFT_PRE_VOTE_ENABLED must be 'true' or 'false'/);
  });

  it('parses RAFT_MAX_CLOCK_DRIFT_MS', () => {
    process.env.RAFT_MAX_CLOCK_DRIFT_MS = '100';
    const result = resolveRaftConfigOverrides();
    expect(result.maxClockDriftMs).toBe(100);
  });

  it('parses RAFT_PROPOSAL_TIMEOUT_MS', () => {
    process.env.RAFT_PROPOSAL_TIMEOUT_MS = '10000';
    const result = resolveRaftConfigOverrides();
    expect(result.proposalTimeoutMs).toBe(10000);
  });

  it('always sets staleReadsAllowed=false and leaseReadEnabled=true (Phase 4.2 fixed)', () => {
    process.env.RAFT_SNAPSHOT_THRESHOLD = '20000';
    const result = resolveRaftConfigOverrides();
    expect(result.staleReadsAllowed).toBe(false);
    expect(result.leaseReadEnabled).toBe(true);
    expect(result.snapshotThreshold).toBe(20000);
  });
});
