import { Router, type Request, type Response } from 'express';
import { DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { makeHealthHandler, type HealthCheck } from '@connorhoehn/service-runtime';
import { insecureDevMode } from '../lib/insecureDevMode';
import { ddbClient } from '../lib/aws-clients';
import { getRedisClient } from '../lib/redis-client';
import { tableName } from '../lib/ddb-table-name';
import { buildFreshnessReadiness } from '../lib/buildFreshness';
import { readModelHealth } from '../lib/modelHealth';
import { runtimeReadiness } from '../lib/runtime-profile';

const PROFILES_TABLE = tableName('social-profiles');
const SERVICE_VERSION = process.env.SERVICE_VERSION ?? '0.1.0';

const ddbCheck: HealthCheck = {
  name: 'dynamodb',
  check: async () => {
    await ddbClient.send(new DescribeTableCommand({ TableName: PROFILES_TABLE }));
    return { ok: true };
  },
};

const redisCheck: HealthCheck = {
  name: 'redis',
  check: async () => {
    const redis = await getRedisClient();
    if (!redis) return { ok: false, details: 'Redis client unavailable' };
    await redis.ping();
    return { ok: true };
  },
};

// NFR #210: a replaced dev pod running an older image than the host source is
// not ready until Tilt's replay restarts it (capped; see buildFreshness.ts).
const buildCheck: HealthCheck = {
  name: 'build',
  check: async () => buildFreshnessReadiness(),
};

// NFR #225: the model's reachability (probe + circuit breaker, shared across
// replicas). Degraded, never failing: the pod serves everything else.
const modelCheck: HealthCheck = {
  name: 'model',
  check: async () => {
    const health = await readModelHealth();
    if (health.available) return { ok: true, ...(health.probe?.note ? { details: health.probe.note } : {}) };
    return { ok: true, degraded: true, details: health.message };
  },
};

// Liveness — "is this process's event loop still turning?" and nothing else.
// No DynamoDB, Redis, build or model call: a dependency outage must pull the
// pod from the Service (readiness on /health), never get it SIGTERMed. When
// /health doubled as the liveness probe, a DynamoDB Local outage 503'd it,
// kubelet killed both replicas into CrashLoopBackOff, and they sat in backoff
// for minutes after DynamoDB came back (realtime-examples, 2026-09-24).
export function livezHandler(_req: Request, res: Response): void {
  res.set('Cache-Control', 'no-store');
  res.status(200).json({
    status: 'alive',
    service: 'platform-api',
    version: SERVICE_VERSION,
    insecureDevMode: insecureDevMode(),
    uptimeSeconds: Math.round(process.uptime()),
  });
}

const router = Router();

router.get(
  '/',
  makeHealthHandler({
    service: 'platform-api',
    version: SERVICE_VERSION,
    // feature:* checks fail only for features the runtime profile makes
    // mandatory; optional features that failed report degraded.
    checks: [ddbCheck, redisCheck, buildCheck, modelCheck, ...runtimeReadiness.healthChecks()],
  }),
);

export default router;
