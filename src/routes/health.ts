import { Router } from 'express';
import { DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { makeHealthHandler, type HealthCheck } from '@connorhoehn/service-runtime';
import { ddbClient } from '../lib/aws-clients';
import { getRedisClient } from '../lib/redis-client';
import { tableName } from '../lib/ddb-table-name';
import { buildFreshnessReadiness } from '../lib/buildFreshness';

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

const router = Router();

router.get(
  '/',
  makeHealthHandler({
    service: 'platform-api',
    version: SERVICE_VERSION,
    checks: [ddbCheck, redisCheck, buildCheck],
  }),
);

export default router;
