import { Router } from 'express';
import { DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { ddbClient } from '../lib/aws-clients';
import { getRedisClient } from '../lib/redis-client';
import { tableName } from '../lib/ddb-table-name';

const PROFILES_TABLE = tableName('social-profiles');

const router = Router();

router.get('/', async (_req, res) => {
  const checks: Record<string, { status: string; latencyMs?: number; error?: string }> = {};

  // DynamoDB check — lightweight DescribeTable call
  const ddbStart = Date.now();
  try {
    await ddbClient.send(new DescribeTableCommand({ TableName: PROFILES_TABLE }));
    checks.dynamodb = { status: 'ok', latencyMs: Date.now() - ddbStart };
  } catch (err) {
    checks.dynamodb = { status: 'error', latencyMs: Date.now() - ddbStart, error: (err as Error).message };
  }

  // Redis check — PING
  const redisStart = Date.now();
  try {
    const redis = await getRedisClient();
    if (redis) {
      await redis.ping();
      checks.redis = { status: 'ok', latencyMs: Date.now() - redisStart };
    } else {
      checks.redis = { status: 'error', latencyMs: Date.now() - redisStart, error: 'Redis client unavailable' };
    }
  } catch (err) {
    checks.redis = { status: 'error', latencyMs: Date.now() - redisStart, error: (err as Error).message };
  }

  const allHealthy = Object.values(checks).every(c => c.status === 'ok');
  const statusCode = allHealthy ? 200 : 503;

  res.status(statusCode).json({
    status: allHealthy ? 'ok' : 'degraded',
    service: 'social-api',
    checks,
  });
});

export default router;
