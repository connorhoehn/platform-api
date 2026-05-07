// Integration tests for /api/health route — covers healthy, degraded, and
// partial-failure states. The healthAlias.test.ts covers route aliasing;
// this file covers the actual health-check logic paths.

import express from 'express';
import http from 'http';
import { AddressInfo } from 'net';

let mockDdbSend: jest.Mock;
let mockRedisPing: jest.Mock;
let mockGetRedisClient: jest.Mock;

jest.mock('../../lib/aws-clients', () => {
  mockDdbSend = jest.fn().mockResolvedValue({});
  return { ddbClient: { send: mockDdbSend } };
});

jest.mock('../../lib/redis-client', () => {
  mockRedisPing = jest.fn().mockResolvedValue('PONG');
  mockGetRedisClient = jest.fn().mockResolvedValue({ ping: mockRedisPing });
  return { getRedisClient: mockGetRedisClient };
});

// eslint-disable-next-line @typescript-eslint/no-require-imports
const healthRouter = require('../health').default as express.Router;

interface HealthBody {
  status: string;
  service: string;
  checks: Record<string, { status: string; latencyMs?: number; error?: string }>;
}

interface JsonResponse { status: number; body: HealthBody }

async function getJson(server: http.Server, path: string): Promise<JsonResponse> {
  const { port } = server.address() as AddressInfo;
  return new Promise<JsonResponse>((resolve, reject) => {
    http.get({ host: '127.0.0.1', port, path }, (res) => {
      let raw = '';
      res.on('data', (chunk: Buffer) => { raw += chunk.toString('utf8'); });
      res.on('end', () => {
        try { resolve({ status: res.statusCode ?? 0, body: JSON.parse(raw) }); }
        catch (err) { reject(err); }
      });
      res.on('error', reject);
    }).on('error', reject);
  });
}

function buildApp(): express.Application {
  const app = express();
  app.use('/api/health', healthRouter);
  return app;
}

describe('GET /api/health', () => {
  let server: http.Server;

  beforeAll(async () => {
    server = http.createServer(buildApp());
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  beforeEach(() => {
    mockDdbSend.mockReset().mockResolvedValue({});
    mockRedisPing.mockReset().mockResolvedValue('PONG');
    mockGetRedisClient.mockReset().mockResolvedValue({ ping: mockRedisPing });
  });

  it('returns 200 + ok when both DynamoDB and Redis are healthy', async () => {
    const res = await getJson(server, '/api/health');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('ok');
    expect(res.body.service).toBe('social-api');
    expect(res.body.checks.dynamodb.status).toBe('ok');
    expect(res.body.checks.redis.status).toBe('ok');
    expect(typeof res.body.checks.dynamodb.latencyMs).toBe('number');
  });

  it('returns 503 + degraded when DynamoDB is down', async () => {
    mockDdbSend.mockRejectedValue(new Error('connection refused'));
    const res = await getJson(server, '/api/health');
    expect(res.status).toBe(503);
    expect(res.body.status).toBe('degraded');
    expect(res.body.checks.dynamodb.status).toBe('error');
    expect(res.body.checks.dynamodb.error).toBe('connection refused');
    expect(res.body.checks.redis.status).toBe('ok');
  });

  it('returns 503 + degraded when Redis is down', async () => {
    mockRedisPing.mockRejectedValue(new Error('ECONNREFUSED'));
    const res = await getJson(server, '/api/health');
    expect(res.status).toBe(503);
    expect(res.body.status).toBe('degraded');
    expect(res.body.checks.dynamodb.status).toBe('ok');
    expect(res.body.checks.redis.status).toBe('error');
    expect(res.body.checks.redis.error).toBe('ECONNREFUSED');
  });

  it('returns 503 when Redis client is unavailable (null)', async () => {
    mockGetRedisClient.mockResolvedValue(null);
    const res = await getJson(server, '/api/health');
    expect(res.status).toBe(503);
    expect(res.body.checks.redis.status).toBe('error');
    expect(res.body.checks.redis.error).toBe('Redis client unavailable');
  });

  it('returns 503 when both services are down', async () => {
    mockDdbSend.mockRejectedValue(new Error('DDB timeout'));
    mockRedisPing.mockRejectedValue(new Error('Redis timeout'));
    const res = await getJson(server, '/api/health');
    expect(res.status).toBe(503);
    expect(res.body.status).toBe('degraded');
    expect(res.body.checks.dynamodb.status).toBe('error');
    expect(res.body.checks.redis.status).toBe('error');
  });

  it('includes latencyMs on both checks even when errored', async () => {
    mockDdbSend.mockRejectedValue(new Error('timeout'));
    const res = await getJson(server, '/api/health');
    expect(typeof res.body.checks.dynamodb.latencyMs).toBe('number');
    expect(typeof res.body.checks.redis.latencyMs).toBe('number');
  });
});
