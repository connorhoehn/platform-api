// Integration tests for /api/health route — covers healthy, unhealthy, and
// partial-failure states. The healthAlias.test.ts covers route aliasing;
// this file covers the actual health-check logic paths.
//
// Body schema is the @connorhoehn/service-runtime `observability-base`
// shape (status: 'healthy' | 'unhealthy' | 'degraded'; checks is an array
// of {name, ok, durationMs, error?, details?}; service + version + ISO
// timestamp at top level). Adopted in M4.3a-followup.

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

interface HealthCheckReport {
  name: string;
  ok: boolean;
  durationMs: number;
  details?: unknown;
  error?: string;
}

interface HealthBody {
  status: 'healthy' | 'unhealthy' | 'degraded';
  service: string;
  version: string;
  timestamp: string;
  checks: HealthCheckReport[];
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

function findCheck(body: HealthBody, name: string): HealthCheckReport | undefined {
  return body.checks.find((c) => c.name === name);
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

  it('returns 200 + healthy when both DynamoDB and Redis are healthy', async () => {
    const res = await getJson(server, '/api/health');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('healthy');
    expect(res.body.service).toBe('platform-api');
    expect(typeof res.body.version).toBe('string');
    expect(typeof res.body.timestamp).toBe('string');
    expect(findCheck(res.body, 'dynamodb')?.ok).toBe(true);
    expect(findCheck(res.body, 'redis')?.ok).toBe(true);
    expect(typeof findCheck(res.body, 'dynamodb')?.durationMs).toBe('number');
  });

  it('returns 503 + unhealthy when DynamoDB is down', async () => {
    mockDdbSend.mockRejectedValue(new Error('connection refused'));
    const res = await getJson(server, '/api/health');
    expect(res.status).toBe(503);
    expect(res.body.status).toBe('unhealthy');
    expect(findCheck(res.body, 'dynamodb')?.ok).toBe(false);
    expect(findCheck(res.body, 'dynamodb')?.error).toBe('connection refused');
    expect(findCheck(res.body, 'redis')?.ok).toBe(true);
  });

  it('returns 503 + unhealthy when Redis is down', async () => {
    mockRedisPing.mockRejectedValue(new Error('ECONNREFUSED'));
    const res = await getJson(server, '/api/health');
    expect(res.status).toBe(503);
    expect(res.body.status).toBe('unhealthy');
    expect(findCheck(res.body, 'dynamodb')?.ok).toBe(true);
    expect(findCheck(res.body, 'redis')?.ok).toBe(false);
    expect(findCheck(res.body, 'redis')?.error).toBe('ECONNREFUSED');
  });

  it('returns 503 when Redis client is unavailable (null)', async () => {
    mockGetRedisClient.mockResolvedValue(null);
    const res = await getJson(server, '/api/health');
    expect(res.status).toBe(503);
    const redis = findCheck(res.body, 'redis');
    expect(redis?.ok).toBe(false);
    // ok:false from a check returning {ok:false, details:...} surfaces as
    // unhealthy without an `error` field — the details (string here) carry
    // the explanation. This matches the observability-base contract.
    expect(redis?.details).toBe('Redis client unavailable');
  });

  it('returns 503 when both services are down', async () => {
    mockDdbSend.mockRejectedValue(new Error('DDB timeout'));
    mockRedisPing.mockRejectedValue(new Error('Redis timeout'));
    const res = await getJson(server, '/api/health');
    expect(res.status).toBe(503);
    expect(res.body.status).toBe('unhealthy');
    expect(findCheck(res.body, 'dynamodb')?.ok).toBe(false);
    expect(findCheck(res.body, 'redis')?.ok).toBe(false);
  });

  it('includes durationMs on both checks even when errored', async () => {
    mockDdbSend.mockRejectedValue(new Error('timeout'));
    const res = await getJson(server, '/api/health');
    expect(typeof findCheck(res.body, 'dynamodb')?.durationMs).toBe('number');
    expect(typeof findCheck(res.body, 'redis')?.durationMs).toBe('number');
  });
});
