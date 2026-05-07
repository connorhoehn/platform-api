// Hub task #6: every other social-api endpoint is under /api/* but the
// health check was only mounted at /health, so /api/health 404'd. We added
// a parallel mount at /api/health that routes through the same router. This
// test asserts the alias resolves to the same handler with the same payload
// shape; the dynamodb/redis probes are stubbed so the test stays hermetic
// and fast.

import express from 'express';
import http from 'http';
import { AddressInfo } from 'net';

jest.mock('../lib/aws-clients', () => ({
  ddbClient: { send: jest.fn().mockResolvedValue({}) },
}));

jest.mock('../lib/redis-client', () => ({
  getRedisClient: jest.fn().mockResolvedValue({ ping: jest.fn().mockResolvedValue('PONG') }),
}));

// Imported AFTER the jest.mock calls above so the router picks up the stubs.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const healthRouter = require('../routes/health').default as express.Router;

interface JsonResponse {
  status: number;
  body: { status?: string; service?: string; checks?: Record<string, unknown> };
}

async function getJson(server: http.Server, path: string): Promise<JsonResponse> {
  const { port } = server.address() as AddressInfo;
  return await new Promise<JsonResponse>((resolve, reject) => {
    http.get({ host: '127.0.0.1', port, path }, (res) => {
      let raw = '';
      res.on('data', (chunk: Buffer) => { raw += chunk.toString('utf8'); });
      res.on('end', () => {
        try {
          resolve({
            status: res.statusCode ?? 0,
            body: JSON.parse(raw) as JsonResponse['body'],
          });
        } catch (err) { reject(err); }
      });
      res.on('error', reject);
    }).on('error', reject);
  });
}

/**
 * Builds the same dual-mount that production app.ts wires up. Imported here
 * (instead of pulling in createApp()) so the test doesn't drag in the full
 * routes/index.ts surface and its ESM-only transitive deps.
 */
function buildAliasApp(): express.Application {
  const app = express();
  app.use(express.json());
  app.use('/health', healthRouter);
  app.use('/api/health', healthRouter);
  return app;
}

describe('GET /api/health alias (hub task #6)', () => {
  let server: http.Server;

  beforeAll(async () => {
    server = http.createServer(buildAliasApp());
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it('serves the same payload as /health', async () => {
    const root = await getJson(server, '/health');
    const alias = await getJson(server, '/api/health');

    expect(alias.status).toBe(root.status);
    expect(alias.body.service).toBe('social-api');
    expect(alias.body.status).toBe(root.body.status);
    expect(alias.body.checks).toMatchObject({
      dynamodb: { status: 'ok' },
      redis: { status: 'ok' },
    });
  });
});
