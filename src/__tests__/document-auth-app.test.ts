import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
const documentId = '11111111-1111-4111-8111-111111111111';
jest.mock('@connorhoehn/realtime-modules/server', () => ({
  createDocumentGrantVerifierFromEnv: () => async (token: string) => {
    if (token !== 'document-grant') throw new Error('invalid grant');
    return { sub: 'alice', tenantId: 'assessment', documentId: '11111111-1111-4111-8111-111111111111', operations: ['read'] };
  },
  documentGrantAllows: (claims: any, scope: any) => claims.documentId === scope.documentId && claims.tenantId === scope.tenantId && claims.operations.includes(scope.operation),
}));
jest.mock('jwks-rsa', () => () => ({ getSigningKey: async () => { throw new Error('not a Cognito token'); } }));
jest.mock('../routes/index', () => {
  const router = require('express').Router();
  router.get('/documents/:documentId/lifecycle', (req: any, res: any) => res.json({ actor: req.user?.sub, delegated: !!req.documentGrant }));
  router.get('/profiles', (_req: any, res: any) => res.json({ private: true }));
  return { __esModule: true, default: router };
});
jest.mock('../routes/health', () => ({ __esModule: true, default: require('express').Router(), livezHandler: (_req: any, res: any) => res.json({ status: 'alive' }) }));
jest.mock('../routes/pipelineWebhooks', () => ({ pipelineWebhooksRouter: require('express').Router() }));
jest.mock('../routes/internal-work-graph', () => ({ internalWorkGraphRouter: require('express').Router() }));
jest.mock('../routes/capabilities', () => ({ createCapabilitiesRouter: () => require('express').Router() }));
jest.mock('../routes/lvsWebhooks', () => ({ lvsWebhooksRouter: require('express').Router() }));
jest.mock('../routes/lvsAdmin', () => ({ lvsAdminRouter: require('express').Router() }));
jest.mock('../routes/hangoutRooms', () => ({ hangoutRoomsRouter: require('express').Router() }));
jest.mock('../routes/media', () => ({ mediaRouter: require('express').Router() }));
jest.mock('../pipeline/definitions-repository', () => ({ definitionsRepo: {} }));
jest.mock('../middleware/rateLimit', () => ({ pipelineReadRateLimit: () => (_req: any, _res: any, next: any) => next() }));
jest.mock('../middleware/request-logger', () => ({ requestLogger: (_req: any, _res: any, next: any) => next() }));
jest.mock('../middleware/http-metrics', () => ({ httpMetrics: (_req: any, _res: any, next: any) => next() }));
jest.mock('../observability/metrics', () => ({ renderPrometheusText: () => '' }));
import { createApp } from '../app';
const previous = { ...process.env };
let server: ReturnType<ReturnType<typeof createApp>['listen']>;
let origin: string;
beforeAll(async () => {
  Object.assign(process.env, { NODE_ENV: 'production', SKIP_AUTH: 'false', DOCUMENT_TENANT_ID: 'assessment', DOCUMENT_GRANT_ISSUER: 'assessment' });
  server = createApp().listen(0, '127.0.0.1'); await once(server, 'listening'); origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
afterAll(async () => { process.env = previous; await new Promise<void>(resolve => server.close(() => resolve())); });
it('authenticates scoped document grants before generic Cognito auth', async () => {
  const result = await fetch(`${origin}/api/documents/${documentId}/lifecycle`, { headers: { Authorization: 'Bearer document-grant' } });
  expect(result.status).toBe(200); expect(await result.json()).toEqual({ actor: 'alice', delegated: true });
});
it('cannot reuse that grant on an unrelated API route', async () => {
  expect((await fetch(`${origin}/api/profiles`, { headers: { Authorization: 'Bearer document-grant' } })).status).toBe(401);
});
it('rejects a different document and invalid grants at the application boundary', async () => {
  expect((await fetch(`${origin}/api/documents/other/lifecycle`, { headers: { Authorization: 'Bearer document-grant' } })).status).toBe(403);
  expect((await fetch(`${origin}/api/documents/${documentId}/lifecycle`, { headers: { Authorization: 'Bearer invalid' } })).status).toBe(401);
});
