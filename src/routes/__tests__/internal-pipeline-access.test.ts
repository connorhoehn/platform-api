/**
 * realtime-examples NFR #142: the gateway asks before a socket that did not
 * start a run subscribes to `pipeline:run:<id>`. The run's owner and a reader
 * of its document (#135's `document:read`) pass; a stranger, an unknown run
 * and an unsigned caller do not.
 */
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() } }));

import express from 'express';
import { SERVICE_AUTH_HEADER, signEnvelope } from '@connorhoehn/service-runtime';
import {
  createInternalPipelineAccessRouter,
  decidePipelineAccess,
  type PipelineAccessDeps,
} from '../internal-pipeline-access';

const SECRET = 'internal-test-secret-that-is-long-enough';
const doc = { createdBy: 'dev-connor', editors: ['dev-alice'], viewers: [], channel: '' };

function deps(overrides: Partial<PipelineAccessDeps> = {}): PipelineAccessDeps {
  return {
    liveRun: async (runId) => (runId === 'run-1' ? { ownerId: 'dev-connor', documentId: 'doc-1' } : runId === 'run-nodoc' ? { ownerId: 'dev-connor' } : null),
    documentRow: async (id) => (id === 'doc-1' ? doc : null),
    ...overrides,
  };
}

describe('decidePipelineAccess', () => {
  const org = 'org-1';
  it('allows the run owner', async () => {
    await expect(decidePipelineAccess({ organizationId: org, userId: 'dev-connor', runId: 'run-1' }, deps())).resolves.toMatchObject({ allow: true, ownerId: 'dev-connor' });
  });
  it('allows a reader of the run document', async () => {
    await expect(decidePipelineAccess({ organizationId: org, userId: 'dev-alice', runId: 'run-1' }, deps())).resolves.toMatchObject({ allow: true, documentId: 'doc-1' });
  });
  it('refuses a stranger to the run and its document', async () => {
    await expect(decidePipelineAccess({ organizationId: org, userId: 'dev-frank', runId: 'run-1' }, deps())).resolves.toMatchObject({ allow: false, ownerId: 'dev-connor' });
  });
  it('refuses a stranger to a run with no document', async () => {
    await expect(decidePipelineAccess({ organizationId: org, userId: 'dev-frank', runId: 'run-nodoc' }, deps())).resolves.toMatchObject({ allow: false });
  });
  it('refuses an unknown run', async () => {
    await expect(decidePipelineAccess({ organizationId: org, userId: 'dev-connor', runId: 'nope' }, deps())).resolves.toMatchObject({ allow: false });
  });
  it('checks a document id directly (deck-revise channel)', async () => {
    await expect(decidePipelineAccess({ organizationId: org, userId: 'dev-alice', documentId: 'doc-1' }, deps())).resolves.toMatchObject({ allow: true });
    await expect(decidePipelineAccess({ organizationId: org, userId: 'dev-frank', documentId: 'doc-1' }, deps())).resolves.toMatchObject({ allow: false });
    await expect(decidePipelineAccess({ organizationId: org, userId: 'dev-alice', documentId: 'doc-missing' }, deps())).resolves.toMatchObject({ allow: false });
  });
});

describe('POST /internal/pipeline-access/authorize', () => {
  const prev = process.env.SERVICE_AUTH_SECRET;
  beforeAll(() => { process.env.SERVICE_AUTH_SECRET = SECRET; });
  afterAll(() => { process.env.SERVICE_AUTH_SECRET = prev; });

  async function post(body: Record<string, unknown>, signed = true): Promise<{ status: number; json: any }> {
    const app = express();
    app.use(express.json());
    app.use('/internal/pipeline-access', createInternalPipelineAccessRouter(deps()));
    const server = app.listen(0);
    try {
      const port = (server.address() as { port: number }).port;
      const headers: Record<string, string> = { 'content-type': 'application/json' };
      if (signed) headers[SERVICE_AUTH_HEADER] = signEnvelope(body, { secret: SECRET, serviceId: 'realtime-examples' });
      const res = await fetch(`http://127.0.0.1:${port}/internal/pipeline-access/authorize`, { method: 'POST', headers, body: JSON.stringify(body) });
      return { status: res.status, json: await res.json() };
    } finally {
      server.close();
    }
  }

  it('answers a signed request', async () => {
    const res = await post({ organizationId: 'org-1', userId: 'dev-alice', runId: 'run-1' });
    expect(res.status).toBe(200);
    expect(res.json.allow).toBe(true);
  });
  it('refuses an unsigned request', async () => {
    const res = await post({ organizationId: 'org-1', userId: 'dev-alice', runId: 'run-1' }, false);
    expect(res.status).toBe(401);
  });
  it('rejects a request naming neither a run nor a document', async () => {
    const res = await post({ organizationId: 'org-1', userId: 'dev-alice' });
    expect(res.status).toBe(400);
  });
});
