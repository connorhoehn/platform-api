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

  async function post(body: Record<string, unknown>, signed = true, d: PipelineAccessDeps = deps()): Promise<{ status: number; json: any }> {
    const app = express();
    app.use(express.json());
    app.use('/internal/pipeline-access', createInternalPipelineAccessRouter(d));
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

  // NFR #172: a run the owner's work graph shows this user is theirs to watch
  // live — the graph's own decision, asked only after owner and document.
  describe('shown by the owner\'s work graph (NFR #172)', () => {
    const graph = { personId: 'dev-connor', day: '2026-09-23', timezone: 'America/New_York' };
    function withGraph(shows: boolean, seen: unknown[] = []): PipelineAccessDeps {
      return deps({
        sharedThroughGraph: async (organizationId, userId, runId, graphs) => {
          seen.push({ organizationId, userId, runId, graphs });
          return shows ? { allow: true, ownerId: 'dev-connor' } : { allow: false, ownerId: 'dev-connor' };
        },
      });
    }
    it('allows a run the graph shows, and hands the graph the socket\'s open scope', async () => {
      const seen: unknown[] = [];
      const res = await post({ organizationId: 'org-1', userId: 'dev-frank', runId: 'run-nodoc', graphs: [graph] }, true, withGraph(true, seen));
      expect(res.json).toMatchObject({ allow: true, ownerId: 'dev-connor', via: 'work-graph' });
      expect(seen).toEqual([{ organizationId: 'org-1', userId: 'dev-frank', runId: 'run-nodoc', graphs: [graph] }]);
    });
    it('refuses a run the graph does not show, and names its owner', async () => {
      const res = await post({ organizationId: 'org-1', userId: 'dev-frank', runId: 'unknown-here' }, true, withGraph(false));
      expect(res.json).toMatchObject({ allow: false, ownerId: 'dev-connor' });
    });
    it('does not ask the graph when owner or document already decided', async () => {
      const seen: unknown[] = [];
      const res = await post({ organizationId: 'org-1', userId: 'dev-alice', runId: 'run-1' }, true, withGraph(true, seen));
      expect(res.json).toMatchObject({ allow: true });
      expect(seen).toHaveLength(0);
    });
    it('never for a document-only (deck-revise) question', async () => {
      const seen: unknown[] = [];
      const res = await post({ organizationId: 'org-1', userId: 'dev-frank', documentId: 'doc-1' }, true, withGraph(true, seen));
      expect(res.json.allow).toBe(false);
      expect(seen).toHaveLength(0);
    });
    it('rejects a malformed graph scope', async () => {
      const res = await post({ organizationId: 'org-1', userId: 'dev-frank', runId: 'run-1', graphs: [{ ...graph, day: 'today' }] }, true, withGraph(true));
      expect(res.status).toBe(400);
    });
  });
});
