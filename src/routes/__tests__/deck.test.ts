// /api/deck — preview and fixture. Both are stateless; the tests assert that,
// and that a spec too big to be a deck is refused as 413 rather than rendered.

import express from 'express';
import http from 'http';
import type { AddressInfo } from 'net';
import { errorHandler } from '../../middleware/error-handler';
import { MAX_SLIDES } from '../../deck/DeckSpec';
import { sampleDeck } from '../../deck/__tests__/fixtures';

const buildDeckFixture = jest.fn();

jest.mock('../../deck/deckFixture', () => {
  const actual = jest.requireActual('../../deck/deckFixture');
  return { ...actual, buildDeckFixture: (...args: unknown[]) => buildDeckFixture(...args) };
});

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { deckRouter } from '../deck';
import { NoFixtureMaterialError } from '../../deck/deckFixture';

function post(path: string, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const { port } = server.address() as AddressInfo;
  const payload = Buffer.from(JSON.stringify(body), 'utf8');
  return new Promise((resolve, reject) => {
    const req = http.request({
      host: '127.0.0.1', port, method: 'POST', path,
      headers: { 'content-type': 'application/json', 'content-length': String(payload.length) },
    }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk: Buffer) => chunks.push(chunk));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        resolve({ status: res.statusCode ?? 0, body: text ? JSON.parse(text) : {} });
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

let server: http.Server;
const originalSkipAuth = process.env.SKIP_AUTH;

beforeAll(async () => {
  process.env.SKIP_AUTH = 'true';
  const app = express();
  app.use(express.json({ limit: '4mb' }));
  app.use('/api/deck', deckRouter);
  app.use(errorHandler);
  server = http.createServer(app);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
});

afterAll(async () => {
  if (originalSkipAuth === undefined) delete process.env.SKIP_AUTH;
  else process.env.SKIP_AUTH = originalSkipAuth;
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

beforeEach(() => buildDeckFixture.mockReset());

describe('POST /api/deck/preview', () => {
  it('renders every slide plus Sources, with warnings, and stores nothing', async () => {
    const res = await post('/api/deck/preview', { spec: sampleDeck() });
    expect(res.status).toBe(200);
    expect(res.body.slideCount).toBe(9);
    expect((res.body.slides as Array<{ id: string; svg: string }>)[0]!.svg.startsWith('<svg')).toBe(true);
    expect(Array.isArray(res.body.warnings)).toBe(true);
  });

  it('accepts the spec at the top level too, the way an editor posts it', async () => {
    expect((await post('/api/deck/preview', sampleDeck())).status).toBe(200);
  });

  it('400s a deck with an uncited slide, listing every problem', async () => {
    const spec = sampleDeck();
    spec.slides[0]!.sources = [];
    const res = await post('/api/deck/preview', { spec });
    expect(res.status).toBe(400);
    expect(String(res.body.error)).toContain('must cite at least one source');
    expect((res.body.errors as string[]).length).toBeGreaterThan(0);
  });

  it('413s a deck past the slide cap', async () => {
    const slide = { id: 's', layout: 'bullets', title: 'T', bullets: ['b'], sources: [{ documentId: 'd', title: 'D' }] };
    const spec = { title: 'Deck', slides: Array.from({ length: MAX_SLIDES + 1 }, (_v, index) => ({ ...slide, id: `s${index}` })) };
    const res = await post('/api/deck/preview', { spec });
    expect(res.status).toBe(413);
    expect(String(res.body.error)).toContain(`the cap is ${MAX_SLIDES}`);
  });
});

describe('POST /api/deck/fixture', () => {
  it('answers with the spec and says where it came from', async () => {
    buildDeckFixture.mockResolvedValue({
      spec: sampleDeck(),
      source: 'model',
      warnings: ['Release plan was provided but not cited.'],
      transcriptIds: ['active-now-planning-transcript'],
      material: { document: { documentId: 'doc-release-plan', title: 'Release plan', text: 'xxx' } },
    });
    const res = await post('/api/deck/fixture', { from: { documentId: 'doc-release-plan' }, title: 'Sprint review' });
    expect(res.status).toBe(200);
    expect(res.body.source).toBe('model');
    expect((res.body.material as Record<string, unknown>).document).toMatchObject({ documentId: 'doc-release-plan', characters: 3 });
    expect(res.body.warnings).toEqual(['Release plan was provided but not cited.']);
    expect(res.body.transcriptIds).toEqual(['active-now-planning-transcript']);
    expect(buildDeckFixture).toHaveBeenCalledWith({ documentId: 'doc-release-plan' }, 'Sprint review', {});
  });

  it('passes the fallback reason through rather than hiding it', async () => {
    buildDeckFixture.mockResolvedValue({ spec: sampleDeck(), source: 'fixture', reason: 'no model is configured on this deployment', warnings: [], transcriptIds: [], material: {} });
    const res = await post('/api/deck/fixture', { from: { recordingId: 'rec-1' } });
    expect(res.status).toBe(200);
    expect(res.body.source).toBe('fixture');
    expect(res.body.reason).toBe('no model is configured on this deployment');
  });

  it('400s when nothing is named, and when what is named is not an id', async () => {
    expect((await post('/api/deck/fixture', { from: {} })).status).toBe(400);
    expect((await post('/api/deck/fixture', { from: { documentId: '../etc/passwd' } })).status).toBe(400);
    expect(buildDeckFixture).not.toHaveBeenCalled();
  });

  it('400s when the named material cannot be read', async () => {
    buildDeckFixture.mockRejectedValue(new NoFixtureMaterialError('name a documentId or a recordingId that can be read'));
    const res = await post('/api/deck/fixture', { from: { recordingId: 'rec-missing' } });
    expect(res.status).toBe(400);
    expect(String(res.body.error)).toContain('can be read');
  });
});
