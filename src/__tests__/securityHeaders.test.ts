import http from 'http';
import { AddressInfo } from 'net';
import { createApp } from '../app';

function get(server: http.Server, path: string, headers: Record<string, string> = {}) {
  const { port } = server.address() as AddressInfo;
  return new Promise<http.IncomingMessage>((resolve, reject) => {
    http.get({ host: '127.0.0.1', port, path, headers }, (res) => { res.resume(); res.on('end', () => resolve(res)); }).on('error', reject);
  });
}

describe('security headers', () => {
  let server: http.Server;
  beforeAll((done) => { server = createApp().listen(0, '127.0.0.1', done); });
  afterAll((done) => { server.close(() => done()); });

  const expectBase = (res: http.IncomingMessage) => {
    expect(res.headers['x-content-type-options']).toBe('nosniff');
    expect(res.headers['referrer-policy']).toBe('strict-origin-when-cross-origin');
    expect(res.headers['x-frame-options']).toBe('DENY');
    expect(res.headers['content-security-policy']).toBe("default-src 'none'; frame-ancestors 'none'");
    expect(res.headers['permissions-policy']).toContain('camera=()');
    expect(res.headers['x-powered-by']).toBeUndefined();
  };

  it('sets them on a JSON route', async () => {
    const res = await get(server, '/livez');
    expectBase(res);
    expect(res.headers['strict-transport-security']).toBeUndefined();
  });

  it('sets them on an unmatched path (auth rejects it before routing, so the error reply is a 401)', async () => {
    const res = await get(server, '/no-such-route-xyz');
    expect([401, 404]).toContain(res.statusCode);
    expectBase(res);
  });

  it('sends HSTS behind a trusted TLS proxy only', async () => {
    const prev = process.env.TRUST_PROXY;
    process.env.TRUST_PROXY = '1';
    try {
      expect((await get(server, '/livez', { 'x-forwarded-proto': 'https' })).headers['strict-transport-security']).toContain('max-age=');
      expect((await get(server, '/livez', { 'x-forwarded-proto': 'http' })).headers['strict-transport-security']).toBeUndefined();
    } finally {
      if (prev === undefined) delete process.env.TRUST_PROXY; else process.env.TRUST_PROXY = prev;
    }
  });
});
