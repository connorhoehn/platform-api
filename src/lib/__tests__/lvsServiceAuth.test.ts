import { lvsSessionAuthHeader } from '../lvsServiceAuth';

describe('lvsSessionAuthHeader', () => {
  const prior = process.env.VIDEO_SERVICE_TOKEN;
  afterEach(() => {
    if (prior === undefined) delete process.env.VIDEO_SERVICE_TOKEN; else process.env.VIDEO_SERVICE_TOKEN = prior;
  });

  it('is the service token as a bearer', () => {
    process.env.VIDEO_SERVICE_TOKEN = ' svc-1 ';
    expect(lvsSessionAuthHeader()).toBe('Bearer svc-1');
  });

  it.each([undefined, '', '   '])('throws 503 naming the variable when unset (%j)', (v) => {
    if (v === undefined) delete process.env.VIDEO_SERVICE_TOKEN; else process.env.VIDEO_SERVICE_TOKEN = v;
    expect(() => lvsSessionAuthHeader()).toThrow(/VIDEO_SERVICE_TOKEN/);
    try { lvsSessionAuthHeader(); } catch (e) { expect((e as { statusCode?: number; status?: number }).statusCode ?? (e as { status?: number }).status).toBe(503); }
  });
});
