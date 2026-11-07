// The LVS admin bearer has no 'admin' default (security review §8, Low).
import { lvsAdminAuthHeader } from '../lvsAdminToken';

const saved = { ...process.env };
afterEach(() => {
  for (const k of ['VIDEO_ADMIN_TOKEN', 'LVS_ADMIN_TOKEN', 'VIDEO_API_URL', 'LVS_WEBHOOK_BOOTSTRAP_URL']) {
    if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
  }
  jest.restoreAllMocks();
});

describe('lvsAdminAuthHeader', () => {
  it('is empty when no token is configured — never Bearer admin', () => {
    delete process.env.VIDEO_ADMIN_TOKEN;
    delete process.env.LVS_ADMIN_TOKEN;
    expect(lvsAdminAuthHeader()).toEqual({});
  });
  it('uses VIDEO_ADMIN_TOKEN, then LVS_ADMIN_TOKEN', () => {
    process.env.LVS_ADMIN_TOKEN = 'lvs';
    delete process.env.VIDEO_ADMIN_TOKEN;
    expect(lvsAdminAuthHeader()).toEqual({ Authorization: 'Bearer lvs' });
    process.env.VIDEO_ADMIN_TOKEN = 'video';
    expect(lvsAdminAuthHeader()).toEqual({ Authorization: 'Bearer video' });
  });
});

describe('bootstrapLvsWebhook', () => {
  it('fails closed: no admin token, no call to LVS', async () => {
    delete process.env.VIDEO_ADMIN_TOKEN;
    delete process.env.LVS_ADMIN_TOKEN;
    process.env.VIDEO_API_URL = 'http://lvs.test';
    process.env.LVS_WEBHOOK_BOOTSTRAP_URL = 'http://platform.test/hooks/lvs/recording';
    const fetchSpy = jest.spyOn(globalThis, 'fetch').mockImplementation(async () => new Response('[]'));
    const { bootstrapLvsWebhook } = await import('../lvs-webhook-bootstrap.js');
    await bootstrapLvsWebhook();
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});
