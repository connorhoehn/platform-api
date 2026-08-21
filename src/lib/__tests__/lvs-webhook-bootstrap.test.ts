// Regression (2026-08-21): LVS GET /api/event-subscriptions returns a BARE
// ARRAY, but bootstrap read `body.subscriptions` — the dedup check always saw
// [] and every 30s reconciler tick POSTed another duplicate subscription
// (2,356 accumulated; each recording event then fanned out into thousands of
// webhook deliveries through the SFU event loop and froze active calls).

jest.mock('../logger', () => ({
  __esModule: true,
  default: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
}));

import { bootstrapLvsWebhook } from '../lvs-webhook-bootstrap';

const RECEIVER = 'http://localhost:13001/hooks/lvs/recording';
const FILTER = 'IVS Recording State Change';

describe('bootstrapLvsWebhook dedup', () => {
  const origEnv = process.env;
  let fetchMock: jest.Mock;

  beforeEach(() => {
    process.env = {
      ...origEnv,
      VIDEO_API_URL: 'http://lvs.test:3901',
      LVS_WEBHOOK_BOOTSTRAP_URL: RECEIVER,
    };
    fetchMock = jest.fn();
    (global as { fetch: unknown }).fetch = fetchMock;
  });

  afterEach(() => {
    process.env = origEnv;
  });

  function jsonRes(body: unknown, status = 200) {
    return {
      ok: status < 400,
      status,
      json: async () => body,
      text: async () => JSON.stringify(body),
    };
  }

  it('no-ops when the bare-array list already contains the subscription', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonRes([{ id: 'sub-1', url: RECEIVER, filter: FILTER }]),
    );
    await bootstrapLvsWebhook();
    // Only the GET — no duplicate POST.
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('still handles the { subscriptions: [...] } envelope shape', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonRes({ subscriptions: [{ id: 'sub-1', url: RECEIVER, filter: FILTER }] }),
    );
    await bootstrapLvsWebhook();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('POSTs when no matching subscription exists', async () => {
    fetchMock
      .mockResolvedValueOnce(jsonRes([{ id: 'other', url: 'http://elsewhere/', filter: FILTER }]))
      .mockResolvedValueOnce(jsonRes({ id: 'new-sub' }, 201));
    await bootstrapLvsWebhook();
    expect(fetchMock).toHaveBeenCalledTimes(2);
    const [, postCall] = fetchMock.mock.calls;
    expect(postCall[1].method).toBe('POST');
  });
});
