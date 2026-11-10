// Integration tests for the ambient voice-capture PCM proxy.
//
// These run against a REAL listening express app rather than a mocked req/res
// pair, because the thing most likely to break here is body parsing: the route
// mounts `express.raw({ type: 'application/octet-stream' })` and a synthetic
// request object would hand it a pre-parsed body and prove nothing.
//
// Only the sidecar itself is stubbed, at `fetch`.

import express, { type NextFunction, type Request, type Response } from 'express';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';

jest.mock('../../lib/logger', () => ({
  __esModule: true,
  default: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
  withContext: jest.fn(() => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() })),
}));

import { voiceCaptureRouter, __resetVoiceCaptureBindings } from '../voiceCapture';
import { errorHandler } from '../../middleware/error-handler';
import log from '../../lib/logger';

const CAPTURE_ID = 'abcdef0123456789abcdef0123456789';
const UTTERANCE_ID = '0123456789abcdef0123456789abcdef';

let currentUser = 'user-alice';
let server: Server;
let baseUrl: string;
/** The real fetch, captured before the sidecar stub replaces the global. */
const realFetch = global.fetch.bind(global);
let sidecarFetch: jest.Mock;

beforeAll(async () => {
  const app = express();
  app.use((req: Request, _res: Response, next: NextFunction) => {
    req.user = { sub: currentUser, email: `${currentUser}@test` };
    next();
  });
  app.use('/api/voice-capture', voiceCaptureRouter);
  app.use(errorHandler);
  await new Promise<void>((resolve) => {
    server = app.listen(0, resolve);
  });
  baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterAll(async () => {
  (global as unknown as { fetch: unknown }).fetch = realFetch;
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

/** POST to the app under test using the REAL fetch, bypassing the sidecar stub. */
async function postPcm(
  bytes: Buffer,
  headers: Record<string, string> = {},
): Promise<{ status: number; body: any }> {
  const res = await realFetch(`${baseUrl}/api/voice-capture/pcm`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/octet-stream',
      'X-Capture-Id': CAPTURE_ID,
      'X-Utterance-Id': UTTERANCE_ID,
      ...headers,
    },
    body: new Uint8Array(bytes),
  });
  const body = await res.json().catch(() => ({}));
  return { status: res.status, body };
}

beforeEach(() => {
  __resetVoiceCaptureBindings();
  currentUser = 'user-alice';
  process.env.LIVE_CAPTIONS_URL = 'http://sidecar.test:3915';
  delete process.env.LIVE_CAPTIONS_SHARED_SECRET;
  (log.warn as jest.Mock).mockClear();
  sidecarFetch = jest.fn().mockResolvedValue({
    ok: true,
    status: 200,
    text: async () => 'ok',
  });
  (global as unknown as { fetch: unknown }).fetch = sidecarFetch;
});

describe('POST /api/voice-capture/pcm', () => {
  it('forwards PCM to the sidecar with a capture: routing key', async () => {
    const res = await postPcm(Buffer.alloc(640)); // 20 ms of s16le @ 16 kHz
    expect(res.status).toBe(202);
    expect(sidecarFetch).toHaveBeenCalledTimes(1);

    const [url, init] = sidecarFetch.mock.calls[0]!;
    expect(url).toBe('http://sidecar.test:3915/pcm');
    // The routing key is what caption-relay hashes into the WS channel. If it
    // drifts, transcripts publish to a channel nobody is listening on — and
    // the failure looks exactly like "the microphone did not work".
    expect(init.headers['X-Channel-Arn']).toBe(`capture:${CAPTURE_ID}`);
    expect(init.headers['X-Sample-Rate']).toBe('16000');
    expect(init.body.byteLength).toBe(640);
  });

  it('uses the utterance id as the sidecar participant id', async () => {
    // This is what gives every push-to-talk press a FRESH accumulator on the
    // sidecar. Without it, a sub-threshold remark stays buffered and is
    // prepended to the next utterance — attaching it to the next one's target.
    await postPcm(Buffer.alloc(320));
    expect(sidecarFetch.mock.calls[0]![1].headers['X-Participant-Id']).toBe(UTTERANCE_ID);
  });

  it('carries no user identity into the caption payload', async () => {
    // participantId is echoed into every caption published on the channel.
    await postPcm(Buffer.alloc(320));
    const headers = JSON.stringify(sidecarFetch.mock.calls[0]![1].headers);
    expect(headers).not.toContain('user-alice');
  });

  it('rejects a capture id that is not plain [A-Za-z0-9_-]', async () => {
    // The id becomes a Redis channel name; a colon or wildcard there addresses
    // a fan-out nobody intended.
    const res = await postPcm(Buffer.alloc(320), { 'X-Capture-Id': 'captions:*' });
    expect(res.status).toBe(400);
    expect(sidecarFetch).not.toHaveBeenCalled();
  });

  it('rejects a missing utterance id', async () => {
    const res = await postPcm(Buffer.alloc(320), { 'X-Utterance-Id': '' });
    expect(res.status).toBe(400);
    expect(sidecarFetch).not.toHaveBeenCalled();
  });

  it('rejects an odd-length body (torn s16le sample)', async () => {
    const res = await postPcm(Buffer.alloc(321));
    expect(res.status).toBe(400);
  });

  it('rejects an empty body', async () => {
    const res = await postPcm(Buffer.alloc(0));
    expect(res.status).toBe(400);
  });

  it('refuses a second user streaming into a capture id already in use', async () => {
    expect((await postPcm(Buffer.alloc(320))).status).toBe(202);
    currentUser = 'user-mallory';
    const res = await postPcm(Buffer.alloc(320));
    expect(res.status).toBe(403);
    expect(sidecarFetch).toHaveBeenCalledTimes(1);
  });

  it('lets the original owner keep streaming', async () => {
    for (let i = 0; i < 3; i += 1) {
      expect((await postPcm(Buffer.alloc(320))).status).toBe(202);
    }
    expect(sidecarFetch).toHaveBeenCalledTimes(3);
  });

  it('reports 503 rather than a generic error when the sidecar is down', async () => {
    // "Transcription is unavailable" and "your microphone did not work" need
    // different words in front of someone who just spoke a whole sentence.
    sidecarFetch.mockRejectedValueOnce(new Error('ECONNREFUSED'));
    const res = await postPcm(Buffer.alloc(320));
    expect(res.status).toBe(503);
    expect(String(res.body.error)).toMatch(/unavailable/);
  });

  it('sends LIVE_CAPTIONS_SHARED_SECRET as X-Live-Captions-Secret', async () => {
    process.env.LIVE_CAPTIONS_SHARED_SECRET = 'cap-secret';
    expect((await postPcm(Buffer.alloc(320))).status).toBe(202);
    expect(sidecarFetch.mock.calls[0]![1].headers['X-Live-Captions-Secret']).toBe('cap-secret');
  });

  it('sends no secret header when LIVE_CAPTIONS_SHARED_SECRET is unset (sidecar in insecure dev mode)', async () => {
    delete process.env.LIVE_CAPTIONS_SHARED_SECRET;
    expect((await postPcm(Buffer.alloc(320))).status).toBe(202);
    expect(Object.keys(sidecarFetch.mock.calls[0]![1].headers)).not.toContain('X-Live-Captions-Secret');
  });

  it('answers 502 on a sidecar 401 and logs the misconfiguration without the secret', async () => {
    process.env.LIVE_CAPTIONS_SHARED_SECRET = 'wrong-secret';
    sidecarFetch.mockResolvedValueOnce({ ok: false, status: 401, text: async () => 'unauthorized' });
    const res = await postPcm(Buffer.alloc(320));
    expect(res.status).toBe(502);
    const warnings = JSON.stringify((log.warn as jest.Mock).mock.calls);
    expect(warnings).toContain('voice.capture.sidecar_unauthorized');
    expect(warnings).toContain('LIVE_CAPTIONS_SHARED_SECRET');
    expect(warnings).not.toContain('wrong-secret');
  });

  it('reports 502 when the sidecar rejects the chunk', async () => {
    sidecarFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      text: async () => 'missing X-Channel-Arn',
    });
    const res = await postPcm(Buffer.alloc(320));
    expect(res.status).toBe(502);
  });
});

describe('GET /api/voice-capture/health', () => {
  it('reports available:false instead of erroring when the sidecar is down', async () => {
    sidecarFetch.mockRejectedValue(new Error('ECONNREFUSED'));
    const res = await realFetch(`${baseUrl}/api/voice-capture/health`);
    expect(res.status).toBe(200);
    expect(((await res.json()) as { available: boolean }).available).toBe(false);
  });

  it('reports available:true when the sidecar answers healthz', async () => {
    sidecarFetch.mockResolvedValue({ ok: true, status: 200, text: async () => 'ok' });
    const res = await realFetch(`${baseUrl}/api/voice-capture/health`);
    expect(((await res.json()) as { available: boolean }).available).toBe(true);
  });
});
