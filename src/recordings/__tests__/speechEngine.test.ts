// The transcribe step's speech engine over HTTP (SPEECH_URL): the dev
// platform-api runs in an Alpine pod and the engine is a macOS app, so the
// step reaches scripts/speech-server.mjs on the host instead of exec'ing the
// CLI. Pins the wire shape both ends agree on and how a failure reads.
import http from 'http';
import { promises as fs } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { transcribeFile } from '../recordingPipelines';

describe('transcribeFile over SPEECH_URL', () => {
  let server: http.Server;
  let url: string;
  let received: { filename: string | undefined; bytes: number; contentType: string | undefined } | null = null;
  let reply: { status: number; body: string } = { status: 200, body: '{"text":"hello there\\nsecond line"}' };
  let file: string;
  const prevUrl = process.env.SPEECH_URL;

  beforeAll(async () => {
    server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', (c) => chunks.push(c));
      req.on('end', () => {
        received = { filename: req.headers['x-filename'] as string | undefined, bytes: Buffer.concat(chunks).length, contentType: req.headers['content-type'] };
        res.writeHead(reply.status, { 'content-type': 'application/json' });
        res.end(reply.body);
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const addr = server.address() as { port: number };
    url = `http://127.0.0.1:${addr.port}/`;
    process.env.SPEECH_URL = url;
    const dir = await fs.mkdtemp(join(tmpdir(), 'speech-test-'));
    file = join(dir, 'composed.mp4');
    await fs.writeFile(file, Buffer.alloc(1234, 7));
  });

  afterAll(async () => {
    if (prevUrl === undefined) delete process.env.SPEECH_URL; else process.env.SPEECH_URL = prevUrl;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it('posts the media bytes and spreads the returned lines over the media length', async () => {
    reply = { status: 200, body: '{"text":"hello there\\nsecond line"}' };
    const segments = await transcribeFile(file, 20_000);
    expect(received).toEqual({ filename: 'composed.mp4', bytes: 1234, contentType: 'application/octet-stream' });
    expect(segments.map((s) => s.text)).toEqual(['hello there', 'second line']);
    expect(segments[0]).toMatchObject({ startMs: 0, endMs: 10_000, speakerId: 'call' });
    expect(segments[1]).toMatchObject({ startMs: 10_000, endMs: 20_000 });
  });

  it('reads an engine failure as one', async () => {
    reply = { status: 502, body: '{"error":"spawn ENOENT"}' };
    await expect(transcribeFile(file, 5_000)).rejects.toThrow(/speech engine failed: HTTP 502 — .*ENOENT/);
  });

  it('names an unreachable engine', async () => {
    process.env.SPEECH_URL = 'http://127.0.0.1:1/';
    await expect(transcribeFile(file, 5_000)).rejects.toThrow(/speech engine unreachable at http:\/\/127\.0\.0\.1:1\//);
    process.env.SPEECH_URL = url;
  });
});
