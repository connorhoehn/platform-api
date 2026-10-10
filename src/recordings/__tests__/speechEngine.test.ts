// The transcribe step's speech engine over HTTP (SPEECH_URL): the dev
// platform-api runs in an Alpine pod and the engine is a macOS app, so the
// step reaches scripts/speech-server.mjs on the host instead of exec'ing the
// CLI. Pins the wire shape both ends agree on and how a failure reads.
import http from 'http';
import { promises as fs } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { parseSpeechResponse, transcribeFile } from '../recordingPipelines';

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
    expect(segments.every(segment => segment.timestampSource === 'text-only')).toBe(true);
  });

  it('retains native timed JSON over HTTP, including quiet gaps and mixed Call attribution', async () => {
    reply = { status: 200, body: JSON.stringify({ text: 'Legacy text should not replace segments.', timestampGranularity: 'segment', segments: [
      { startMs: 1250, endMs: 2340, text: 'Raw recognized words.', speakerId: 'untrusted', speakerLabel: 'Someone' },
      { startMs: 7125, endMs: 8875, text: 'Second line.' },
    ] }) };
    expect(await transcribeFile(file, 20_000)).toEqual([
      { startMs: 1250, endMs: 2340, text: 'Raw recognized words.', speakerId: 'call', speakerLabel: 'Call', timestampSource: 'engine' },
      { startMs: 7125, endMs: 8875, text: 'Second line.', speakerId: 'call', speakerLabel: 'Call', timestampSource: 'engine' },
    ]);
  });

  it('does not silently turn malformed HTTP timed segments into an estimated transcript', async () => {
    reply = { status: 200, body: JSON.stringify({ text: 'Plausible text.', segments: [{ startMs: 7000, endMs: 1000, text: 'Plausible text.' }] }) };
    await expect(transcribeFile(file, 20_000)).rejects.toThrow('invalid segment timestamps');
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

describe('speech timing contract', () => {
  it('preserves engine timing even if legacy recording duration is unknown', () => {
    expect(parseSpeechResponse(JSON.stringify({ segments: [{ startMs: 250, endMs: 1250, text: 'Native timing.' }] }), 0)[0])
      .toMatchObject({ startMs: 250, endMs: 1250, timestampSource: 'engine' });
  });

  it('keeps explicit engine silence without substituting stale text', () => {
    expect(parseSpeechResponse('{"segments":[],"text":"stale text"}', 30_000)).toEqual([]);
    expect(parseSpeechResponse('{"text":""}', 30_000)).toEqual([]);
  });

  it('preserves overlapping native segments and does not normalize recognized text', () => {
    const segments = parseSpeechResponse(JSON.stringify({ segments: [
      { startMs: 500, endMs: 1500, text: ' first ' }, { startMs: 1200, endMs: 2500, text: 'second' },
    ] }), 3000);
    expect(segments.map(segment => [segment.startMs, segment.endMs, segment.text])).toEqual([[500, 1500, ' first '], [1200, 2500, 'second']]);
  });

  it.each([
    { segments: null }, { segments: {} }, { segments: [null] },
    { segments: [{ startMs: '100', endMs: 200, text: 'words' }] },
    { segments: [{ startMs: -1, endMs: 200, text: 'words' }] },
    { segments: [{ startMs: 100, endMs: 99, text: 'words' }] },
    { segments: [{ startMs: 100.5, endMs: 200, text: 'words' }] },
    { segments: [{ startMs: 100, endMs: Number.MAX_SAFE_INTEGER + 1, text: 'words' }] },
    { segments: [{ startMs: 100, endMs: 200, text: 123 }] },
    { segments: [{ startMs: 1000, endMs: 2000, text: 'first' }, { startMs: 100, endMs: 300, text: 'second' }] },
  ])('rejects malformed timed output without falling back: %j', body => {
    expect(() => parseSpeechResponse(JSON.stringify({ text: 'fallback text', ...body }), 30_000)).toThrow(/invalid/);
  });

  it('rejects nonfinite values in native JSON', () => {
    expect(() => parseSpeechResponse('{"segments":[{"startMs":1e999,"endMs":1e999,"text":"words"}]}', 30_000)).toThrow(/invalid/);
  });

  it('makes the text-only unknown-duration failure explicit', () => {
    expect(() => parseSpeechResponse('{"text":"words"}', 0)).toThrow('positive media duration');
  });

  it('keeps a short text-only estimate ordered and bounded without claiming measured timing', () => {
    const segments = parseSpeechResponse('{"text":"first\\nsecond\\nthird"}', 2);
    expect(segments.map(segment => [segment.startMs, segment.endMs, segment.timestampSource]))
      .toEqual([[0, 0, 'text-only'], [0, 1, 'text-only'], [1, 2, 'text-only']]);
  });
});
