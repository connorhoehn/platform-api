import { FluxImageGenClient } from '../FluxImageGenClient';
import { ImageGenThrottlingError } from '../ImageGenClient';

const TINY_B64 = Buffer.from('png-bytes').toString('base64');

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

describe('FluxImageGenClient', () => {
  it('posts prompt+dims to {endpoint}{path} and parses { images: [b64] }', async () => {
    const calls: Array<{ url: string; body: any }> = [];
    const fetchImpl = (async (url: any, init: any) => {
      calls.push({ url: String(url), body: JSON.parse(init.body) });
      return jsonResponse({ images: [TINY_B64] });
    }) as unknown as typeof fetch;

    const client = new FluxImageGenClient({
      endpointUrl: 'http://127.0.0.1:22002/',
      generatePath: '/generate',
      fetchImpl,
    });
    const res = await client.generate({ prompt: 'a cat', width: 512, height: 512, numImages: 1 });

    expect(res.images).toEqual([TINY_B64]);
    expect(calls).toHaveLength(1);
    // Trailing slash on endpoint is normalized; path joined cleanly.
    expect(calls[0]!.url).toBe('http://127.0.0.1:22002/generate');
    expect(calls[0]!.body).toMatchObject({ prompt: 'a cat', width: 512, height: 512 });
  });

  it('issues one request per image with incrementing seeds', async () => {
    const seeds: Array<number | undefined> = [];
    const fetchImpl = (async (_url: any, init: any) => {
      seeds.push(JSON.parse(init.body).seed);
      return jsonResponse({ image: TINY_B64 });
    }) as unknown as typeof fetch;

    const client = new FluxImageGenClient({ endpointUrl: 'http://x', fetchImpl });
    const res = await client.generate({
      prompt: 'p', width: 256, height: 256, numImages: 3, seed: 100,
    });

    expect(res.images).toHaveLength(3);
    expect(seeds).toEqual([100, 101, 102]);
  });

  it('parses the server\'s { image_b64 } field', async () => {
    const fetchImpl = (async () =>
      jsonResponse({ model: 'flux', steps: 4, size: [512, 512], image_b64: TINY_B64 })) as unknown as typeof fetch;
    const client = new FluxImageGenClient({ endpointUrl: 'http://x', fetchImpl });
    const res = await client.generate({ prompt: 'p', width: 512, height: 512, numImages: 1 });
    expect(res.images).toEqual([TINY_B64]);
  });

  it('parses the single { image } shape and strips a data: URL prefix', async () => {
    const fetchImpl = (async () =>
      jsonResponse({ image: `data:image/png;base64,${TINY_B64}` })) as unknown as typeof fetch;
    const client = new FluxImageGenClient({ endpointUrl: 'http://x', fetchImpl });
    const res = await client.generate({ prompt: 'p', width: 64, height: 64, numImages: 1 });
    expect(res.images).toEqual([TINY_B64]);
  });

  it('handles a raw image/* body', async () => {
    const fetchImpl = (async () =>
      new Response(Buffer.from('png-bytes'), {
        status: 200,
        headers: { 'content-type': 'image/png' },
      })) as unknown as typeof fetch;
    const client = new FluxImageGenClient({ endpointUrl: 'http://x', fetchImpl });
    const res = await client.generate({ prompt: 'p', width: 64, height: 64, numImages: 1 });
    expect(res.images).toEqual([TINY_B64]);
  });

  it('maps 429/503 to ImageGenThrottlingError', async () => {
    const fetchImpl = (async () =>
      new Response('busy', { status: 503 })) as unknown as typeof fetch;
    const client = new FluxImageGenClient({ endpointUrl: 'http://x', fetchImpl });
    await expect(
      client.generate({ prompt: 'p', width: 64, height: 64, numImages: 1 }),
    ).rejects.toBeInstanceOf(ImageGenThrottlingError);
  });

  it('surfaces a non-ok HTTP error with the body snippet', async () => {
    const fetchImpl = (async () =>
      new Response('boom-detail', { status: 500 })) as unknown as typeof fetch;
    const client = new FluxImageGenClient({ endpointUrl: 'http://x', fetchImpl });
    await expect(
      client.generate({ prompt: 'p', width: 64, height: 64, numImages: 1 }),
    ).rejects.toThrow(/HTTP 500: boom-detail/);
  });

  it('throws when FLUX_ENDPOINT_URL is absent and no endpoint injected', () => {
    const saved = process.env['FLUX_ENDPOINT_URL'];
    delete process.env['FLUX_ENDPOINT_URL'];
    try {
      expect(() => new FluxImageGenClient()).toThrow(/FLUX_ENDPOINT_URL/);
    } finally {
      if (saved !== undefined) process.env['FLUX_ENDPOINT_URL'] = saved;
    }
  });

  it('passes steps when configured', async () => {
    let sentBody: any;
    const fetchImpl = (async (_u: any, init: any) => {
      sentBody = JSON.parse(init.body);
      return jsonResponse({ images: [TINY_B64] });
    }) as unknown as typeof fetch;
    const client = new FluxImageGenClient({ endpointUrl: 'http://x', steps: 4, fetchImpl });
    await client.generate({ prompt: 'p', width: 64, height: 64, numImages: 1 });
    expect(sentBody.steps).toBe(4);
  });
});
