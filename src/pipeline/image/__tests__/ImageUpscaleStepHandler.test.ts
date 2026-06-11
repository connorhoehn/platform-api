import sharp from 'sharp';
import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import { InMemoryImageBlobStore } from '../ImageBlobStore';
import { createImageUpscaleStepHandler } from '../ImageUpscaleStepHandler';
import { SelfHostedUpscaleClient } from '../SelfHostedUpscaleClient';
import { UpscaleThrottlingError, type UpscaleClient } from '../UpscaleClient';

function makeHelpers(): { helpers: StepHandlerHelpers; emitted: Array<{ event: string; payload: unknown }> } {
  const emitted: Array<{ event: string; payload: unknown }> = [];
  const helpers = {
    emit: (async (event: string, payload: unknown) => { emitted.push({ event, payload }); }) as never,
    runId: 'r', ownerNodeId: 'n', stepId: 's',
    abortSignal: new AbortController().signal, sleep: async () => {}, isCancelled: () => false,
  } as unknown as StepHandlerHelpers;
  return { helpers, emitted };
}

function node(data: Record<string, unknown>): PipelineNode {
  return { id: 'up', type: 'image:upscale', position: { x: 0, y: 0 }, data: { type: 'image:upscale', ...data } } as unknown as PipelineNode;
}

async function seedImage(blob: InMemoryImageBlobStore, id: string, w: number, h: number) {
  const png = await sharp(Buffer.alloc(w * h * 3, 120), { raw: { width: w, height: h, channels: 3 } }).png().toBuffer();
  await blob.put(id, png, { mimeType: 'image/png', sizeBytes: png.length, width: w, height: h, uploadedAt: 'now' });
}

/** Upscale client that returns a 2048×2048 image and records the request. */
function fakeUpscaleClient(): { client: UpscaleClient; last: () => { imageB64: string; prompt?: string } | null } {
  let last: { imageB64: string; prompt?: string } | null = null;
  return {
    client: {
      provider: 'fake',
      async upscale(req) {
        last = { imageB64: req.imageB64, ...(req.prompt ? { prompt: req.prompt } : {}) };
        const big = await sharp(Buffer.alloc(2048 * 2048 * 3, 90), { raw: { width: 2048, height: 2048, channels: 3 } }).jpeg().toBuffer();
        return { imageB64: big.toString('base64') };
      },
    },
    last: () => last,
  };
}

describe('image:upscale', () => {
  it('upscales a source image, cap-fits to <=1024, stores it, and emits produced', async () => {
    const blob = new InMemoryImageBlobStore();
    await seedImage(blob, 'src', 512, 512);
    const { client, last } = fakeUpscaleClient();
    const handler = createImageUpscaleStepHandler({ imageBlobStore: blob, upscaleClient: client });
    const { helpers, emitted } = makeHelpers();

    const outcome = await handler.execute(node({ prompt: 'sharp details' }), { imageId: 'src' }, helpers);

    expect(outcome.status).toBe('completed');
    const o = outcome.output as Record<string, unknown>;
    expect(o.sourceImageId).toBe('src');
    expect(o.width).toBeLessThanOrEqual(1024);
    expect(o.height).toBeLessThanOrEqual(1024);
    // the upscaler received the source's bytes + the prompt
    expect(last()?.prompt).toBe('sharp details');
    // stored + emitted
    const stored = await blob.get(o.imageId as string);
    expect(stored).not.toBeNull();
    expect(emitted.some((e) => e.event === 'pipeline:step:image:produced')).toBe(true);
  });

  it('honors a custom maxEdge', async () => {
    const blob = new InMemoryImageBlobStore();
    await seedImage(blob, 'src', 512, 512);
    const { client } = fakeUpscaleClient();
    const handler = createImageUpscaleStepHandler({ imageBlobStore: blob, upscaleClient: client });
    const outcome = await handler.execute(node({ maxEdge: 768 }), { imageId: 'src' }, makeHelpers().helpers);
    const o = outcome.output as Record<string, number>;
    expect(o.width).toBeLessThanOrEqual(768);
  });

  it('fails clearly when the source image is missing', async () => {
    const blob = new InMemoryImageBlobStore();
    const { client } = fakeUpscaleClient();
    const handler = createImageUpscaleStepHandler({ imageBlobStore: blob, upscaleClient: client });
    const outcome = await handler.execute(node({}), { imageId: 'nope' }, makeHelpers().helpers);
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/not found/);
  });

  it('maps throttling to a retriable failure', async () => {
    const blob = new InMemoryImageBlobStore();
    await seedImage(blob, 'src', 512, 512);
    const client: UpscaleClient = {
      provider: 'fake',
      async upscale() { throw new UpscaleThrottlingError(); },
    };
    const handler = createImageUpscaleStepHandler({ imageBlobStore: blob, upscaleClient: client });
    const outcome = await handler.execute(node({}), { imageId: 'src' }, makeHelpers().helpers);
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/rate limited/);
  });
});

describe('SelfHostedUpscaleClient', () => {
  const TINY = Buffer.from('img').toString('base64');
  function jsonRes(body: unknown, status = 200): Response {
    return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
  }

  it('POSTs image+prompt to {endpoint}/upscale and parses image_b64', async () => {
    let sent: any;
    const fetchImpl = (async (url: any, init: any) => { sent = { url: String(url), body: JSON.parse(init.body) }; return jsonRes({ image_b64: TINY }); }) as unknown as typeof fetch;
    const client = new SelfHostedUpscaleClient({ endpointUrl: 'http://127.0.0.1:22004/', fetchImpl });
    const r = await client.upscale({ imageB64: 'SRC', prompt: 'p', steps: 30 });
    expect(r.imageB64).toBe(TINY);
    expect(sent.url).toBe('http://127.0.0.1:22004/upscale');
    expect(sent.body).toMatchObject({ image: 'SRC', prompt: 'p', steps: 30 });
  });

  it('maps 503 to UpscaleThrottlingError', async () => {
    const fetchImpl = (async () => new Response('busy', { status: 503 })) as unknown as typeof fetch;
    const client = new SelfHostedUpscaleClient({ endpointUrl: 'http://x', fetchImpl });
    await expect(client.upscale({ imageB64: 'x' })).rejects.toBeInstanceOf(UpscaleThrottlingError);
  });

  it('throws when UPSCALE_ENDPOINT_URL is absent', () => {
    const saved = process.env['UPSCALE_ENDPOINT_URL'];
    delete process.env['UPSCALE_ENDPOINT_URL'];
    try {
      expect(() => new SelfHostedUpscaleClient()).toThrow(/UPSCALE_ENDPOINT_URL/);
    } finally {
      if (saved !== undefined) process.env['UPSCALE_ENDPOINT_URL'] = saved;
    }
  });
});
