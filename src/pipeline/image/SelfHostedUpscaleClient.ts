// platform-api/src/pipeline/image/SelfHostedUpscaleClient.ts
//
// UpscaleClient backed by a self-hosted stable-diffusion-x4-upscaler
// server (diffusers StableDiffusionUpscalePipeline + FastAPI), reached
// over an SSM port-forward — plain fetch, zero AWS.
//
// ── Server contract (what the EC2 box must expose) ──────────────────
// POST {UPSCALE_ENDPOINT_URL}{UPSCALE_GENERATE_PATH}   (default /upscale)
//   request  JSON: { image: "<base64>", prompt?, steps?, noise_level? }
//   response JSON: { image_b64: "<base64>", ... }
// (also accepts { images:[b64] } / { image } / raw image bytes; data:
//  URL prefixes are stripped). Mirror the FLUX server's image_b64
//  convention so both boxes look the same to this client.
//
// Env:
//   UPSCALE_ENDPOINT_URL   (required to select the upscale provider)
//   UPSCALE_GENERATE_PATH  (default "/upscale")
//   UPSCALE_STEPS          (optional int → `steps`)
//   UPSCALE_TIMEOUT_MS     (default 180000 — x4 diffusion on a big image is slow)

import {
  UpscaleThrottlingError,
  type UpscaleClient,
  type UpscaleRequest,
  type UpscaleResult,
} from './UpscaleClient';

function stripDataUrl(s: string): string {
  const comma = s.startsWith('data:') ? s.indexOf(',') : -1;
  return comma >= 0 ? s.slice(comma + 1) : s;
}

export interface SelfHostedUpscaleClientOpts {
  endpointUrl?: string;
  path?: string;
  steps?: number;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
}

export class SelfHostedUpscaleClient implements UpscaleClient {
  readonly provider = 'self-hosted-x4';
  private readonly endpoint: string;
  private readonly path: string;
  private readonly steps: number | undefined;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;

  constructor(opts: SelfHostedUpscaleClientOpts = {}) {
    const endpoint = opts.endpointUrl ?? process.env['UPSCALE_ENDPOINT_URL'] ?? '';
    if (!endpoint) {
      throw new Error('SelfHostedUpscaleClient: UPSCALE_ENDPOINT_URL is not set');
    }
    this.endpoint = endpoint.replace(/\/+$/, '');
    this.path = opts.path ?? process.env['UPSCALE_GENERATE_PATH'] ?? '/upscale';
    const envSteps = process.env['UPSCALE_STEPS'];
    this.steps =
      opts.steps ?? (envSteps !== undefined ? Number.parseInt(envSteps, 10) : undefined);
    this.timeoutMs =
      opts.timeoutMs ??
      (process.env['UPSCALE_TIMEOUT_MS']
        ? Number.parseInt(process.env['UPSCALE_TIMEOUT_MS'], 10)
        : 180_000);
    this.fetchImpl = opts.fetchImpl ?? globalThis.fetch;
  }

  async upscale(req: UpscaleRequest): Promise<UpscaleResult> {
    const body: Record<string, unknown> = { image: req.imageB64 };
    if (req.prompt) body['prompt'] = req.prompt;
    if (req.noiseLevel !== undefined) body['noise_level'] = req.noiseLevel;
    const steps = req.steps ?? this.steps;
    if (steps !== undefined && Number.isFinite(steps)) body['steps'] = steps;

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    let res: Response;
    try {
      res = await this.fetchImpl(`${this.endpoint}${this.path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
    } catch (err) {
      throw new Error(
        `upscale request failed (${this.endpoint}${this.path}): ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
    } finally {
      clearTimeout(timer);
    }

    if (res.status === 429 || res.status === 503) {
      throw new UpscaleThrottlingError(`upscale server busy (HTTP ${res.status})`);
    }
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(`upscale server HTTP ${res.status}: ${text.slice(0, 200)}`);
    }

    const contentType = res.headers.get('content-type') ?? '';
    if (contentType.startsWith('image/')) {
      return { imageB64: Buffer.from(await res.arrayBuffer()).toString('base64') };
    }

    const json = (await res.json()) as {
      image_b64?: unknown;
      images?: unknown;
      image?: unknown;
      error?: unknown;
    };
    if (typeof json.error === 'string' && json.error) {
      throw new Error(`upscale server error: ${json.error}`);
    }
    if (typeof json.image_b64 === 'string') return { imageB64: stripDataUrl(json.image_b64) };
    if (Array.isArray(json.images) && typeof json.images[0] === 'string') {
      return { imageB64: stripDataUrl(json.images[0]) };
    }
    if (typeof json.image === 'string') return { imageB64: stripDataUrl(json.image) };
    throw new Error('upscale server: response had no image_b64/images/image field');
  }
}
