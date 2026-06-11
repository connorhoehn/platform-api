// platform-api/src/pipeline/image/FluxImageGenClient.ts
//
// ImageGenClient backed by a self-hosted FLUX text-to-image server
// (e.g. black-forest-labs/FLUX.2-klein on an EC2 box, reached over an
// SSM port-forward at http://127.0.0.1:22002). Bedrock has no active
// image-output model on the operator's account, so this is the live
// image-generation path.
//
// ── Server contract (configurable) ──────────────────────────────────
// Request:  POST {FLUX_ENDPOINT_URL}{FLUX_GENERATE_PATH}   (default /generate)
//   JSON body: { prompt, width, height, seed?, steps? }
// Response: this server returns { "image_b64": "<base64 png>", ... }
// (see ec2-flux1-imagegen/server.py — `return {... "image_b64": ...}`).
// We also accept { images:[...] } / { image } / raw image bytes so the
// client survives a server tweak. A data: URL prefix is stripped.
//
// FLUX.2-klein on a single GPU is single-image-per-call, so we issue
// `numImages` sequential requests (seed = baseSeed + i for variety)
// rather than asking the server to batch — this guarantees the handler
// gets the count it asked for regardless of server batch support.
//
// Env:
//   FLUX_ENDPOINT_URL   (required to select flux provider; e.g. http://127.0.0.1:22002)
//   FLUX_GENERATE_PATH  (default "/generate")
//   FLUX_STEPS          (optional int; passed as `steps` when set — klein wants few)
//   FLUX_TIMEOUT_MS     (default 120000 — a single FLUX image can take tens of seconds)

import {
  ImageGenThrottlingError,
  type ImageGenClient,
  type ImageGenRequest,
  type ImageGenResult,
} from './ImageGenClient';

function stripDataUrl(s: string): string {
  const comma = s.startsWith('data:') ? s.indexOf(',') : -1;
  return comma >= 0 ? s.slice(comma + 1) : s;
}

export interface FluxImageGenClientOpts {
  endpointUrl?: string;
  generatePath?: string;
  steps?: number;
  timeoutMs?: number;
  /** Injectable for tests; defaults to global fetch. */
  fetchImpl?: typeof fetch;
}

export class FluxImageGenClient implements ImageGenClient {
  readonly provider = 'flux';
  private readonly endpoint: string;
  private readonly path: string;
  private readonly steps: number | undefined;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;

  constructor(opts: FluxImageGenClientOpts = {}) {
    const endpoint = opts.endpointUrl ?? process.env['FLUX_ENDPOINT_URL'] ?? '';
    if (!endpoint) {
      throw new Error('FluxImageGenClient: FLUX_ENDPOINT_URL is not set');
    }
    this.endpoint = endpoint.replace(/\/+$/, '');
    this.path = opts.generatePath ?? process.env['FLUX_GENERATE_PATH'] ?? '/generate';
    const envSteps = process.env['FLUX_STEPS'];
    this.steps =
      opts.steps ?? (envSteps !== undefined ? Number.parseInt(envSteps, 10) : undefined);
    this.timeoutMs =
      opts.timeoutMs ??
      (process.env['FLUX_TIMEOUT_MS']
        ? Number.parseInt(process.env['FLUX_TIMEOUT_MS'], 10)
        : 120_000);
    this.fetchImpl = opts.fetchImpl ?? globalThis.fetch;
  }

  async generate(req: ImageGenRequest): Promise<ImageGenResult> {
    const images: string[] = [];
    for (let i = 0; i < req.numImages; i++) {
      const seed = req.seed !== undefined ? req.seed + i : undefined;
      images.push(await this.generateOne(req, seed));
    }
    return { images };
  }

  private async generateOne(req: ImageGenRequest, seed: number | undefined): Promise<string> {
    const body: Record<string, unknown> = {
      prompt: req.prompt,
      width: req.width,
      height: req.height,
    };
    if (seed !== undefined) body['seed'] = seed;
    if (this.steps !== undefined && Number.isFinite(this.steps)) body['steps'] = this.steps;

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
        `FLUX request failed (${this.endpoint}${this.path}): ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
    } finally {
      clearTimeout(timer);
    }

    if (res.status === 429 || res.status === 503) {
      throw new ImageGenThrottlingError(`FLUX server busy (HTTP ${res.status})`);
    }
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(`FLUX server HTTP ${res.status}: ${text.slice(0, 200)}`);
    }

    const contentType = res.headers.get('content-type') ?? '';

    // Shape 3: raw image bytes.
    if (contentType.startsWith('image/')) {
      const buf = Buffer.from(await res.arrayBuffer());
      return buf.toString('base64');
    }

    // JSON shapes — image_b64 is this server's field; the rest are fallbacks.
    const json = (await res.json()) as {
      image_b64?: unknown;
      images?: unknown;
      image?: unknown;
      error?: unknown;
    };
    if (typeof json.error === 'string' && json.error) {
      throw new Error(`FLUX server error: ${json.error}`);
    }
    if (typeof json.image_b64 === 'string') {
      return stripDataUrl(json.image_b64);
    }
    if (Array.isArray(json.images) && typeof json.images[0] === 'string') {
      return stripDataUrl(json.images[0]);
    }
    if (typeof json.image === 'string') {
      return stripDataUrl(json.image);
    }
    throw new Error('FLUX server: response had no image_b64/images/image field');
  }
}
