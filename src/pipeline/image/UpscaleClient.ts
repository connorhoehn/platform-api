// platform-api/src/pipeline/image/UpscaleClient.ts
//
// Provider-agnostic image-upscale seam. Mirrors ImageGenClient. The only
// implementation is a self-hosted diffusers server
// (stabilityai/stable-diffusion-x4-upscaler) reached over an SSM
// port-forward — NO cloud SDKs. The step handler owns everything around
// it (source resolution, blob store, events).

export interface UpscaleRequest {
  /** Base64-encoded source image (the low-res input). */
  imageB64: string;
  /** Optional text prompt to guide the upscale (the x4 model accepts one). */
  prompt?: string;
  /** Diffusion steps. Server default applies when omitted. */
  steps?: number;
  /** Noise level (x4 upscaler param). Server default applies when omitted. */
  noiseLevel?: number;
}

export interface UpscaleResult {
  /** Base64-encoded upscaled image. */
  imageB64: string;
}

/** Thrown on provider back-pressure → mapped to a retriable failure. */
export class UpscaleThrottlingError extends Error {
  constructor(message = 'upscale provider busy') {
    super(message);
    this.name = 'UpscaleThrottlingError';
  }
}

export interface UpscaleClient {
  readonly provider: string;
  upscale(req: UpscaleRequest): Promise<UpscaleResult>;
}
