// platform-api/src/pipeline/image/ImageGenClient.ts
//
// Provider-agnostic text-to-image client seam for the `image:generate`
// step. Mirrors the LLMClient / VisionClientLike pattern: the step
// handler depends on this interface, and a factory picks the concrete
// provider from env (Bedrock, or a self-hosted FLUX server).
//
// The handler owns everything downstream of generation — cap-fit, blob
// store, per-image events, fan-out — so a client's only job is:
//   prompt + dims + count  →  N base64-encoded images.

/** A single text-to-image request. Dims/count already defaulted by the handler. */
export interface ImageGenRequest {
  prompt: string;
  width: number;
  height: number;
  numImages: number;
  /** Optional deterministic seed. */
  seed?: number;
  /**
   * Provider-specific model id (Bedrock modelId, or a FLUX checkpoint
   * name). When omitted the client falls back to its own default.
   */
  model?: string;
}

export interface ImageGenResult {
  /** Base64-encoded image payloads (PNG/JPEG — provider's native format). */
  images: string[];
}

/**
 * Thrown by a client when the provider signals back-pressure. The
 * handler maps this to a retriable "rate limited" failure rather than a
 * terminal error.
 */
export class ImageGenThrottlingError extends Error {
  constructor(message = 'image generation throttled') {
    super(message);
    this.name = 'ImageGenThrottlingError';
  }
}

export interface ImageGenClient {
  /** The provider label, for error messages + telemetry. */
  readonly provider: string;
  /** Generate `req.numImages` images. Throws ImageGenThrottlingError on back-pressure. */
  generate(req: ImageGenRequest): Promise<ImageGenResult>;
}
