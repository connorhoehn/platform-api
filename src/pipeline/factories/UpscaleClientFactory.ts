// platform-api/src/pipeline/factories/UpscaleClientFactory.ts
//
// Picks the image:upscale transport. There is only one — a self-hosted
// stable-diffusion-x4-upscaler server (no cloud SDKs). Kept as a factory
// for parity with ImageGenClientFactory and to leave room for future
// providers without touching the handler.

import type { UpscaleClient } from '../image/UpscaleClient';
import { SelfHostedUpscaleClient } from '../image/SelfHostedUpscaleClient';

export function createUpscaleClient(): UpscaleClient {
  return new SelfHostedUpscaleClient();
}
