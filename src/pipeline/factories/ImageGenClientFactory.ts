// platform-api/src/pipeline/factories/ImageGenClientFactory.ts
//
// Picks the image:generate transport. Mirrors LLMClientFactory.
//
// Provider resolution:
//   explicit arg  →  IMAGE_GENERATE_PROVIDER env  →  auto.
// "auto" selects flux when FLUX_ENDPOINT_URL is set (the operator's
// only working image path right now — Bedrock has no active
// image-output model), else bedrock.

import type { ImageGenClient } from '../image/ImageGenClient';
import { BedrockImageGenClient } from '../image/BedrockImageGenClient';
import { FluxImageGenClient } from '../image/FluxImageGenClient';

export type ImageGenProvider = 'bedrock' | 'flux';

export function createImageGenClient(provider?: ImageGenProvider): ImageGenClient {
  const explicit =
    provider ?? (process.env['IMAGE_GENERATE_PROVIDER'] as ImageGenProvider | undefined);

  const resolved: ImageGenProvider =
    explicit ?? (process.env['FLUX_ENDPOINT_URL'] ? 'flux' : 'bedrock');

  switch (resolved) {
    case 'flux':
      return new FluxImageGenClient();
    case 'bedrock':
    default:
      return new BedrockImageGenClient();
  }
}
