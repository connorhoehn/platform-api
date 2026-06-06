// platform-api/src/pipeline/image/visionServices.ts
//
// Single source of construction for the vision repositories + stores +
// client so the step handler (registerVisionHandlers.ts) and the HTTP
// routes (routes/images.ts) share the exact same instances + env
// wiring.
//
// Lazy construction: the `get*` accessors instantiate on first call.
// This matters for tests + envs where env vars are read at runtime,
// and keeps the boot cost paid only by the consumer that actually
// uses the service.
//
// Code parity with platform-api/src/pipeline/judge/judgeServices.ts.

import { VisionClient, type VisionClientLike } from './VisionClient';
import { AnthropicVisionClient } from './AnthropicVisionClient';
import { VisionResultRepository } from './VisionResultRepository';
import { DdbImageBlobStore, type ImageBlobStore } from './ImageBlobStore';

let _imageBlobStore: ImageBlobStore | null = null;
let _visionResultRepo: VisionResultRepository | null = null;
let _visionClient: VisionClientLike | null = null;

/**
 * Singleton DdbImageBlobStore (`pipeline-images` DDB table). Operator
 * provisions the table — see docs/vision-PROVISIONING.md. When the
 * table is missing, put() throws and the HTTP route surfaces a 500;
 * get() soft-fails to null and the step's pipeline:image:vision:failed
 * event surfaces the cause.
 */
export function getImageBlobStore(): ImageBlobStore {
  if (!_imageBlobStore) {
    _imageBlobStore = new DdbImageBlobStore();
  }
  return _imageBlobStore;
}

/**
 * Singleton VisionResultRepository. v1 does NOT wire a rawTextStore
 * — vision responses are typically small (caption + minimal structured
 * JSON), well under the 32KB inline cap. If a real workload starts
 * tripping the cap, add a DdbRawTextStore-equivalent for the
 * `pipeline-vision-rawtext` table and pass it as `rawTextStore` here.
 */
export function getVisionResultRepo(): VisionResultRepository {
  if (!_visionResultRepo) {
    _visionResultRepo = new VisionResultRepository();
  }
  return _visionResultRepo;
}

/**
 * Singleton vision client. Selection is env-gated:
 *
 *   VISION_CLIENT=anthropic  (default) — SDK path via @anthropic-ai/sdk;
 *     base64 image content block, real `messages.stream`, parallel-safe.
 *     Required for scatter / fan-out pipelines where N vision-ish calls
 *     run concurrently — the CLI path's per-call subprocess overhead is
 *     prohibitive at N > 1.
 *
 *   VISION_CLIENT=cli — legacy subprocess path that spawns `claude -p`
 *     with a tempfile. Kept as a Bedrock-OAuth fallback for envs where
 *     ANTHROPIC_API_KEY is unavailable. Will be removed in a follow-up
 *     once Bedrock support is wired into the SDK client.
 *
 * Both clients honor the same `VisionClientLike` interface — handler
 * code (`VisionStepHandler`) is unaffected by the choice.
 */
export function getVisionClient(): VisionClientLike {
  if (!_visionClient) {
    const mode = (process.env['VISION_CLIENT'] ?? 'anthropic').toLowerCase();
    _visionClient = mode === 'cli'
      ? new VisionClient()
      : new AnthropicVisionClient();
  }
  return _visionClient;
}

/**
 * Test-only — drop every cached singleton so the next `get*` call
 * rebuilds with current env vars. Mirrors the judge pattern.
 */
export function __resetVisionServicesForTests(): void {
  _imageBlobStore = null;
  _visionResultRepo = null;
  _visionClient = null;
}
