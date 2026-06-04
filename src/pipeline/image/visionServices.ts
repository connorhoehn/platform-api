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

import { VisionClient } from './VisionClient';
import { VisionResultRepository } from './VisionResultRepository';
import { DdbImageBlobStore, type ImageBlobStore } from './ImageBlobStore';

let _imageBlobStore: ImageBlobStore | null = null;
let _visionResultRepo: VisionResultRepository | null = null;
let _visionClient: VisionClient | null = null;

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
 * Singleton VisionClient. Reads ANTHROPIC_API_KEY at first access.
 * Vision can't use the Claude Code CLI OAuth path (see VisionClient.ts
 * header for the deviation rationale).
 */
export function getVisionClient(): VisionClient {
  if (!_visionClient) {
    _visionClient = new VisionClient();
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
