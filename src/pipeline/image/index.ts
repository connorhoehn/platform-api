// platform-api/src/pipeline/image/ — barrel.
//
// `image:vision-analyze` step type (DC-21 customStepHandler) — code
// parity with the judge feature. Wire by calling
// buildVisionStepHandlers({moduleRef}) at boot and merging the
// returned map with other custom-step-handler maps before passing to
// PipelineModule.

export { createVisionStepHandler } from './VisionStepHandler';
export type {
  VisionStepHandlerOpts,
  VisionResultRepositoryLike,
} from './VisionStepHandler';

export { VisionClient, buildVisionSystemPrompt, extractJsonObject } from './VisionClient';
export type { VisionClientCallResult, VisionClientOpts, VisionRawOutput } from './VisionClient';

export {
  computeVisionCost,
  computeVisionCacheHitRatio,
  getVisionPricing,
} from './computeVisionCost';

export { VisionResultRepository } from './VisionResultRepository';
export type {
  StoredVisionResult,
  ListVisionResultsOptions,
  ListVisionResultsResult,
  VisionResultRepositoryOptions,
} from './VisionResultRepository';

export {
  DdbImageBlobStore,
  InMemoryImageBlobStore,
  IMAGE_BLOB_SIZE_CAP_BYTES,
} from './ImageBlobStore';
export type { ImageBlobStore, DdbImageBlobStoreOptions } from './ImageBlobStore';

export {
  buildVisionStepHandlers,
} from './registerVisionHandlers';
export type { PipelineModuleRef, BuildVisionStepHandlersOpts } from './registerVisionHandlers';

export {
  getImageBlobStore,
  getVisionResultRepo,
  getVisionClient,
  __resetVisionServicesForTests,
} from './visionServices';

export type {
  ImageMeta,
  ImageVisionAnalyzeStepConfig,
  VisionAnalyzeInput,
  VisionFailureReason,
  VisionMode,
  VisionResult,
  AnthropicVisionUsage,
} from './types';
