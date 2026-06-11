// platform-api/src/pipeline/image/registerVisionHandlers.ts
//
// Boot-time wiring for the image:vision-analyze custom step handler.
// Called from `bootstrap.ts` to produce the Map<string, StepHandler>
// that gets forwarded into `PipelineModule` via
// `buildPipelineModuleConfig`. Returned map merges with the judge
// handlers map at the boot site.
//
// Lazy + defensive: when ANTHROPIC_API_KEY is missing, the vision
// handler is still constructed (it surfaces a clean dispatch-time
// failure via VisionClient's non-throwing semantics —
// failureReason='http-error'). Diagnostics > silent omission.
//
// Code parity with registerJudgeHandlers.ts.

import type { Logger } from 'pino';
import type { StepHandler } from 'distributed-core/applications/pipeline';
import { createVisionStepHandler } from './VisionStepHandler';
import { createScatterStepHandler } from './ScatterStepHandler';
import { createImageCropStepHandler } from './ImageCropStepHandler';
import { createImageSourceStepHandler } from './ImageSourceStepHandler';
import { createImageAdjustBwStepHandler } from './ImageAdjustBwStepHandler';
import { createImageAdjustLayerStepHandler } from './ImageAdjustLayerStepHandler';
import { createImageSaveStepHandler } from './ImageSaveStepHandler';
import { createImageResizeStepHandler } from './ImageResizeStepHandler';
import { createImageDimensionsStepHandler } from './ImageDimensionsStepHandler';
import { createImageGeometricTransformStepHandler } from './ImageGeometricTransformStepHandler';
import { createImageFormatConvertStepHandler } from './ImageFormatConvertStepHandler';
import { createImageSharpenStepHandler } from './ImageSharpenStepHandler';
import { createImageWatermarkStepHandler } from './ImageWatermarkStepHandler';
import { createImageGenerateStepHandler } from './ImageGenerateStepHandler';
import { createImageVariationStepHandler } from './ImageVariationStepHandler';
import { createImageEditPromptStepHandler } from './ImageEditPromptStepHandler';
import { createImageVisionClassifyStepHandler } from './ImageVisionClassifyStepHandler';
import { createImageVisionDescribeStepHandler } from './ImageVisionDescribeStepHandler';
import { createImageVisionTagStepHandler } from './ImageVisionTagStepHandler';
import { createImageVisionExtractTextStepHandler } from './ImageVisionExtractTextStepHandler';
import { getImageBlobStore, getVisionClient, getVisionResultRepo } from './visionServices';

/** Mutable reference cell — bootstrap patches `current` after `new PipelineModule(...)`. */
export interface PipelineModuleRef {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  current: any | null;
}

export interface BuildVisionStepHandlersOpts {
  /**
   * Mutable reference to the PipelineModule. Same chicken-and-egg
   * pattern as judge handlers: handlers are passed INTO the module
   * config, so the module doesn't exist yet when we build the map.
   * Bootstrap patches `.current` after the module is constructed.
   */
  moduleRef: PipelineModuleRef;
  logger?: Logger;
}

export function buildVisionStepHandlers(
  opts: BuildVisionStepHandlersOpts,
): Map<string, StepHandler> {
  const handlers = new Map<string, StepHandler>();

  // Anthropic Messages API auth: ANTHROPIC_API_KEY env var (set by
  // operator — see docs/vision-PROVISIONING.md). Unlike judge, vision
  // can't use Claude Code OAuth because the CLI strips image content
  // blocks (see VisionClient.ts header).
  //
  // We still register the handler unconditionally so missing-key
  // surfaces as a clean dispatch-time failure rather than "unreachable
  // pipeline node type". Diagnostics > silent omission.
  const visionClient = getVisionClient();
  const imageBlobStore = getImageBlobStore();
  const repository = getVisionResultRepo();

  const visionHandler: StepHandler = createVisionStepHandler({
    visionClient,
    imageBlobStore,
    repository,
    pipelineIdLookup: (runId: string) => {
      const mod = opts.moduleRef.current;
      if (!mod || typeof mod.getRun !== 'function') return undefined;
      try {
        const run = mod.getRun(runId);
        return run?.pipelineId;
      } catch {
        return undefined;
      }
    },
  });

  handlers.set('image:vision-analyze', visionHandler);

  // Phase 1 vision-pipeline node types — see PipelineExecutor.ts (DC commit
  // 6475ad3c) for the `StepOutcome.fanOut` semantics that ScatterStepHandler
  // returns. All three handlers are non-throwing and share the same
  // ImageBlobStore singleton so crops + sources see the same DDB rows.
  handlers.set('scatter', createScatterStepHandler());
  handlers.set('image:crop', createImageCropStepHandler({ imageBlobStore }));
  handlers.set('image-source', createImageSourceStepHandler({ imageBlobStore }));

  // Phase 3 image transform handlers — B&W conversion, layered adjustments,
  // and the terminal save node. All share the same ImageBlobStore singleton.
  handlers.set('image:adjust-bw', createImageAdjustBwStepHandler({ imageBlobStore }));
  handlers.set('image:adjust-layer', createImageAdjustLayerStepHandler({ imageBlobStore }));
  handlers.set('image:save', createImageSaveStepHandler({ imageBlobStore }));

  // Phase 4 Sharp transform handlers — resize, geometric transforms,
  // format conversion, sharpening, and watermarking.
  handlers.set('image:resize', createImageResizeStepHandler({ imageBlobStore }));
  handlers.set('image:dimensions', createImageDimensionsStepHandler({ imageBlobStore }));
  handlers.set('image:transform', createImageGeometricTransformStepHandler({ imageBlobStore }));
  handlers.set('image:format-convert', createImageFormatConvertStepHandler({ imageBlobStore }));
  handlers.set('image:sharpen', createImageSharpenStepHandler({ imageBlobStore }));
  handlers.set('image:watermark', createImageWatermarkStepHandler({ imageBlobStore }));

  // Phase 5 generative AI handlers — Bedrock Titan + Nova Canvas.
  handlers.set('image:generate', createImageGenerateStepHandler({ imageBlobStore }));
  handlers.set('image:variation', createImageVariationStepHandler({ imageBlobStore }));
  handlers.set('image:edit-prompt', createImageEditPromptStepHandler({ imageBlobStore }));

  // Phase 6 chained-vision handlers — specialized vision analysis nodes
  // that delegate to the shared VisionClient via createVisionLikeHandler.
  const chainedVisionOpts = {
    visionClient,
    imageBlobStore,
    repository,
    pipelineIdLookup: (runId: string) => {
      const mod = opts.moduleRef.current;
      if (!mod || typeof mod.getRun !== 'function') return undefined;
      try {
        const run = mod.getRun(runId);
        return run?.pipelineId;
      } catch {
        return undefined;
      }
    },
  };
  handlers.set('image:vision-classify', createImageVisionClassifyStepHandler(chainedVisionOpts));
  handlers.set('image:vision-describe', createImageVisionDescribeStepHandler(chainedVisionOpts));
  handlers.set('image:vision-tag', createImageVisionTagStepHandler(chainedVisionOpts));
  handlers.set('image:vision-extract-text', createImageVisionExtractTextStepHandler(chainedVisionOpts));

  return handlers;
}
