// social-api/src/pipeline/ingest-steps/index.ts
//
// Public surface for the ingest-aware pipeline step types. The integrator
// imports `registerIngestSteps()` and the resolver/trigger constructors;
// nothing else from this folder needs to be re-exported.

export { StepHandlerRegistry, isIngestStepType, INGEST_STEP_TYPES } from './registry';
export type { IngestStepType } from './registry';

export { createFetchStep } from './fetch-step';
export { createMapStep } from './map-step';
export { createMaterializeStep } from './materialize-step';
export { createWritebackStep } from './writeback-step';

export { createDefaultPipelineResolver } from './default-pipeline-resolver';
export type { DefaultPipelineResolver } from './default-pipeline-resolver';

export { IngestEventTrigger, buildTriggerPayload } from './event-trigger';
export type { RegisteredIngestTrigger, IRunStarter } from './event-trigger';

export type {
  StepContext,
  StepHandler,
  StepOutcome,
  ISourceFetcher,
  IMappingEngine,
  IWriteBackDispatcher,
  IDocTypeLookup,
  ISourceLookup,
  IMaterializer,
  MappingDecision,
  IngestTriggerBinding,
  IngestTriggerPayload,
  DefaultPipelineResolution,
  IngestFetchConfig,
  IngestMapConfig,
  IngestMaterializeConfig,
  IngestWritebackConfig,
} from './types';

import { StepHandlerRegistry } from './registry';
import { createFetchStep } from './fetch-step';
import { createMapStep } from './map-step';
import { createMaterializeStep } from './materialize-step';
import { createWritebackStep } from './writeback-step';
import type { DefaultPipelineResolver } from './default-pipeline-resolver';
import type {
  IMappingEngine,
  IMaterializer,
  ISourceFetcher,
  IWriteBackDispatcher,
} from './types';

/**
 * One-shot wiring helper. Builds a registry and registers all four step
 * types against it. The integrator typically calls this at bootstrap and
 * threads the resulting registry into the executor's dispatch shim.
 */
export function registerIngestSteps(deps: {
  sourceFetcher: ISourceFetcher;
  mappingEngine: IMappingEngine;
  materializer: IMaterializer;
  writebackDispatcher: IWriteBackDispatcher;
  resolveDefaultPipeline: DefaultPipelineResolver;
  registry?: StepHandlerRegistry;
}): StepHandlerRegistry {
  const registry = deps.registry ?? new StepHandlerRegistry();
  registry.register('ingest:fetch', createFetchStep({ sourceFetcher: deps.sourceFetcher }));
  registry.register('ingest:map', createMapStep({ mappingEngine: deps.mappingEngine }));
  registry.register(
    'ingest:materialize',
    createMaterializeStep({
      materializer: deps.materializer,
      resolveDefaultPipeline: deps.resolveDefaultPipeline,
    }),
  );
  registry.register(
    'ingest:writeback',
    createWritebackStep({ writebackDispatcher: deps.writebackDispatcher }),
  );
  return registry;
}
