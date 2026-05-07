// social-api/src/pipeline/ingest-steps/default-pipeline-resolver.ts
//
// Resolves the pipeline that should run after an item materializes.
//
// Resolution order (per AGENTIC_ETL_VISION.md §C):
//   1. The mapped DocType's `defaultPipelineId`.
//   2. The originating source's `defaultPipelineId`.
//   3. None — the item materializes and no pipeline runs.
//
// This logic lives next to the `ingest:materialize` step because that's
// where the resolver fires: once the typed document exists, the resolver
// picks the right pipeline (if any) and the integrator hands the result
// to `PipelineModule.createResource()`.

import type {
  DefaultPipelineResolution,
  IDocTypeLookup,
  ISourceLookup,
} from './types';

export function createDefaultPipelineResolver(deps: {
  docTypeLookup: IDocTypeLookup;
  sourceLookup: ISourceLookup;
}) {
  return async function resolveDefaultPipelineId(input: {
    documentTypeId: string;
    sourceId: string;
  }): Promise<DefaultPipelineResolution> {
    // Layer 1 — DocType wins.
    const fromType = await deps.docTypeLookup.getDefaultPipelineId(input.documentTypeId);
    if (fromType) {
      return { pipelineId: fromType, via: 'doctype' };
    }

    // Layer 2 — fall back to the source.
    const source = await deps.sourceLookup.getById(input.sourceId);
    const fromSource = source?.defaultPipelineId ?? null;
    if (fromSource) {
      return { pipelineId: fromSource, via: 'source' };
    }

    // Layer 3 — no default. Item is materialized; no pipeline runs.
    return { pipelineId: null, via: 'none' };
  };
}

export type DefaultPipelineResolver = ReturnType<typeof createDefaultPipelineResolver>;
