// platform-api/src/pipeline/ingest-steps/materialize-step.ts
//
// `ingest:materialize` step type. Given (item, mappingDecision), writes a
// typed document via the materializer. After materializing, runs the
// default-pipeline resolver (DocType > Source > none) and surfaces the
// resolution on the step output so the integrator can chain into a child
// pipeline or skip cleanly.
//
// The resolver decision is left as data — this step does NOT itself trigger
// a child pipeline run. Spawning a child run is a higher-level concern that
// happens in the engine wiring (see ingest engine integration). Putting the
// decision on the step output keeps the step's responsibilities sharp:
//   - persist the document
//   - audit which fallback won
//   - hand the runId-to-spawn (or `null`) to the executor's downstream edges

import type {
  FileContent,
  FileDescriptor,
} from '../../ingest/types';
import type {
  DefaultPipelineResolution,
  IMaterializer,
  IngestMaterializeConfig,
  MappingDecision,
  StepContext,
  StepHandler,
  StepOutcome,
} from './types';
import type { DefaultPipelineResolver } from './default-pipeline-resolver';

export function createMaterializeStep(deps: {
  materializer: IMaterializer;
  resolveDefaultPipeline: DefaultPipelineResolver;
}): StepHandler<IngestMaterializeConfig> {
  return async function ingestMaterializeStep(
    config: IngestMaterializeConfig,
    ctx: StepContext,
  ): Promise<StepOutcome> {
    const sourceId = config.sourceId ?? readStringFromTrigger(ctx, 'sourceId');
    if (!sourceId) {
      return {
        status: 'failed',
        error: 'ingest:materialize: sourceId is required (none in config or triggerPayload)',
        sourceHandle: 'error',
      };
    }

    const file = readFileDescriptor(ctx);
    if (!file) {
      return {
        status: 'failed',
        error: 'ingest:materialize: no file descriptor on context (run ingest:fetch first)',
        sourceHandle: 'error',
      };
    }
    const content = readFileContent(ctx);
    if (!content) {
      return {
        status: 'failed',
        error: 'ingest:materialize: no file content on context (run ingest:fetch first)',
        sourceHandle: 'error',
      };
    }
    const mapping = readMappingDecision(ctx);
    if (!mapping) {
      return {
        status: 'failed',
        error: 'ingest:materialize: no mapping decision on context (run ingest:map first)',
        sourceHandle: 'error',
      };
    }

    const ingestId = config.ingestId
      ?? readStringFromTrigger(ctx, 'ingestId')
      ?? `pipeline-${ctx.runId}-${ctx.nodeId}`;

    let materialization: { documentId: string; created: boolean; warnings: string[] };
    try {
      materialization = await deps.materializer.materialize({
        sourceId,
        ingestId,
        file,
        content,
        mapping,
      });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      return {
        status: 'failed',
        error: `ingest:materialize: ${msg}`,
        sourceHandle: 'error',
      };
    }

    let resolution: DefaultPipelineResolution;
    try {
      resolution = await deps.resolveDefaultPipeline({
        documentTypeId: mapping.targetTypeId,
        sourceId,
      });
    } catch (err: unknown) {
      // Resolver lookup failures are non-fatal for materialization — surface
      // the error in the output but emit a `none` resolution so the run can
      // continue along the default-no-pipeline edge. This matches the
      // vision-doc behaviour: "if neither, no pipeline runs".
      const msg = err instanceof Error ? err.message : String(err);
      resolution = { pipelineId: null, via: 'none' };
      return {
        status: 'completed',
        output: {
          documentId: materialization.documentId,
          created: materialization.created,
          warnings: [...materialization.warnings, `default-pipeline lookup failed: ${msg}`],
          defaultPipeline: resolution,
        },
        sourceHandle: 'out',
      };
    }

    return {
      status: 'completed',
      output: {
        documentId: materialization.documentId,
        created: materialization.created,
        warnings: materialization.warnings,
        defaultPipeline: resolution,
      },
      sourceHandle: 'out',
    };
  };
}

function readStringFromTrigger(ctx: StepContext, key: string): string | undefined {
  const v = ctx.triggerPayload[key];
  return typeof v === 'string' ? v : undefined;
}

function readFileDescriptor(ctx: StepContext): FileDescriptor | undefined {
  const direct = ctx.context['file'];
  if (isFileDescriptor(direct)) return direct;
  const steps = ctx.context['steps'] as Record<string, unknown> | undefined;
  if (steps) {
    for (const out of Object.values(steps)) {
      const f = (out as Record<string, unknown> | null)?.['file'];
      if (isFileDescriptor(f)) return f;
    }
  }
  return undefined;
}

function readFileContent(ctx: StepContext): FileContent | undefined {
  const direct = ctx.context['content'];
  if (isFileContent(direct)) return direct;
  const steps = ctx.context['steps'] as Record<string, unknown> | undefined;
  if (steps) {
    for (const out of Object.values(steps)) {
      const c = (out as Record<string, unknown> | null)?.['content'];
      if (isFileContent(c)) return c;
    }
  }
  return undefined;
}

function readMappingDecision(ctx: StepContext): MappingDecision | undefined {
  const direct = ctx.context['mapping'];
  if (isMappingDecision(direct)) return direct;
  const steps = ctx.context['steps'] as Record<string, unknown> | undefined;
  if (steps) {
    for (const out of Object.values(steps)) {
      const m = (out as Record<string, unknown> | null)?.['mapping'];
      if (isMappingDecision(m)) return m;
    }
  }
  return undefined;
}

function isFileDescriptor(v: unknown): v is FileDescriptor {
  if (!v || typeof v !== 'object') return false;
  const o = v as Record<string, unknown>;
  return typeof o['path'] === 'string' && typeof o['hash'] === 'string';
}

function isFileContent(v: unknown): v is FileContent {
  if (!v || typeof v !== 'object') return false;
  const o = v as Record<string, unknown>;
  return Buffer.isBuffer(o['data']);
}

function isMappingDecision(v: unknown): v is MappingDecision {
  if (!v || typeof v !== 'object') return false;
  const o = v as Record<string, unknown>;
  return typeof o['targetTypeId'] === 'string'
    && typeof o['confidence'] === 'number'
    && typeof o['method'] === 'string';
}
