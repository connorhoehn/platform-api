// What a step says it produced, read from the step's own output.
//
// `producesFor` in runPersistence already reads the `{ kind: 'document',
// documentId }` result convention to say what a step is WRITING. This is the
// other half: what it finished — an artifact the run is the provenance of, which
// the work-graph event carries as `artifactIds` so the run and the thing it made
// are the same fact rather than two that happen to line up.
//
// It is a declaration, never an inference: a step that says nothing produces
// nothing here, and an id is only ever taken from the step's own result.

import type { PipelineArtifactReference } from '../work-graph/sources/mapPipeline';

/** One artifact a step declares on its output's `artifacts` array. */
export interface DeclaredArtifact {
  /** The resource the artifact IS — a documentId for anything document-shaped. */
  artifactId: string;
  /** What kind of thing it is, for a reader that wants to say so. */
  kind?: string;
  [extra: string]: unknown;
}

const MAX_ARTIFACTS = 10;

function id(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim().slice(0, 200) : undefined;
}

/**
 * The artifact references a step output declares, stamped with the run they
 * belong to (mapPipeline refuses a reference that names another run).
 */
export function declaredArtifactRefs(
  output: unknown,
  run: { pipelineId: string; runId: string },
): PipelineArtifactReference[] {
  const declared = (output as { artifacts?: unknown } | undefined)?.artifacts;
  if (!Array.isArray(declared)) return [];
  const refs: PipelineArtifactReference[] = [];
  const seen = new Set<string>();
  for (const entry of declared) {
    if (!entry || typeof entry !== 'object') continue;
    const item = entry as { artifactId?: unknown; documentId?: unknown };
    const artifactId = id(item.artifactId) ?? id(item.documentId);
    if (!artifactId || seen.has(artifactId)) continue;
    seen.add(artifactId);
    refs.push({ artifactId, pipelineId: run.pipelineId, runId: run.runId });
    if (refs.length >= MAX_ARTIFACTS) break;
  }
  return refs;
}
