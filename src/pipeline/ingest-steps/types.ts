// social-api/src/pipeline/ingest-steps/types.ts
//
// Local type contracts for the four ingest-aware pipeline step types
// (`ingest:fetch`, `ingest:map`, `ingest:materialize`, `ingest:writeback`),
// the `ingest:event` trigger, and default-pipeline resolution.
//
// These step types extend the existing pipeline executor in
// `distributed-core` via the project-local `StepHandlerRegistry` (see
// `./registry.ts`). The integrator wires the registry into the executor's
// dispatch path; until then the registry is a stand-alone mechanism that
// other agents (mapping engine, writeback dispatcher) can target.
//
// The minimal external interfaces below — `IMappingEngine`,
// `IWriteBackDispatcher`, `IDocTypeLookup`, `ISourceLookup`, `ISourceFetcher`
// — are intentionally narrow; the integrator reconciles them with the real
// implementations as those land in sibling agents' branches.

import type {
  FileContent,
  FileDescriptor,
  IngestEvent,
  IngestSource,
  MappingResult,
} from '../../ingest/types';

// ---------------------------------------------------------------------------
// Local copy of `WriteBackChange`. Agent F (Bidirectional sync engine) is
// landing the canonical type in `ingest/types.ts`. Until that lands, this
// local definition keeps the `ingest:writeback` step free-standing. The
// integrator replaces this with a re-export from `../../ingest/types`.
// ---------------------------------------------------------------------------
export interface WriteBackChange {
  /** Canonical doc id inside the platform. */
  documentId: string;
  /** The source-side identity (e.g. Drive file ID, Confluence page ID). */
  sourceRef: string;
  /** Source-specific version stamp captured at read time, used for conflict detection. */
  baseVersion?: string;
  /** Opaque payload — JSON Patch, CRDT diff, or full-content replacement, per driver. */
  change: unknown;
  /** ISO8601 timestamp of when the local edit was finalized. */
  timestamp: string;
}

// ---------------------------------------------------------------------------
// Step-handler context + outcome
// ---------------------------------------------------------------------------

/**
 * Context passed to every step handler. Mirrors the shape of the pipeline
 * executor's per-run context object — opaque key/value bag accumulated as
 * the run progresses. `runId` and `nodeId` are surfaced for telemetry +
 * idempotency-key derivation.
 */
export interface StepContext {
  runId: string;
  nodeId: string;
  /**
   * Free-form context carried across steps. Earlier step outputs land here
   * under `context.steps[<nodeId>]` per the upstream executor convention.
   */
  context: Record<string, unknown>;
  /**
   * Trigger payload supplied at run-creation time. For ingest-triggered runs
   * this is the originating `IngestEvent`.
   */
  triggerPayload: Record<string, unknown>;
}

/**
 * Step outcome. Mirrors `StepOutcome` in distributed-core's PipelineExecutor
 * — a discriminated union over `status`. `output` is merged into the run
 * context by the executor on a `completed` outcome.
 */
export type StepOutcome =
  | { status: 'completed'; output: Record<string, unknown>; sourceHandle?: string }
  | { status: 'failed'; error: string; sourceHandle?: string };

/**
 * The function shape every step-type handler implements. Async — handlers
 * may do remote I/O (fetching a file, posting a writeback). Errors thrown
 * are caught by the dispatcher and converted to a `failed` outcome.
 */
export type StepHandler<Cfg = Record<string, unknown>> = (
  config: Cfg,
  ctx: StepContext,
) => Promise<StepOutcome>;

// ---------------------------------------------------------------------------
// External dependency interfaces — minimal local shape; integrator reconciles
// ---------------------------------------------------------------------------

/**
 * Fetches typed file content from a configured source. The ingest engine
 * already owns this surface via `SourceAdapter.fetch`; the step type wraps
 * that so a pipeline can call it ad-hoc.
 *
 * The method takes `sourceId` rather than a full `SourceConfig` because the
 * pipeline definition doesn't (and shouldn't) carry per-source secrets. The
 * implementation looks up the source, builds a `FileDescriptor` from
 * `path`, and calls the matching adapter.
 */
export interface ISourceFetcher {
  fetch(sourceId: string, path: string): Promise<{ file: FileDescriptor; content: FileContent }>;
}

/**
 * Mapping engine surface. Agent E (Mapping engine v2) owns the real
 * implementation; we only need the call-out shape so the `ingest:map` step
 * is portable. `MappingDecision` extends `MappingResult` with audit fields
 * the engine produces (review queue routing, decision-id, model used).
 */
export interface MappingDecision extends MappingResult {
  /** Stable identifier for the audit row. */
  decisionId?: string;
  /** Optional escalation flag — true when the decision needs operator review. */
  needsReview?: boolean;
}

export interface IMappingEngine {
  map(input: {
    sourceId: string;
    file: FileDescriptor;
    content?: FileContent;
  }): Promise<MappingDecision>;
}

/**
 * Writeback dispatcher surface. Agent F (Bidirectional sync engine) owns
 * the real implementation. Drivers may run out-of-process (Redis-stream
 * workers per the vision doc); the dispatcher's job is to enqueue.
 */
export interface IWriteBackDispatcher {
  enqueue(change: WriteBackChange): Promise<{ enqueuedAt: string; ackId: string }>;
}

/**
 * DocType lookup. Returns the `defaultPipelineId` declared by the type, if
 * any. Today the `DocumentTypeRepository` doesn't carry this column; the
 * integrator widens the row when persistence catches up. The lookup is
 * minimal so the resolver doesn't depend on the full repository surface.
 */
export interface IDocTypeLookup {
  /** Returns null when the type is unknown OR has no default pipeline. */
  getDefaultPipelineId(typeId: string): Promise<string | null>;
}

/**
 * Source lookup. Returns the source's `defaultPipelineId` field (which
 * already exists on `IngestSource`). Defined as its own interface so a
 * test double can stand in without spinning up the full source repository.
 */
export interface ISourceLookup {
  getById(sourceId: string): Promise<IngestSource | null>;
}

// ---------------------------------------------------------------------------
// Materialization surface
// ---------------------------------------------------------------------------

/**
 * Slim view of `DocumentMaterializer.materialize` that the `ingest:materialize`
 * step depends on. Defined narrowly so tests don't need a TypedDocumentRepository.
 */
export interface IMaterializer {
  materialize(input: {
    sourceId: string;
    ingestId: string;
    file: FileDescriptor;
    content: FileContent;
    mapping: MappingDecision;
  }): Promise<{ documentId: string; created: boolean; warnings: string[] }>;
}

// ---------------------------------------------------------------------------
// Ingest event trigger
// ---------------------------------------------------------------------------

/**
 * Subscription criteria for the `ingest:event` trigger. The trigger node's
 * config narrows the global ingest event stream to the events a pipeline
 * cares about — typically "new doc of type X" but supports broader filters.
 *
 * All criteria are AND'd together; an event that fails any criterion does
 * not start the pipeline.
 */
export interface IngestTriggerBinding {
  /**
   * Trigger fires only on these `IngestEvent.type` values. Defaults to
   * `['item.materialized']` since that's the canonical "new doc landed"
   * signal — the moment downstream pipelines should run.
   */
  eventTypes?: Array<IngestEvent['type']>;
  /** Restricts to one or more sources. */
  sourceIds?: string[];
  /** Restricts to a specific mapped DocType. */
  documentTypeId?: string;
}

/**
 * Payload the trigger hands to `PipelineModule.createResource()` as the
 * `triggerPayload`. This is the canonical shape every ingest-triggered run
 * sees on `triggerPayload` — also used by `ingest:fetch` / `ingest:map`
 * step handlers when no explicit args are supplied (they fall back to the
 * trigger payload).
 */
export interface IngestTriggerPayload {
  /** Always 'ingest:event' so consumers can disambiguate vs other triggers. */
  triggerType: 'ingest:event';
  /** The full originating event so downstream steps can introspect. */
  event: IngestEvent;
  /** Convenience accessors copied off the event for direct context lookup. */
  ingestId?: string;
  documentId?: string;
  sourceId?: string;
  documentTypeId?: string;
}

// ---------------------------------------------------------------------------
// Default-pipeline resolution
// ---------------------------------------------------------------------------

/**
 * Outcome of `resolveDefaultPipelineId`. Surfaces *which* fallback layer
 * supplied the answer so callers can log + audit the decision without
 * re-running the lookup.
 */
export interface DefaultPipelineResolution {
  pipelineId: string | null;
  /**
   * The layer that produced the answer:
   *  - 'doctype' — the mapped DocType declared a default
   *  - 'source'  — the source declared a default and the type didn't
   *  - 'none'    — neither declared one; the item materializes only
   */
  via: 'doctype' | 'source' | 'none';
}

// ---------------------------------------------------------------------------
// Step config shapes (per-step input — narrow, project-local)
// ---------------------------------------------------------------------------

export interface IngestFetchConfig {
  /** Falls back to `triggerPayload.sourceId` when omitted. */
  sourceId?: string;
  /** Required — the file path inside the source. */
  path?: string;
}

export interface IngestMapConfig {
  /** Falls back to `triggerPayload.sourceId` when omitted. */
  sourceId?: string;
}

export interface IngestMaterializeConfig {
  /** Falls back to `triggerPayload.sourceId` when omitted. */
  sourceId?: string;
  /** Optional — the ingestId to record on the document. */
  ingestId?: string;
}

export interface IngestWritebackConfig {
  /** The canonical document id to push back. */
  documentId?: string;
  /** Source-side identity (e.g. Drive file ID). */
  sourceRef?: string;
  /** Source version captured at read time, for conflict detection. */
  baseVersion?: string;
  /** Opaque change payload — JSON Patch / CRDT diff / full content per driver. */
  change?: unknown;
}
