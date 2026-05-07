// Source adapter types

export interface FileDescriptor {
  path: string;
  name: string;
  hash: string;           // content hash for change detection
  size: number;
  mimeType: string;
  lastModified: string;   // ISO8601
  tags?: string[];
  metadata?: Record<string, unknown>;
}

export interface FileContent {
  data: Buffer;
  encoding: 'utf-8' | 'binary';
  parsed?: unknown;       // pre-parsed content (JSON, CSV rows, etc.)
}

export interface SourceConfig {
  [key: string]: unknown; // adapter-specific config
}

export interface SyncCursor {
  lastSyncAt: string | null;
  checkpoint?: string;    // adapter-specific continuation token
}

// Mapping types

export interface MappingRule {
  ruleId: string;
  condition: {
    field: 'extension' | 'path' | 'mimeType' | 'tag' | 'name';
    op: 'eq' | 'contains' | 'regex' | 'startsWith';
    value: string;
  };
  targetTypeId: string;
  fieldMapping: Record<string, string>;  // sourceField → docTypeField
  priority: number;
}

export interface MappingResult {
  targetTypeId: string;
  fieldMapping: Record<string, string>;
  method: 'rule' | 'llm-match' | 'llm-generate';
  confidence: number;     // 1.0 for rule-based, 0-1 for LLM
  ruleId?: string;        // which rule matched (if rule-based)
}

// Mapping engine v2 — audit + review-queue + proposal-queue types

/**
 * Per-item audit row written for every mapping attempt. Persists alongside
 * the item so operators can trace why a given DocType was chosen.
 */
export interface MappingDecision {
  decisionId: string;     // PK
  ingestId: string;       // GSI HASH (so we can list all decisions for an item)
  sourceId: string;       // GSI HASH (so we can list decisions per source)
  method: 'rule' | 'llm-match' | 'llm-generate';
  targetTypeId: string | null;
  confidence: number;
  latencyMs: number;
  model: string | null;   // LLM model id (null for rule-based)
  ruleId: string | null;  // for rule-based decisions
  routedTo: 'accepted' | 'needs-review' | 'doctype-proposal' | 'failed';
  fieldMapping: Record<string, string>;
  reason: string | null;  // free-text rationale (LLM may produce this)
  createdAt: string;      // GSI RANGE — sorted-by-time per ingestId/sourceId
}

/**
 * A "needs review" queue entry. Created when an LLM mapping comes back with
 * confidence below the per-source threshold. Operators triage these in the
 * UI; clearing one promotes the suggested mapping or rejects it.
 */
export interface ReviewQueueEntry {
  ingestId: string;       // PK — one entry per item (most recent decision wins)
  sourceId: string;       // GSI HASH (list per-source review queue)
  decisionId: string;     // pointer back to the MappingDecision row
  suggestedTypeId: string | null;
  suggestedFieldMapping: Record<string, string>;
  confidence: number;
  threshold: number;      // the threshold at decision time (for context)
  reason: string | null;
  status: 'pending' | 'approved' | 'rejected';  // GSI RANGE
  createdAt: string;
  reviewedAt: string | null;
  reviewedBy: string | null;
}

/**
 * Auto-discovered DocType proposal. The generative tier produces these when
 * N similar low-confidence items pile up; a human promotes them into a
 * concrete document type.
 */
export interface DocTypeProposalEntry {
  proposalId: string;     // PK
  sourceId: string;       // GSI HASH (proposals per source)
  // Cluster key — the system groups similar low-confidence items by this
  // signature (currently mimeType|extension; can grow into embeddings).
  signature: string;
  sampleIngestIds: string[];        // up to N item ids that triggered this
  proposedName: string;
  proposedDescription: string;
  proposedFields: Array<{ name: string; sectionType: string; required: boolean }>;
  reasoning: string;
  status: 'pending' | 'promoted' | 'rejected';  // GSI RANGE
  createdAt: string;
  reviewedAt: string | null;
  reviewedBy: string | null;
}

/**
 * One row in the ingest audit log. Every mapping decision, source change,
 * and write-back attempt produces one of these via the appendAudit helper.
 * AGENTIC_ETL_VISION §F. Schema fixed in hub#2213 — read by hub#2214's
 * GET /api/ingest/audit + page; written by hub#2215's instrumentation pass.
 */
export type IngestAuditEventType =
  | 'mapping.decided'
  | 'source.created'
  | 'source.updated'
  | 'source.paused'
  | 'source.resumed'
  | 'writeback.attempted'
  | 'writeback.failed';

export type IngestAuditTargetType = 'source' | 'item' | 'document';

export interface IngestAuditEntry {
  /** PK — UUID/KSUID assigned at append time. */
  auditId: string;
  /** ISO 8601 timestamp the row was appended. Used as RANGE on both GSIs. */
  createdAt: string;
  /** Operator email when the call originated from a UI action; else
   *  `'system'` (engine-driven) or `'llm:haiku'` (LLM mapper decisions). */
  actor: string;
  eventType: IngestAuditEventType;
  /** Coarse type tag — narrows the GSI partition reads in the viewer page. */
  targetType: IngestAuditTargetType;
  /** sourceId / itemId / documentId — keyed for the targetId GSI. */
  targetId: string;
  /** Human-readable one-line summary the audit-viewer renders unexpanded. */
  summary: string;
  /** Structured per-event detail (LLM prompt+response, before/after diff,
   *  error message + stack, etc). Surfaced in the expandable JSON row. */
  detail: Record<string, unknown>;
}

/**
 * Per-source mapping configuration. Lives on `IngestSource.config` under
 * adapter-agnostic keys; documented here so the engine and UI agree on
 * the shape.
 *
 * Config keys:
 *   - mapping.confidenceThreshold:  number (default 0.7) — below this, LLM
 *     match results are routed to the needs-review queue instead of being
 *     auto-accepted.
 *   - mapping.llmModel:             string (default 'claude-haiku-4-5') —
 *     the LLM model used for both match and generate tiers. Operators may
 *     override per-source (e.g. Sonnet for high-stakes docs).
 *   - mapping.llmProvider:          string (default 'claude-code') — the
 *     LLM provider plumbing key (matches LLMClientFactory).
 *   - mapping.proposalClusterSize:  number (default 3) — how many similar
 *     low-confidence items must accumulate before the generative tier
 *     emits a DocType proposal.
 */
export interface MappingSourceConfig {
  'mapping.confidenceThreshold'?: number;
  'mapping.llmModel'?: string;
  'mapping.llmProvider'?: string;
  'mapping.proposalClusterSize'?: number;
}

// Core entity types

export interface IngestSource {
  sourceId: string;
  name: string;
  type: 'local' | 's3' | 'google-drive' | 'webhook' | 'confluence' | 'jira';
  config: SourceConfig;
  syncSchedule: string | null;    // cron expression or null for manual-only
  mappingRules: MappingRule[];
  defaultPipelineId: string | null;
  /**
   * Per-DocType pipeline override. Keys are `targetTypeId` (the DocType id
   * a mapping rule resolved to); values are pipeline ids that should run
   * instead of `defaultPipelineId` for items of that type. Absent / empty
   * map ⇒ use defaultPipelineId for everything. AGENTIC_ETL_VISION §C.
   */
  pipelineByDocType?: Record<string, string>;
  status: 'active' | 'paused' | 'error';
  lastSyncAt: string | null;
  lastSyncStats: SyncStats | null;
  createdAt: string;
  updatedAt: string;
}

export type IngestItemStatus = 'queued' | 'mapping' | 'materializing' | 'triggering' | 'done' | 'failed';

export interface IngestItem {
  ingestId: string;
  sourceId: string;
  filePath: string;
  fileHash: string;
  mimeType: string;
  fileSize: number;
  status: IngestItemStatus;
  mappedTypeId: string | null;
  mappingConfidence: number | null;
  mappingMethod: 'rule' | 'llm-match' | 'llm-generate' | null;
  documentId: string | null;
  pipelineRunId: string | null;
  error: string | null;
  attempts: number;
  createdAt: string;
  updatedAt: string;
}

export interface SyncCursorEntry {
  sourceId: string;       // PK
  filePath: string;       // SK
  fileHash: string;
  lastModified: string;
  lastIngestId: string;
  documentId: string | null;
}

// Queue task types

export interface SyncTask {
  sourceId: string;
  triggeredBy: 'schedule' | 'webhook' | 'manual';
  requestedAt: string;
}

// Stats

export interface SyncStats {
  filesFound: number;
  filesNew: number;
  filesUpdated: number;
  filesSkipped: number;
  errors: number;
  durationMs: number;
}

// Progress events (published to EventBus, bridged to WebSocket)

export type IngestEvent =
  | { type: 'sync.started'; sourceId: string; sourceName: string; fileCount: number; timestamp: string }
  | { type: 'sync.file.fetched'; sourceId: string; filePath: string; index: number; total: number; timestamp: string }
  | { type: 'item.status'; ingestId: string; status: IngestItemStatus; detail?: string; timestamp: string }
  | { type: 'item.mapped'; ingestId: string; typeId: string; typeName: string; method: string; confidence: number; timestamp: string }
  | { type: 'item.materialized'; ingestId: string; documentId: string; timestamp: string }
  | { type: 'item.pipeline.spawned'; ingestId: string; runId: string; pipelineName: string; timestamp: string }
  | { type: 'item.failed'; ingestId: string; error: string; retriable: boolean; timestamp: string }
  | { type: 'item.review.triggered'; ingestId: string; documentId: string; pipelineRunId: string; timestamp: string }
  | { type: 'sync.completed'; sourceId: string; stats: SyncStats; timestamp: string }
  | { type: 'source.status'; sourceId: string; status: IngestSource['status']; timestamp: string };

// Adapter interface
//
// SourceAdapter and WriteBackDriver are peer interfaces describing the two
// halves of a connector. Today every adapter implements SourceAdapter; some
// will additionally implement WriteBackDriver as bidirectional sync lands
// per-source. Capability declarations let the engine and operator UI
// reason about each connector's feature set without instantiating it.

/**
 * Static capability declaration for a connector. A class-side `capabilities`
 * constant is the single source of truth — the engine reads it to decide
 * whether to enable schedule-based polling vs webhook subscription, expose
 * a "push edits back" UI, or kick off an OAuth flow during configuration.
 *
 * Both SourceAdapter and WriteBackDriver implementations carry one. They
 * may share a class — a future bidirectional connector implements both
 * interfaces and exposes a single `capabilities` block.
 */
export interface AdapterCapabilities {
  /** True when the connector cannot mutate the source (no writeback even if implemented). */
  readonly: boolean;
  /** True when the connector also implements WriteBackDriver. */
  supportsWriteBack: boolean;
  /** True when the connector requires an OAuth flow during source configuration. */
  requiresOauth: boolean;
  /** True when the connector exposes a push-based subscribe(handler) instead of pull-based polling. */
  supportsWebhook: boolean;
}

/**
 * Push-driven update sink. Webhook adapters and (future) streaming connectors
 * call `handler` whenever the source notifies us of new content. Pull-only
 * adapters omit this method.
 */
export type IngestPushHandler = (
  file: FileDescriptor,
  content: FileContent,
) => void | Promise<void>;

/**
 * Connector read surface. Every connector implements this.
 *
 *   list(config, cursor)       — paginated discovery; honors cursor.lastSyncAt
 *                                and may stash a continuation token in
 *                                cursor.checkpoint.
 *   fetch(config, file)        — materialize the bytes for a discovered
 *                                FileDescriptor (separate so list() can stay
 *                                cheap on metadata-only sources).
 *   subscribe(config, handler) — optional, push-based sources only. Returns
 *                                an unsubscribe function. Capability declared
 *                                via `supportsWebhook`.
 *
 * Implementing classes expose a `static readonly capabilities` declaration
 * — see AdapterCapabilities. (Modeled as a class-side property, not an
 * instance one, so the operator UI can introspect without constructing.)
 */
export interface SourceAdapter {
  list(config: SourceConfig, cursor: SyncCursor): AsyncGenerator<FileDescriptor>;
  fetch(config: SourceConfig, file: FileDescriptor): Promise<FileContent>;
  subscribe?(config: SourceConfig, handler: IngestPushHandler): Promise<() => void>;
}

/**
 * Constructor shape for SourceAdapter classes that exposes the static
 * capability block. Use this when the engine/UI needs to introspect a
 * connector class without instantiating it:
 *
 *   const klass: SourceAdapterClass = MockLocalAdapter;
 *   if (klass.capabilities.supportsWebhook) { ... }
 */
export interface SourceAdapterClass {
  new (...args: never[]): SourceAdapter;
  readonly capabilities: AdapterCapabilities;
}

/**
 * A single change emitted by the platform's collaboration layer that needs
 * to be replayed against the source of record. The shape is intentionally
 * minimal — drivers are expected to translate `change` into source-specific
 * API calls (Drive revision, Confluence page update, Jira issue patch).
 */
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

/**
 * Result of a successful (or partially successful) writeback.
 */
export interface WriteBackAck {
  /** New version stamp returned by the source after the write. */
  newVersion?: string;
  /** ISO8601 timestamp acknowledged by the source. */
  acknowledgedAt: string;
  /** True when the driver auto-merged a conflict during push. */
  merged?: boolean;
}

/**
 * Connector write surface. Peer to SourceAdapter — implemented by drivers
 * that can replay platform-side edits back to the source of record.
 *
 *   push(config, change)                  — replay a single change. Throws
 *                                            on unresolved conflict; success
 *                                            returns a WriteBackAck with the
 *                                            new source-side version.
 *   resolveConflict(local, remote)        — driver-specific merge policy
 *                                            (e.g. last-write-wins, field-level
 *                                            rule-based). Called when push()
 *                                            detects baseVersion is stale.
 *
 * Implementing classes expose a `static readonly capabilities` block — same
 * shape as SourceAdapter so the UI treats the two halves uniformly.
 *
 * NOTE: This interface is declared but not yet implemented in this slice
 * (Agent A scope). Concurrent agents (B, C, D) ship per-connector drivers
 * against a local placeholder in `adapters/writeback-types.ts`; the
 * integrator collapses that placeholder onto these canonical types.
 */
export interface WriteBackDriver {
  push(config: SourceConfig, change: WriteBackChange): Promise<WriteBackAck>;
  resolveConflict(
    config: SourceConfig,
    local: WriteBackChange,
    remote: WriteBackChange,
  ): Promise<WriteBackChange>;
}

/**
 * Constructor shape for WriteBackDriver classes carrying their capability
 * declaration on the class itself.
 */
export interface WriteBackDriverClass {
  new (...args: never[]): WriteBackDriver;
  readonly capabilities: AdapterCapabilities;
}
