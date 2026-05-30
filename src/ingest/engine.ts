// platform-api/src/ingest/engine.ts
//
// Core orchestration logic for the ingest subsystem. Manages the lifecycle of
// syncing sources and processing ingest items through the pipeline:
//   fetch → map type → materialize document → update status
//
// Phase 54.6.3 — partition queues + per-source worker coroutines were removed
// in favor of the durable `ingest:item.queued` event. The engine produces by
// calling the injected `enqueueItem` dep; the IngestQueueConsumer (separate
// module) subscribes to the durable queue and routes each delivery to
// `processItem(ingestId, tags)`. Per-source FIFO ordering is preserved by the
// message-queue's FIFO + partitionKey:sourceId configuration. This makes the
// processing path safe for multi-replica deployment.

import { randomUUID } from 'crypto';
import { hostname } from 'os';
import { LeasedKVLock } from 'distributed-core';
import { createDDBLeaseStore } from '../lib/ddb-lease-store';
import type { IngestSourceRepository } from './repositories/IngestSourceRepository';
import type { IngestItemRepository } from './repositories/IngestItemRepository';
import type { SyncCursorRepository } from './repositories/SyncCursorRepository';
import type { TypedDocumentRepository, TypedDocumentItem } from '../repositories/TypedDocumentRepository';
import type {
  IngestSource,
  IngestItem,
  IngestItemStatus,
  IngestEvent,
  FileDescriptor,
  SourceConfig,
  SyncStats,
  SyncCursor,
  MappingResult,
} from './types';
import { createAdapter } from './adapters/adapter';
import { matchByRules } from './mapping/rule-matcher';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import {
  recordIngestItem,
  recordIngestSyncDuration,
  recordIngestQueueDepthDelta,
  recordIngestMappingConfidence,
  recordIngestDlqDepthDelta,
  recordIngestLeaderAcquired,
  recordIngestLeaderSkipped,
  recordIngestLeaderError,
  recordIngestLeaderLeaseLost,
} from '../observability/metrics';
import log from '../lib/logger';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Typed event bus interface for ingest progress events. The bootstrap layer
 * provides the concrete implementation (EventEmitter-based for single-node,
 * or DC EventBus-backed for multi-node). The engine depends on this minimal
 * contract so it stays decoupled from the transport choice.
 */
export interface IngestEventBus {
  emit(event: IngestEvent): void;
  on(handler: (event: IngestEvent) => void): void;
  off(handler: (event: IngestEvent) => void): void;
}

/**
 * Phase 54.6.3 — publish a discovered ingest item to the durable queue.
 * Default production binding is `publishDurable('ingest:item.queued', …)`;
 * tests inject a fake that captures + replays into a fake consumer.
 */
export type EnqueueItem = (params: {
  ingestId: string;
  sourceId: string;
  tags?: string[];
}) => Promise<void>;

export interface IngestEngineDeps {
  sourceRepository: IngestSourceRepository;
  itemRepository: IngestItemRepository;
  cursorRepository: SyncCursorRepository;
  documentRepository: TypedDocumentRepository;
  eventBus: IngestEventBus;
  enqueueItem: EnqueueItem;
  /**
   * Optional override (tests inject a LeasedKVLock backed by an in-memory
   * LeaseStore). Production gets a DC LeasedKVLock backed by the DDB
   * adapter — see `createDefaultLeaderLock` below.
   */
  leaderLock?: LeasedKVLock;
}

// ---------------------------------------------------------------------------
// Leader-lock (default DDB-backed via DC's LeasedKVLock primitive)
// ---------------------------------------------------------------------------

const LEADER_LOCK_TABLE = tableName(process.env.DYNAMODB_OUTBOX_TABLE || 'social-outbox');
const LEADER_LOCK_KEY = 'INGEST_SCHEDULE_LOCK#evaluator';
const LEADER_LEASE_TTL_MS = 30_000;

/**
 * Phase 54.6.3b → M4.3a-followup: the inline `createDDBIngestLeaderLock`
 * was replaced with DC's LeasedKVLock primitive backed by the shared
 * createDDBLeaseStore adapter. Same DDB row format, same TTL/heartbeat
 * semantics, less duplicated boilerplate. The LeasedKVLock instance
 * handles tryClaim → heartbeat → release on its own; the engine just
 * polls `start()` per tick and runs `evaluateSchedules()` while leader.
 */
function createDefaultLeaderLock(): LeasedKVLock {
  return new LeasedKVLock({
    store: createDDBLeaseStore({ docClient, tableName: LEADER_LOCK_TABLE }),
    lockKey: LEADER_LOCK_KEY,
    ttlMs: LEADER_LEASE_TTL_MS,
    ownerId: `${process.pid}@${hostname()}-${randomUUID()}`,
    onLeaseLost: () => {
      // Heartbeat-renewal failure — DDB hiccup, clock skew, or row
      // overwritten. The next tickSchedule() will re-race start() cleanly.
      recordIngestLeaderLeaseLost();
      log.warn(
        { lockKey: LEADER_LOCK_KEY },
        '[ingest-engine] leader lease lost (heartbeat renewal failed)',
      );
    },
  });
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

export class IngestEngine {
  private readonly sourceRepo: IngestSourceRepository;
  private readonly itemRepo: IngestItemRepository;
  private readonly cursorRepo: SyncCursorRepository;
  private readonly documentRepo: TypedDocumentRepository;
  private readonly eventBus: IngestEventBus;
  private readonly enqueueItem: EnqueueItem;
  private readonly leaderLock: LeasedKVLock;

  private running = false;
  private scheduleTimer: ReturnType<typeof setInterval> | null = null;

  constructor(deps: IngestEngineDeps) {
    this.sourceRepo = deps.sourceRepository;
    this.itemRepo = deps.itemRepository;
    this.cursorRepo = deps.cursorRepository;
    this.documentRepo = deps.documentRepository;
    this.eventBus = deps.eventBus;
    this.enqueueItem = deps.enqueueItem;
    this.leaderLock = deps.leaderLock ?? createDefaultLeaderLock();
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;

    // Schedule checker — evaluates cron expressions every 60s. Phase 54.6.3b
    // gates the tick behind a DDB-backed leader lease so only one replica
    // fires cron syncs per minute.
    this.scheduleTimer = setInterval(() => {
      void this.tickSchedule();
    }, 60_000);
    this.scheduleTimer.unref();
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;

    if (this.scheduleTimer) {
      clearInterval(this.scheduleTimer);
      this.scheduleTimer = null;
    }
    await this.leaderLock.stop();
  }

  /**
   * Leader-gated 60s tick. `LeasedKVLock.start()` is idempotent — it's a
   * no-op when we're already leader, and re-attempts acquisition when
   * we're not. The internal heartbeat keeps the lease alive between
   * ticks; on heartbeat-renewal failure isLeader() flips false and the
   * next tick re-races cleanly.
   */
  private async tickSchedule(): Promise<void> {
    if (!this.running) return;
    let acquired: boolean;
    try {
      acquired = await this.leaderLock.start();
    } catch (err: unknown) {
      // Treat any unexpected throw as non-acquired so a sick DDB doesn't
      // crash the setInterval callback as an unhandled rejection. The next
      // tick will re-race cleanly.
      recordIngestLeaderError();
      log.warn(
        { err: err instanceof Error ? err.message : String(err) },
        '[ingest-engine] leader lock acquire failed (treating as non-leader)',
      );
      return;
    }
    if (!acquired) {
      recordIngestLeaderSkipped();
      return;
    }
    recordIngestLeaderAcquired();
    await this.evaluateSchedules();
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /**
   * Trigger a sync for a given source. Lists files from the source adapter,
   * compares against the cursor repository for changes, and enqueues
   * new/changed files as IngestItems.
   */
  async triggerSync(
    sourceId: string,
    triggeredBy: 'manual' | 'schedule' | 'webhook',
  ): Promise<void> {
    const source = await this.sourceRepo.getById(sourceId);
    if (!source) {
      throw new Error(`Source not found: ${sourceId}`);
    }

    if (source.status === 'paused') {
      throw new Error(`Source is paused: ${sourceId}`);
    }

    const startTime = Date.now();
    const stats: SyncStats = {
      filesFound: 0,
      filesNew: 0,
      filesUpdated: 0,
      filesSkipped: 0,
      errors: 0,
      durationMs: 0,
    };

    try {
      const adapter = createAdapter(source);
      const cursor: SyncCursor = {
        lastSyncAt: source.lastSyncAt,
        checkpoint: undefined,
      };

      // Collect file descriptors from the adapter
      const files: FileDescriptor[] = [];
      for await (const file of adapter.list(source.config, cursor)) {
        files.push(file);
      }
      stats.filesFound = files.length;

      // Emit sync started
      this.eventBus.emit({
        type: 'sync.started',
        sourceId,
        sourceName: source.name,
        fileCount: files.length,
        timestamp: new Date().toISOString(),
      });

      // Compare each file against cursor state
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        try {
          const existingCursor = await this.cursorRepo.get(sourceId, file.path);

          if (existingCursor && existingCursor.fileHash === file.hash) {
            // File unchanged — skip
            stats.filesSkipped++;
            continue;
          }

          const isNew = !existingCursor;
          if (isNew) {
            stats.filesNew++;
          } else {
            stats.filesUpdated++;
          }

          // Create an IngestItem for this file
          const ingestId = randomUUID();
          const item: IngestItem = {
            ingestId,
            sourceId,
            filePath: file.path,
            fileHash: file.hash,
            mimeType: file.mimeType,
            fileSize: file.size,
            status: 'queued',
            mappedTypeId: null,
            mappingConfidence: null,
            mappingMethod: null,
            documentId: null,
            pipelineRunId: null,
            error: null,
            attempts: 0,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
          };

          await this.itemRepo.put(item);

          // Update the cursor entry
          await this.cursorRepo.put({
            sourceId,
            filePath: file.path,
            fileHash: file.hash,
            lastModified: file.lastModified,
            lastIngestId: ingestId,
            documentId: null,
          });

          // Phase 54.6.3 — publish to durable queue; consumer in any
          // replica picks it up. Tags travel in the payload so the
          // processing replica doesn't need a producer-replica cache.
          await this.enqueueItem({ ingestId, sourceId, tags: file.tags });
          recordIngestItem('queued');
          recordIngestQueueDepthDelta(1);

          this.eventBus.emit({
            type: 'sync.file.fetched',
            sourceId,
            filePath: file.path,
            index: i + 1,
            total: files.length,
            timestamp: new Date().toISOString(),
          });
        } catch (err) {
          stats.errors++;
          // Continue processing remaining files
        }
      }

      stats.durationMs = Date.now() - startTime;

      // Update source with sync stats
      await this.sourceRepo.updateSource(sourceId, {
        lastSyncAt: new Date().toISOString(),
        lastSyncStats: stats,
        updatedAt: new Date().toISOString(),
      });

      recordIngestSyncDuration(stats.durationMs / 1000);

      this.eventBus.emit({
        type: 'sync.completed',
        sourceId,
        stats,
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      stats.durationMs = Date.now() - startTime;
      stats.errors++;

      // Mark source as errored
      await this.sourceRepo.updateSource(sourceId, {
        status: 'error',
        lastSyncStats: stats,
        updatedAt: new Date().toISOString(),
      });

      this.eventBus.emit({
        type: 'source.status',
        sourceId,
        status: 'error',
        timestamp: new Date().toISOString(),
      });
    }
  }

  /**
   * Check tag-based sync triggers for a source. If the source has
   * config.triggerOnTag set, lists files matching that tag that have changed
   * since the last sync, and enqueues them for processing with a tag-triggered
   * flag.
   */
  async checkTagTriggers(sourceId: string): Promise<void> {
    const source = await this.sourceRepo.getById(sourceId);
    if (!source) {
      throw new Error(`Source not found: ${sourceId}`);
    }

    const triggerTag = source.config.triggerOnTag as string | undefined;
    if (!triggerTag) {
      return; // No tag trigger configured
    }

    const adapter = createAdapter(source);
    const cursor: SyncCursor = {
      lastSyncAt: source.lastSyncAt,
      checkpoint: undefined,
    };

    // List files that have the trigger tag and changed since last sync
    const configWithTag: SourceConfig = { ...source.config, tagFilter: triggerTag };
    const files: FileDescriptor[] = [];
    for await (const file of adapter.list(configWithTag, cursor)) {
      files.push(file);
    }

    // Enqueue matching files that have changed
    for (const file of files) {
      const existingCursor = await this.cursorRepo.get(sourceId, file.path);
      if (existingCursor && existingCursor.fileHash === file.hash) {
        continue; // File unchanged — skip
      }

      const ingestId = randomUUID();
      const item: IngestItem = {
        ingestId,
        sourceId,
        filePath: file.path,
        fileHash: file.hash,
        mimeType: file.mimeType,
        fileSize: file.size,
        status: 'queued',
        mappedTypeId: null,
        mappingConfidence: null,
        mappingMethod: null,
        documentId: null,
        pipelineRunId: null,
        error: null,
        attempts: 0,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };

      await this.itemRepo.put(item);

      await this.cursorRepo.put({
        sourceId,
        filePath: file.path,
        fileHash: file.hash,
        lastModified: file.lastModified,
        lastIngestId: ingestId,
        documentId: null,
      });

      await this.enqueueItem({ ingestId, sourceId, tags: file.tags });
      recordIngestItem('queued');
      recordIngestQueueDepthDelta(1);
    }
  }

  /**
   * Process a single ingest item through the pipeline:
   *   queued → mapping → materializing → triggering → done
   *
   * Phase 54.6.3 — `tags` is supplied by the IngestQueueConsumer (which
   * pulled the durable `ingest:item.queued` payload). Tags formerly lived
   * in an in-process Map populated by `triggerSync` on the producer
   * replica; that cache is gone — any replica can process any item.
   *
   * On failure, the item is marked 'failed' with the error message.
   */
  async processItem(ingestId: string, tags?: string[]): Promise<void> {
    const item = await this.itemRepo.getById(ingestId);
    if (!item) {
      return; // Item may have been deleted — skip silently
    }

    if (item.status !== 'queued') {
      return; // Already processed or in-progress
    }

    try {
      // --- Stage 1: Mapping ---
      await this.transitionStatus(item, 'mapping');

      const source = await this.sourceRepo.getById(item.sourceId);
      if (!source) {
        throw new Error(`Source not found for item: ${item.sourceId}`);
      }

      // Build a FileDescriptor from the item for rule matching
      const cachedTags = tags;
      const fileDescriptor: FileDescriptor = {
        path: item.filePath,
        name: item.filePath.split('/').pop() || item.filePath,
        hash: item.fileHash,
        size: item.fileSize,
        mimeType: item.mimeType,
        lastModified: item.createdAt,
        tags: cachedTags,
      };

      const mappingResult = matchByRules(fileDescriptor, source.mappingRules);

      if (!mappingResult) {
        // No mapping rule matched — mark as failed (future: fall back to LLM)
        throw new Error(
          `No mapping rule matched for file: ${item.filePath} (mimeType=${item.mimeType})`,
        );
      }

      // Update item with mapping result
      await this.itemRepo.updateStatus(ingestId, 'mapping', {
        mappedTypeId: mappingResult.targetTypeId,
        mappingConfidence: mappingResult.confidence,
        mappingMethod: mappingResult.method,
      });
      item.mappedTypeId = mappingResult.targetTypeId;
      item.mappingConfidence = mappingResult.confidence;
      item.mappingMethod = mappingResult.method;

      recordIngestItem('mapping');
      recordIngestMappingConfidence(mappingResult.confidence);

      this.eventBus.emit({
        type: 'item.mapped',
        ingestId,
        typeId: mappingResult.targetTypeId,
        typeName: mappingResult.targetTypeId, // type name resolution deferred
        method: mappingResult.method,
        confidence: mappingResult.confidence,
        timestamp: new Date().toISOString(),
      });

      // --- Stage 2: Materialization ---
      await this.transitionStatus(item, 'materializing');

      const documentId = await this.materializeDocument(
        item,
        source,
        fileDescriptor,
        mappingResult,
      );

      // Update item and cursor with document reference
      await this.itemRepo.updateStatus(ingestId, 'materializing', {
        documentId,
      });
      item.documentId = documentId;

      await this.cursorRepo.put({
        sourceId: item.sourceId,
        filePath: item.filePath,
        fileHash: item.fileHash,
        lastModified: fileDescriptor.lastModified,
        lastIngestId: ingestId,
        documentId,
      });

      this.eventBus.emit({
        type: 'item.materialized',
        ingestId,
        documentId,
        timestamp: new Date().toISOString(),
      });

      // --- Stage 3: Pipeline trigger (optional) ---
      if (source.defaultPipelineId) {
        await this.transitionStatus(item, 'triggering');
        // Pipeline triggering is a future integration point — for now, just
        // transition to done. When pipeline execution integration is wired,
        // this will call PipelineModule.triggerRun() and store the runId.
        this.eventBus.emit({
          type: 'item.pipeline.spawned',
          ingestId,
          runId: 'pending', // placeholder until pipeline integration
          pipelineName: source.defaultPipelineId,
          timestamp: new Date().toISOString(),
        });
      }

      // --- Stage 3b: Agentic review trigger (optional) ---
      const agenticReviewTag = source.config.agenticReviewTag as string | undefined;
      const reviewPipelineId = source.config.reviewPipelineId as string | undefined;
      if (agenticReviewTag && reviewPipelineId && fileDescriptor.tags?.includes(agenticReviewTag)) {
        const pipelineRunId = await this.triggerReviewPipeline(reviewPipelineId, ingestId, documentId);
        item.pipelineRunId = pipelineRunId;
        await this.itemRepo.updateStatus(ingestId, item.status, {
          pipelineRunId,
        });
        this.eventBus.emit({
          type: 'item.review.triggered',
          ingestId,
          documentId,
          pipelineRunId,
          timestamp: new Date().toISOString(),
        });
      }

      // --- Done ---
      await this.transitionStatus(item, 'done');
      recordIngestItem('done');
      recordIngestQueueDepthDelta(-1);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      await this.itemRepo.updateStatus(ingestId, 'failed', {
        error: errorMessage,
        attempts: (item.attempts ?? 0) + 1,
      });

      recordIngestItem('failed');
      recordIngestQueueDepthDelta(-1);
      if ((item.attempts ?? 0) + 1 >= 3) {
        recordIngestDlqDepthDelta(1);
      }

      this.eventBus.emit({
        type: 'item.failed',
        ingestId,
        error: errorMessage,
        retriable: (item.attempts ?? 0) + 1 < 3,
        timestamp: new Date().toISOString(),
      });
    }
  }

  // -------------------------------------------------------------------------
  // Private — schedule evaluation
  // -------------------------------------------------------------------------

  private async evaluateSchedules(): Promise<void> {
    if (!this.running) return;

    try {
      const sources = await this.sourceRepo.listByStatus('active');
      const now = new Date();

      for (const source of sources) {
        if (!source.syncSchedule) continue;

        // Simple schedule evaluation: check if enough time has elapsed since
        // last sync based on a basic interval interpretation of the schedule.
        // Full cron parsing (e.g. via croner or node-cron) is a future
        // enhancement — for now, this supports manual + webhook triggers as
        // the primary sync paths.
        if (this.shouldSync(source, now)) {
          void this.triggerSync(source.sourceId, 'schedule').catch(() => {
            // Schedule-triggered sync errors are surfaced via events, not thrown
          });
        }
      }
    } catch {
      // Schedule evaluation errors are non-fatal
    }
  }

  private shouldSync(source: IngestSource, now: Date): boolean {
    if (!source.lastSyncAt) return true;

    // Parse simple interval from cron-like schedule. For now, support:
    //   "*/N * * * *" → every N minutes
    // Full cron support deferred to a library integration.
    const match = source.syncSchedule?.match(/^\*\/(\d+)\s/);
    if (!match) return false;

    const intervalMinutes = parseInt(match[1], 10);
    const lastSync = new Date(source.lastSyncAt);
    const elapsedMs = now.getTime() - lastSync.getTime();

    return elapsedMs >= intervalMinutes * 60_000;
  }

  // -------------------------------------------------------------------------
  // Private — materialization
  // -------------------------------------------------------------------------

  /**
   * Materialize an ingest item into a TypedDocument using the mapping result's
   * field mapping to construct document values from file metadata.
   */
  private async materializeDocument(
    item: IngestItem,
    source: IngestSource,
    file: FileDescriptor,
    mapping: MappingResult,
  ): Promise<string> {
    const documentId = randomUUID();

    // Build document values from field mapping + file metadata.
    const values: Record<string, string | string[] | number | number[] | boolean> = {};

    for (const [sourceField, docField] of Object.entries(mapping.fieldMapping)) {
      const value = this.extractFieldValue(sourceField, file, item);
      if (value !== undefined) {
        values[docField] = value;
      }
    }

    const document: TypedDocumentItem = {
      documentId,
      typeId: mapping.targetTypeId,
      values,
      createdBy: `ingest:${source.sourceId}`,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    await this.documentRepo.create(document);
    return documentId;
  }

  private extractFieldValue(
    sourceField: string,
    file: FileDescriptor,
    item: IngestItem,
  ): string | string[] | number | boolean | undefined {
    // Map known source fields to file/item metadata
    switch (sourceField) {
      case 'name': return file.name;
      case 'path': return file.path;
      case 'mimeType': return file.mimeType;
      case 'size': return file.size;
      case 'hash': return file.hash;
      case 'lastModified': return file.lastModified;
      case 'tags': return file.tags ?? [];
      case 'filePath': return item.filePath;
      default:
        // Check file metadata for custom fields
        if (file.metadata && sourceField in file.metadata) {
          const val = file.metadata[sourceField];
          if (typeof val === 'string' || typeof val === 'number' || typeof val === 'boolean') {
            return val;
          }
          if (Array.isArray(val)) {
            return val as string[];
          }
        }
        return undefined;
    }
  }

  // -------------------------------------------------------------------------
  // Private — review pipeline trigger
  // -------------------------------------------------------------------------

  /**
   * Trigger a review pipeline for a document that has the agentic review tag.
   * Uses the PipelineBridge if available; otherwise generates a stub runId.
   */
  private async triggerReviewPipeline(
    pipelineId: string,
    ingestId: string,
    documentId: string,
  ): Promise<string> {
    const bridge = getPipelineBridge();
    if (bridge?.trigger) {
      const result = await bridge.trigger({
        pipelineId,
        triggerPayload: { ingestId, documentId, triggerType: 'agentic-review' },
        triggeredBy: { userId: `ingest:${ingestId}` },
      });
      return result.runId;
    }
    // No bridge wired — generate a stub runId
    return `review-${randomUUID()}`;
  }

  // -------------------------------------------------------------------------
  // Private — helpers
  // -------------------------------------------------------------------------

  private async transitionStatus(item: IngestItem, status: IngestItemStatus): Promise<void> {
    await this.itemRepo.updateStatus(item.ingestId, status);
    item.status = status;

    this.eventBus.emit({
      type: 'item.status',
      ingestId: item.ingestId,
      status,
      timestamp: new Date().toISOString(),
    });
  }

}
