// platform-api/src/pipeline/definitions-cache.ts
//
// Cross-user, in-memory cache of pipeline definitions, refreshed on a fixed
// interval (default 60s) by calling `definitionsRepo.listAll()`.
//
// Why this exists
// ---------------
// Two surfaces need a synchronous, all-users view of every pipeline
// definition:
//
//   1. The schedule evaluator in `src/index.ts`. It iterates every
//      published pipeline once per minute and fires the ones whose cron
//      expression matches "now". It used to read from a sync in-memory
//      mirror that the route layer wrote through to on every PUT — but
//      after a process restart that mirror was empty until each pipeline
//      got re-touched, so scheduled pipelines went silently dark.
//
//   2. The public webhook router in `src/routes/pipelineWebhooks.ts`.
//      `lookupWebhookBinding()` needs to map the URL path back to a
//      pipeline definition (which is owned by a specific user, but the URL
//      itself is global, and the route is unauthenticated so it has no
//      userId to read that definition with). Same restart
//      blindness as above. Webhook receipt is a hot path — calling
//      DynamoDB on every POST would add ~10ms of latency and burn read
//      capacity on every signature check.
//
// What we do
// ----------
// Refresh on `start()` (eagerly, so cold-start blindness is bounded by
// Scan latency, not the tick interval) and then every `intervalMs` ms.
// Reads are sync — callers see whatever the last refresh wrote. Stale
// reads are bounded by `intervalMs`; we explicitly accept a ~60s
// propagation delay between PUT and the schedule/webhook surfaces seeing
// it (the route's PUT handler triggers a synchronous best-effort refresh
// to shrink that window for the common "save then test" UX).

import { withContext } from '../lib/logger';
import {
  recordCacheAccess,
  recordPipelineDefinitionsRefresh,
} from '../observability/metrics';
import { definitionsRepo, type PipelineDefinition } from './definitions-repository';

const log = withContext({ module: 'pipelineDefinitionsCache' });

export interface PipelineDefinitionsCacheOptions {
  /** Refresh interval in ms. Defaults to 60_000. */
  intervalMs?: number;
  /** Override the loader (tests). Defaults to `definitionsRepo.listAll()`. */
  loader?: () => Promise<PipelineDefinition[]>;
  /** Override clock (tests, currently unused — refresh is timer-driven). */
  now?: () => number;
}

export interface WebhookIndexEntry {
  pipelineId: string;
  secret?: string;
  /**
   * The full stored definition that declared this binding.
   *
   * The webhook route is unauthenticated, so it has no `userId` with which
   * to call `definitionsRepo.get(userId, pipelineId)` — but it MUST pass a
   * definition to `bridge.trigger` (PipelineModule.createResource rejects
   * the call without one). The cache is already holding every definition in
   * memory to build this index, so carrying it here costs nothing and needs
   * no owner lookup: the binding was authored by the definition's owner, so
   * "the definition that claimed this path" is exactly the one the owner
   * asked us to run.
   */
  definition: PipelineDefinition;
  /**
   * True when two or more stored definitions claim the same `webhookPath`.
   *
   * The index is a Map keyed by path and the Scan order that feeds it is not
   * stable, so a collision would otherwise resolve to an arbitrary winner —
   * meaning one user's webhook payload could be delivered into another
   * user's pipeline (and the loser's webhook would silently stop firing).
   * We refuse to pick a winner; the route fails the request closed.
   */
  ambiguous?: boolean;
}

/**
 * Sync, refresh-on-interval snapshot of every pipeline definition across
 * every user. Intended for cross-user consumers that need an O(1)-ish
 * read on every request. Per-user reads should still go through
 * `definitionsRepo.list(userId)` directly so they're authoritative and
 * isolated.
 */
export class PipelineDefinitionsCache {
  private snapshot: PipelineDefinition[] = [];
  private webhookIndex: Map<string, WebhookIndexEntry> = new Map();
  private timer: ReturnType<typeof setInterval> | null = null;
  private readonly intervalMs: number;
  private readonly loader: () => Promise<PipelineDefinition[]>;
  /** Promise of the currently-in-flight refresh, if any. Coalesces concurrent calls. */
  private inFlight: Promise<void> | null = null;
  /**
   * Wave A fix: a refresh() call that lands while another refresh is
   * mid-Scan must NOT be silently coalesced away — the in-flight Scan's
   * snapshot predates the write that prompted the new call, so the write
   * would stay invisible until the next timer tick (observed live: a
   * publish poked refresh while a delete-poked refresh was in flight; the
   * event dispatcher then missed the freshly published pipeline for up to
   * 60s). Instead we mark the in-flight refresh dirty and chain one
   * follow-up refresh after it settles.
   */
  private dirty = false;
  /** Set true once the first refresh has completed (success or failure). */
  private primed = false;
  private generationCount = 0;

  constructor(opts: PipelineDefinitionsCacheOptions = {}) {
    this.intervalMs = opts.intervalMs ?? 60_000;
    this.loader = opts.loader ?? (() => definitionsRepo.listAll());
  }

  /**
   * Kick off the periodic refresh. Returns a promise that resolves when
   * the FIRST refresh completes — callers that want bounded cold-start
   * blindness can `await` it before announcing the process as ready.
   * Repeated calls are no-ops.
   */
  async start(): Promise<void> {
    if (this.timer) return this.refresh();
    // Schedule the recurring refresh first so we don't lose ticks if the
    // initial refresh takes a while.
    this.timer = setInterval(() => {
      // Swallow errors — the periodic-refresh path logs and keeps the
      // last-known-good snapshot. Throwing here would crash the timer.
      void this.refresh().catch((err) => {
        log.error({ err }, 'periodic refresh failed');
      });
    }, this.intervalMs);
    if (typeof this.timer === 'object' && this.timer && 'unref' in this.timer) {
      (this.timer as unknown as { unref: () => void }).unref();
    }
    return this.refresh();
  }

  /** Stop the recurring refresh. Safe to call multiple times. */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Force an immediate refresh. Coalesces concurrent callers onto the
   * same in-flight promise so the route's "PUT then refresh" optimization
   * doesn't trigger a Scan storm under burst-write workloads.
   */
  refresh(): Promise<void> {
    // Anything keyed on a definition read before this call is suspect from
    // here on: a write published its invalidation before the refresh ran.
    this.generationCount += 1;
    if (this.inFlight) {
      // Coalesce onto the in-flight Scan, but remember that state may have
      // changed after it started — one follow-up refresh runs when it
      // settles (see `dirty` field note). Callers still resolve when the
      // CURRENT refresh finishes; the follow-up is best-effort.
      if (!this.dirty) {
        this.dirty = true;
        void this.inFlight
          .catch(() => {})
          .then(() => {
            this.dirty = false;
            return this.refresh();
          })
          .catch(() => {});
      }
      return this.inFlight;
    }
    this.inFlight = (async () => {
      try {
        const next = await this.loader();
        this.snapshot = next;
        this.rebuildWebhookIndex(next);
        this.primed = true;
        // Stamp the freshness gauge AFTER snapshot/index commit so a thrown
        // loader doesn't advance the timestamp. Alerts like
        // `time() - pipeline_definitions_last_refresh_timestamp > 90`
        // then correctly fire when the loader is silently failing.
        recordPipelineDefinitionsRefresh();
      } catch (err) {
        // Keep the previous snapshot — we'd rather serve slightly stale
        // data than nothing. Mark `primed` so the schedule evaluator
        // doesn't loop forever waiting for a successful first refresh.
        this.primed = true;
        throw err;
      } finally {
        this.inFlight = null;
      }
    })();
    // Detach error so unhandled-rejection noise doesn't leak when callers
    // use `void cache.refresh()` (the inner promise still rejects for
    // explicit awaiters).
    this.inFlight.catch(() => {});
    return this.inFlight;
  }

  /**
   * Build O(1) lookup map from webhookPath → { pipelineId, secret }.
   * Called after each snapshot refresh.
   */
  private rebuildWebhookIndex(defs: PipelineDefinition[]): void {
    const index = new Map<string, WebhookIndexEntry>();
    const conflicts: string[] = [];
    for (const def of defs) {
      const d = def as {
        id?: string;
        triggerBinding?: {
          event?: string;
          webhookPath?: string;
          webhookSecret?: string;
        };
      };
      const tb = d?.triggerBinding;
      if (!tb || tb.event !== 'webhook') continue;
      if (!tb.webhookPath || typeof d.id !== 'string') continue;

      // Collision: a second definition claims a path another one already
      // took. Mark it and keep going — WHICH entry we retain is irrelevant
      // because the route refuses ambiguous bindings outright.
      const existing = index.get(tb.webhookPath);
      if (existing) {
        existing.ambiguous = true;
        conflicts.push(tb.webhookPath);
        continue;
      }

      const entry: WebhookIndexEntry = { pipelineId: d.id, definition: def };
      if (typeof tb.webhookSecret === 'string' && tb.webhookSecret.length > 0) {
        entry.secret = tb.webhookSecret;
      }
      index.set(tb.webhookPath, entry);
    }
    if (conflicts.length > 0) {
      log.warn(
        { webhookPaths: Array.from(new Set(conflicts)) },
        'webhook path claimed by more than one pipeline definition; those paths will be refused',
      );
    }
    this.webhookIndex = index;
  }

  /** Synchronous read of the latest snapshot. */
  all(): readonly PipelineDefinition[] {
    return this.snapshot;
  }

  /** O(1) lookup: webhookPath → { pipelineId, secret }. Returns undefined when no match. */
  getWebhookEntry(path: string): WebhookIndexEntry | undefined {
    const entry = this.webhookIndex.get(path);
    recordCacheAccess('definitions', entry !== undefined);
    return entry;
  }

  /**
   * Bumped by every refresh — the 60 s tick and each invalidation any
   * replica publishes after a definition write. A cache keyed on it (the
   * work graph's definition reads, realtime-examples NFR #208) drops what it
   * held the moment a write is announced.
   */
  generation(): number {
    return this.generationCount;
  }

  /** True after the first refresh has completed (success or failure). */
  isPrimed(): boolean {
    return this.primed;
  }

  /** Test helper — replace the snapshot directly without going through the loader. */
  __setSnapshotForTests(items: PipelineDefinition[]): void {
    this.snapshot = items;
    this.rebuildWebhookIndex(items);
    this.primed = true;
  }
}

/**
 * Process-singleton cache. Started from `src/index.ts` on boot and
 * consulted synchronously by the schedule evaluator and the webhook
 * router. Tests that don't boot `src/index.ts` (most route tests)
 * inject their own snapshot via `__setSnapshotForTests`.
 */
export const pipelineDefinitionsCache = new PipelineDefinitionsCache();
