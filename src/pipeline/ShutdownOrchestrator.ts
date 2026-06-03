import type { RunPersistenceService } from './runPersistence';
import type { StuckRunDetector } from './StuckRunDetector';
import type { ExecutorAuditSubscriber } from './executor-audit-subscriber';
import type { JudgeAggregatorSubscriber } from './judge/judge-aggregator-subscriber';

interface ShutdownDeps {
  keepAlive: ReturnType<typeof setInterval>;
  runQueueSubIds: string[];
  eventBus: { unsubscribe(id: string): void };
  runPersistence: RunPersistenceService | null;
  stuckRunDetector: StuckRunDetector | null;
  executorAuditSubscriber: ExecutorAuditSubscriber | null;
  judgeAggregatorSubscriber: JudgeAggregatorSubscriber | null;
  /** Stop fn for the audit sweeper interval (no-op when sweeper disabled). */
  auditSweeperStop: (() => void) | null;
  moduleRegistry: { stop(): Promise<void> };
  resourceRegistry: { stop(): Promise<void> };
  hintedHandoff: { stop(): Promise<void> } | null;
  fdBridge: { stop(): Promise<void> } | null;
  cluster: { stop(): Promise<void> };
  logger: { error(ctx: Record<string, unknown>, msg: string): void };
}

export function createShutdown(deps: ShutdownDeps): () => Promise<void> {
  const {
    keepAlive,
    runQueueSubIds,
    eventBus,
    runPersistence,
    stuckRunDetector,
    executorAuditSubscriber,
    judgeAggregatorSubscriber,
    auditSweeperStop,
    moduleRegistry,
    resourceRegistry,
    hintedHandoff,
    fdBridge,
    cluster,
    logger,
  } = deps;

  return async (): Promise<void> => {
    clearInterval(keepAlive);
    for (const subId of runQueueSubIds) {
      try { eventBus.unsubscribe(subId); } catch { /* idempotent */ }
    }
    try { if (auditSweeperStop) auditSweeperStop(); } catch (err) { logger.error({ err: String(err) }, 'auditSweeperStop() failed'); }
    try { if (executorAuditSubscriber) await executorAuditSubscriber.stop(); } catch (err) { logger.error({ err: String(err) }, 'executorAuditSubscriber.stop() failed'); }
    try { if (judgeAggregatorSubscriber) await judgeAggregatorSubscriber.stop(); } catch (err) { logger.error({ err: String(err) }, 'judgeAggregatorSubscriber.stop() failed'); }
    try { if (stuckRunDetector) stuckRunDetector.stop(); } catch (err) { logger.error({ err: String(err) }, 'stuckRunDetector.stop() failed'); }
    try { if (runPersistence) await runPersistence.stop(); } catch (err) { logger.error({ err: String(err) }, 'runPersistence.stop() failed'); }
    try {
      await moduleRegistry.stop();
    } catch (err) {
      logger.error({ err: String(err) }, 'moduleRegistry.stop() failed');
    }
    try {
      await resourceRegistry.stop();
    } catch (err) {
      logger.error({ err: String(err) }, 'resourceRegistry.stop() failed');
    }
    try { if (hintedHandoff) await hintedHandoff.stop(); } catch (err) { logger.error({ err: String(err) }, 'hintedHandoff.stop() failed'); }
    try { if (fdBridge) await fdBridge.stop(); } catch (err) { logger.error({ err: String(err) }, 'fdBridge.stop() failed'); }
    try {
      await cluster.stop();
    } catch (err) {
      logger.error({ err: String(err) }, 'cluster.stop() failed');
    }
  };
}
