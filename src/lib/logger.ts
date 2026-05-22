/**
 * Structured JSON logger for platform-api.
 *
 * Adopts @connorhoehn/service-runtime's `createLogger` so platform-api
 * is its own first end-user of the extracted observability conventions.
 * The pino-pretty dev-mode try-require + ISO timestamps + base service
 * field all live in the shared lib now.
 *
 * Usage unchanged for callers:
 *   import log, { withContext } from './lib/logger';
 *   const reqLog = withContext({ correlationId, userId });
 *   reqLog.info({ action: 'trigger.create' }, 'pipeline triggered');
 *
 * Configuration (read by createLogger from process.env):
 *   - SERVICE_NAME  (default: 'platform-api')
 *   - LOG_LEVEL     (default: 'info')
 *   - NODE_ENV=development → pino-pretty if installed, else plain JSON
 */

import { createLogger, type Logger } from '@connorhoehn/service-runtime';

const serviceName = process.env.SERVICE_NAME || 'platform-api';

const logger = createLogger({
  service: serviceName,
  level: process.env.LOG_LEVEL ?? 'info',
  autoEnrichFromContext: true,
});

/**
 * Returns a child logger with the given fields bound to every log line.
 * Handy for per-request loggers carrying correlationId, runId, userId, etc.
 */
export function withContext(ctx: Record<string, unknown>): Logger {
  return logger.child(ctx);
}

export default logger;
