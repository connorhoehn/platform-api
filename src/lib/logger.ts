/**
 * Structured JSON logger (pino) for social-api.
 *
 * Usage:
 *   import log, { withContext } from './lib/logger';
 *   const reqLog = withContext({ correlationId, userId });
 *   reqLog.info({ action: 'trigger.create' }, 'pipeline triggered');
 *
 * Configuration:
 *   - SERVICE_NAME (default: 'social-api')
 *   - LOG_LEVEL    (default: 'info')
 *   - NODE_ENV=development uses pino-pretty if available, else plain JSON.
 */

import pino, { Logger, LoggerOptions } from 'pino';

const serviceName = process.env.SERVICE_NAME || 'social-api';
const level = process.env.LOG_LEVEL || 'info';
const isDev = process.env.NODE_ENV === 'development';

const baseOptions: LoggerOptions = {
  level,
  base: { service: serviceName },
  timestamp: pino.stdTimeFunctions.isoTime,
};

let logger: Logger;

if (isDev) {
  let prettyTransport: LoggerOptions['transport'] | undefined;
  try {
    // try-require pino-pretty; if not installed, fall back to plain JSON.
    require.resolve('pino-pretty');
    prettyTransport = {
      target: 'pino-pretty',
      options: {
        colorize: true,
        translateTime: 'SYS:standard',
        ignore: 'pid,hostname',
      },
    };
  } catch {
    prettyTransport = undefined;
  }
  logger = prettyTransport
    ? pino({ ...baseOptions, transport: prettyTransport })
    : pino(baseOptions);
} else {
  logger = pino(baseOptions);
}

/**
 * Returns a child logger with the given fields bound to every log line.
 * Handy for per-request loggers carrying correlationId, runId, userId, etc.
 */
export function withContext(ctx: Record<string, unknown>): Logger {
  return logger.child(ctx);
}

export default logger;
