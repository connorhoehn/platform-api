import { Request, Response, NextFunction } from 'express';
import log from '../lib/logger';

/**
 * Lightweight request logging middleware.
 * Logs: method, path, status code, and response time in ms.
 *
 * On 5xx responses, additionally surfaces error details (errorMessage,
 * errorName, errorStackTop) so operators can diagnose failures without
 * tailing a separate error log. The error is sourced from
 * `res.locals.__loggedError`, which the central errorHandler stashes
 * before sending the 500 response. Bumps log level to error (50) for 5xx.
 */
export function requestLogger(req: Request, res: Response, next: NextFunction): void {
  const start = Date.now();

  res.on('finish', () => {
    const duration = Date.now() - start;
    const status = res.statusCode;
    const base: Record<string, unknown> = {
      method: req.method,
      url: req.originalUrl,
      status,
      durationMs: duration,
    };

    if (status >= 500) {
      const errRaw = (res.locals && (res.locals as { __loggedError?: unknown }).__loggedError);
      const err = errRaw as { message?: unknown; name?: unknown; stack?: unknown } | undefined;
      if (err) {
        if (typeof err.message === 'string') base.errorMessage = err.message;
        if (typeof err.name === 'string') base.errorName = err.name;
        if (typeof err.stack === 'string') base.errorStackTop = err.stack.slice(0, 200);
      }
      log.error(base, 'http request');
      return;
    }

    log.info(base, 'http request');
  });

  next();
}
