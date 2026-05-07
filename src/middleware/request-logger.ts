import { Request, Response, NextFunction } from 'express';
import log from '../lib/logger';

/**
 * Lightweight request logging middleware.
 * Logs: method, path, status code, and response time in ms.
 */
export function requestLogger(req: Request, res: Response, next: NextFunction): void {
  const start = Date.now();

  res.on('finish', () => {
    const duration = Date.now() - start;
    log.info({
      method: req.method,
      url: req.originalUrl,
      status: res.statusCode,
      durationMs: duration
    }, 'http request');
  });

  next();
}
