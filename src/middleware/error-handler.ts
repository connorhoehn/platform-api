/**
 * Central error middleware for platform-api.
 *
 * Usage in route handlers:
 *   throw new NotFoundError('Post not found');
 *   throw new ValidationError('content is required (max 10000 chars)');
 *
 * Thrown AppError subclasses are translated to the appropriate HTTP status and
 * the existing JSON shape `{ error: string }` so clients are unaffected.
 *
 * Unknown errors are logged server-side and returned as a generic 500 so internal
 * details never leak.
 *
 * The app runs on Express 4, which does NOT auto-forward thrown errors from async
 * handlers — wrap async routes with `asyncHandler(fn)` so rejections reach here.
 */
import type { Request, Response, NextFunction, RequestHandler } from 'express';
import log from '../lib/logger';

export class AppError extends Error {
  public readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
    this.name = new.target.name;
    // Ensure instanceof works across transpilation targets
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class ValidationError extends AppError {
  constructor(message: string) {
    super(400, message);
  }
}

export class UnauthorizedError extends AppError {
  constructor(message = 'Unauthorized') {
    super(401, message);
  }
}

export class ForbiddenError extends AppError {
  constructor(message = 'Forbidden') {
    super(403, message);
  }
}

export class NotFoundError extends AppError {
  constructor(message = 'Not found') {
    super(404, message);
  }
}

export class ConflictError extends AppError {
  constructor(message = 'Conflict') {
    super(409, message);
  }
}

/**
 * Wrap an async Express handler so thrown errors / rejected promises reach
 * the error middleware (Express 4 does not do this automatically).
 */
export function asyncHandler<P = unknown, ResBody = unknown, ReqBody = unknown, ReqQuery = unknown>(
  fn: (req: Request<P, ResBody, ReqBody, ReqQuery>, res: Response<ResBody>, next: NextFunction) => Promise<unknown>,
): RequestHandler<P, ResBody, ReqBody, ReqQuery> {
  return ((req, res, next) => {
    // Return the chained promise so callers (e.g. tests) can await it.
    // Express itself ignores the return value — it only reacts to `next(err)`.
    return Promise.resolve(fn(req, res, next)).catch(next);
  }) as RequestHandler<P, ResBody, ReqBody, ReqQuery>;
}

/**
 * Express error-handling middleware. MUST be registered after all routes.
 */
export function errorHandler(
  err: unknown,
  _req: Request,
  res: Response,
  next: NextFunction,
): void {
  // If headers already sent, delegate to the default Express handler (which
  // closes the connection). Prevents "Cannot set headers after they are sent".
  if (res.headersSent) {
    next(err);
    return;
  }

  if (err instanceof AppError) {
    res.status(err.status).json({ error: err.message });
    return;
  }

  // Unknown error — log the real thing server-side, return a generic message.
  const label = (err as Error | undefined)?.name ?? 'Error';
  log.error({ err, errorType: label }, 'unhandled error');
  // Stash for the request-logger so the structured "http request" line on the
  // 500 carries errorMessage/errorName/errorStackTop. Operators reading the
  // request log no longer have to cross-reference a separate "unhandled error"
  // entry to see why the request failed. `res.locals` may be undefined when
  // callers pass a hand-rolled mock response (tests), so guard defensively.
  if (res.locals) {
    (res.locals as { __loggedError?: unknown }).__loggedError = err;
  }
  res.status(500).json({ error: 'Internal server error' });
}
