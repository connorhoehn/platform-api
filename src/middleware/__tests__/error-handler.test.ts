// Unit tests for error-handler middleware.
//
// Exercises AppError subclass hierarchy, asyncHandler wrapper, and
// errorHandler middleware without touching any external services.

import type { Request, Response, NextFunction } from 'express';
import {
  AppError,
  ValidationError,
  UnauthorizedError,
  ForbiddenError,
  NotFoundError,
  ConflictError,
  asyncHandler,
  errorHandler,
} from '../error-handler';

function mockRes() {
  const res: Partial<Response> & { statusCode: number; body: unknown; headersSent: boolean } = {
    statusCode: 200,
    body: undefined,
    headersSent: false,
    status(code: number) { this.statusCode = code; return this as unknown as Response; },
    json(body: unknown) { this.body = body; return this as unknown as Response; },
  };
  return res as unknown as Response & { statusCode: number; body: unknown };
}

describe('AppError hierarchy', () => {
  it('AppError stores status and message', () => {
    const err = new AppError(418, 'I am a teapot');
    expect(err.status).toBe(418);
    expect(err.message).toBe('I am a teapot');
    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(AppError);
  });

  it('ValidationError is 400', () => {
    const err = new ValidationError('bad input');
    expect(err.status).toBe(400);
    expect(err.name).toBe('ValidationError');
    expect(err).toBeInstanceOf(AppError);
  });

  it('UnauthorizedError is 401 with default message', () => {
    const err = new UnauthorizedError();
    expect(err.status).toBe(401);
    expect(err.message).toBe('Unauthorized');
  });

  it('ForbiddenError is 403 with default message', () => {
    const err = new ForbiddenError();
    expect(err.status).toBe(403);
    expect(err.message).toBe('Forbidden');
  });

  it('NotFoundError is 404 with default message', () => {
    const err = new NotFoundError();
    expect(err.status).toBe(404);
    expect(err.message).toBe('Not found');
  });

  it('ConflictError is 409 with default message', () => {
    const err = new ConflictError();
    expect(err.status).toBe(409);
    expect(err.message).toBe('Conflict');
  });

  it('subclasses accept custom messages', () => {
    expect(new UnauthorizedError('no token').message).toBe('no token');
    expect(new ForbiddenError('admin only').message).toBe('admin only');
    expect(new NotFoundError('user 42').message).toBe('user 42');
    expect(new ConflictError('duplicate').message).toBe('duplicate');
  });
});

describe('asyncHandler', () => {
  it('calls next with error when handler rejects', async () => {
    const handler = asyncHandler(async () => { throw new Error('boom'); });
    const next = jest.fn() as unknown as NextFunction;
    await handler({} as Request, {} as Response, next);
    expect(next).toHaveBeenCalledWith(expect.any(Error));
  });

  it('does not call next when handler resolves', async () => {
    const handler = asyncHandler(async (_req, res) => { (res as unknown as { done: boolean }).done = true; });
    const next = jest.fn() as unknown as NextFunction;
    const res = { done: false } as unknown as Response;
    await handler({} as Request, res, next);
    expect(next).not.toHaveBeenCalled();
    expect((res as unknown as { done: boolean }).done).toBe(true);
  });
});

describe('errorHandler', () => {
  const next = jest.fn() as unknown as NextFunction;
  const req = {} as Request;

  beforeEach(() => jest.clearAllMocks());

  it('returns status and message for AppError subclass', () => {
    const res = mockRes();
    errorHandler(new ValidationError('bad field'), req, res, next);
    expect(res.statusCode).toBe(400);
    expect(res.body).toEqual({ error: 'bad field' });
  });

  it('returns 500 for unknown errors', () => {
    const res = mockRes();
    const spy = jest.spyOn(console, 'error').mockImplementation(() => {});
    errorHandler(new Error('unexpected'), req, res, next);
    expect(res.statusCode).toBe(500);
    expect(res.body).toEqual({ error: 'Internal server error' });
    spy.mockRestore();
  });

  it('delegates to next when headers already sent', () => {
    const res = mockRes();
    (res as unknown as { headersSent: boolean }).headersSent = true;
    const nextFn = jest.fn() as unknown as NextFunction;
    errorHandler(new Error('late'), req, res, nextFn);
    expect(nextFn).toHaveBeenCalledWith(expect.any(Error));
    expect(res.statusCode).toBe(200);
  });

  it('handles non-Error thrown values', () => {
    const res = mockRes();
    const spy = jest.spyOn(console, 'error').mockImplementation(() => {});
    errorHandler('string-error', req, res, next);
    expect(res.statusCode).toBe(500);
    spy.mockRestore();
  });
});
