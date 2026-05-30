// Unit tests for request-logger middleware.

import type { Request, Response, NextFunction } from 'express';
import { EventEmitter } from 'events';

// Mock the logger module at the import boundary — pino's logger methods
// are exposed via getters that `jest.spyOn` cannot replace, so we swap the
// whole module for a plain object of jest.fn()s.
jest.mock('../../lib/logger', () => {
  const make = () => jest.fn();
  return {
    __esModule: true,
    default: {
      info: make(),
      error: make(),
      warn: make(),
      debug: make(),
      trace: make(),
      fatal: make(),
    },
    withContext: jest.fn(),
  };
});

import { requestLogger } from '../request-logger';
import log from '../../lib/logger';

const mockedLog = log as unknown as {
  info: jest.Mock;
  error: jest.Mock;
};

describe('requestLogger', () => {
  beforeEach(() => {
    mockedLog.info.mockReset();
    mockedLog.error.mockReset();
  });

  it('calls next immediately', () => {
    const res = new EventEmitter() as unknown as Response;
    (res as unknown as { statusCode: number }).statusCode = 200;
    const next = jest.fn() as unknown as NextFunction;
    const req = { method: 'GET', originalUrl: '/test' } as Request;
    requestLogger(req, res, next);
    expect(next).toHaveBeenCalled();
  });

  it('logs method, path, status, and duration on finish', () => {
    const res = new EventEmitter() as unknown as Response;
    (res as unknown as { statusCode: number }).statusCode = 201;
    const next = jest.fn() as unknown as NextFunction;
    const req = { method: 'POST', originalUrl: '/api/foo' } as Request;

    requestLogger(req, res, next);
    (res as unknown as EventEmitter).emit('finish');

    expect(mockedLog.info).toHaveBeenCalledWith(
      expect.objectContaining({
        method: 'POST',
        url: '/api/foo',
        status: 201,
        durationMs: expect.any(Number),
      }),
      'http request'
    );
  });

  it('on 5xx emits at log.error with errorMessage + errorName from res.locals', () => {
    const res = new EventEmitter() as unknown as Response;
    (res as unknown as { statusCode: number }).statusCode = 500;
    const stashedError = new TypeError('binding lookup failed');
    (res as unknown as { locals: Record<string, unknown> }).locals = {
      __loggedError: stashedError,
    };
    const next = jest.fn() as unknown as NextFunction;
    const req = { method: 'POST', originalUrl: '/api/video/sessions' } as Request;

    requestLogger(req, res, next);
    (res as unknown as EventEmitter).emit('finish');

    // Level bumped to error (50) on 5xx — info path is NOT taken.
    expect(mockedLog.info).not.toHaveBeenCalled();
    expect(mockedLog.error).toHaveBeenCalledTimes(1);
    expect(mockedLog.error).toHaveBeenCalledWith(
      expect.objectContaining({
        method: 'POST',
        url: '/api/video/sessions',
        status: 500,
        durationMs: expect.any(Number),
        errorMessage: 'binding lookup failed',
        errorName: 'TypeError',
        errorStackTop: expect.any(String),
      }),
      'http request'
    );
  });
});
