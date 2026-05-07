// Unit tests for request-logger middleware.

import type { Request, Response, NextFunction } from 'express';
import { EventEmitter } from 'events';
import { requestLogger } from '../request-logger';
import log from '../../lib/logger';

describe('requestLogger', () => {
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
    const spy = jest.spyOn(log, 'info').mockImplementation(() => log);

    requestLogger(req, res, next);
    (res as unknown as EventEmitter).emit('finish');

    expect(spy).toHaveBeenCalledWith(
      expect.objectContaining({
        method: 'POST',
        url: '/api/foo',
        status: 201,
        durationMs: expect.any(Number),
      }),
      'http request'
    );
    spy.mockRestore();
  });
});
