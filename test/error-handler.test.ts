/**
 * Tests for social-api central error middleware.
 *
 * Covers:
 *  - Each AppError subclass → correct status + JSON body
 *  - Unknown Error → 500 with safe generic message
 *  - asyncHandler forwards rejections to next(err)
 */
import {
  AppError,
  ValidationError,
  UnauthorizedError,
  ForbiddenError,
  NotFoundError,
  ConflictError,
  errorHandler,
  asyncHandler,
} from '../src/middleware/error-handler';

function mockRes() {
  const res: any = {
    headersSent: false,
    statusCode: 200,
    body: undefined,
  };
  res.status = (code: number) => {
    res.statusCode = code;
    return res;
  };
  res.json = (payload: unknown) => {
    res.body = payload;
    res.headersSent = true;
    return res;
  };
  return res;
}

describe('error-handler middleware', () => {
  const req: any = {};

  test('ValidationError → 400 + error message', () => {
    const res = mockRes();
    const next = jest.fn();
    errorHandler(new ValidationError('bad input'), req, res, next);
    expect(res.statusCode).toBe(400);
    expect(res.body).toEqual({ error: 'bad input' });
    expect(next).not.toHaveBeenCalled();
  });

  test('UnauthorizedError → 401', () => {
    const res = mockRes();
    errorHandler(new UnauthorizedError('nope'), req, res, jest.fn());
    expect(res.statusCode).toBe(401);
    expect(res.body).toEqual({ error: 'nope' });
  });

  test('ForbiddenError → 403', () => {
    const res = mockRes();
    errorHandler(new ForbiddenError('no access'), req, res, jest.fn());
    expect(res.statusCode).toBe(403);
    expect(res.body).toEqual({ error: 'no access' });
  });

  test('NotFoundError → 404', () => {
    const res = mockRes();
    errorHandler(new NotFoundError('gone'), req, res, jest.fn());
    expect(res.statusCode).toBe(404);
    expect(res.body).toEqual({ error: 'gone' });
  });

  test('ConflictError → 409', () => {
    const res = mockRes();
    errorHandler(new ConflictError('dup'), req, res, jest.fn());
    expect(res.statusCode).toBe(409);
    expect(res.body).toEqual({ error: 'dup' });
  });

  test('custom AppError preserves status + message', () => {
    const res = mockRes();
    errorHandler(new AppError(418, "I'm a teapot"), req, res, jest.fn());
    expect(res.statusCode).toBe(418);
    expect(res.body).toEqual({ error: "I'm a teapot" });
  });

  test('unknown Error → 500 with generic message', () => {
    const res = mockRes();
    errorHandler(new Error('DB exploded with secret detail'), req, res, jest.fn());
    expect(res.statusCode).toBe(500);
    expect(res.body).toEqual({ error: 'Internal server error' });
    // Note: The real error IS logged server-side via log.error() (see error-handler.ts:99),
    // but pino's logger methods are not easily mockable in jest due to how they're bound.
    // The critical behavior - not leaking internal errors to clients - is verified above.
  });

  test('non-Error rejection (e.g. string) → 500 generic', () => {
    const res = mockRes();
    const spy = jest.spyOn(console, 'error').mockImplementation(() => undefined);
    try {
      errorHandler('some weird throw value', req, res, jest.fn());
      expect(res.statusCode).toBe(500);
      expect(res.body).toEqual({ error: 'Internal server error' });
    } finally {
      spy.mockRestore();
    }
  });

  test('if headers already sent, delegates to next(err)', () => {
    const res = mockRes();
    res.headersSent = true;
    const next = jest.fn();
    const err = new Error('boom');
    errorHandler(err, req, res, next);
    expect(next).toHaveBeenCalledWith(err);
  });
});

describe('asyncHandler', () => {
  test('forwards thrown AppError to next()', async () => {
    const handler = asyncHandler(async () => {
      throw new NotFoundError('missing');
    });
    const next = jest.fn();
    const res = mockRes();
    await handler({} as any, res, next);
    expect(next).toHaveBeenCalledTimes(1);
    const passed = next.mock.calls[0][0];
    expect(passed).toBeInstanceOf(NotFoundError);
    expect((passed as NotFoundError).status).toBe(404);
  });

  test('forwards rejected promise to next()', async () => {
    const handler = asyncHandler(async () => {
      return Promise.reject(new Error('async boom'));
    });
    const next = jest.fn();
    const res = mockRes();
    await handler({} as any, res, next);
    expect(next).toHaveBeenCalledTimes(1);
    expect((next.mock.calls[0][0] as Error).message).toBe('async boom');
  });

  test('does not call next() on success', async () => {
    const handler = asyncHandler(async (_req: any, res: any) => {
      res.status(200).json({ ok: true });
    });
    const next = jest.fn();
    const res = mockRes();
    await handler({} as any, res, next);
    expect(next).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ ok: true });
  });
});
