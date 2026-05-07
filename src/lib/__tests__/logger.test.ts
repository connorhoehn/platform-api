// social-api/src/lib/__tests__/logger.test.ts
//
// Tests for social-api logger: shape, withContext child loggers, env config.

// Reset modules between tests so env-var changes take effect.
beforeEach(() => {
    jest.resetModules();
    delete process.env.LOG_LEVEL;
    delete process.env.SERVICE_NAME;
    delete process.env.NODE_ENV;
});

describe('logger — shape', () => {
    test('default export has standard pino log methods', () => {
        const log = require('../logger').default;
        expect(typeof log.info).toBe('function');
        expect(typeof log.warn).toBe('function');
        expect(typeof log.error).toBe('function');
        expect(typeof log.debug).toBe('function');
    });

    test('withContext is exported as a function', () => {
        const { withContext } = require('../logger');
        expect(typeof withContext).toBe('function');
    });
});

describe('logger — withContext', () => {
    test('returns a child logger with standard methods', () => {
        const { withContext } = require('../logger');
        const child = withContext({ correlationId: 'req-123', userId: 'u-1' });
        expect(typeof child.info).toBe('function');
        expect(typeof child.warn).toBe('function');
        expect(typeof child.error).toBe('function');
        expect(typeof child.debug).toBe('function');
    });

    test('child is distinct from the parent logger', () => {
        const log = require('../logger').default;
        const { withContext } = require('../logger');
        expect(withContext({ module: 'auth' })).not.toBe(log);
    });

    test('accepts empty context', () => {
        const { withContext } = require('../logger');
        expect(() => withContext({})).not.toThrow();
    });

    test('multiple calls produce independent children', () => {
        const { withContext } = require('../logger');
        const a = withContext({ module: 'auth' });
        const b = withContext({ module: 'ingest' });
        expect(a).not.toBe(b);
    });
});

describe('logger — env var configuration', () => {
    test('loads cleanly with SERVICE_NAME set', () => {
        process.env.SERVICE_NAME = 'test-svc';
        expect(() => require('../logger')).not.toThrow();
    });

    test('loads cleanly without SERVICE_NAME (defaults to social-api)', () => {
        delete process.env.SERVICE_NAME;
        expect(() => require('../logger')).not.toThrow();
    });

    test('loads cleanly with LOG_LEVEL=debug', () => {
        process.env.LOG_LEVEL = 'debug';
        expect(() => require('../logger')).not.toThrow();
    });
});
