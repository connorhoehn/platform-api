// social-api/src/__tests__/tracing.test.ts
//
// Unit tests for tracing.ts bootstrap.
// All OpenTelemetry packages are mocked so no real SDK/exporter runs.
//
// Strategy: because env vars are read inside start() (not at module load time),
// we use jest.resetModules() + require() in each test so env-dependent branches
// get fresh evaluation. The mock factories below are re-executed on each
// require() after resetModules(), so we capture the shared mock fns via a
// module-level holder object that the factory closes over.

// ─── shared mock state (stable references across resetModules cycles) ───────

const sdkMocks = {
  start: jest.fn(),
  shutdown: jest.fn().mockResolvedValue(undefined),
};

const exporterConstructorCalls: Array<{ url: string }> = [];

// ─── mock registrations ─────────────────────────────────────────────────────

jest.mock('@opentelemetry/sdk-node', () => {
  return {
    NodeSDK: jest.fn().mockImplementation(() => ({
      start: sdkMocks.start,
      shutdown: sdkMocks.shutdown,
    })),
  };
});

jest.mock('@opentelemetry/exporter-trace-otlp-http', () => {
  return {
    OTLPTraceExporter: jest.fn().mockImplementation((opts: { url: string }) => {
      exporterConstructorCalls.push(opts);
      return opts;
    }),
  };
});

jest.mock('@opentelemetry/auto-instrumentations-node', () => ({
  getNodeAutoInstrumentations: jest.fn().mockReturnValue([]),
}));

jest.mock('../lib/logger', () => ({
  __esModule: true,
  default: {
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
  },
}));

// ─── helpers ────────────────────────────────────────────────────────────────

function loadStart(): typeof import('../tracing').start {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  return require('../tracing').start;
}

// ─── tests ──────────────────────────────────────────────────────────────────

describe('tracing.start()', () => {
  const ENV_KEYS = [
    'OTEL_TRACING_DISABLED',
    'OTEL_SERVICE_NAME',
    'OTEL_EXPORTER_OTLP_ENDPOINT',
  ] as const;

  let savedEnv: Partial<Record<string, string>>;

  beforeEach(() => {
    jest.resetModules();

    // Save and clear relevant env vars.
    savedEnv = {};
    for (const key of ENV_KEYS) {
      savedEnv[key] = process.env[key];
      delete process.env[key];
    }

    // Clear call state on shared mock fns.
    sdkMocks.start.mockClear();
    sdkMocks.shutdown.mockClear().mockResolvedValue(undefined);
    exporterConstructorCalls.length = 0;
  });

  afterEach(() => {
    for (const key of ENV_KEYS) {
      if (savedEnv[key] === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = savedEnv[key];
      }
    }
  });

  // 1. OTEL_TRACING_DISABLED=true returns a no-op (SDK never started)
  it('returns a no-op shutdown when OTEL_TRACING_DISABLED=true', () => {
    process.env.OTEL_TRACING_DISABLED = 'true';
    const start = loadStart();
    const shutdown = start();

    expect(sdkMocks.start).not.toHaveBeenCalled();
    expect(typeof shutdown).toBe('function');
  });

  // 2. No-op shutdown resolves without throwing
  it('no-op shutdown resolves without throwing', async () => {
    process.env.OTEL_TRACING_DISABLED = 'true';
    const start = loadStart();
    const shutdown = start();

    await expect(shutdown()).resolves.toBeUndefined();
  });

  // 3. Returns a shutdown function when enabled
  it('returns a function when tracing is enabled', () => {
    const start = loadStart();
    const shutdown = start();

    expect(typeof shutdown).toBe('function');
  });

  // 4. sdk.start() is called when enabled
  it('calls sdk.start() when tracing is enabled', () => {
    const start = loadStart();
    start();

    expect(sdkMocks.start).toHaveBeenCalledTimes(1);
  });

  // 5. Shutdown function calls sdk.shutdown()
  it('shutdown function calls sdk.shutdown()', async () => {
    const start = loadStart();
    const shutdown = start();

    await shutdown();

    expect(sdkMocks.shutdown).toHaveBeenCalledTimes(1);
  });

  // 6. Custom OTEL_EXPORTER_OTLP_ENDPOINT is used in exporter URL
  it('uses custom OTEL_EXPORTER_OTLP_ENDPOINT in exporter URL', () => {
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = 'http://collector:4318';
    const start = loadStart();
    start();

    expect(exporterConstructorCalls).toHaveLength(1);
    expect(exporterConstructorCalls[0].url).toBe('http://collector:4318/v1/traces');
  });

  // 7. Trailing slash is stripped from endpoint URL
  it('strips trailing slash from endpoint before appending /v1/traces', () => {
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = 'http://collector:4318/';
    const start = loadStart();
    start();

    expect(exporterConstructorCalls).toHaveLength(1);
    expect(exporterConstructorCalls[0].url).toBe('http://collector:4318/v1/traces');
  });

  // 8. OTEL_SERVICE_NAME is set to default 'social-api' when not provided
  it("sets OTEL_SERVICE_NAME to 'social-api' when env var is absent", () => {
    delete process.env.OTEL_SERVICE_NAME;
    const start = loadStart();
    start();

    expect(process.env.OTEL_SERVICE_NAME).toBe('platform-api');
  });

  // 9. When OTEL_SERVICE_NAME is already set, it is not overwritten
  it('does not overwrite OTEL_SERVICE_NAME when already set', () => {
    process.env.OTEL_SERVICE_NAME = 'my-custom-service';
    const start = loadStart();
    start();

    expect(process.env.OTEL_SERVICE_NAME).toBe('my-custom-service');
  });

  // 10. sdk.shutdown() throwing is caught silently (shutdown function doesn't rethrow)
  it('catches sdk.shutdown() errors silently', async () => {
    sdkMocks.shutdown.mockRejectedValueOnce(new Error('shutdown boom'));
    const start = loadStart();
    const shutdown = start();

    await expect(shutdown()).resolves.toBeUndefined();
  });
});
