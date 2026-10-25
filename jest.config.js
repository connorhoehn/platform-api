// Jest config for platform-api unit tests.
//
// platform-api is a standalone repo (extracted from websocket-gateway/social-api);
// jest + ts-jest are installed in its own devDependencies.

const path = require('path');

// Worker concurrency is bounded by default to avoid thrashing dev machines.
// ts-jest workers can each balloon past 1GB while compiling our 200+ test
// files, and jest's default of `numCpus - 1` workers (15 on a 16-core box)
// has previously OOM'd the host. Override with JEST_MAX_WORKERS=N when you
// need more parallelism (e.g. CI with dedicated RAM).
const maxWorkers = process.env.JEST_MAX_WORKERS || '25%';

module.exports = {
  rootDir: __dirname,
  roots: ['<rootDir>/src', '<rootDir>/test'],
  testMatch: [
    '<rootDir>/src/**/__tests__/**/*.test.ts',
    '<rootDir>/src/**/*.test.ts',
    '<rootDir>/test/**/*.test.ts',
  ],
  testEnvironment: 'node',
  maxWorkers,
  // Recycle a worker once its heap crosses this threshold. Stops ts-jest
  // workers from accumulating compiler caches across hundreds of test files
  // until the host swaps.
  workerIdleMemoryLimit: '512MB',
  // Per-worker setup. Sets the env vars that distributed-core v0.6.3's
  // IS_TEST_ENV check + our PIPELINE_TEST_FAST_MODE flag read at
  // bootstrap time so the cluster comes up with fast timers + log
  // suppression by default.
  setupFiles: ['<rootDir>/jest.setup.js'],
  // Closes the shared Redis clients after each file (see the file header).
  setupFilesAfterEnv: ['<rootDir>/jest.after-env.js'],
  transform: {
    '^.+\\.tsx?$': [
      'ts-jest',
      {
        // Transpile each test independently; build/typecheck retain full typing.
        // Disabling diagnostics alone still creates a full TypeScript program
        // in every recycled worker, so use a test-only isolatedModules config.
        // Keep this Jest runner's existing CommonJS/dynamic-import mock behavior;
        // production continues to compile with the unchanged Node16 config.
        diagnostics: false,
        tsconfig: path.resolve(__dirname, 'tsconfig.test.json'),
      },
    ],
  },
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json'],
  // Map ESM-only local packages so jest's CJS resolver can find them.
  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1',
    '^@connorhoehn/event-catalog$': '<rootDir>/node_modules/@connorhoehn/event-catalog/dist/index.js',
  },
};
