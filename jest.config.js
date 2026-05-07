// Jest config for social-api unit tests.
//
// Hoists transformer + paths so `cd social-api && npx jest <pattern>` works
// without needing jest installed in the social-api workspace — the root
// repo provides `jest` and `ts-jest` via its devDependencies. This config is
// scoped to social-api's own `src/` tree so it doesn't trip over the gateway
// tests at `<rootDir>/../test`.

const path = require('path');

module.exports = {
  rootDir: __dirname,
  roots: ['<rootDir>/src'],
  testMatch: [
    '<rootDir>/src/**/__tests__/**/*.test.ts',
    '<rootDir>/src/**/*.test.ts',
  ],
  testEnvironment: 'node',
  // Per-worker setup. Sets the env vars that distributed-core v0.6.3's
  // IS_TEST_ENV check + our PIPELINE_TEST_FAST_MODE flag read at
  // bootstrap time so the cluster comes up with fast timers + log
  // suppression by default.
  setupFiles: ['<rootDir>/jest.setup.js'],
  transform: {
    '^.+\\.tsx?$': [
      // ts-jest is hoisted to the workspace root.
      path.resolve(__dirname, '..', 'node_modules', 'ts-jest'),
      {
        // Keep type-checks fast: rely on `tsc --noEmit` for full typing.
        // ts-jest's per-file isolatedModules option moved into tsconfig in
        // v30, but we can't modify tsconfig.json (owned by app build), so
        // we disable diagnostics instead — same effect for test runs.
        diagnostics: false,
        tsconfig: path.resolve(__dirname, 'tsconfig.json'),
      },
    ],
  },
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json'],
};
