// social-api/jest.setup.js
//
// Per-worker setup referenced from jest.config.js — mirrors the root-level
// test/jest.setup.js. Sets env vars that distributed-core's IS_TEST_ENV
// detection + PIPELINE_TEST_FAST_MODE read at bootstrap time.

if (!process.env.NODE_ENV) {
  process.env.NODE_ENV = 'test';
}

if (process.env.PIPELINE_TEST_FAST_MODE == null) {
  process.env.PIPELINE_TEST_FAST_MODE = 'true';
}
