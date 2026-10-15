/* global afterAll, require */
/* eslint-disable @typescript-eslint/no-require-imports */
// Per-test-file teardown (lane P1, 2026-10-14).
//
// Many route tests reach the real shared Redis client (`getRedisClient`)
// through code they do not mock — a definitions write publishing its
// invalidation, a video session recording its ARN index. With no Redis that
// client keeps retrying `redis:6379` once a second for a few seconds, and it
// outlived the test file that made it: the worker then logged
// `getaddrinfo ENOTFOUND redis` through the following files. Closing the
// shared clients after every file (a no-op when none was made, and when the
// file mocked the module) ends that.
afterAll(async () => {
  let mod;
  try { mod = require('./src/lib/redis-client'); } catch { return; }
  if (mod && typeof mod.closeRedisClients === 'function') {
    await mod.closeRedisClients().catch(() => undefined);
  }
});
