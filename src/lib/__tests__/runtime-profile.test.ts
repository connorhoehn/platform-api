import { makeHealthHandler } from '@connorhoehn/service-runtime';
import {
  RuntimeReadiness,
  resolveProfileConfig,
  validateProfile,
} from '../runtime-profile';

const durableEnv = {
  PLATFORM_RUNTIME_PROFILE: 'durable',
  PLATFORM_DATASTORE: 'dynamodb-local',
  DYNAMODB_ENDPOINT: 'http://127.0.0.1:8000',
  MESSAGE_QUEUE_URL: 'http://mq:3004',
  REDIS_ENDPOINT: 'redis',
};

async function health(readiness: RuntimeReadiness) {
  const handler = makeHealthHandler({ service: 'platform-api', version: 'test', checks: readiness.healthChecks() });
  let code = 0;
  let body: any; // eslint-disable-line @typescript-eslint/no-explicit-any
  const res = {
    status(c: number) { code = c; return this; },
    json(b: unknown) { body = b; return this; },
    set() { return this; },
    setHeader() { return this; },
  };
  await handler({} as never, res as never);
  return { code, body };
}

describe('static runtime profile', () => {
  it('defaults to local-ephemeral with nothing mandatory and no required providers', () => {
    const config = resolveProfileConfig({});
    expect(config.profile).toBe('local-ephemeral');
    expect(config.mandatory.size).toBe(0);
    expect(config.datastore).toBe('aws-dynamodb');
    expect(validateProfile(config)).toMatchObject({ ok: true, problems: [] });
  });

  it('accepts a fully declared durable profile and makes every enabled feature mandatory', () => {
    const config = resolveProfileConfig({ ...durableEnv, ACTIVE_NOW_WORK_GRAPH_ENABLED: 'true' });
    expect(validateProfile(config).ok).toBe(true);
    expect([...config.mandatory].sort()).toEqual(
      ['durable-consumers', 'event-catalog', 'outbox', 'pipelines', 'scheduler', 'work-graph'],
    );
  });

  it.each([
    ['no queue URL', { MESSAGE_QUEUE_URL: undefined }, /message-queue.*MESSAGE_QUEUE_URL/],
    ['no Redis endpoint', { REDIS_ENDPOINT: undefined }, /redis.*REDIS_ENDPOINT/],
    ['an undeclared endpoint override', { PLATFORM_DATASTORE: undefined }, /datastore.*PLATFORM_DATASTORE/],
    ['a store without the transaction contract', { PLATFORM_DATASTORE: 'managed-nosql-store' }, /"conditional-write" from datastore/],
    ['in-memory pipeline runs', { PIPELINE_RUN_PERSISTENCE_ENABLED: 'false' }, /"persistent" from pipeline-run-store/],
    ['an unknown profile', { PLATFORM_RUNTIME_PROFILE: 'durable-ish' }, /PLATFORM_RUNTIME_PROFILE/],
    ['an unknown datastore', { PLATFORM_DATASTORE: 'sqlite' }, /PLATFORM_DATASTORE must be one of/],
  ])('refuses the durable profile with %s', (_label, override, problem) => {
    const report = validateProfile(resolveProfileConfig({ ...durableEnv, ...override }));
    expect(report.ok).toBe(false);
    expect(report.problems.join('\n')).toMatch(problem);
  });

  it('tolerates the same gaps in the local-ephemeral profile', () => {
    const config = resolveProfileConfig({ DYNAMODB_ENDPOINT: 'http://localhost:8000', PIPELINE_RUN_PERSISTENCE_ENABLED: 'false' });
    expect(config.datastore).toBeUndefined();
    expect(validateProfile(config).ok).toBe(true);
  });
});

describe('live feature readiness', () => {
  it('fails health while a mandatory feature is starting, then passes once all are ready', async () => {
    const readiness = new RuntimeReadiness(resolveProfileConfig(durableEnv));
    let result = await health(readiness);
    expect(result.code).toBe(503);
    expect(result.body.checks.find((c: any) => c.name === 'feature:pipelines')).toMatchObject({ ok: false });
    for (const feature of readiness.config.features) readiness.ready(feature);
    result = await health(readiness);
    expect(result.code).toBe(200);
    expect(result.body.status).toBe('healthy');
  });

  it('reports a null pipeline bootstrap as unavailable: 503 when mandatory, degraded when optional', async () => {
    for (const [env, code, status] of [[durableEnv, 503, 'unhealthy'], [{}, 200, 'degraded']] as const) {
      const readiness = new RuntimeReadiness(resolveProfileConfig(env));
      for (const feature of readiness.config.features) readiness.ready(feature);
      readiness.set('pipelines', 'failed', 'pipeline bridge unavailable (mode in-process)');
      const result = await health(readiness);
      expect(result.code).toBe(code);
      expect(result.body.status).toBe(status);
      const check = result.body.checks.find((c: any) => c.name === 'feature:pipelines');
      expect(check.details ?? check.error).toMatch(/pipeline bridge unavailable/);
      // Unrelated features stay healthy.
      expect(result.body.checks.find((c: any) => c.name === 'feature:outbox')).toMatchObject({ ok: true });
    }
  });

  it('does not degrade the default profile while optional features are still starting', async () => {
    const readiness = new RuntimeReadiness(resolveProfileConfig({}));
    const result = await health(readiness);
    expect(result).toMatchObject({ code: 200, body: { status: 'healthy' } });
  });

  it('consults the live probe of a ready feature, so losing the broker fails a mandatory catalog', async () => {
    const readiness = new RuntimeReadiness(resolveProfileConfig(durableEnv));
    for (const feature of readiness.config.features) readiness.ready(feature);
    let broker = 'connected';
    readiness.probe('event-catalog', async () => ({ ok: broker === 'connected', details: `message-queue ${broker}` }));
    expect((await health(readiness)).code).toBe(200);
    broker = 'disconnected';
    const lost = await health(readiness);
    expect(lost.code).toBe(503);
    expect(JSON.stringify(lost.body)).toMatch(/message-queue disconnected/);
    broker = 'connected';
    expect((await health(readiness)).code).toBe(200);
    readiness.probe('event-catalog', async () => { throw new Error('probe timeout'); });
    expect((await health(readiness)).code).toBe(503);
  });

  it('drops readiness on drain and ignores later transitions', async () => {
    const readiness = new RuntimeReadiness(resolveProfileConfig(durableEnv));
    for (const feature of readiness.config.features) readiness.ready(feature);
    readiness.beginDrain();
    readiness.ready('pipelines');
    expect(readiness.isDraining()).toBe(true);
    expect(readiness.snapshot().pipelines).toMatchObject({ state: 'draining', mandatory: true });
    expect((await health(readiness)).code).toBe(503);
  });
});
