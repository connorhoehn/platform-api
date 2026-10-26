/**
 * Runtime profile: which enabled features this process must be able to run,
 * the static guarantees their providers declare, and the live state of each
 * feature for readiness.
 *
 * Two profiles:
 * - `local-ephemeral` (default): every feature is optional. A failed catalog
 *   connection, pipeline bootstrap or consumer start reports `degraded` and
 *   the process keeps serving, which is the historical behaviour.
 * - `durable`: accepted work must not be silently dropped. Static provider
 *   declarations are validated before resources open (exit 2 on failure), and
 *   readiness fails while any mandatory feature is starting, failed or
 *   draining.
 *
 * The profile is chosen by `PLATFORM_RUNTIME_PROFILE`, never inferred from
 * NODE_ENV. Provider capabilities describe guarantees proven by adapter
 * contract tests; they are not probes and never replace authorization.
 */
import {
  evaluateCapabilities,
  type CapabilityIssue,
  type CapabilityProvider,
  type CapabilityRequirement,
} from '@connorhoehn/service-runtime/capabilities';
import type { HealthCheck, HealthCheckResult } from '@connorhoehn/service-runtime';

export type RuntimeProfile = 'local-ephemeral' | 'durable';
export type Datastore = 'aws-dynamodb' | 'dynamodb-local' | 'managed-nosql-store';

export type FeatureName =
  | 'outbox'
  | 'event-catalog'
  | 'durable-consumers'
  | 'pipelines'
  | 'scheduler'
  | 'work-graph';

type Env = Record<string, string | undefined>;

/**
 * Guarantees each datastore adapter has demonstrated against the platform's
 * conditional-write and transaction contract tests. managed-nosql-store
 * speaks the DynamoDB wire, but the URL alone does not prove those semantics,
 * so it declares nothing until it passes the same suite.
 */
export const DATASTORE_CAPABILITIES: Readonly<Record<Datastore, readonly string[]>> = {
  'aws-dynamodb': ['conditional-write', 'transactions', 'shared'],
  'dynamodb-local': ['conditional-write', 'transactions', 'shared'],
  'managed-nosql-store': [],
};

export interface ProfileConfig {
  profile: RuntimeProfile;
  datastore: Datastore | undefined;
  features: FeatureName[];
  mandatory: ReadonlySet<FeatureName>;
  providers: CapabilityProvider[];
  requirements: CapabilityRequirement[];
  /** Configuration errors found before capability evaluation (bad enum values). */
  configErrors: string[];
}

const trueish = (value: string | undefined, fallback: boolean): boolean =>
  value === undefined ? fallback : value.trim().toLowerCase() !== 'false';

export function resolveProfileConfig(env: Env = process.env): ProfileConfig {
  const configErrors: string[] = [];
  const rawProfile = env.PLATFORM_RUNTIME_PROFILE?.trim() || 'local-ephemeral';
  let profile: RuntimeProfile = 'local-ephemeral';
  if (rawProfile === 'durable' || rawProfile === 'local-ephemeral') profile = rawProfile;
  else configErrors.push(`PLATFORM_RUNTIME_PROFILE must be "local-ephemeral" or "durable", got "${rawProfile}"`);

  const rawStore = env.PLATFORM_DATASTORE?.trim();
  let datastore: Datastore | undefined;
  if (rawStore) {
    if (rawStore in DATASTORE_CAPABILITIES) datastore = rawStore as Datastore;
    else configErrors.push(`PLATFORM_DATASTORE must be one of ${Object.keys(DATASTORE_CAPABILITIES).join(', ')}, got "${rawStore}"`);
  } else if (!env.DYNAMODB_ENDPOINT?.trim()) {
    datastore = 'aws-dynamodb';
  }
  // An endpoint override with no declaration could be DynamoDB Local or a
  // DynamoDB-wire store with different semantics: leave it undeclared.

  const workGraph = env.ACTIVE_NOW_WORK_GRAPH_ENABLED === 'true';
  const runPersistence = trueish(env.PIPELINE_RUN_PERSISTENCE_ENABLED, true);
  const features: FeatureName[] = ['outbox', 'event-catalog', 'durable-consumers', 'pipelines', 'scheduler'];
  if (workGraph) features.push('work-graph');

  const providers: CapabilityProvider[] = [];
  if (datastore) providers.push({ id: 'datastore', capabilities: DATASTORE_CAPABILITIES[datastore] });
  if (env.MESSAGE_QUEUE_URL?.trim()) providers.push({ id: 'message-queue', capabilities: ['durable-delivery'] });
  if (env.REDIS_ENDPOINT?.trim()) providers.push({ id: 'redis', capabilities: ['pubsub', 'lease'] });
  providers.push({ id: 'pipeline-run-store', capabilities: runPersistence ? ['persistent'] : [] });

  const requirements: CapabilityRequirement[] = [];
  if (profile === 'durable') {
    requirements.push(
      { feature: 'outbox', provider: 'datastore', capabilities: ['conditional-write', 'transactions'] },
      { feature: 'event-catalog', provider: 'message-queue', capabilities: ['durable-delivery'] },
      { feature: 'event-catalog', provider: 'redis', capabilities: ['pubsub'] },
      { feature: 'pipelines', provider: 'datastore', capabilities: ['conditional-write'] },
      { feature: 'pipelines', provider: 'pipeline-run-store', capabilities: ['persistent'] },
      { feature: 'scheduler', provider: 'redis', capabilities: ['lease'] },
    );
    if (workGraph)
      requirements.push(
        { feature: 'work-graph', provider: 'datastore', capabilities: ['conditional-write', 'transactions'] },
        { feature: 'work-graph', provider: 'message-queue', capabilities: ['durable-delivery'] },
      );
  }
  const mandatory = new Set<FeatureName>(profile === 'durable' ? features : []);
  return { profile, datastore, features, mandatory, providers, requirements, configErrors };
}

export interface StaticReport {
  ok: boolean;
  problems: string[];
  issues: readonly CapabilityIssue[];
}

/** Validate declarations before any socket, table or timer opens. */
export function validateProfile(config: ProfileConfig): StaticReport {
  const report = evaluateCapabilities(config.providers, config.requirements);
  const problems = [...config.configErrors];
  for (const issue of report.issues) {
    if (issue.code === 'missing-provider')
      problems.push(`${issue.feature} needs provider "${issue.provider}": ${providerHint(issue.provider)}`);
    else if (issue.code === 'missing-capability')
      problems.push(`${issue.feature} needs "${issue.capability}" from ${issue.provider}: ${capabilityHint(issue.provider, issue.capability)}`);
    else problems.push(`${issue.path}: ${issue.code}`);
  }
  return { ok: problems.length === 0, problems, issues: report.issues };
}

function providerHint(provider: string | undefined): string {
  switch (provider) {
    case 'datastore':
      return 'set PLATFORM_DATASTORE (aws-dynamodb or dynamodb-local) when DYNAMODB_ENDPOINT overrides the AWS endpoint';
    case 'message-queue':
      return 'set MESSAGE_QUEUE_URL; the durable profile does not fall back to localhost';
    case 'redis':
      return 'set REDIS_ENDPOINT; the durable profile does not fall back to the "redis" hostname';
    default:
      return 'declare it';
  }
}

function capabilityHint(provider: string | undefined, capability: string | undefined): string {
  if (provider === 'pipeline-run-store') return 'PIPELINE_RUN_PERSISTENCE_ENABLED=false keeps runs in memory';
  if (provider === 'datastore') return `the declared datastore has not passed the ${capability} contract`;
  return 'not declared by the provider';
}

// ---------------------------------------------------------------------------
// Live feature state
// ---------------------------------------------------------------------------

export type FeatureState = 'starting' | 'ready' | 'failed' | 'disabled' | 'draining';

interface FeatureEntry {
  state: FeatureState;
  reason?: string;
  since: number;
}

export type LiveProbe = () => Promise<{ ok: boolean; details?: string }>;

export class RuntimeReadiness {
  private readonly entries = new Map<FeatureName, FeatureEntry>();
  private readonly probes = new Map<FeatureName, LiveProbe>();
  private draining = false;

  constructor(readonly config: ProfileConfig, private readonly now: () => number = Date.now) {
    for (const feature of config.features) this.entries.set(feature, { state: 'starting', since: now() });
  }

  set(feature: FeatureName, state: FeatureState, reason?: string): void {
    if (!this.entries.has(feature)) return;
    if (this.draining && state !== 'draining') return;
    this.entries.set(feature, { state, reason, since: this.now() });
  }

  ready(feature: FeatureName): void { this.set(feature, 'ready'); }
  fail(feature: FeatureName, error: unknown): void {
    this.set(feature, 'failed', error instanceof Error ? error.message : String(error));
  }

  /** A live dependency probe consulted while the feature reports ready. */
  probe(feature: FeatureName, check: LiveProbe): void { this.probes.set(feature, check); }

  /** Readiness drops for every feature the moment shutdown begins. */
  beginDrain(): void {
    this.draining = true;
    for (const [feature] of this.entries) this.entries.set(feature, { state: 'draining', since: this.now() });
  }

  isDraining(): boolean { return this.draining; }

  snapshot(): Record<string, { state: FeatureState; mandatory: boolean; reason?: string }> {
    const out: Record<string, { state: FeatureState; mandatory: boolean; reason?: string }> = {};
    for (const [feature, entry] of this.entries)
      out[feature] = { state: entry.state, mandatory: this.config.mandatory.has(feature), ...(entry.reason ? { reason: entry.reason } : {}) };
    return out;
  }

  /** One health check per feature, so the existing /health schema lists each by name. */
  healthChecks(): HealthCheck[] {
    return this.config.features.map((feature) => ({
      name: `feature:${feature}`,
      check: () => this.evaluate(feature),
    }));
  }

  async evaluate(feature: FeatureName): Promise<HealthCheckResult> {
    const entry = this.entries.get(feature)!;
    const mandatory = this.config.mandatory.has(feature);
    let ok = entry.state === 'ready' || entry.state === 'disabled';
    let details = entry.reason ? `${entry.state}: ${entry.reason}` : entry.state;
    if (entry.state === 'ready') {
      const probe = this.probes.get(feature);
      if (probe) {
        try {
          const live = await probe();
          if (!live.ok) { ok = false; details = `ready but dependency unavailable${live.details ? `: ${live.details}` : ''}`; }
        } catch (error) {
          ok = false;
          details = `ready but dependency probe failed: ${error instanceof Error ? error.message : String(error)}`;
        }
      }
    }
    if (ok) return { ok: true, details: `${details} (${mandatory ? 'mandatory' : 'optional'})` };
    if (mandatory) return { ok: false, details: `${details} (mandatory in ${this.config.profile} profile)` };
    // Optional features never pull the pod. One that is still starting is
    // not a fault; one that failed or lost its dependency reports degraded.
    if (entry.state === 'starting') return { ok: true, details: `${details} (optional)` };
    return { ok: true, degraded: true, details: `${details} (optional)` };
  }
}

export const runtimeProfile = resolveProfileConfig();
export const runtimeReadiness = new RuntimeReadiness(runtimeProfile);
