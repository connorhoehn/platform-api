// realtime-examples NFR #226 (Loop 38): dev load probes (`audit-rollup-probe`
// and friends, ~2.2k runs a day) kept their run rows 30 days and their audit
// rows 30 days, like any user's run — the next few hundred MB of a DynamoDB
// Local file that never shrinks. This is config, not a constant: a run whose
// pipelineId starts with one of PIPELINE_PROBE_RUN_PREFIXES (comma-separated)
// keeps its run row and audit rows PIPELINE_PROBE_RUN_RETENTION_HOURS. Both
// unset (production) → null, and the caller's normal retention applies.

export function probeRunRetentionSeconds(
  pipelineId: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): number | null {
  if (!pipelineId) return null;
  const hours = Number(env.PIPELINE_PROBE_RUN_RETENTION_HOURS);
  if (!Number.isFinite(hours) || hours <= 0) return null;
  const prefixes = (env.PIPELINE_PROBE_RUN_PREFIXES ?? '')
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean);
  if (!prefixes.some((p) => pipelineId.startsWith(p))) return null;
  return Math.round(hours * 3600);
}
