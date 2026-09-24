import { probeRunRetentionSeconds } from '../probeRunRetention';

describe('probeRunRetentionSeconds (realtime-examples NFR #226)', () => {
  const dev = { PIPELINE_PROBE_RUN_PREFIXES: 'audit-, nfr-probe', PIPELINE_PROBE_RUN_RETENTION_HOURS: '6' } as NodeJS.ProcessEnv;

  it('gives a probe pipeline the configured hours', () => {
    expect(probeRunRetentionSeconds('audit-rollup-probe', dev)).toBe(6 * 3600);
    expect(probeRunRetentionSeconds('nfr-probe-x', dev)).toBe(6 * 3600);
  });

  it('leaves every other pipeline to the normal retention', () => {
    expect(probeRunRetentionSeconds('deck-revise', dev)).toBeNull();
    expect(probeRunRetentionSeconds(undefined, dev)).toBeNull();
  });

  it('is off unless both settings are present (production)', () => {
    expect(probeRunRetentionSeconds('audit-rollup-probe', {} as NodeJS.ProcessEnv)).toBeNull();
    expect(probeRunRetentionSeconds('audit-rollup-probe', { PIPELINE_PROBE_RUN_PREFIXES: 'audit-' } as NodeJS.ProcessEnv)).toBeNull();
    expect(probeRunRetentionSeconds('audit-rollup-probe', { PIPELINE_PROBE_RUN_RETENTION_HOURS: '6' } as NodeJS.ProcessEnv)).toBeNull();
  });
});
