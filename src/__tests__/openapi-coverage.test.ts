// OpenAPI path-coverage drift guard.
//
// Re-runs the lint script (scripts/openapi-coverage-lint.js) and asserts
// every Express route under documents/social/video has a path entry in
// the corresponding openapi/<prefix>.yaml. New routes added without a
// YAML entry fail this test, which is the bootstrap safety net under
// hub#1009 option B (skeleton coverage now, schemas in follow-up tasks).

import { spawnSync } from 'node:child_process';
import path from 'node:path';

const SCRIPT = path.resolve(__dirname, '../../scripts/openapi-coverage-lint.js');

describe('openapi path coverage', () => {
  it('lint exits 0 for all configured prefixes', () => {
    const r = spawnSync(process.execPath, [SCRIPT, '--json'], { encoding: 'utf8' });
    expect(r.error).toBeUndefined();
    const report = JSON.parse(r.stdout || '{}');
    const drift: string[] = [];
    for (const [prefix, info] of Object.entries(report as Record<string, { ok: boolean; missing: { method: string; path: string }[] }>)) {
      if (info.ok) continue;
      for (const m of info.missing) {
        drift.push(`${prefix}: ${m.method} ${m.path}`);
      }
    }
    if (drift.length > 0) {
      throw new Error(
        `openapi path-coverage drift detected — undocumented routes:\n` +
          drift.map((d) => '  - ' + d).join('\n') +
          `\n\nRegenerate via: node scripts/openapi-coverage-lint.js --emit <prefix> > openapi/<prefix>.yaml`,
      );
    }
    expect(r.status).toBe(0);
  });
});
