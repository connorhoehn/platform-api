import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  checkBuildFreshness,
  resetBuildFreshnessForTests,
  buildFreshnessReadiness,
  STALE_NOT_READY_CAP_MS,
  sourceFingerprint,
  whenBuildFresh,
  type BuildFreshness,
} from '../buildFreshness';

describe('build freshness gate (NFR #65)', () => {
  let root: string;
  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'build-fresh-'));
    mkdirSync(join(root, 'src', 'lib', '__tests__'), { recursive: true });
    writeFileSync(join(root, 'src', 'index.ts'), 'export const a = 1;\n');
    writeFileSync(join(root, 'src', 'lib', 'x.ts'), 'export const x = 1;\n');
    resetBuildFreshnessForTests();
  });
  afterEach(() => rmSync(root, { recursive: true, force: true }));

  it('ignores what Tilt does not sync: tests, __tests__, dotfiles', () => {
    const before = sourceFingerprint(join(root, 'src'));
    writeFileSync(join(root, 'src', 'lib', '__tests__', 'x.test.ts'), 'it();');
    writeFileSync(join(root, 'src', 'lib', 'y.test.ts'), 'it();');
    writeFileSync(join(root, 'src', '.DS_Store'), 'junk');
    expect(sourceFingerprint(join(root, 'src'))).toBe(before);
    writeFileSync(join(root, 'src', 'lib', 'x.ts'), 'export const x = 2;\n');
    expect(sourceFingerprint(join(root, 'src'))).not.toBe(before);
  });

  it('is fresh when dist was built from the host source, stale after a host edit, unchecked without a mount', () => {
    const file = join(root, '.build-src-fingerprint');
    writeFileSync(file, `${sourceFingerprint(join(root, 'src'))}\n`);
    expect(checkBuildFreshness({ hostSrcDir: join(root, 'src'), fingerprintFile: file }).state).toBe('fresh');
    writeFileSync(join(root, 'src', 'lib', 'x.ts'), 'export const x = 3;\n');
    expect(checkBuildFreshness({ hostSrcDir: join(root, 'src'), fingerprintFile: file }).state).toBe('stale');
    expect(checkBuildFreshness({ hostSrcDir: join(root, 'nope'), fingerprintFile: file }).state).toBe('unchecked');
    expect(checkBuildFreshness({ hostSrcDir: join(root, 'src'), fingerprintFile: join(root, 'missing') }))
      .toMatchObject({ state: 'stale', built: 'missing' });
  });

  it('judges the process by the build it loaded, not by a fingerprint rewritten under it', () => {
    const file = join(root, '.build-src-fingerprint');
    const loaded = 'image-build-fingerprint';
    // The live_update replay writes the new fingerprint before it restarts the process.
    writeFileSync(file, `${sourceFingerprint(join(root, 'src'))}\n`);
    expect(checkBuildFreshness({ hostSrcDir: join(root, 'src'), builtFingerprint: loaded }).state).toBe('stale');
  });

  it('holds lease holders while stale, logs loudly, releases them once the rebuild lands', async () => {
    const results: BuildFreshness[] = [
      { state: 'stale', built: 'old', host: 'new' },
      { state: 'stale', built: 'old', host: 'new' },
      { state: 'fresh', fingerprint: 'new' },
    ];
    const errors: string[] = [];
    const log = { info: jest.fn(), error: (_o: object, msg: string) => { errors.push(msg); } };
    let released = false;
    const gate = whenBuildFresh(log, { pollMs: 5, logEveryMs: 0, check: () => results.shift() ?? { state: 'fresh', fingerprint: 'new' } });
    void gate.then(() => { released = true; });
    await new Promise((r) => setTimeout(r, 1));
    expect(released).toBe(false);
    await gate;
    expect(released).toBe(true);
    expect(errors.length).toBeGreaterThanOrEqual(2);
    expect(errors[0]).toContain('STALE BUILD');
    // Shared: a second holder does not re-run the check.
    expect(whenBuildFresh(log)).toBe(gate);
  });

  it('reports not ready while stale, for at most the cap, and ready once fresh (NFR #210)', async () => {
    const results: BuildFreshness[] = [{ state: 'stale', built: 'old', host: 'new' }];
    const log = { info: jest.fn(), error: jest.fn() };
    const gate = whenBuildFresh(log, { pollMs: 5, logEveryMs: 0, check: () => results.shift() ?? { state: 'fresh', fingerprint: 'new' } });
    expect(buildFreshnessReadiness().ok).toBe(false);
    expect(buildFreshnessReadiness(Date.now() + STALE_NOT_READY_CAP_MS).ok).toBe(true);
    await gate;
    expect(buildFreshnessReadiness().ok).toBe(true);
  });
});
