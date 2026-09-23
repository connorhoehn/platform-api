/**
 * NFR #65 — never let a stale dev build take cluster leases.
 *
 * Under Tilt, platform-api is `live_update`d: source is synced into the RUNNING
 * container and recompiled there. A pod that is REPLACED (crash recovery, a
 * force-delete, a node restart) starts from the last BUILT image, which can be
 * hours older than the source, and runs that old code until Tilt replays the
 * sync ~35 s later. Measured in NFR #59: the replacement pod booted with the
 * old 30 s lease and 60 s sweep, won sweep leadership, and resumed runs.
 *
 * Every build records the fingerprint of the source its `dist/` was compiled
 * from (`.build-src-fingerprint`: the image build and each live_update write
 * it after tsc). When `PLATFORM_HOST_SRC_DIR` points at the host checkout
 * (Tilt mounts it read-only), boot compares the two. On a mismatch the process
 * logs loudly and holds off every cluster-wide lease (sweep leader, outbox
 * drainer) until the fingerprints agree — which, under Tilt, is the restart
 * the live_update replay ends with. Unset (prod, tests): always fresh.
 * Measured live in the NFR doc, row #65.
 *
 * The fingerprint covers the files Tilt syncs: `src/**` minus `__tests__/`
 * and `*.test.ts` (the Tiltfile's ignore list), dotfiles skipped.
 *
 * Holding only the cluster-wide singletons is deliberate: the replica's own
 * liveness lease still runs, so its HTTP-started runs are not orphaned.
 *
 * CLI (image build / live_update): `node dist/lib/buildFreshness.js <srcDir> <outFile>`.
 */
import { createHash } from 'node:crypto';
import { existsSync, readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { join, relative, sep } from 'node:path';

export const BUILD_FINGERPRINT_FILE = process.env.PLATFORM_BUILD_FINGERPRINT_FILE ?? '/app/.build-src-fingerprint';

function listSourceFiles(root: string, dir: string = root, out: string[] = []): string[] {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.name.startsWith('.')) continue;
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === '__tests__' || entry.name === 'node_modules') continue;
      listSourceFiles(root, full, out);
    } else if (entry.isFile() && !entry.name.endsWith('.test.ts')) {
      out.push(relative(root, full).split(sep).join('/'));
    }
  }
  return out;
}

/** sha1 over every synced source file's relative path and bytes, in path order. */
export function sourceFingerprint(srcDir: string): string {
  const hash = createHash('sha1');
  for (const file of listSourceFiles(srcDir).sort()) {
    hash.update(file);
    hash.update('\0');
    hash.update(readFileSync(join(srcDir, file)));
    hash.update('\0');
  }
  return hash.digest('hex');
}

export type BuildFreshness =
  | { state: 'unchecked'; reason: string }
  | { state: 'fresh'; fingerprint: string }
  | { state: 'stale'; built: string; host: string };

/** The fingerprint recorded next to dist/ right now ('missing' when absent). */
export function readBuiltFingerprint(file: string = BUILD_FINGERPRINT_FILE): string {
  return existsSync(file) ? readFileSync(file, 'utf8').trim() : 'missing';
}

export function checkBuildFreshness(opts: {
  hostSrcDir?: string;
  fingerprintFile?: string;
  /** What this PROCESS loaded; defaults to reading the file now. */
  builtFingerprint?: string;
} = {}): BuildFreshness {
  const hostSrcDir = opts.hostSrcDir ?? process.env.PLATFORM_HOST_SRC_DIR;
  if (!hostSrcDir) return { state: 'unchecked', reason: 'PLATFORM_HOST_SRC_DIR unset' };
  if (!existsSync(hostSrcDir) || !statSync(hostSrcDir).isDirectory()) {
    return { state: 'unchecked', reason: `host source ${hostSrcDir} not mounted` };
  }
  const built = opts.builtFingerprint ?? readBuiltFingerprint(opts.fingerprintFile);
  const host = sourceFingerprint(hostSrcDir);
  return built === host ? { state: 'fresh', fingerprint: host } : { state: 'stale', built, host };
}

interface FreshnessLogger {
  info(obj: object, msg: string): void;
  error(obj: object, msg: string): void;
}

let pending: Promise<void> | null = null;

/**
 * Resolves once this process runs the host's current source (at once when
 * unchecked or fresh). While stale it re-checks every `pollMs` and logs an
 * error every `logEveryMs`. Shared: every lease holder awaits the same check.
 */
export function whenBuildFresh(log: FreshnessLogger, opts: {
  pollMs?: number;
  logEveryMs?: number;
  check?: () => BuildFreshness;
} = {}): Promise<void> {
  if (pending) return pending;
  // Snapshot what this process loaded. The live_update replay rewrites the
  // file after tsc and only THEN restarts the process, so re-reading it would
  // let the stale process pass the gate in that window (seen live: ~7 s).
  const builtFingerprint = readBuiltFingerprint();
  const check = opts.check ?? (() => checkBuildFreshness({ builtFingerprint }));
  const pollMs = opts.pollMs ?? 2_000;
  const logEveryMs = opts.logEveryMs ?? 10_000;
  pending = new Promise<void>((resolve) => {
    const startedAt = Date.now();
    let lastLog = 0;
    const attempt = (): void => {
      let result: BuildFreshness;
      try {
        result = check();
      } catch (err) {
        result = { state: 'unchecked', reason: `check failed: ${String(err)}` };
      }
      if (result.state !== 'stale') {
        log.info({ ...result, waitedMs: Date.now() - startedAt }, 'build freshness: cluster leases enabled');
        resolve();
        return;
      }
      if (Date.now() - lastLog >= logEveryMs) {
        lastLog = Date.now();
        log.error(
          { built: result.built, host: result.host, waitedMs: Date.now() - startedAt },
          '##### STALE BUILD: dist/ was compiled from older source than the host checkout. '
            + 'Not taking cluster leases (sweep leader, outbox drainer) until Tilt resyncs and restarts this process, '
            + 'or run `tilt trigger wsg-realtime-examples-platform-api --port=11350`. #####',
        );
      }
      setTimeout(attempt, pollMs).unref?.();
    };
    attempt();
  });
  return pending;
}

/** Test hook: forget the shared check. */
export function resetBuildFreshnessForTests(): void {
  pending = null;
}

if (require.main === module) {
  const [srcDir, outFile] = process.argv.slice(2);
  if (!srcDir || !outFile) {
    console.error('usage: node dist/lib/buildFreshness.js <srcDir> <outFile>');
    process.exit(2);
  }
  const fingerprint = sourceFingerprint(srcDir);
  writeFileSync(outFile, `${fingerprint}\n`);
  console.log(`[build-freshness] ${outFile} = ${fingerprint}`);
}
