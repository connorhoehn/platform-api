// social-api/src/pipeline/config/walPreflight.ts
//
// Fail-fast writability check for WAL parent directories. Run BEFORE
// Cluster.create() so operators get a clear, actionable error instead of an
// opaque crash from deep inside cluster setup.

import * as fs from 'fs';
import * as path from 'path';

/**
 * Verify the parent directory of `walFilePath` is writable. Throws a clear
 * error message that names the env var to set if the check fails.
 *
 * `envVarName` is included in the error so operators know exactly which knob
 * to flip; `disableHint` is appended only when the path supports a 'disabled'
 * magic value (PIPELINE_WAL_PATH does, PIPELINE_REGISTRY_WAL_PATH does not).
 */
export function verifyWalParentWritable(
  walFilePath: string,
  envVarName: string,
  disableHint: boolean,
): void {
  const parentDir = path.dirname(walFilePath);
  try {
    fs.accessSync(parentDir, fs.constants.W_OK);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const tail = disableHint
      ? `, or set ${envVarName}=disabled to opt out`
      : '';
    throw new Error(
      `[pipeline] ${envVarName} (${walFilePath}) is not writable: ${message}. `
      + `Fix the filesystem permission, set ${envVarName} to a writable path${tail}.`,
    );
  }
}
