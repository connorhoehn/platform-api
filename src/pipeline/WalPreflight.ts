import { verifyWalParentWritable } from './config/walPreflight';
import type { PipelineRegistryMode } from './config/cluster';

export interface WalPreflightParams {
  walFilePath: string | undefined;
  registryWalFilePath: string | undefined;
  registryMode: PipelineRegistryMode;
  raftDataDir: string | undefined;
}

export async function runWalPreflight(params: WalPreflightParams): Promise<void> {
  const { walFilePath, registryWalFilePath, registryMode, raftDataDir } = params;

  if (walFilePath) {
    verifyWalParentWritable(walFilePath, 'PIPELINE_WAL_PATH', true);
  }
  if (registryWalFilePath) {
    verifyWalParentWritable(registryWalFilePath, 'PIPELINE_REGISTRY_WAL_PATH', false);
  }
  if (registryMode === 'raft' && raftDataDir) {
    const fs = await import('fs');
    try {
      fs.mkdirSync(raftDataDir, { recursive: true });
      fs.accessSync(raftDataDir, fs.constants.W_OK);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new Error(
        `[pipeline] PIPELINE_RAFT_DATA_DIR (${raftDataDir}) is not writable: ${message}. `
        + `Fix the filesystem permission or set PIPELINE_RAFT_DATA_DIR to a writable path.`,
      );
    }
  }
}

/**
 * NFR #55: what a restart of this process recovers from, said once at boot.
 *
 * The pipeline EventBus WAL is node-local and lives in the container
 * filesystem (`/tmp/pipeline-wal.log` in dev). It survives a process restart
 * inside the pod (Tilt live_update, the in-container supervisor) but not a pod
 * replacement, and it only restores the bus version counter and per-run
 * `getHistory` replay. In-flight runs are recovered from the DynamoDB run
 * checkpoint by the cluster sweeper (orphan-scan + conditional resume claim),
 * on whichever replica holds the sweep lease — which a per-pod volume could
 * not give another replica anyway. Read BEFORE the bus opens the WAL.
 */
export function describePipelineRecovery(
  walFilePath: string | undefined,
  statSize: (path: string) => number | undefined = defaultStatSize,
): { walState: 'disabled' | 'fresh' | 'carried'; walBytes: number; message: string } {
  const runs = 'in-flight runs resume from the DynamoDB run checkpoint (cluster sweep: orphan-scan + resume claim)';
  if (!walFilePath) {
    return { walState: 'disabled', walBytes: 0, message: `pipeline recovery: no bus WAL; ${runs}; run history before this boot is not replayable` };
  }
  const bytes = statSize(walFilePath) ?? 0;
  if (bytes === 0) {
    return {
      walState: 'fresh',
      walBytes: 0,
      message: `pipeline recovery: fresh bus WAL at ${walFilePath} (new pod, or first boot); ${runs}; bus history from before this boot is gone`,
    };
  }
  return {
    walState: 'carried',
    walBytes: bytes,
    message: `pipeline recovery: bus WAL at ${walFilePath} carried over (${bytes} bytes, same pod) for version counter + history; ${runs}`,
  };
}

function defaultStatSize(path: string): number | undefined {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const fs = require('fs') as typeof import('fs');
  try { return fs.statSync(path).size; } catch { return undefined; }
}
