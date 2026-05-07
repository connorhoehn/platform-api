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
