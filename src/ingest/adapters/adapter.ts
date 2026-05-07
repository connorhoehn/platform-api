import { SourceAdapter, IngestSource } from '../types';
import { MockLocalAdapter } from './mock-local';
import { MockWebhookAdapter } from './mock-webhook';
import { MockDriveAdapter } from './mock-drive';
import { S3Adapter } from './s3';
import { GoogleDriveAdapter } from './google-drive';
import { ConfluenceSourceAdapter } from './confluence';
import { JiraSourceAdapter } from './jira';

export function createAdapter(source: IngestSource): SourceAdapter {
  switch (source.type) {
    case 'local':
      return new MockLocalAdapter();
    case 'webhook':
      return new MockWebhookAdapter();
    case 's3':
      return new S3Adapter();
    case 'google-drive':
      return new GoogleDriveAdapter();
    case 'confluence':
      return new ConfluenceSourceAdapter();
    case 'jira':
      return new JiraSourceAdapter();
    default:
      throw new Error(`Unsupported source type: ${source.type}`);
  }
}

export { SourceAdapter } from '../types';
export { MockLocalAdapter } from './mock-local';
export { MockWebhookAdapter } from './mock-webhook';
export { MockDriveAdapter, mockDriveAdapter } from './mock-drive';
export { S3Adapter } from './s3';
export { GoogleDriveAdapter } from './google-drive';

// Bidirectional sync: writeback-types.ts is the canonical source for the
// WriteBack* contract (Agent A). Connector-specific drivers ship beside the
// adapter and re-export only their own surface; type re-exports for shared
// shapes go through writeback-types so future connectors don't drift.
export { GoogleDriveWriteBackDriver } from './google-drive-writeback';
export {
  ConfluenceSourceAdapter,
  ConfluenceWriteBackDriver,
} from './confluence';
export type { ConfluenceConfig } from './confluence';
export { JiraSourceAdapter, JiraWriteBackDriver } from './jira';
export type {
  WriteBackDriver,
  WriteBackChange,
  WriteBackResult,
  WriteBackAck,
  WriteBackConflict,
  WriteBackError,
  ResolvedChange,
} from './writeback-types';
