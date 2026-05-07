import {
  SourceAdapter,
  SourceConfig,
  SyncCursor,
  FileDescriptor,
  FileContent,
  AdapterCapabilities,
} from '../types';

export class MockWebhookAdapter implements SourceAdapter {
  static readonly capabilities: AdapterCapabilities = {
    readonly: true,
    supportsWriteBack: false,
    requiresOauth: false,
    supportsWebhook: true,
  };

  private buffer: FileDescriptor[] = [];
  private contentMap: Map<string, FileContent> = new Map();

  receive(file: FileDescriptor, content: FileContent): void {
    this.buffer.push(file);
    this.contentMap.set(file.path, content);
  }

  async *list(_config: SourceConfig, _cursor: SyncCursor): AsyncGenerator<FileDescriptor> {
    const files = [...this.buffer];
    this.buffer = [];

    for (const file of files) {
      yield file;
    }
  }

  async fetch(_config: SourceConfig, file: FileDescriptor): Promise<FileContent> {
    const content = this.contentMap.get(file.path);
    if (!content) {
      throw new Error(`No content found for file: ${file.path}`);
    }
    this.contentMap.delete(file.path);
    return content;
  }
}

// Singleton instance shared between webhook route and ingest engine
export const mockWebhookAdapter = new MockWebhookAdapter();
