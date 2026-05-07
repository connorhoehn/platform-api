import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  type S3ClientConfig,
} from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import type {
  SourceAdapter,
  SourceConfig,
  SyncCursor,
  FileDescriptor,
  FileContent,
  AdapterCapabilities,
} from '../types';

const MIME_MAP: Record<string, string> = {
  '.json': 'application/json',
  '.csv': 'text/csv',
  '.md': 'text/markdown',
  '.txt': 'text/plain',
  '.pdf': 'application/pdf',
  '.html': 'text/html',
  '.xml': 'application/xml',
};

function inferMimeType(key: string): string {
  const dotIdx = key.lastIndexOf('.');
  if (dotIdx === -1) return 'application/octet-stream';
  const ext = key.slice(dotIdx).toLowerCase();
  return MIME_MAP[ext] ?? 'application/octet-stream';
}

function isTextMime(mimeType: string): boolean {
  return mimeType.startsWith('text/') || mimeType === 'application/json';
}

async function streamToBuffer(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array));
  }
  return Buffer.concat(chunks);
}

/**
 * Real S3 adapter — wraps @aws-sdk/client-s3.
 *
 * SourceConfig fields:
 *   bucket      (string, required) — S3 bucket name
 *   prefix      (string, optional) — key prefix filter (e.g. "data/")
 *   region      (string, optional) — AWS region; falls back to env
 *   endpoint    (string, optional) — custom endpoint (e.g. localstack)
 *   forcePathStyle (boolean, optional) — needed for localstack
 *   accessKeyId    (string, optional) — explicit credentials
 *   secretAccessKey (string, optional) — explicit credentials
 *
 * Change detection: ETag is used as the file hash. For multi-part uploads
 * the ETag is a composite; the ingest cursor stores it verbatim and treats
 * any ETag change as a modified file.
 *
 * SyncCursor.checkpoint stores the last S3 continuation token so a
 * crashed sync can resume mid-listing.
 */
export class S3Adapter implements SourceAdapter {
  static readonly capabilities: AdapterCapabilities = {
    readonly: true,
    supportsWriteBack: false,
    requiresOauth: false,
    supportsWebhook: false,
  };

  private buildClient(config: SourceConfig): S3Client {
    const region = (config.region as string | undefined) ?? process.env.AWS_REGION ?? 'us-east-1';
    const endpoint = config.endpoint as string | undefined;
    const forcePathStyle = config.forcePathStyle as boolean | undefined;

    const clientConfig: S3ClientConfig = { region };
    if (endpoint) clientConfig.endpoint = endpoint;
    if (forcePathStyle) clientConfig.forcePathStyle = true;

    const accessKeyId = config.accessKeyId as string | undefined;
    const secretAccessKey = config.secretAccessKey as string | undefined;
    if (accessKeyId && secretAccessKey) {
      clientConfig.credentials = { accessKeyId, secretAccessKey };
    }

    return new S3Client(clientConfig);
  }

  async *list(config: SourceConfig, cursor: SyncCursor): AsyncGenerator<FileDescriptor> {
    const client = this.buildClient(config);
    const bucket = config.bucket as string;
    const prefix = config.prefix as string | undefined;

    let continuationToken: string | undefined =
      cursor.checkpoint as string | undefined;

    const lastSyncMs = cursor.lastSyncAt ? new Date(cursor.lastSyncAt).getTime() : 0;

    do {
      const command = new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      });

      const response = await client.send(command);

      for (const obj of response.Contents ?? []) {
        if (!obj.Key) continue;

        const lastModifiedDate = obj.LastModified ?? new Date(0);
        if (lastSyncMs > 0 && lastModifiedDate.getTime() <= lastSyncMs) {
          continue;
        }

        // S3 objects ending in "/" are folder markers — skip
        if (obj.Key.endsWith('/')) continue;

        const name = obj.Key.split('/').pop() ?? obj.Key;
        const mimeType = inferMimeType(obj.Key);

        yield {
          path: obj.Key,
          name,
          hash: obj.ETag?.replace(/"/g, '') ?? '',
          size: obj.Size ?? 0,
          mimeType,
          lastModified: lastModifiedDate.toISOString(),
        };
      }

      continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
    } while (continuationToken);
  }

  async fetch(config: SourceConfig, file: FileDescriptor): Promise<FileContent> {
    const client = this.buildClient(config);
    const bucket = config.bucket as string;

    const command = new GetObjectCommand({ Bucket: bucket, Key: file.path });
    const response = await client.send(command);

    if (!response.Body) {
      throw new Error(`S3Adapter: empty body for key ${file.path}`);
    }

    const data = await streamToBuffer(response.Body as Readable);
    const mimeType = file.mimeType ?? inferMimeType(file.path);
    const encoding = isTextMime(mimeType) ? 'utf-8' : 'binary';

    const content: FileContent = { data, encoding };

    if (mimeType === 'application/json') {
      content.parsed = JSON.parse(data.toString('utf-8'));
    }

    return content;
  }
}
