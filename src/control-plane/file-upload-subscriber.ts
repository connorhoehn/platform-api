// platform-api/src/control-plane/file-upload-subscriber.ts
//
// Stub subscriber for the FileUploadEndpoint CRD kind. Today this is a
// register-and-log handler: the operator's Phase-3 reconciler pushes a
// FileUploadEndpoint manifest on 'feature.file-upload.*' and the
// subscriber acks 'applied' without touching state. Real S3 bucket
// validation + FileUploadDefinition write lands in a follow-up turn
// (see CRD plan W1 #3 — "validate S3 bucket existence + record
// FileUploadDefinition for the upload-route to consume").
//
// The point of shipping the stub now: prove the
// ControlPlaneSubscriberRegistry shape works for multiple CRD kinds
// sharing one Redis connection (today's d0add83 was a single-kind
// proof). Adding a second registration via the same factory pattern is
// the surface that downstream subscribers (VideoHangout,
// CollaborativeDocument) will follow.

import type { ReconcileAck } from 'distributed-core';
import type { RegisteredSubscriber } from './registry';
import { withContext } from '../lib/logger';

const log = withContext({ service: 'file-upload-subscriber' });

/** Shape the operator emits for a FileUploadEndpoint manifest. */
export interface FileUploadEndpointConfig {
  /** Stable id — matches the CRD metadata.name. */
  name: string;
  /** S3 bucket the upload-route should write to. */
  bucket: string;
  /** AWS region. */
  region: string;
  /** Optional prefix path inside the bucket. */
  prefix?: string;
  /** Max upload size in bytes; 0 = no app-level limit. */
  maxSizeBytes?: number;
  /** Optional allowed MIME types whitelist. */
  allowedMimeTypes?: string[];
}

const DEFAULT_CHANNEL = 'feature.file-upload.*';

export interface FileUploadRegistrationOptions {
  /** Channel pattern override. Defaults to 'feature.file-upload.*'. */
  channel?: string;
  /** Node id for ack attribution. Defaults to `${hostname}-${pid}`. */
  nodeId?: string;
}

export function createFileUploadEndpointRegistration(
  opts: FileUploadRegistrationOptions = {},
): RegisteredSubscriber<FileUploadEndpointConfig> {
  const channel = opts.channel ?? DEFAULT_CHANNEL;
  const nodeId = opts.nodeId
    ?? `${process.env.HOSTNAME ?? 'localhost'}-${process.pid}`;

  return {
    name: 'file-upload',
    channel,
    handler: async (payload): Promise<ReconcileAck> => {
      const { config, version, channel: ch } = payload;
      const at = new Date().toISOString();
      // Stub: log the apply and ack. S3 bucket validation +
      // FileUploadDefinition write come later — see header.
      log.info(
        { channel: ch, version, name: config.name, bucket: config.bucket, region: config.region },
        'file-upload-endpoint manifest received (stub apply)',
      );
      return {
        channel: ch,
        appliedVersion: version,
        appliedAt: at,
        subscriberNodeId: nodeId,
        status: 'applied',
      };
    },
  };
}
