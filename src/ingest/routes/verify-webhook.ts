// platform-api/src/ingest/routes/verify-webhook.ts
//
// Shared HMAC verification for ingest webhook handlers. Lifted from the
// inline block in ingestWebhooks.ts so Confluence + Jira variants apply
// the same posture without copy-paste drift.
//
// Posture (matches ingestWebhooks.ts):
//   - If `source.config.webhookSecret` is unset or empty → pass-through
//     (dev-friendly; sources opt in by configuring a secret).
//   - If configured → require a valid `x-webhook-signature` header that
//     matches HMAC-SHA256(secret, JSON.stringify(req.body)).
//   - Mismatch → 403 with `{ error: 'invalid webhook signature' }`.
//
// Note: signs over the re-serialized JSON, not raw body bytes. This matches
// the pre-existing ingestWebhooks.ts convention; switching to raw-body
// signing is a separate, larger change (would require a router-scoped raw
// parser per lvsWebhooks.ts:87).

import { verifySignature } from 'distributed-core';
import type { Request } from 'express';
import type { IngestSource } from '../types';

export interface VerifyWebhookResult {
  ok: boolean;
  /** Populated when `ok` is false; suitable for use as the JSON response error string. */
  reason?: string;
}

/**
 * Verify the inbound webhook signature against the source's configured
 * webhookSecret. Returns `{ ok: true }` when the source has no secret
 * configured (parity with ingestWebhooks.ts dev posture), when the signature
 * matches, or otherwise `{ ok: false, reason }`.
 */
export function verifyWebhookSignature(
  req: Request,
  source: IngestSource,
): VerifyWebhookResult {
  const config = source.config as Record<string, unknown>;
  const webhookSecret = config.webhookSecret;
  if (typeof webhookSecret !== 'string' || webhookSecret.length === 0) {
    return { ok: true };
  }
  const bodyString = JSON.stringify(req.body);
  const signature = req.headers['x-webhook-signature'] as string | undefined;
  if (!verifySignature(webhookSecret, Buffer.from(bodyString, 'utf8'), signature)) {
    return { ok: false, reason: 'invalid webhook signature' };
  }
  return { ok: true };
}
