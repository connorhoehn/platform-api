// platform-api/src/ingest/routes/__tests__/verify-webhook.test.ts
//
// Unit tests for the shared verifyWebhookSignature helper. The helper is
// exercised end-to-end via ingestWebhooks.test.ts / ingestConfluenceWebhook
// .test.ts / ingestJiraWebhook.test.ts; this file pins the helper's
// contract directly so a downstream caller change can't silently shift it.

import * as crypto from 'crypto';
import type { Request } from 'express';
import { verifyWebhookSignature } from '../verify-webhook';
import type { IngestSource } from '../../types';

function makeSource(overrides: Partial<IngestSource> = {}): IngestSource {
  return {
    sourceId: 'src-test',
    name: 'Test',
    type: 'confluence',
    config: {},
    syncSchedule: null,
    mappingRules: [],
    defaultPipelineId: null,
    status: 'active',
    lastSyncAt: null,
    lastSyncStats: null,
    createdAt: '2026-01-01T00:00:00Z',
    updatedAt: '2026-01-01T00:00:00Z',
    ...overrides,
  };
}

const OMIT_TS = Symbol('omit-timestamp');

function makeReq(
  body: unknown,
  signature?: string,
  timestamp: string | typeof OMIT_TS = new Date().toISOString(),
): Request {
  const headers: Record<string, string> = { 'content-type': 'application/json' };
  if (signature !== undefined) headers['x-webhook-signature'] = signature;
  if (timestamp !== OMIT_TS) headers['x-webhook-timestamp'] = timestamp;
  return { body, headers } as unknown as Request;
}

function sign(secret: string, body: unknown): string {
  const payload = JSON.stringify(body);
  return 'sha256=' + crypto.createHmac('sha256', secret).update(payload).digest('hex');
}

describe('verifyWebhookSignature', () => {
  test('passes through when source has no webhookSecret', () => {
    const source = makeSource({ config: {} });
    const req = makeReq({ files: [] }, undefined, OMIT_TS);
    expect(verifyWebhookSignature(req, source)).toEqual({ ok: true });
  });

  test('passes through when webhookSecret is empty string', () => {
    const source = makeSource({ config: { webhookSecret: '' } });
    const req = makeReq({ files: [] }, 'sha256=anything', OMIT_TS);
    expect(verifyWebhookSignature(req, source)).toEqual({ ok: true });
  });

  test('passes through when webhookSecret is non-string', () => {
    const source = makeSource({ config: { webhookSecret: 123 as unknown as string } });
    const req = makeReq({ files: [] }, 'sha256=anything', OMIT_TS);
    expect(verifyWebhookSignature(req, source)).toEqual({ ok: true });
  });

  test('rejects when secret is configured and signature header is missing', () => {
    const source = makeSource({ config: { webhookSecret: 'shh' } });
    const req = makeReq({ files: [] });
    const result = verifyWebhookSignature(req, source);
    expect(result.ok).toBe(false);
    expect(result.reason).toBe('invalid webhook signature');
    expect(result.status).toBe(403);
  });

  test('rejects when secret is configured and signature is wrong', () => {
    const source = makeSource({ config: { webhookSecret: 'shh' } });
    const req = makeReq({ files: [] }, 'sha256=deadbeef');
    const result = verifyWebhookSignature(req, source);
    expect(result.ok).toBe(false);
    expect(result.status).toBe(403);
  });

  test('accepts when signature matches HMAC-SHA256 of the body and timestamp is fresh', () => {
    const secret = 'correct-horse-battery-staple';
    const body = { event: 'page_updated', page: { id: '1' } };
    const source = makeSource({ config: { webhookSecret: secret } });
    const req = makeReq(body, sign(secret, body));
    expect(verifyWebhookSignature(req, source)).toEqual({ ok: true });
  });

  test('rejects when body differs from what was signed', () => {
    const secret = 'shh';
    const signedBody = { event: 'page_updated' };
    const tamperedBody = { event: 'page_deleted' };
    const source = makeSource({ config: { webhookSecret: secret } });
    const req = makeReq(tamperedBody, sign(secret, signedBody));
    expect(verifyWebhookSignature(req, source).ok).toBe(false);
  });

  // hub#1260 — replay-window guard
  test('rejects with 401 when secret is set but timestamp header is missing', () => {
    const secret = 'shh';
    const body = { x: 1 };
    const source = makeSource({ config: { webhookSecret: secret } });
    const req = makeReq(body, sign(secret, body), OMIT_TS);
    const result = verifyWebhookSignature(req, source);
    expect(result.ok).toBe(false);
    expect(result.status).toBe(401);
    expect(result.reason).toContain('x-webhook-timestamp');
  });

  test('rejects with 401 when timestamp is more than 5 minutes old', () => {
    const secret = 'shh';
    const body = { x: 1 };
    const source = makeSource({ config: { webhookSecret: secret } });
    const stale = new Date(Date.now() - 10 * 60_000).toISOString();
    const req = makeReq(body, sign(secret, body), stale);
    const result = verifyWebhookSignature(req, source);
    expect(result.ok).toBe(false);
    expect(result.status).toBe(401);
    expect(result.reason).toContain('too old');
  });

  test('rejects with 401 when timestamp is more than 5 minutes in the future', () => {
    const secret = 'shh';
    const body = { x: 1 };
    const source = makeSource({ config: { webhookSecret: secret } });
    const future = new Date(Date.now() + 10 * 60_000).toISOString();
    const req = makeReq(body, sign(secret, body), future);
    const result = verifyWebhookSignature(req, source);
    expect(result.ok).toBe(false);
    expect(result.status).toBe(401);
    expect(result.reason).toContain('future');
  });

  test('no timestamp check when source has no webhookSecret (paired with HMAC)', () => {
    const source = makeSource({ config: {} });
    // Stale timestamp — but no secret, so we never look at it.
    const stale = new Date(Date.now() - 10 * 60_000).toISOString();
    const req = makeReq({ x: 1 }, undefined, stale);
    expect(verifyWebhookSignature(req, source)).toEqual({ ok: true });
  });
});
