// platform-api/src/routes/voiceCapture.ts
//
// Authenticated proxy in front of the live-captions sidecar, so ambient voice
// capture can work on any page without exposing an unauthenticated PCM sink to
// the browser.
//
// The sidecar (live-video-streaming/services/live-captions) is a GENERIC HTTP
// speech-to-text service: it keys an accumulator by the free-form
// `X-Channel-Arn` header, cuts utterance windows, and PUBLISHes to Redis
// `captions:<channelArn>`. The gateway's caption-relay psubscribes `captions:*`
// and fans out to the WS channel `captions:<sha1(arn)[:24]>`. Nothing about
// that path is call-specific — only its usual FEEDER (the LVS per-participant
// tap) was. So this route feeds the same sidecar with a routing key of
// `capture:<captureId>` and the whole transcript path works unchanged.
//
// What this route adds over POSTing the sidecar directly:
//
//   * Auth. The sidecar has none, and must never be reachable from a browser.
//   * Ownership binding. A captureId is a fan-out address: anyone who knows one
//     can read its transcripts off the WS channel. First writer wins, and a
//     second user POSTing to the same id is rejected — otherwise one user could
//     inject audio into another's capture session.
//   * Charset + size validation on both ids, since they become a Redis channel
//     name and an accumulator key.
//
// What it deliberately does NOT do: retain audio. The request body is forwarded
// and dropped. There is no buffer, no disk write, no MediaRecorder, and no
// speaker embedding anywhere in this path — we keep the transcript, never the
// voice. (In re Otter.AI Privacy Litigation, N.D. Cal. 13 Aug 2026: retention
// and training on captured audio is what turns a tool into an eavesdropper.)

import express, { Router, type Request, type Response } from 'express';
import { asyncHandler, ValidationError, ForbiddenError } from '../middleware/error-handler';
import log from '../lib/logger';

export const voiceCaptureRouter = Router();

/** Ids become a Redis channel name and an accumulator key — keep them boring. */
const ID_PATTERN = /^[A-Za-z0-9_-]{8,64}$/;

/**
 * The sidecar caps a single POST at 1 MB. The flush tail (~900 ms of silence,
 * ~28 KB) and a normal 200 ms chunk (~6.4 KB) are both far below that; anything
 * approaching the cap is not a capture chunk.
 */
const MAX_CHUNK_BYTES = 256 * 1024;

function sidecarUrl(): string {
  return (
    process.env.LIVE_CAPTIONS_URL ??
    // platform-api runs in-cluster while the sidecar runs as a host process in
    // dev (see the live-captions local_resource). host.docker.internal is the
    // same escape hatch the LVS proxy config already uses.
    'http://host.docker.internal:3915'
  );
}

/**
 * captureId -> owning userId. In-memory and bounded: this is a liveness guard
 * on an ephemeral fan-out address, not an authorization record. A restart
 * forgets bindings, which costs nothing — the next POST re-binds.
 */
const MAX_BINDINGS = 5000;
const ownerByCapture = new Map<string, string>();

function bindOwner(captureId: string, userId: string): boolean {
  const existing = ownerByCapture.get(captureId);
  if (existing !== undefined) {
    if (existing !== userId) return false;
    // Refresh LRU position.
    ownerByCapture.delete(captureId);
    ownerByCapture.set(captureId, userId);
    return true;
  }
  if (ownerByCapture.size >= MAX_BINDINGS) {
    const oldest = ownerByCapture.keys().next();
    if (!oldest.done) ownerByCapture.delete(oldest.value);
  }
  ownerByCapture.set(captureId, userId);
  return true;
}

/** Test seam — the binding map is process-global by design. */
export function __resetVoiceCaptureBindings(): void {
  ownerByCapture.clear();
}

/**
 * POST /api/voice-capture/pcm
 *
 * Body: raw s16le mono 16 kHz PCM (Content-Type: application/octet-stream).
 * Headers:
 *   X-Capture-Id     stable per capture session; selects the fan-out channel
 *   X-Utterance-Id   fresh per push-to-talk press
 *   X-Capture-Flush  '1' on the trailing silence that closes an utterance
 *
 * X-Utterance-Id becomes the sidecar's `X-Participant-Id`, which is what keys
 * its accumulator. That is the point: a NEW id per press means a FRESH
 * accumulator, so a sub-threshold remark ("yes") cannot sit in the buffer and
 * get prepended to the next press's transcript — which would attach it to the
 * next press's target. Mis-attachment is the one failure mode this feature
 * cannot have, so the isolation is structural rather than best-effort.
 *
 * The participant id carries no user identity for the same reason: it is
 * echoed into every caption payload on the channel.
 */
voiceCaptureRouter.post(
  '/pcm',
  express.raw({ type: 'application/octet-stream', limit: MAX_CHUNK_BYTES }),
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const captureId = String(req.headers['x-capture-id'] ?? '');
    const utteranceId = String(req.headers['x-utterance-id'] ?? '');
    const isFlush = String(req.headers['x-capture-flush'] ?? '') === '1';
    const userId = req.user!.sub;

    if (!ID_PATTERN.test(captureId)) {
      throw new ValidationError('X-Capture-Id must be 8-64 chars of [A-Za-z0-9_-]');
    }
    if (!ID_PATTERN.test(utteranceId)) {
      throw new ValidationError('X-Utterance-Id must be 8-64 chars of [A-Za-z0-9_-]');
    }
    const body = req.body;
    if (!Buffer.isBuffer(body) || body.length === 0) {
      throw new ValidationError('body must be raw s16le PCM (application/octet-stream)');
    }
    if (body.length % 2 !== 0) {
      throw new ValidationError('body length must be even (s16le samples)');
    }
    if (!bindOwner(captureId, userId)) {
      // Someone else is already streaming into this capture id. Refuse rather
      // than mixing two speakers into one transcript on a channel one of them
      // is listening to.
      throw new ForbiddenError('capture id is in use by another user');
    }

    const target = `${sidecarUrl().replace(/\/$/, '')}/pcm`;
    let upstream: globalThis.Response;
    try {
      upstream = await fetch(target, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/octet-stream',
          'Content-Length': String(body.length),
          'X-Channel-Arn': `capture:${captureId}`,
          'X-Participant-Id': utteranceId,
          'X-Sample-Rate': '16000',
        },
        body: new Uint8Array(body),
      });
    } catch (err) {
      // The sidecar is optional in dev. Say so plainly — a capture UI that
      // silently swallows this looks like "the mic did not work".
      log.warn(
        { event: 'voice.capture.sidecar_unreachable', target, err: err instanceof Error ? err.message : String(err) },
        'live-captions sidecar unreachable',
      );
      res.status(503).json({
        error: 'transcription service unavailable',
        detail: `could not reach ${target}`,
      });
      return;
    }

    if (!upstream.ok) {
      const text = await upstream.text().catch(() => '');
      log.warn(
        { event: 'voice.capture.sidecar_error', status: upstream.status, text: text.slice(0, 200) },
        'live-captions rejected chunk',
      );
      res.status(502).json({ error: 'transcription service rejected chunk' });
      return;
    }

    if (isFlush) {
      log.info(
        {
          event: 'voice.capture.utterance_flushed',
          captureId,
          utteranceId,
          userId,
          silenceBytes: body.length,
        },
        'voice capture utterance closed',
      );
    }
    res.status(202).json({ accepted: body.length });
  }),
);

/**
 * GET /api/voice-capture/health — is transcription actually available?
 *
 * The capture UI must be able to tell "you have no microphone" from "the
 * transcriber is down", because they need different words in front of a user
 * who just spoke a sentence into the void.
 */
voiceCaptureRouter.get(
  '/health',
  asyncHandler(async (_req: Request, res: Response): Promise<void> => {
    const target = `${sidecarUrl().replace(/\/$/, '')}/healthz`;
    try {
      const upstream = await fetch(target, {
        method: 'GET',
        signal: AbortSignal.timeout(2000),
      });
      res.status(200).json({ available: upstream.ok, sidecar: target });
    } catch (err) {
      res.status(200).json({
        available: false,
        sidecar: target,
        detail: err instanceof Error ? err.message : String(err),
      });
    }
  }),
);
