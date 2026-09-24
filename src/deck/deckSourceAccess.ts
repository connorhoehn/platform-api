// platform-api/src/deck/deckSourceAccess.ts
//
// Loop 29c: the deck routes read material by body-supplied id — `sources[]` on
// /deck/revise and /deck/revise-run, `from.documentId` / `from.recordingId` on
// /deck/fixture. The deck being revised was already checked (document:edit);
// the material was not, so a caller could ground a deck in anyone's page or
// call transcript. This checks each one before the handler reads it:
//
//   document   → document:read (documentReadGate); a legacy transcript filed as
//                a document (deckRevise.readSource's fallback) passes if its
//                recording is one the caller took part in
//   transcript → its recording, participant only
//   recording  → participant only (the `recording:read` rule's input)
//
// Refused → 404, as for a missing source. No bearer → 401 (the gate's answer).

import type { NextFunction, Request, Response } from 'express';
import { documentReadGate } from '../documents/documentReadGate';
import { markAccess, type AccessMarkedHandler } from '../middleware/resource-access';
import { AppError, NotFoundError } from '../middleware/error-handler';
import { callRecordingRepo } from '../repositories';
import { findRecordingIdByTranscript } from './deckRevise';

export interface DeckSourceAccessDeps {
  assertCanRead(req: Request, documentId: string): Promise<unknown>;
  isParticipant(userId: string, recordingId: string): Promise<boolean>;
  recordingOfTranscript(transcriptId: string): Promise<string | null>;
}

const defaultDeps: DeckSourceAccessDeps = {
  assertCanRead: (req, id) => documentReadGate().assertCanRead(req, id),
  isParticipant: async (userId, recordingId) => (await callRecordingRepo.resolveForUser(userId, recordingId))?.isParticipant === true,
  recordingOfTranscript: findRecordingIdByTranscript,
};

type Ref = { kind: string; id: string };

function refsOf(body: Record<string, unknown>): Ref[] {
  const refs: Ref[] = [];
  if (Array.isArray(body.sources)) {
    for (const raw of body.sources) {
      const e = (raw ?? {}) as Record<string, unknown>;
      if (typeof e.kind === 'string' && typeof e.id === 'string' && e.id) refs.push({ kind: e.kind, id: e.id });
    }
  }
  const from = (body.from ?? null) as Record<string, unknown> | null;
  if (from && typeof from === 'object') {
    if (typeof from.documentId === 'string' && from.documentId) refs.push({ kind: 'document', id: from.documentId });
    if (typeof from.recordingId === 'string' && from.recordingId) refs.push({ kind: 'recording', id: from.recordingId });
  }
  return refs;
}

export async function assertDeckSourcesReadable(req: Request, deps: DeckSourceAccessDeps = defaultDeps): Promise<void> {
  const refs = refsOf((req.body ?? {}) as Record<string, unknown>);
  if (!refs.length) return;
  if (!req.headers.authorization?.startsWith('Bearer ') && !req.documentGrant) throw new AppError(401, 'Authorization required');
  const userId = req.user?.sub ?? '';
  const viaRecording = async (recordingId: string | null): Promise<boolean> => !!recordingId && deps.isParticipant(userId, recordingId);
  for (const ref of refs) {
    if (ref.kind === 'recording') {
      if (!(await viaRecording(ref.id))) throw new NotFoundError('Source not found');
    } else if (ref.kind === 'transcript') {
      if (!(await viaRecording(await deps.recordingOfTranscript(ref.id)))) throw new NotFoundError('Source not found');
    } else {
      try {
        await deps.assertCanRead(req, ref.id);
      } catch (err) {
        if ((err as { status?: number }).status !== 404) throw err;
        if (!(await viaRecording(await deps.recordingOfTranscript(ref.id).catch(() => null)))) throw err;
      }
    }
  }
}

export function requireDeckSourcesReadable(deps: DeckSourceAccessDeps = defaultDeps): AccessMarkedHandler {
  return markAccess((req: Request, _res: Response, next: NextFunction) => {
    assertDeckSourcesReadable(req, deps).then(() => next(), next);
  }, 'deck:sources-read');
}
