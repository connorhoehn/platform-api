// Server-backed chat commands — the registry (src/chat/chatCommandRegistry.ts) over HTTP.
//
//   GET  /api/chat/commands                → { commands: [{ trigger, pipelineId, description, label?, hint?, requires? }] }
//   POST /api/chat/commands/:trigger/run   → 202 { runId, pipelineId, card }
//
// `:trigger` is accepted with or without its slash — `/api/chat/commands/summarize/run`
// and `/api/chat/commands/%2Fsummarize/run` are the same command (Express
// decodes the segment; the registry normalises). Unknown trigger → 404 with the
// known ones; unmet `requires` → 400 `{ error, requires }`. Auth is applied at
// the mount (routes/index.ts); `requestedBy` is `req.user.sub`. A body with
// `clientPostsCard: true` says the client posts the returned card itself, so
// neither the dispatcher nor the pipeline's publish step posts a second one.
import { Router, type Request, type Response } from 'express';
import { asyncHandler, NotFoundError, ValidationError } from '../middleware/error-handler';
import { markAccess } from '../middleware/resource-access';
import { documentReadGate, type DocumentReadGate } from '../documents/documentReadGate';
import { callRecordingRepo } from '../repositories';
import {
  ChatCommandRequiresError, UnknownChatCommandError,
  getChatCommandPipeline, listChatCommandPipelines, runChatCommand, type ChatCommandRunInput,
} from '../chat/chatCommandRegistry';
import { registerBuiltinChatCommands } from '../chat/builtinChatCommands';
import { installBuiltinChatCommandPlugins } from '../chat/plugins';
import { DocumentEditForbiddenError, isChannelMember } from '../deck/documentEditAccess';

registerBuiltinChatCommands();
// The platform's own plugins (/search …) install after the built-ins through the plugin contract;
// external ones (CHAT_COMMAND_PLUGINS) are loaded by src/index.ts into the same live registries.
installBuiltinChatCommandPlugins();

export const chatCommandsRouter = Router();

chatCommandsRouter.get('/', (_req: Request, res: Response) => {
  res.json({ commands: listChatCommandPipelines() });
});

/**
 * Loop 29c: a command names its document and material in the body. The run
 * writes `documentId` (document:edit — a row that does not exist yet is the
 * new page the command creates, as /deck's own check allows), reads
 * `extra.sourceDocumentId` and each `extra.documents[]` (document:read) and
 * `extra.recordingId` (a participant), and /summarize reads `channel`'s
 * history (a member). 401 / 404 / 403 as everywhere else.
 */
export async function assertChatCommandAccess(req: Request, deps: {
  gate?: Pick<DocumentReadGate, 'assertCanRead' | 'assertCanEdit' | 'ownerOf'>;
  isParticipant?: (userId: string, recordingId: string) => Promise<boolean>;
  isMember?: typeof isChannelMember;
} = {}): Promise<void> {
  const gate = deps.gate ?? documentReadGate();
  const body = (req.body ?? {}) as Record<string, unknown>;
  const extra = (body.extra && typeof body.extra === 'object' ? body.extra : {}) as Record<string, unknown>;
  const userId = req.user?.sub ?? '';
  const documentId = typeof body.documentId === 'string' ? body.documentId.trim() : '';
  if (documentId) {
    try {
      await gate.assertCanEdit(req, documentId);
    } catch (err) {
      if ((err as { status?: number }).status !== 404 || (await gate.ownerOf(documentId)) !== undefined) throw err;
    }
  }
  const reads: string[] = [];
  if (typeof extra.sourceDocumentId === 'string' && extra.sourceDocumentId) reads.push(extra.sourceDocumentId);
  if (Array.isArray(extra.documents)) {
    for (const entry of extra.documents.slice(0, 50)) {
      const item = (entry ?? {}) as { id?: unknown; documentId?: unknown };
      const id = typeof item.documentId === 'string' && item.documentId ? item.documentId : typeof item.id === 'string' ? item.id : '';
      if (id && id !== documentId) reads.push(id);
    }
  }
  for (const id of reads) await gate.assertCanRead(req, id);
  if (typeof extra.recordingId === 'string' && extra.recordingId) {
    const isParticipant = deps.isParticipant ?? (async (u: string, r: string) => (await callRecordingRepo.resolveForUser(u, r))?.isParticipant === true);
    if (!(await isParticipant(userId, extra.recordingId))) throw new NotFoundError('Recording not found');
  }
  if (getChatCommandPipeline(String(req.params.trigger ?? ''))?.trigger === '/summarize') {
    const channel = typeof body.channel === 'string' ? body.channel.trim() : '';
    if (channel && !(await (deps.isMember ?? isChannelMember)(channel, userId))) throw new NotFoundError('Channel not found');
  }
}

const requireChatCommandAccess = markAccess((req, res, next) => {
  assertChatCommandAccess(req).then(() => next(), (err: unknown) => {
    if (err instanceof DocumentEditForbiddenError) { res.status(403).json({ error: err.message, code: err.code, documentId: err.documentId }); return; }
    next(err);
  });
}, 'chat-command:document+sources');

chatCommandsRouter.post('/:trigger/run', requireChatCommandAccess, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const trigger = String(req.params.trigger ?? '');
  const body = (req.body ?? {}) as Record<string, unknown>;
  const channel = typeof body.channel === 'string' ? body.channel.trim() : '';
  if (!channel) throw new ValidationError('channel is required');
  const text = typeof body.text === 'string' ? body.text : '';
  const userId = req.user!.sub;
  const argument = body.argument && typeof body.argument === 'object'
    ? (() => { const a = body.argument as { id?: unknown; label?: unknown }; return typeof a.id === 'string' && typeof a.label === 'string' ? { id: a.id, label: a.label } : undefined; })()
    : undefined;
  const input: ChatCommandRunInput = {
    trigger, channel, text,
    argumentText: typeof body.argumentText === 'string' ? body.argumentText : text.replace(/^\S+\s*/, ''),
    ...(argument ? { argument } : {}),
    ...(typeof body.documentId === 'string' && body.documentId ? { documentId: body.documentId } : {}),
    requestedBy: userId,
    // The verified org claim, not a body field: it decides whether this run is
    // ever projected into the requester's work graph.
    ...(req.user!.orgId ? { organizationId: req.user!.orgId } : {}),
    requestedByName: typeof body.requestedByName === 'string' && body.requestedByName ? body.requestedByName.slice(0, 80) : userId,
    ...(body.extra && typeof body.extra === 'object' ? { extra: body.extra as Record<string, unknown> } : {}),
    ...(body.clientPostsCard === true ? { clientPostsCard: true } : {}),
  };
  try {
    const out = await runChatCommand(input);
    res.status(202).json(out);
  } catch (err) {
    if (err instanceof UnknownChatCommandError) { res.status(404).json({ error: err.message, known: err.known }); return; }
    if (err instanceof ChatCommandRequiresError) { res.status(400).json({ error: err.message, requires: err.requires }); return; }
    if (err instanceof DocumentEditForbiddenError) { res.status(403).json({ error: err.message, code: err.code, documentId: err.documentId }); return; }
    throw err;
  }
}));
