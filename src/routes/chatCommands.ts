// Server-backed chat commands — the registry (src/chat/chatCommandRegistry.ts) over HTTP.
//
//   GET  /api/chat/commands                → { commands: [{ trigger, pipelineId, description, requires? }] }
//   POST /api/chat/commands/:trigger/run   → 202 { runId, pipelineId, card }
//
// `:trigger` is accepted with or without its slash — `/api/chat/commands/summarize/run`
// and `/api/chat/commands/%2Fsummarize/run` are the same command (Express
// decodes the segment; the registry normalises). Unknown trigger → 404 with the
// known ones; unmet `requires` → 400 `{ error, requires }`. Auth is applied at
// the mount (routes/index.ts); `requestedBy` is `req.user.sub`.
import { Router, type Request, type Response } from 'express';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import {
  ChatCommandRequiresError, UnknownChatCommandError,
  listChatCommandPipelines, runChatCommand, type ChatCommandRunInput,
} from '../chat/chatCommandRegistry';
import { registerBuiltinChatCommands } from '../chat/builtinChatCommands';

registerBuiltinChatCommands();

export const chatCommandsRouter = Router();

chatCommandsRouter.get('/', (_req: Request, res: Response) => {
  res.json({ commands: listChatCommandPipelines() });
});

chatCommandsRouter.post('/:trigger/run', asyncHandler(async (req: Request, res: Response): Promise<void> => {
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
    requestedByName: typeof body.requestedByName === 'string' && body.requestedByName ? body.requestedByName.slice(0, 80) : userId,
    ...(body.extra && typeof body.extra === 'object' ? { extra: body.extra as Record<string, unknown> } : {}),
  };
  try {
    const out = await runChatCommand(input);
    res.status(202).json(out);
  } catch (err) {
    if (err instanceof UnknownChatCommandError) { res.status(404).json({ error: err.message, known: err.known }); return; }
    if (err instanceof ChatCommandRequiresError) { res.status(400).json({ error: err.message, requires: err.requires }); return; }
    throw err;
  }
}));
