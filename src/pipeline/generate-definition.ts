// platform-api/src/pipeline/generate-definition.ts
//
// "Create draft" / "Create and run" on the pipelines page: the chat's /agent
// planner, pointed at the composer's instruction, producing a DEFINITION the
// caller owns instead of a run in a chat. The grammar is the chat's
// (`parseAgentLoopCommand`: leading /hints, a trailing `200k`, `--model`), the
// plan is the chat's (`planAgentLoop`, one model call, never throws — a
// planner outage yields a single-step `fallback` plan and says so), and the
// graph is the chat's (`buildAgentLoopDefinition`). What differs is the
// envelope: a fresh id, `status: 'draft'`, `origin.kind: 'agent'` with the
// instruction verbatim, and `createdBy` = the verified caller.
//
// No chat message is posted from here: the card the chat path posts is the
// chat command's job, not the definition's.

import {
  buildAgentLoopDefinition,
  buildAgentLoopRequest,
  parseAgentLoopCommand,
  planAgentLoop,
  type AgentLoopRequest,
  type LoopPlan,
} from '../chat/agentLoopPlanner';
import { MAX_INSTRUCTION_CHARS, routeSummary, type DefinitionOrigin } from './definition-metadata';

export class InvalidInstructionError extends Error {
  constructor(public readonly detail: string) {
    super(detail);
    this.name = 'InvalidInstructionError';
  }
}

export interface GenerateDefinitionInput {
  instruction: string;
  hints?: string[];
  model?: string;
  contextBudgetTokens?: number;
  /** The verified caller — owner of the draft and requester of any run. */
  userId: string;
  userName?: string;
  organizationId?: string;
}

export interface GenerateDefinitionDeps {
  /** The planner. Default: the chat's CLI-backed planner client, imported lazily. */
  plan?: (request: AgentLoopRequest) => Promise<LoopPlan>;
  now?: () => string;
  /** Six URL-safe characters. */
  suffix?: () => string;
}

export interface GeneratedDefinition {
  definition: Record<string, unknown> & { id: string };
  plan: LoopPlan;
  request: AgentLoopRequest;
  /** What a run of this definition needs in its context — the chat path's payload, minus the chat. */
  triggerPayload: Record<string, unknown>;
}

const NAME_WORDS = 6;
const SLUG_WORDS = 4;

/** "Create a pipeline that reviews a document, asks me…" → "Create a pipeline that reviews a…" */
export function nameFromInstruction(instruction: string): string {
  const words = instruction.trim().split(/\s+/).filter(Boolean);
  const head = words.slice(0, NAME_WORDS).join(' ').replace(/[,.;:!?]+$/, '');
  const name = head.charAt(0).toUpperCase() + head.slice(1);
  return words.length > NAME_WORDS ? `${name}…` : name;
}

function slugFromInstruction(instruction: string): string {
  const slug = instruction.toLowerCase().replace(/[^a-z0-9\s-]/g, ' ').trim().split(/\s+/).filter(Boolean)
    .filter((w) => !['a', 'an', 'the', 'that', 'to', 'me', 'and', 'of', 'it'].includes(w))
    .slice(0, SLUG_WORDS).join('-');
  return slug || 'pipeline';
}

function randomSuffix(): string {
  const alphabet = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let out = '';
  for (let i = 0; i < 6; i++) out += alphabet[Math.floor(Math.random() * alphabet.length)];
  return out;
}

async function defaultPlan(request: AgentLoopRequest): Promise<LoopPlan> {
  // Lazy: builtinChatCommands pulls the whole command registry in with it.
  const { getAgentLoopPlannerClient } = await import('../chat/builtinChatCommands.js');
  return planAgentLoop(request, getAgentLoopPlannerClient());
}

/** The planner's model is unavailable (breaker open): "Model unavailable: <reason>". */
export class PlannerUnavailableError extends Error {
  constructor(message: string) { super(message); this.name = 'PlannerUnavailableError'; }
}

export function validateInstruction(raw: unknown): string {
  if (typeof raw !== 'string') throw new InvalidInstructionError('instruction must be a string');
  const instruction = raw.trim();
  if (!instruction) throw new InvalidInstructionError('instruction is empty');
  if (instruction.length > MAX_INSTRUCTION_CHARS) throw new InvalidInstructionError(`instruction is longer than ${MAX_INSTRUCTION_CHARS} characters`);
  return instruction;
}

/** The run context the chat path gives an agent loop, without a chat behind it. */
export function generatedTriggerPayload(request: AgentLoopRequest, plan: LoopPlan, organizationId?: string): Record<string, unknown> {
  return {
    ...(organizationId ? { organizationId } : {}),
    instruction: request.instruction,
    hints: request.hints,
    contextBudgetTokens: request.contextBudgetTokens,
    model: request.model,
    plan,
    chatChannel: request.chatChannel,
    requestedBy: request.requestedBy,
    ...(request.requestedByName ? { requestedByName: request.requestedByName } : {}),
    deckTranscript: '(no transcript for this pipeline)',
    deckPriorWork: '(this is the first step)',
  };
}

/** Plan the instruction and shape it as a DRAFT the caller owns. Throws only InvalidInstructionError. */
export async function generateDraftDefinition(input: GenerateDefinitionInput, deps: GenerateDefinitionDeps = {}): Promise<GeneratedDefinition> {
  const instruction = validateInstruction(input.instruction);
  const now = deps.now ?? (() => new Date().toISOString());
  const suffix = deps.suffix ?? randomSuffix;

  const parsed = parseAgentLoopCommand(instruction, {
    ...(input.hints ? { hints: input.hints } : {}),
    ...(input.model ? { model: input.model } : {}),
    ...(input.contextBudgetTokens !== undefined ? { contextBudgetTokens: input.contextBudgetTokens } : {}),
  });
  // No chat behind a page-made definition: the channel names the page so a
  // step that looks for one finds a marker, not a person's thread.
  const request = buildAgentLoopRequest(parsed, {
    chatChannel: `pipelines:${input.userId}`,
    requestedBy: input.userId,
    ...(input.userName ? { requestedByName: input.userName } : {}),
  });
  if (!request.instruction) throw new InvalidInstructionError('instruction has no text after its /hints and options');

  const plan = await (deps.plan ?? defaultPlan)(request);
  if (plan.unavailable) throw new PlannerUnavailableError(plan.unavailable);
  const plannedAt = now();
  const base = buildAgentLoopDefinition(plan, request);
  const id = `agent-${slugFromInstruction(request.instruction)}-${suffix()}`;
  const origin: DefinitionOrigin = {
    kind: 'agent',
    instruction: instruction.slice(0, MAX_INSTRUCTION_CHARS),
    plannerSource: plan.source === 'planner' ? 'model' : 'fallback',
    plannedAt,
  };
  const { publishedVersion: _published, ...rest } = base as Record<string, unknown> & { publishedVersion?: unknown };
  const definition = {
    ...rest,
    id,
    name: nameFromInstruction(request.instruction),
    description: instruction,
    ...(request.hints.length ? { tags: [...request.hints] } : {}),
    workType: 'agents',
    kind: 'agent',
    route: routeSummary(base),
    origin,
    sourceInstruction: instruction,
    status: 'draft',
    version: 1,
    createdBy: input.userId,
    createdAt: plannedAt,
    updatedAt: plannedAt,
  } as Record<string, unknown> & { id: string };

  return { definition, plan, request, triggerPayload: generatedTriggerPayload(request, plan, input.organizationId) };
}
