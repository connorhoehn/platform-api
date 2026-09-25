// `photo-adjust` — the photo editor's assistant (realtime-examples
// docs/design/media-photo-editor/PLAN.md, lane E3). "When I use the command
// input, pipelines drive the actual changes": the composer's Send starts this
// run, and the run — not the page — asks the model.
//
//   trigger (the composer) → propose (photo:propose-adjust)
//
// The step reads the instruction, the photo's masks (which ones the
// instruction references with `#`) and its current ops, asks the model through
// the gate's own `photo` pool, and checks the answer against the allowed ops
// and their bounds (photoAdjustOps.ts). Any op out of that set refuses the
// whole run; nothing is clamped. Its output is a PROPOSAL — `proposedOps` (the
// current ops with the changes merged in), `changes`, `summary`,
// `changedKinds` — that the editor previews until the person keeps or discards
// it. The run writes nothing: keeping is the person's own versioned save.
//
// Started through the generic trigger (`draftable`): the requester is pinned
// to the caller there, and the step checks the photo is in the catalog.

import type { PipelineNode, StepOutcome } from 'distributed-core/applications/pipeline';
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import type { LLMClient } from '../pipeline/LLMClient';
import { createLLMClient } from '../pipeline/createLLMClient';
import { gatedLLMClient, isModelUnavailable } from '../lib/modelGate';
import { extractJsonObject } from '../pipeline/image/VisionClient';
import log from '../lib/logger';
import {
  MAX_INSTRUCTION,
  PHOTO_ADJUST_PIPELINE_ID,
  PHOTO_ADJUST_SYSTEM_PROMPT,
  PhotoOpRefusal,
  buildPhotoAdjustPrompt,
  mergeOps,
  readCurrentOps,
  readMasks,
  validateAssistantOps,
} from './photoAdjustOps';

export { PHOTO_ADJUST_PIPELINE_ID } from './photoAdjustOps';
export const PHOTO_PROPOSE_ADJUST_KIND = 'photo:propose-adjust';

const DEFAULT_MODEL = process.env['PHOTO_ADJUST_MODEL'] ?? 'sonnet';
const DEFAULT_TIMEOUT_MS = Number(process.env['PHOTO_ADJUST_TIMEOUT_MS'] ?? 90_000);
const HEX64 = /^[0-9a-f]{64}$/;
/** The commands whose instruction this pipeline turns into ops today. */
export const PHOTO_ADJUST_COMMANDS = ['adjust'] as const;

export function buildPhotoAdjustDefinition(): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  return {
    id: PHOTO_ADJUST_PIPELINE_ID,
    name: 'Photo assistant: adjust',
    description: 'The photo editor\'s /adjust: turn an instruction, the masks it references and the current edit into proposed Light and Color ops (photo model pool), checked against the allowed ops and bounds. Writes nothing — the editor previews the proposal until the person keeps it.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'manual', label: 'Photo composer' } },
      { id: 'propose', type: PHOTO_PROPOSE_ADJUST_KIND, position: pos(200), data: { type: PHOTO_PROPOSE_ADJUST_KIND, label: 'Propose adjustments', retryPolicy: { maxAttempts: 2, backoffMs: 1500, backoffMultiplier: 2, retryUnknown: false } } },
    ],
    edges: [{ id: 'trigger->propose', source: 'trigger', sourceHandle: 'out', target: 'propose', targetHandle: 'in' }],
    workType: 'other', kind: 'media', route: ['Composer', 'Propose'], origin: { kind: 'system' },
  };
}

export interface PhotoProposeAdjustDeps {
  /** Tests inject a model client; the platform's, through the gate's `photo` pool, by default. */
  llm?: LLMClient;
  /** Is this file in the catalog? The catalog repository by default. */
  photoExists?: (fileId: string) => Promise<{ name?: string } | null>;
  model?: string;
  timeoutMs?: number;
}

function str(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

async function catalogPhoto(fileId: string): Promise<{ name?: string } | null> {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { mediaCatalogRepo } = require('../repositories') as typeof import('../repositories');
  const row = await mediaCatalogRepo.getFile(fileId);
  if (!row) return null;
  const rel = (row as { relPath?: unknown; path?: unknown }).relPath ?? (row as { path?: unknown }).path;
  return { ...(typeof rel === 'string' ? { name: rel.split('/').pop() } : {}) };
}

export function createPhotoProposeAdjustHandler(deps: PhotoProposeAdjustDeps = {}): DescribedStepHandler {
  const refuse = (error: string): StepOutcome => ({ status: 'failed', error, retry: false } as StepOutcome);
  return {
    kind: PHOTO_PROPOSE_ADJUST_KIND,
    description: 'Asks the model (photo pool, behind the model gate) for Light and Color ops that do what the instruction says, scoped to the referenced masks; refuses any op outside the allowed kinds and bounds.',
    reads: 'fileId, instruction, command (adjust), masks [{maskId,label,kind,subjects}], referencedMaskIds, currentOps, requestedBy.',
    emits: 'proposedOps (currentOps with the changes merged), changes, changedKinds, summary, model, referencedMaskIds.',
    async execute(_node: PipelineNode, context: Record<string, unknown>): Promise<StepOutcome> {
      const requestedBy = str(context.requestedBy);
      if (!requestedBy) return refuse('photo-adjust: the run names no requester — nothing was proposed');
      const fileId = str(context.fileId);
      if (!fileId || !HEX64.test(fileId)) return refuse('photo-adjust: fileId must be the photo\'s sha256 — nothing was proposed');
      const command = str(context.command) ?? 'adjust';
      if (!(PHOTO_ADJUST_COMMANDS as readonly string[]).includes(command)) return refuse(`photo-adjust: /${command} is not something this pipeline can do yet — only /adjust`);
      const instruction = str(context.instruction);
      if (!instruction) return refuse('photo-adjust: say what to change — the instruction is empty');
      if (instruction.length > MAX_INSTRUCTION) return refuse(`photo-adjust: the instruction is over ${MAX_INSTRUCTION} characters`);

      let masks; let currentOps;
      try {
        masks = readMasks(context.masks);
        currentOps = readCurrentOps(context.currentOps);
      } catch (err) {
        return refuse(`photo-adjust: ${(err as Error).message}`);
      }
      const referencedMaskIds = Array.isArray(context.referencedMaskIds)
        ? [...new Set(context.referencedMaskIds.filter((id): id is string => typeof id === 'string'))]
        : [];
      const unknownRef = referencedMaskIds.find((id) => !masks.some((m) => m.maskId === id));
      if (unknownRef) return refuse(`photo-adjust: the instruction references mask ${JSON.stringify(unknownRef)}, which is not one of this photo's — nothing was proposed`);

      let photo: { name?: string } | null;
      try {
        photo = await (deps.photoExists ?? catalogPhoto)(fileId);
      } catch (err) {
        return refuse(`photo-adjust: the photo could not be looked up (${(err as Error).message})`);
      }
      if (!photo) return refuse('photo-adjust: no such photo in the catalog — nothing was proposed');

      const llm = deps.llm ?? gatedLLMClient(createLLMClient(), 'photo', { singleFlight: true });
      const model = deps.model ?? DEFAULT_MODEL;
      const prompt = buildPhotoAdjustPrompt({ ...(photo.name ? { name: photo.name } : {}), instruction, masks, referencedMaskIds, currentOps });
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), deps.timeoutMs ?? DEFAULT_TIMEOUT_MS);
      const startedAt = Date.now();
      let response = '';
      try {
        for await (const chunk of llm.stream(model, PHOTO_ADJUST_SYSTEM_PROMPT, prompt, { signal: controller.signal })) {
          if (chunk.done) response = chunk.response;
        }
      } catch (err) {
        // The gate's refusal reads "Model unavailable: <reason>" — say it as it is.
        if (isModelUnavailable(err)) return refuse(err.message);
        return refuse(`photo-adjust: the model could not answer (${controller.signal.aborted ? 'timed out' : (err as Error).message})`);
      } finally {
        clearTimeout(timer);
      }
      const parsed = extractJsonObject(response) as { ops?: unknown; summary?: unknown } | null;
      if (!parsed) return refuse('photo-adjust: the model\'s answer was not a JSON proposal — nothing was proposed');
      let changes;
      try {
        changes = validateAssistantOps(parsed.ops, masks);
      } catch (err) {
        if (err instanceof PhotoOpRefusal) {
          log.warn({ fileId, requestedBy, reason: err.message }, 'photo-adjust: model answer refused');
          return refuse(`photo-adjust: refused the model's answer — ${err.message}`);
        }
        throw err;
      }
      if (!changes.length) return refuse('photo-adjust: the model proposed no change for that instruction');
      const summary = (str(parsed.summary) ?? `${changes.length} suggested changes`).slice(0, 200);
      const proposedOps = mergeOps(currentOps, changes);
      log.info({ event: 'photo.adjust.proposed', fileId, requestedBy, changes: changes.length, model, elapsedMs: Date.now() - startedAt }, 'photo-adjust proposal');
      return {
        status: 'completed',
        output: {
          fileId,
          proposedOps,
          changes,
          changedKinds: [...new Set(changes.map((c) => c.kind))],
          summary,
          model,
          provider: llm.provider,
          referencedMaskIds,
          elapsedMs: Date.now() - startedAt,
        },
      };
    },
  };
}
