// `document:check-sources` — the gate a DRAFTABLE document pipeline runs first.
//
// `document:read` joins a document as the agent and checks no one, and
// `document:apply` writes the run's `documentId` the same way. A pipeline a
// run draft may start (systemPipelines.ts, `draftable: true`) must therefore
// authorize, as the run's `requestedBy`, everything it reads and writes:
//
//   - `document:read` on every document source (runSourceAccess.ts); any that
//     fails refuses the run naming every failing id;
//   - `document:edit` on the target (`documentId`, the page the draft is on);
//   - the target is not one of the sources — the source is never modified.
//
// Refusals are `retry: false`: a retry would be refused the same way.
// On success it hands the read step `sourceDocumentId` (the first source).

import type { PipelineNode, StepOutcome } from 'distributed-core/applications/pipeline';
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import { documentSourceIds, runSourceAccessDeps, unreadableSourcesFor, MAX_RUN_SOURCE_DOCUMENTS, type RunSourceAccessDeps } from '../pipeline/runSourceAccess';
import { canEditDocument, type EditableDocumentRow } from '../deck/documentEditAccess';
import log from '../lib/logger';

export const DOCUMENT_CHECK_SOURCES_KIND = 'document:check-sources';

function str(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

function targetIdOf(context: Record<string, unknown>): string | undefined {
  const trigger = (context.steps as Record<string, { output?: Record<string, unknown> }> | undefined)?.trigger?.output;
  return str(context.documentId) ?? str(trigger?.documentId);
}

export interface DocumentCheckSourcesDeps {
  /** Rows and policy for both checks; the platform's by default. */
  access?: RunSourceAccessDeps;
  canEdit?: typeof canEditDocument;
}

export function createDocumentCheckSourcesHandler(deps: DocumentCheckSourcesDeps = {}): DescribedStepHandler {
  const refuse = (error: string): StepOutcome => ({ status: 'failed', error, retry: false } as StepOutcome);
  return {
    kind: DOCUMENT_CHECK_SOURCES_KIND,
    description:
      'Checks, as the requester, that every source document may be read and the target page may be edited, and that the target is not a source. Refuses the run otherwise.',
    reads: 'sources ({kind:"document", id}), documentId (the target), requestedBy, organizationId.',
    emits: 'sourceDocumentId (the first source, for the read step), sourceIds, targetDocumentId.',
    async execute(_node: PipelineNode, context: Record<string, unknown>): Promise<StepOutcome> {
      const ids = documentSourceIds(context.sources);
      if (!ids.length) return refuse('document: no source document — nothing was written');
      if (ids.length > MAX_RUN_SOURCE_DOCUMENTS) return refuse(`document: at most ${MAX_RUN_SOURCE_DOCUMENTS} source documents — nothing was written`);
      const target = targetIdOf(context);
      if (!target) return refuse('document: the run names no target document — nothing was written');
      if (ids.includes(target)) return refuse(`document: the target ${target} is also a source; the source is never modified — nothing was written`);
      const requestedBy = str(context.requestedBy) ?? str((context.trigger as Record<string, unknown> | undefined)?.requestedBy);
      if (!requestedBy) return refuse('document: the run names no requester, so its documents cannot be checked — nothing was written');
      const orgId = str(context.organizationId);
      const user = { sub: requestedBy, ...(orgId ? { orgId } : {}) };
      const access = deps.access ?? runSourceAccessDeps();

      // 1. document:read on every source, as the requester.
      let unreadable: string[];
      try {
        unreadable = await unreadableSourcesFor(user, ids, access);
      } catch (err) {
        return refuse(`document: the sources could not be checked (${err instanceof Error ? err.message : String(err)}) — nothing was written`);
      }
      if (unreadable.length) {
        log.warn({ requestedBy, unreadable }, 'document: a source is not readable by the requester');
        return refuse(`document: source ${unreadable.length === 1 ? 'document' : 'documents'} ${unreadable.join(', ')} cannot be read by the requester — nothing was written`);
      }

      // 2. document:edit on the target. No row yet means a page being created
      // for this run (canEditDocument allows it); a lifecycle owner stands in
      // for a row, as it does for the read check.
      try {
        let row: EditableDocumentRow | null = await access.getDocumentRow(target);
        if (!row) {
          const owner = await access.getLifecycleOwner(target);
          row = owner ? { createdBy: owner } : null;
        }
        const decision = await (deps.canEdit ?? canEditDocument)(row, user, target);
        if (!decision.allow) {
          log.warn({ requestedBy, target }, 'document: the requester may not edit the target');
          return refuse(`document: the requester cannot edit the target document ${target} — nothing was written`);
        }
      } catch (err) {
        return refuse(`document: the target could not be checked (${err instanceof Error ? err.message : String(err)}) — nothing was written`);
      }

      return { status: 'completed', output: { sourceDocumentId: ids[0], sourceIds: ids, targetDocumentId: target } };
    },
  };
}
