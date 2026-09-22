// Boot-time wiring for the capability steps an agent loop's plan expands into.
// Same shape as registerDeckHandlers / registerVisionHandlers; merged into the
// custom-handler map in bootstrap.ts.

import type { StepHandler } from 'distributed-core/applications/pipeline';
import { createMeetingStepHandler } from './meetingStepHandler';
import { createAgendaStepHandler } from './agendaStepHandler';

export function buildLoopStepHandlers(): Map<string, StepHandler> {
  const handlers = new Map<string, StepHandler>();
  for (const handler of [createMeetingStepHandler(), createAgendaStepHandler()]) {
    handlers.set(handler.kind, handler);
  }
  return handlers;
}
