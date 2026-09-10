// Chat-command plugins — commands written against the registry the way an
// embedder would write them, each in its own module. Two seams:
//
//   registerChatCommandPlugins()   → the commands, called where the built-ins are registered
//   buildChatCommandPluginHandlers() → their custom step kinds, merged in the pipeline bootstrap
//
// Adding a plugin is adding it to both lists below; nothing else changes.
import type { StepHandler } from 'distributed-core/applications/pipeline';
import { registerPeopleSearchCommand, buildPeopleSearchHandlers } from './peopleSearch';

export function registerChatCommandPlugins(): void {
  registerPeopleSearchCommand();
}

export function buildChatCommandPluginHandlers(): Map<string, StepHandler> {
  const m = new Map<string, StepHandler>();
  for (const src of [buildPeopleSearchHandlers()]) for (const [k, h] of src) m.set(k, h);
  return m;
}
