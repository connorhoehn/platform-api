// The platform's own chat-command plugins — written against the same contract
// an external project uses (src/chat/chatCommandPlugin.ts) and installed
// through the same `installChatCommandPlugin`, so the contract is exercised by
// a real plugin on every boot. External plugins come from CHAT_COMMAND_PLUGINS
// (loaded in src/index.ts); adding a built-in is adding it to the list below.
//
// Installation is synchronous so the chat-commands route lists `/search` from
// its first request: every built-in's `setup` is sync. Step handlers land in
// the live registry (src/pipeline/customStepHandlers.ts) the pipeline module
// runs on, so the order between this and the pipeline bootstrap is irrelevant.
import { createChatCommandPluginContext, installChatCommandPlugin, type ChatCommandPlugin, type ChatCommandPluginContext } from '../chatCommandPlugin';
import { peopleSearchPlugin } from './peopleSearch';
import log from '../../lib/logger';

export const builtinChatCommandPlugins: readonly ChatCommandPlugin[] = [peopleSearchPlugin];

/** Install every built-in plugin. Idempotent: commands replace by trigger, handlers by kind. */
export function installBuiltinChatCommandPlugins(ctx: ChatCommandPluginContext = createChatCommandPluginContext()): void {
  for (const plugin of builtinChatCommandPlugins) {
    try {
      const r = installChatCommandPlugin(ctx, plugin);
      if (r && typeof (r as Promise<void>).then === 'function') {
        (r as Promise<void>).catch((err: unknown) => log.error({ plugin: plugin.name, err: err instanceof Error ? err.message : String(err) }, 'built-in chat command plugin failed'));
      }
    } catch (err) {
      log.error({ plugin: plugin.name, err: err instanceof Error ? err.message : String(err) }, 'built-in chat command plugin failed');
    }
  }
}
