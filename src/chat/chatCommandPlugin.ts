// Chat-command plugins — how a project adds slash commands to a platform-api
// deployment WITHOUT editing its source.
//
// A plugin is a module that exports `{ name, setup }` (as the default export,
// or as `plugin`). `setup` receives a context with the four registries a
// command can need and registers into them; nothing else about the process is
// exposed. Plugins are named in `CHAT_COMMAND_PLUGINS` — comma-separated
// module specifiers, a package name (`@acme/org-graph-plugin`) or a path
// relative to the platform-api root (`./plugins/org-graph`) — and loaded once
// at boot by `loadChatCommandPlugins`. A plugin that fails to load or throws
// in `setup` is logged and skipped; the platform boots without it.
//
// Why loading can happen at ANY point of boot: the registries are all live.
//   - commands       → the chat-command registry (`GET /api/chat/commands` reads it on every request)
//   - step handlers  → src/pipeline/customStepHandlers.ts, the Map the pipeline module runs on by reference
//   - system pipelines → src/pipeline/systemPipelines.ts (the definitions pages read it on every request)
//   - panel providers → src/chat/chatPanelProviders.ts (the panels route asks on every request)
// So `index.ts` fires the loader alongside the pipeline bootstrap and neither
// has to wait for the other. The platform's own `/search` (src/chat/plugins/
// peopleSearch.ts) is installed through the same `installChatCommandPlugin`
// as an external one, which is what keeps this contract honest.
import path from 'path';
import type { Logger } from '@connorhoehn/service-runtime';
import type { StepHandler } from 'distributed-core/applications/pipeline';
import { registerChatCommandPipeline } from './chatCommandRegistry';
import { registerSystemPipeline } from '../pipeline/systemPipelines';
import { registerCustomStepHandlers } from '../pipeline/customStepHandlers';
import { registerPanelDataProvider, type PanelDataProvider } from './chatPanelProviders';
import log from '../lib/logger';

export interface ChatCommandPluginContext {
  /** Register a slash command backed by a pipeline (`/graph <query>`); idempotent by trigger. */
  registerCommand: typeof registerChatCommandPipeline;
  /** Register the step kinds the command's definition names; live — dispatchable by the next run. */
  registerStepHandlers: (handlers: Map<string, StepHandler> | StepHandler[]) => void;
  /** Register the pipeline eagerly so it lists on the definitions pages before its first run (optional; the first run registers it too). */
  registerSystemPipeline: typeof registerSystemPipeline;
  /** Serve `GET /api/chat/panels/:kind/:id` for a panel kind from the plugin's own store; null falls back to the run. */
  registerPanelDataProvider: (kind: string, provider: (id: string, ctx: { userId: string }) => Promise<unknown | null>) => void;
  log: Logger;
}

export interface ChatCommandPlugin {
  /** Stable, unique; what the boot log and `loaded` report. */
  name: string;
  setup: (ctx: ChatCommandPluginContext) => void | Promise<void>;
}

export interface ChatCommandPluginLoadResult {
  loaded: string[];
  failed: Array<{ spec: string; error: string }>;
}

export interface LoadChatCommandPluginsOptions {
  /** What a relative spec (`./plugins/x`) is resolved against. Default: the platform-api root. */
  rootDir?: string;
  /** How a resolved specifier becomes a module. Default: `require`. */
  requireModule?: (resolved: string) => unknown;
}

/** The platform-api root — `src/chat` and `dist/chat` are both two levels below it. */
export const PLATFORM_API_ROOT = path.resolve(__dirname, '..', '..');

/** The env var the loader reads. */
export const CHAT_COMMAND_PLUGINS_ENV = 'CHAT_COMMAND_PLUGINS';

export function createChatCommandPluginContext(logger: Logger = log): ChatCommandPluginContext {
  return {
    registerCommand: registerChatCommandPipeline,
    registerStepHandlers: (handlers) => registerCustomStepHandlers(handlers),
    registerSystemPipeline,
    registerPanelDataProvider: (kind, provider) => registerPanelDataProvider(kind, provider as PanelDataProvider),
    log: logger,
  };
}

/** `CHAT_COMMAND_PLUGINS="./plugins/org-graph, @acme/triage"` → the two specs; unset or blank → none. */
export function chatCommandPluginSpecsFromEnv(env: NodeJS.ProcessEnv = process.env): string[] {
  return String(env[CHAT_COMMAND_PLUGINS_ENV] ?? '').split(',').map((s) => s.trim()).filter(Boolean);
}

/** A relative spec is a path under the platform-api root; anything else (a package, an absolute path) is passed through. */
export function resolveChatCommandPluginSpec(spec: string, rootDir: string = PLATFORM_API_ROOT): string {
  const s = spec.trim();
  return s.startsWith('./') || s.startsWith('../') ? path.resolve(rootDir, s) : s;
}

function isPlugin(v: unknown): v is ChatCommandPlugin {
  return !!v && typeof v === 'object' && typeof (v as ChatCommandPlugin).name === 'string' && (v as ChatCommandPlugin).name.trim() !== ''
    && typeof (v as ChatCommandPlugin).setup === 'function';
}

/** The plugin a loaded module exports: the module itself, its default export, or its `plugin` export (either level). */
export function pluginFromModule(mod: unknown): ChatCommandPlugin | undefined {
  if (!mod || typeof mod !== 'object') return undefined;
  const m = mod as { default?: unknown; plugin?: unknown };
  const dflt = m.default as { plugin?: unknown } | undefined;
  for (const candidate of [m, m.plugin, m.default, dflt && typeof dflt === 'object' ? dflt.plugin : undefined]) {
    if (isPlugin(candidate)) return candidate;
  }
  return undefined;
}

/** Run one plugin's `setup` against the context. Throws what `setup` throws; the loader turns that into a `failed` entry. */
export function installChatCommandPlugin(ctx: ChatCommandPluginContext, plugin: ChatCommandPlugin): void | Promise<void> {
  if (!isPlugin(plugin)) throw new Error('not a chat command plugin: needs a name and a setup()');
  return plugin.setup(ctx);
}

// eslint-disable-next-line @typescript-eslint/no-require-imports
const defaultRequire = (resolved: string): unknown => require(resolved);

/**
 * Load plugins in order. A spec is a module specifier (see the header) or,
 * for the platform's own built-ins, the plugin object itself. Every failure —
 * a module that cannot be resolved, one that exports no plugin, a `setup`
 * that throws — is logged with its spec and reported in `failed`; nothing is
 * thrown, so a bad plugin never keeps the platform from booting.
 */
export async function loadChatCommandPlugins(
  ctx: ChatCommandPluginContext,
  specs: ReadonlyArray<string | ChatCommandPlugin>,
  options: LoadChatCommandPluginsOptions = {},
): Promise<ChatCommandPluginLoadResult> {
  const requireModule = options.requireModule ?? defaultRequire;
  const loaded: string[] = [];
  const failed: Array<{ spec: string; error: string }> = [];
  for (const spec of specs) {
    const specLabel = typeof spec === 'string' ? spec : `<plugin ${spec?.name ?? '?'}>`;
    try {
      let plugin: ChatCommandPlugin | undefined;
      if (typeof spec === 'string') {
        if (!spec.trim()) continue;
        const mod = requireModule(resolveChatCommandPluginSpec(spec, options.rootDir));
        plugin = pluginFromModule(mod);
        if (!plugin) throw new Error('module exports no chat command plugin (default export or `plugin` with { name, setup })');
      } else {
        plugin = spec;
      }
      await installChatCommandPlugin(ctx, plugin);
      loaded.push(plugin.name);
      ctx.log.info({ plugin: plugin.name, spec: specLabel }, 'chat command plugin loaded');
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      failed.push({ spec: specLabel, error });
      ctx.log.warn({ spec: specLabel, err: error }, 'chat command plugin skipped');
    }
  }
  return { loaded, failed };
}
