// The chat-command plugin contract and loader: a fixture plugin loaded from a
// temp path (default export and `plugin` export), failed specs recorded
// without throwing, step handlers landing in the LIVE map the pipeline module
// dispatches from, panel providers registered for the panels route, the
// built-in /search installed through the same contract, and the shipped
// example (`./plugins/example-org-graph`) loading by its env-style spec.

import fs from 'fs';
import os from 'os';
import path from 'path';

const mockPostCard = jest.fn().mockResolvedValue(true);
jest.mock('../../pipeline/runPersistence', () => ({ getRunPersistenceService: () => ({ registerTrigger: jest.fn() }) }));
jest.mock('../../documents/chatRunCard', () => ({
  postRunCardToChannel: (...args: unknown[]) => mockPostCard(...args),
  runCardMetadata: (i: { runId: string }) => ({ kind: 'agent-run', runId: i.runId }),
  str: (v: unknown) => (typeof v === 'string' && v ? v : undefined),
}));
jest.mock('../../repositories', () => ({ profileRepo: { listProfiles: jest.fn().mockResolvedValue([]) } }));
jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => l };
  return { __esModule: true, default: l, withContext: () => l };
});

import type { Logger } from '@connorhoehn/service-runtime';
import type { PipelineNode, StepOutcome } from 'distributed-core/applications/pipeline';
import {
  chatCommandPluginSpecsFromEnv, createChatCommandPluginContext, loadChatCommandPlugins, pluginFromModule,
  resolveChatCommandPluginSpec, installChatCommandPlugin, PLATFORM_API_ROOT, type ChatCommandPlugin,
} from '../chatCommandPlugin';
import { clearChatCommandPipelines, getChatCommandPipeline, listChatCommandPipelines } from '../chatCommandRegistry';
import { clearCustomStepHandlers, getCustomStepHandlers, getCustomStepHandler } from '../../pipeline/customStepHandlers';
import { clearPanelDataProviders, getPanelDataProvider } from '../chatPanelProviders';
import { lintSystemPipeline } from '../../pipeline/systemPipelineLint';
import { installBuiltinChatCommandPlugins } from '../plugins';

const log = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() } as unknown as Logger;
const ctx = () => createChatCommandPluginContext(log);
const node = { id: 'n', type: 'x', position: { x: 0, y: 0 }, data: { type: 'x' } } as unknown as PipelineNode;
const helpers = { runId: 'run-42', ownerNodeId: 'n', emit: jest.fn() } as unknown as Parameters<NonNullable<ReturnType<typeof getCustomStepHandler>>['execute']>[2];

let dir: string;
beforeAll(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-plugins-')); });
afterAll(() => { fs.rmSync(dir, { recursive: true, force: true }); });
beforeEach(() => {
  clearChatCommandPipelines();
  clearCustomStepHandlers();
  clearPanelDataProviders();
  (log.warn as jest.Mock).mockReset();
  (log.info as jest.Mock).mockReset();
});

const FIXTURE = `
const step = { kind: 'acme:triage', async execute(_n, ctx) { return { status: 'completed', output: { triaged: ctx.query, result: { kind: 'link', href: 'https://acme.test/t/1', label: 'Triage' } } }; } };
const plugin = {
  name: 'acme-triage',
  setup(ctx) {
    ctx.registerCommand({ trigger: '/triage', pipelineId: 'acme-triage', description: 'Triage it', requires: { argumentText: true },
      buildDefinition: () => ({ id: 'acme-triage', nodes: [], edges: [] }), triggerPayload: (i) => ({ query: i.argumentText }) });
    ctx.registerStepHandlers([step]);
    ctx.registerPanelDataProvider('triage', async (id) => id === 'triage:known' ? { title: 'Known' } : null);
  },
};
`;
function writeFixture(name: string, body: string): string {
  const file = path.join(dir, `${name}.js`);
  fs.writeFileSync(file, body);
  return file;
}

describe('specs', () => {
  test('CHAT_COMMAND_PLUGINS is comma-separated, trimmed, blanks dropped; unset is none', () => {
    expect(chatCommandPluginSpecsFromEnv({ CHAT_COMMAND_PLUGINS: ' ./plugins/a, @acme/b ,, ' })).toEqual(['./plugins/a', '@acme/b']);
    expect(chatCommandPluginSpecsFromEnv({})).toEqual([]);
    expect(chatCommandPluginSpecsFromEnv({ CHAT_COMMAND_PLUGINS: '  ' })).toEqual([]);
  });
  test('a relative spec resolves under the platform-api root; a package or absolute path passes through', () => {
    expect(resolveChatCommandPluginSpec('./plugins/org-graph')).toBe(path.join(PLATFORM_API_ROOT, 'plugins', 'org-graph'));
    expect(resolveChatCommandPluginSpec('./x', '/srv/api')).toBe('/srv/api/x');
    expect(resolveChatCommandPluginSpec('@acme/plugin')).toBe('@acme/plugin');
    expect(resolveChatCommandPluginSpec('/abs/plugin.js')).toBe('/abs/plugin.js');
    expect(fs.existsSync(path.join(PLATFORM_API_ROOT, 'package.json'))).toBe(true);
  });
  test('pluginFromModule accepts the module, its default export, or `plugin` — and nothing else', () => {
    const p: ChatCommandPlugin = { name: 'p', setup: () => undefined };
    expect(pluginFromModule(p)).toBe(p);
    expect(pluginFromModule({ default: p })).toBe(p);
    expect(pluginFromModule({ plugin: p })).toBe(p);
    expect(pluginFromModule({ default: { plugin: p } })).toBe(p);
    expect(pluginFromModule({ name: 'no-setup' })).toBeUndefined();
    expect(pluginFromModule(null)).toBeUndefined();
  });
});

describe('loadChatCommandPlugins', () => {
  test('loads a fixture plugin from a temp path: command listed, step in the live map, panel provider registered', async () => {
    const file = writeFixture('acme-default', `${FIXTURE}\nmodule.exports = plugin;`);
    const live = getCustomStepHandlers();
    const out = await loadChatCommandPlugins(ctx(), [file]);
    expect(out).toEqual({ loaded: ['acme-triage'], failed: [] });
    expect(listChatCommandPipelines()).toEqual([{ trigger: '/triage', pipelineId: 'acme-triage', description: 'Triage it', requires: { argumentText: true } }]);
    // The map the bootstrap hands to PipelineModule is THIS object; the plugin's kind is in it now, so the next run dispatches it.
    expect(getCustomStepHandlers()).toBe(live);
    const handler = live.get('acme:triage')!;
    expect(handler.kind).toBe('acme:triage');
    const outcome: StepOutcome = await handler.execute(node, { query: 'q' }, helpers);
    expect(outcome).toMatchObject({ status: 'completed', output: { triaged: 'q', result: { kind: 'link' } } });
    await expect(getPanelDataProvider('triage')!('triage:known', { userId: 'u' })).resolves.toEqual({ title: 'Known' });
    await expect(getPanelDataProvider('triage')!('triage:other', { userId: 'u' })).resolves.toBeNull();
    expect(log.info).toHaveBeenCalledWith(expect.objectContaining({ plugin: 'acme-triage' }), 'chat command plugin loaded');
  });

  test('a `plugin` named export loads too', async () => {
    const file = writeFixture('acme-named', `${FIXTURE}\nexports.plugin = plugin;`);
    await expect(loadChatCommandPlugins(ctx(), [file])).resolves.toEqual({ loaded: ['acme-triage'], failed: [] });
  });

  test('records every failed spec — unresolvable, no plugin exported, setup throws — and keeps loading the rest', async () => {
    const noPlugin = writeFixture('no-plugin', 'module.exports = { name: "x" };');
    const throws = writeFixture('throws', 'module.exports = { name: "boom", setup() { throw new Error("setup exploded"); } };');
    const good = writeFixture('good', `${FIXTURE}\nmodule.exports = plugin;`);
    const out = await loadChatCommandPlugins(ctx(), [path.join(dir, 'does-not-exist'), noPlugin, throws, good, '  ']);
    expect(out.loaded).toEqual(['acme-triage']);
    expect(out.failed.map((f) => f.spec)).toEqual([path.join(dir, 'does-not-exist'), noPlugin, throws]);
    expect(out.failed[0].error).toMatch(/Cannot find module/);
    expect(out.failed[1].error).toMatch(/exports no chat command plugin/);
    expect(out.failed[2].error).toBe('setup exploded');
    expect(log.warn).toHaveBeenCalledTimes(3);
  });

  test('a step kind that shadows a built-in, or whose key disagrees with handler.kind, is a failed plugin — never in the map', async () => {
    const shadow: ChatCommandPlugin = { name: 'shadow', setup: (c) => c.registerStepHandlers([{ kind: 'llm', execute: async () => ({ status: 'completed' }) }]) };
    const mismatch: ChatCommandPlugin = { name: 'mismatch', setup: (c) => c.registerStepHandlers(new Map([['a:x', { kind: 'a:y', execute: async () => ({ status: 'completed' }) }]])) };
    const out = await loadChatCommandPlugins(ctx(), [shadow, mismatch]);
    expect(out.loaded).toEqual([]);
    expect(out.failed[0].error).toMatch(/shadows a built-in NodeType/);
    expect(out.failed[1].error).toMatch(/does not match key/);
    expect(getCustomStepHandlers().size).toBe(0);
    // `agent` is the one canonical kind DC lets a consumer override.
    expect(() => installChatCommandPlugin(ctx(), { name: 'a', setup: (c) => c.registerStepHandlers([{ kind: 'agent', execute: async () => ({ status: 'completed' }) }]) })).not.toThrow();
    expect(getCustomStepHandler('agent')).toBeDefined();
  });

  test('an injected requireModule and rootDir stand in for require', async () => {
    const requireModule = jest.fn().mockReturnValue({ default: { name: 'virtual', setup: () => undefined } });
    await expect(loadChatCommandPlugins(ctx(), ['./plugins/virtual'], { rootDir: '/srv/api', requireModule })).resolves.toEqual({ loaded: ['virtual'], failed: [] });
    expect(requireModule).toHaveBeenCalledWith('/srv/api/plugins/virtual');
  });
});

describe('the built-in /search goes through the same contract', () => {
  test('installBuiltinChatCommandPlugins registers the command and both step kinds', () => {
    installBuiltinChatCommandPlugins(ctx());
    expect(getChatCommandPipeline('/search')?.pipelineId).toBe('people-search');
    expect(getCustomStepHandler('people:search')?.kind).toBe('people:search');
    expect(getCustomStepHandler('people:publish')?.kind).toBe('people:publish');
  });
  test('and loads as a module spec relative to the root, like an external one would', async () => {
    const out = await loadChatCommandPlugins(ctx(), ['./src/chat/plugins/peopleSearch']);
    expect(out).toEqual({ loaded: ['people-search'], failed: [] });
    expect(getChatCommandPipeline('/search')).toBeDefined();
  });
});

describe('the shipped example: CHAT_COMMAND_PLUGINS=./plugins/example-org-graph', () => {
  test('loads, lists /graph, its step answers with a graph panel + panelData, and its provider serves that panel', async () => {
    const out = await loadChatCommandPlugins(ctx(), chatCommandPluginSpecsFromEnv({ CHAT_COMMAND_PLUGINS: './plugins/example-org-graph' }));
    expect(out).toEqual({ loaded: ['org-graph'], failed: [] });
    const def = getChatCommandPipeline('/graph')!;
    expect(def).toMatchObject({ pipelineId: 'org-graph', requires: { argumentText: true } });
    const input = { trigger: '/graph', channel: 'room:r', text: '/graph platform team', argumentText: 'platform team', requestedBy: 'u', requestedByName: 'U' };
    expect(def.triggerPayload(input)).toEqual({ query: 'platform team' });
    expect(lintSystemPipeline(def.buildDefinition(input))).toEqual([]);

    const step = getCustomStepHandler('org:graph')!;
    const outcome = await step.execute(node, { query: 'platform team' }, helpers);
    expect(outcome).toMatchObject({
      status: 'completed',
      output: { result: { kind: 'panel', panel: { kind: 'graph', id: 'org-graph:run-42', title: 'Org graph: platform team' } }, panelData: { title: 'Org graph: platform team' } },
    });
    const provider = getPanelDataProvider('graph')!;
    await expect(provider('org-graph:run-42', { userId: 'u' })).resolves.toEqual((outcome as { output: { panelData: unknown } }).output.panelData);
    await expect(provider('people-search:run-1', { userId: 'u' })).resolves.toBeNull();
  });
});
