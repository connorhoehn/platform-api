# Chat-command plugins

A plugin adds slash commands to a platform-api deployment without editing its source: a module
exporting `{ name, setup }` (default export or `plugin`). `setup` gets four LIVE registries, read on
demand by the routes and the pipeline module, so load order never matters:
| `ctx.`                       | what it does                                                                 |
|------------------------------|------------------------------------------------------------------------------|
| `registerCommand(def)`       | `/trigger` → pipeline; listed by `GET /api/chat/commands`, run by `POST …/:trigger/run` |
| `registerStepHandlers(h)`    | custom step kinds (`org:graph`); land in the Map the pipeline module dispatches from |
| `registerSystemPipeline(e)`  | optional: list the pipeline before its first run (the first run registers it anyway) |
| `registerPanelDataProvider(kind, fn)` | serve `GET /api/chat/panels/:kind/:id` from your store; `null` falls back to the run |

Enable: `CHAT_COMMAND_PLUGINS=./plugins/example-org-graph,@acme/triage-plugin` (a package name, or a
path relative to the platform-api root). A plugin that fails to resolve or throws in `setup` is logged and skipped.
## Minimal plugin — `plugins/example-org-graph.js` (complete, runnable)

```js
const PIPELINE_ID = 'org-graph', STEP_KIND = 'org:graph', panels = new Map();
const graphStep = {
  kind: STEP_KIND,
  async execute(_node, ctx, helpers) {
    const query = String(ctx.query || '').trim(), id = `${PIPELINE_ID}:${helpers.runId}`;
    const panelData = { title: `Org graph: ${query}`,
      nodes: [{ id: 'q', label: query, group: 'center' }, { id: 'ada', label: 'Ada' }],
      edges: [{ from: 'q', to: 'ada', label: 'works with' }] };
    panels.set(id, panelData);
    return { status: 'completed', output: { panelData, result: { kind: 'panel', panel: { kind: 'graph', id, title: panelData.title } } } };
  },
};
module.exports = {
  name: 'org-graph',
  setup(ctx) {
    ctx.registerCommand({
      trigger: '/graph', pipelineId: PIPELINE_ID, description: 'Draw the org graph around a query', requires: { argumentText: true },
      buildDefinition: () => ({ id: PIPELINE_ID, name: 'Org graph', version: 1, status: 'published', publishedVersion: 1,
        nodes: [{ id: 'trigger', type: 'trigger', position: { x: 0, y: 0 }, data: { type: 'trigger', triggerType: 'manual', label: '/graph' } },
                { id: 'graph', type: STEP_KIND, position: { x: 200, y: 0 }, data: { type: STEP_KIND, label: 'Build the graph', retryPolicy: { maxAttempts: 2, backoffMs: 500 } } }],
        edges: [{ id: 'trigger->graph', source: 'trigger', sourceHandle: 'out', target: 'graph', targetHandle: 'in' }] }),
      triggerPayload: (input) => ({ query: input.argumentText.trim().slice(0, 200) }),
    });
    ctx.registerStepHandlers([graphStep]);
    ctx.registerPanelDataProvider('graph', async (id) => panels.get(id) ?? null);
  },
};
```

Conventions: `output.result` (`panel` | `document` | `link`, `src/pipeline/runResult.ts`) is what the card opens and
`output.panelData` what a `panel` shows; every non-trigger node needs a `retryPolicy` (the system-pipeline lint
refuses it otherwise); a `<pipelineId>:<runId>` panel id lets the route fall back to the run. The platform's own
`/search` (`src/chat/plugins/peopleSearch.ts`) is this same contract. Client + server guide: `realtime-examples/docs/chat-command-plugins.md`.
