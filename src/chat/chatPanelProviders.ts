// Panel data providers — how a plugin serves `GET /api/chat/panels/:kind/:id` from its own store.
//
// By default the panels route answers from the RUN: the id names a run, the
// run's final context carries `panelData`, that is what the panel shows. A
// plugin whose panel outlives its runs (a graph it keeps in a table, a search
// it can redo) registers a provider for its panel kind here; the route asks
// the provider first and falls back to the run context when the provider
// answers null. Dependency-free on purpose so the route stays light.

export interface PanelDataProviderContext { userId: string }

/** The data a panel of this kind shows for this id, or null when the provider does not know the id. */
export type PanelDataProvider = (id: string, ctx: PanelDataProviderContext) => Promise<unknown | null> | unknown | null;

const PROVIDERS = new Map<string, PanelDataProvider>();

/** One provider per kind; a later registration replaces an earlier one. */
export function registerPanelDataProvider(kind: string, provider: PanelDataProvider): void {
  const k = String(kind ?? '').trim();
  if (!k) throw new Error('a panel data provider needs a kind');
  if (typeof provider !== 'function') throw new Error(`panel data provider for '${k}' must be a function`);
  PROVIDERS.set(k, provider);
}

export function getPanelDataProvider(kind: string): PanelDataProvider | undefined {
  return PROVIDERS.get(kind);
}

export function listPanelDataProviderKinds(): string[] {
  return [...PROVIDERS.keys()];
}

/** Test seam. */
export function clearPanelDataProviders(): void {
  PROVIDERS.clear();
}
