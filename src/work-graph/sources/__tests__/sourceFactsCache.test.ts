import { withFullReads } from '../../buildMemo';
import { pipelineDefinitionsCache } from '../../../pipeline/definitions-cache';
import { cachedDefinitionRow, cachedRevisionFacts, clearSourceFactsCaches } from '../sourceFactsCache';

describe('sourceFactsCache (NFR #208)', () => {
  beforeEach(() => clearSourceFactsCaches());

  it('reuses a deck revision read until the row moves to a new saved state', async () => {
    const load = jest.fn(async () => ['facts']);
    await cachedRevisionFacts('doc', 3, 'v3', load);
    await cachedRevisionFacts('doc', 3, 'v3', load);
    expect(load).toHaveBeenCalledTimes(1);
    await cachedRevisionFacts('doc', 4, 'v4', load);
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('never caches a row without a revision count, a failed read, or a full-read build', async () => {
    const load = jest.fn(async () => ['facts']);
    await cachedRevisionFacts('doc', undefined, undefined, load);
    await cachedRevisionFacts('doc', undefined, undefined, load);
    expect(load).toHaveBeenCalledTimes(2);

    const failing = jest.fn().mockRejectedValueOnce(new Error('down')).mockResolvedValue(['ok']);
    await expect(cachedRevisionFacts('doc', 1, 'v1', failing)).rejects.toThrow('down');
    await expect(cachedRevisionFacts('doc', 1, 'v1', failing)).resolves.toEqual(['ok']);
    expect(failing).toHaveBeenCalledTimes(2);

    await withFullReads(() => cachedRevisionFacts('doc', 1, 'v1', failing));
    expect(failing).toHaveBeenCalledTimes(3);
  });

  it('drops a definition row when a definition write is announced', async () => {
    pipelineDefinitionsCache.__setSnapshotForTests([]);
    const load = jest.fn(async () => ({ Item: { name: 'Agent' } }));
    await cachedDefinitionRow('u', 'p', load);
    await cachedDefinitionRow('u', 'p', load);
    expect(load).toHaveBeenCalledTimes(1);
    const refresh = jest.spyOn(pipelineDefinitionsCache as unknown as { loader: () => Promise<unknown[]> }, 'loader').mockResolvedValue([]);
    await pipelineDefinitionsCache.refresh();
    await cachedDefinitionRow('u', 'p', load);
    expect(load).toHaveBeenCalledTimes(2);
    refresh.mockRestore();
  });
});
