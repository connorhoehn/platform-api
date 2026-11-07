import { backfillMediaOrg, resolveWalkerOrg, BACKFILL_BATCH } from '../backfillOrg';
import type { MediaCatalogRow } from '../../repositories/MediaCatalogRepository';

/** In-memory stand-in for the table: pages by index, adoption re-keys the row. */
function memoryRepo(legacy: number) {
  const rows = new Map<string, Partial<MediaCatalogRow>>();
  for (let i = 0; i < legacy; i++) rows.set(`h${i}`, { fileId: `h${i}`, contentId: `h${i}`, bucket: 'all' });
  rows.set('o1#h9', { fileId: 'o1#h9', contentId: 'h9', orgId: 'o1', bucket: 'o1' });
  return {
    rows,
    listUnscoped: jest.fn(async (limit: number, cursor?: Record<string, unknown>) => {
      const unscoped = [...rows.values()].filter((r) => !r.orgId);
      void cursor;
      const items = unscoped.slice(0, limit) as MediaCatalogRow[];
      return { items, cursor: unscoped.length > limit ? { fileId: 'more' } : undefined };
    }),
    adoptRow: jest.fn(async (row: MediaCatalogRow, orgId: string) => {
      rows.delete(row.fileId);
      rows.set(`${orgId}#${row.contentId}`, { ...row, fileId: `${orgId}#${row.contentId}`, orgId, bucket: orgId });
    }),
  };
}
const logger = () => ({ info: jest.fn(), warn: jest.fn() });

describe('media orgId backfill', () => {
  it('stamps the one configured org onto org-less rows across batches, leaving scoped rows alone', async () => {
    const repo = memoryRepo(BACKFILL_BATCH + 30), log = logger();
    const out = await backfillMediaOrg(repo, log, { MEDIA_WALKER_ORG_ID: 'acme' });
    expect(out).toEqual({ stamped: BACKFILL_BATCH + 30, hidden: 0 });
    expect(repo.listUnscoped).toHaveBeenCalledTimes(2);
    expect([...repo.rows.values()].filter((r) => !r.orgId)).toHaveLength(0);
    expect(repo.rows.get('acme#h0')).toMatchObject({ orgId: 'acme', bucket: 'acme', contentId: 'h0' });
    expect(repo.rows.get('o1#h9')?.orgId).toBe('o1');
    expect(log.info).toHaveBeenCalled();
  });

  it('with no single org, changes nothing and logs the hidden count once', async () => {
    for (const env of [{}, { MEDIA_WALKER_ORG_ID: 'a,b' }]) {
      const repo = memoryRepo(7), log = logger();
      const out = await backfillMediaOrg(repo, log, env);
      expect(out).toEqual({ stamped: 0, hidden: 7 });
      expect(repo.adoptRow).not.toHaveBeenCalled();
      expect(log.warn).toHaveBeenCalledTimes(1);
      expect(log.warn.mock.calls[0][0]).toEqual({ hidden: 7 });
    }
  });

  it('resolves default-org only under the insecure dev switch', () => {
    expect(resolveWalkerOrg({ SKIP_AUTH: 'true', INSECURE_DEV_MODE: 'true' })).toBe('default-org');
    expect(resolveWalkerOrg({ SKIP_AUTH: 'true' })).toBeNull();
  });
});
