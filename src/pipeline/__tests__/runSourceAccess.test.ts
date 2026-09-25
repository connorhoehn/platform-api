jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn(async () => ({})) }, ddbClient: { send: jest.fn() } }));

import { AppError, NotFoundError } from '../../middleware/error-handler';
import { assertRunSourcesReadable, documentSourceIds, unreadableSourcesFor } from '../runSourceAccess';

const ROWS: Record<string, { createdBy: string; viewers?: string[] }> = {
  mine: { createdBy: 'dev-connor' },
  shared: { createdBy: 'dev-frank', viewers: ['dev-connor'] },
  theirs: { createdBy: 'dev-frank' },
};
const access = { getDocumentRow: async (id: string) => ROWS[id] ?? null, getLifecycleOwner: async (id: string) => (id === 'lifecycle-only' ? 'dev-connor' : undefined) };

describe('runSourceAccess', () => {
  it('reads document ids from run-draft and deck shapes, skipping other kinds', () => {
    expect(documentSourceIds([{ kind: 'document', id: 'a' }, { documentId: 'b' }, { kind: 'transcript', documentId: 't' }, { kind: 'document', id: 'a' }, null])).toEqual(['a', 'b']);
    expect(documentSourceIds(undefined)).toEqual([]);
  });

  it('context form: lists every id the user may not read (missing rows included, lifecycle owner honoured)', async () => {
    expect(await unreadableSourcesFor({ sub: 'dev-connor' }, ['mine', 'shared', 'theirs', 'missing', 'lifecycle-only'], access)).toEqual(['theirs', 'missing']);
  });

  it('request form: checks every source and names each failure in one 404', async () => {
    const assertOne = async (_r: unknown, id: string) => { if (id !== 'mine') throw new NotFoundError('Document not found'); };
    await expect(assertRunSourcesReadable({} as never, [{ kind: 'document', id: 'mine' }], { assertOne })).resolves.toBeUndefined();
    const err = await assertRunSourcesReadable({} as never, [{ kind: 'document', id: 'mine' }, { kind: 'document', id: 'x' }, { documentId: 'y' }], { assertOne }).catch((e) => e);
    expect(err.status).toBe(404);
    expect(err.body).toMatchObject({ code: 'source-unreadable', sourceIds: ['x', 'y'] });
  });

  it('request form: a 401 passes through untouched; no sources checks nothing', async () => {
    const assertOne = jest.fn(async () => { throw new AppError(401, 'Authorization required'); });
    await expect(assertRunSourcesReadable({} as never, [{ documentId: 'a' }], { assertOne })).rejects.toMatchObject({ status: 401 });
    const none = jest.fn();
    await assertRunSourcesReadable({} as never, [{ kind: 'recording', id: 'r' }], { assertOne: none });
    expect(none).not.toHaveBeenCalled();
  });
});
