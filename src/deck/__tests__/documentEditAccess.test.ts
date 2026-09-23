// NFR #41: who may change a document — the rule every deck write path shares.
import { canEditDocument, documentRoleOf, editRefusal } from '../documentEditAccess';

const row = { createdBy: 'dev-connor', editors: ['dev-erin'], viewers: ['dev-frank'] };

describe('document edit access', () => {
  it.each([
    ['dev-connor', 'owner', true],
    ['dev-erin', 'editor', true],
    ['dev-frank', 'viewer', false],
    ['dev-mallory', 'stranger', false],
  ])('%s is %s → allow=%s', async (sub, role, allow) => {
    expect(documentRoleOf(row, sub)).toBe(role);
    const decision = await canEditDocument(row, { sub }, 'doc-1');
    expect(decision.allow).toBe(allow);
    if (!decision.allow) expect(decision.status).toBe(403);
  });

  it('fails closed: no owner on the row, or no caller, is a refusal', async () => {
    expect((await canEditDocument({ editors: ['dev-erin'] }, { sub: 'dev-mallory' }, 'doc-1')).allow).toBe(false);
    expect((await canEditDocument(row, { sub: '' }, 'doc-1')).allow).toBe(false);
    expect((await canEditDocument(row, undefined, 'doc-1')).allow).toBe(false);
  });

  it('a row that does not exist yet is the caller\'s to create', async () => {
    expect((await canEditDocument(null, { sub: 'dev-mallory' }, 'doc-new')).allow).toBe(true);
  });

  it('tells a viewer they can view but not change it', () => {
    expect(editRefusal(row, 'dev-frank')).toMatch(/view this document but not change it/);
    expect(editRefusal(row, 'dev-mallory')).toMatch(/owner or an editor/);
  });

  it('assertCanEditDocument throws a 403 forbidden naming the document, and passes owner/editor/new rows', async () => {
    const { assertCanEditDocument, DocumentEditForbiddenError } = await import('../documentEditAccess');
    const row = { createdBy: 'u-owner', editors: ['u-ed'], viewers: ['u-view'] };
    await expect(assertCanEditDocument(row, { sub: 'u-owner' }, 'd1')).resolves.toBeUndefined();
    await expect(assertCanEditDocument(row, { sub: 'u-ed' }, 'd1')).resolves.toBeUndefined();
    await expect(assertCanEditDocument(null, { sub: 'u-anyone' }, 'd1')).resolves.toBeUndefined();
    for (const [sub, role] of [['u-view', 'viewer'], ['u-x', 'stranger']] as const) {
      const err = await assertCanEditDocument(row, { sub }, 'd1').catch((e: unknown) => e);
      expect(err).toBeInstanceOf(DocumentEditForbiddenError);
      expect((err as InstanceType<typeof DocumentEditForbiddenError>).status).toBe(403);
      expect((err as InstanceType<typeof DocumentEditForbiddenError>).code).toBe('forbidden');
      expect((err as InstanceType<typeof DocumentEditForbiddenError>).documentId).toBe('d1');
      expect((err as InstanceType<typeof DocumentEditForbiddenError>).role).toBe(role);
    }
  });
});
