// Two saves of the same presentation at once. Before the conditional write,
// both read "3 revisions", both computed v4, both wrote their bytes under
// deck-<doc>-v4.*, and the second row write silently replaced the first:
// one revision lost, and the survivor's row pointing at the other's .pptx.

jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { GetCommand, PutCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import {
  DdbPresentationDocumentStore,
  PresentationRevisionConflictError,
  resolveRevisionSlides,
  type PresentationDocumentRow,
} from '../presentationDocuments';

function conditionalFailure(): Error {
  return Object.assign(new Error('The conditional request failed'), { name: 'ConditionalCheckFailedException' });
}

/**
 * A one-table DynamoDB that evaluates the two conditions the store uses, and
 * lets every Get through a gate so a test can make both writers read before
 * either writes — the interleaving that loses a revision.
 */
function racingTable(initial: PresentationDocumentRow | null, readers: number) {
  let row: PresentationDocumentRow | null = initial ? structuredClone(initial) : null;
  let waiting: Array<() => void> = [];
  let gateOpen = false;
  const client = {
    async send(command: unknown) {
      const input = (command as { input: Record<string, any> }).input;
      if (command instanceof GetCommand) {
        const snapshot = row ? structuredClone(row) : null;
        if (!gateOpen) {
          await new Promise<void>((resolve) => {
            waiting.push(resolve);
            if (waiting.length >= readers) { gateOpen = true; waiting.forEach((go) => go()); waiting = []; }
          });
        }
        return { Item: snapshot ?? undefined };
      }
      if (command instanceof PutCommand) {
        if (input.ConditionExpression === 'attribute_not_exists(documentId)' && row) throw conditionalFailure();
        row = structuredClone(input.Item) as PresentationDocumentRow;
        return {};
      }
      if (command instanceof UpdateCommand) {
        const expected = input.ExpressionAttributeValues[':expectedCount'];
        if (!row || (row.revisions?.length ?? 0) !== expected) throw conditionalFailure();
        const values = input.ExpressionAttributeValues;
        row = {
          ...row,
          revisions: values[':revisions'],
          revisionSlides: values[':revisionSlides'],
          revisionPreviews: values[':revisionPreviews'],
        };
        return {};
      }
      return {};
    },
  } as unknown as DynamoDBDocumentClient;
  return { client, current: () => row };
}

const slides = (tag: string) => [
  { id: 'cover', title: 'Cover', svg: `<svg>${tag} cover</svg>` },
  { id: 'sources', title: 'Sources', svg: `<svg>${tag} sources</svg>` },
];

function seededRow(count: number): PresentationDocumentRow {
  const revisions = Array.from({ length: count }, (_v, index) => ({
    id: `v${index + 1}`, label: `v${index + 1}`, createdAt: '2026-09-22T00:00:00.000Z', createdBy: 'dev-connor',
    slideCount: 0, asset: { key: `k${index + 1}`, contentType: 'x', sizeBytes: 1 }, slides: [],
  }));
  return { documentId: 'doc-1', title: 'Sprint review', type: 'presentation', revisions, revisionSlides: {}, revisionPreviews: {} };
}

describe('two saves of the same presentation at once', () => {
  it('a pipeline append and another append both land: v5 and v6, each with its own bytes', async () => {
    const table = racingTable(seededRow(4), 2);
    const blobStore = new InMemoryImageBlobStore();
    const store = new DdbPresentationDocumentStore({ docClient: table.client, blobStore, mirror: null });

    const [a, b] = await Promise.all([
      store.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'dev-connor', pptx: Buffer.from('PK-connor'), slides: slides('connor') }),
      store.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'dev-frank', pptx: Buffer.from('PK-frank'), slides: slides('frank') }),
    ]);

    expect([a.revision.id, b.revision.id].sort()).toEqual(['v5', 'v6']);
    expect(table.current()!.revisions!.map((revision) => revision.id)).toEqual(['v1', 'v2', 'v3', 'v4', 'v5', 'v6']);
    // Nothing overwritten: every recorded key reads back the bytes its own writer wrote.
    for (const [result, who] of [[a, 'connor'], [b, 'frank']] as const) {
      const recorded = table.current()!.revisions!.find((revision) => revision.id === result.revision.id)!;
      expect((await blobStore.getBlob(recorded.asset.key))!.body.toString('utf8')).toBe(`PK-${who}`);
      // v5 is no longer the head: its slide list is read back from its manifest (NFR #36).
      const recordedSlides = (await resolveRevisionSlides(store, recorded))!;
      expect(recordedSlides[0]!.svg).toBeUndefined();
      expect((await blobStore.getBlob(recordedSlides[0]!.asset.key))!.body.toString('utf8')).toBe(`<svg>${who} cover</svg>`);
    }
  });

  it('two editors saving on top of v4: one gets v5, the other a conflict naming v5', async () => {
    const table = racingTable(seededRow(4), 2);
    const store = new DdbPresentationDocumentStore({ docClient: table.client, blobStore: new InMemoryImageBlobStore(), mirror: null });

    const outcomes = await Promise.allSettled([
      store.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'dev-connor', pptx: Buffer.from('PK1'), slides: slides('connor'), expectedHead: 'v4' }),
      store.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'dev-frank', pptx: Buffer.from('PK2'), slides: slides('frank'), expectedHead: 'v4' }),
    ]);

    const won = outcomes.filter((outcome) => outcome.status === 'fulfilled');
    const lost = outcomes.filter((outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected');
    expect(won).toHaveLength(1);
    expect(lost).toHaveLength(1);
    const conflict = lost[0]!.reason as PresentationRevisionConflictError;
    expect(conflict).toBeInstanceOf(PresentationRevisionConflictError);
    expect(conflict.head?.id).toBe('v5');
    expect(conflict.revisionCount).toBe(5);
    expect(table.current()!.revisions!.map((revision) => revision.id)).toEqual(['v1', 'v2', 'v3', 'v4', 'v5']);
  });

  it('refuses up front a save whose base is already behind, before writing any bytes', async () => {
    const table = racingTable(seededRow(5), 1);
    const blobStore = new InMemoryImageBlobStore();
    const put = jest.spyOn(blobStore, 'putBlob');
    const store = new DdbPresentationDocumentStore({ docClient: table.client, blobStore, mirror: null });
    await expect(store.appendRevision({
      documentId: 'doc-1', title: 'Deck', createdBy: 'dev-frank', pptx: Buffer.from('PK'), slides: slides('frank'), expectedHead: 'v4',
    })).rejects.toMatchObject({ name: 'PresentationRevisionConflictError', head: { id: 'v5' } });
    expect(put).not.toHaveBeenCalled();
  });

  it('two first saves of a brand-new document: both land as v1 and v2', async () => {
    const table = racingTable(null, 2);
    const store = new DdbPresentationDocumentStore({ docClient: table.client, blobStore: new InMemoryImageBlobStore(), mirror: null });
    const results = await Promise.all([
      store.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'a', pptx: Buffer.from('A'), slides: slides('a') }),
      store.appendRevision({ documentId: 'doc-1', title: 'Deck', createdBy: 'b', pptx: Buffer.from('B'), slides: slides('b') }),
    ]);
    expect(results.map((result) => result.revision.id).sort()).toEqual(['v1', 'v2']);
    expect(table.current()!.revisions).toHaveLength(2);
  });
});
