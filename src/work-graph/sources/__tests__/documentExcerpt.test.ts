import { describe, it, expect } from '@jest/globals';
import { gzipSync } from 'node:zlib';
import { GetCommand, QueryCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { EXCERPT_MAX, excerptOfUpdate, readDocumentExcerpt } from '../documentExcerpt';

// eslint-disable-next-line @typescript-eslint/no-require-imports
const Y = require('yjs');

function page(blocks: Array<{ kind: 'heading' | 'paragraph'; text: string }>): Uint8Array {
  const doc = new Y.Doc();
  const body = doc.getXmlFragment('body');
  body.insert(0, blocks.map((block) => {
    const el = new Y.XmlElement(block.kind);
    el.insert(0, [new Y.XmlText(block.text)]);
    return el;
  }));
  return Y.encodeStateAsUpdate(doc);
}

describe('document excerpt (NFR #209)', () => {
  it('is the first prose block, never the title heading', () => {
    const update = page([
      { kind: 'heading', text: 'Release plan' },
      { kind: 'paragraph', text: 'Scope, readiness and rollback for the sprint release.' },
      { kind: 'paragraph', text: 'Second.' },
    ]);
    expect(excerptOfUpdate(update, 'Release plan')).toBe('Scope, readiness and rollback for the sprint release.');
  });

  it('is capped, and absent for a page with no prose or bytes that do not decode', () => {
    expect(excerptOfUpdate(page([{ kind: 'paragraph', text: 'x'.repeat(400) }]))!.length).toBe(EXCERPT_MAX);
    expect(excerptOfUpdate(page([{ kind: 'heading', text: 'Only a heading' }]))).toBeNull();
    expect(excerptOfUpdate(new Uint8Array([1, 2, 3]))).toBeNull();
  });

  it('reads the newest gzipped snapshot once per saved version', async () => {
    const calls: string[] = [];
    let timestamp = 1;
    const client = {
      send: async (command: unknown) => {
        if (command instanceof QueryCommand) { calls.push('query'); return { Items: [{ timestamp }] }; }
        if (command instanceof GetCommand) {
          calls.push('get');
          return { Item: { snapshot: gzipSync(Buffer.from(page([{ kind: 'paragraph', text: `v${timestamp}` }]))) } };
        }
        throw new Error('unexpected');
      },
    } as unknown as DynamoDBDocumentClient;
    expect(await readDocumentExcerpt(client, 'd1', 'T')).toBe('v1');
    expect(await readDocumentExcerpt(client, 'd1', 'T')).toBe('v1');
    timestamp = 2;
    expect(await readDocumentExcerpt(client, 'd1', 'T')).toBe('v2');
    expect(calls).toEqual(['query', 'get', 'query', 'query', 'get']);
  });
});
