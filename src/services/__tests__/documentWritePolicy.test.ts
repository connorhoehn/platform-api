jest.mock('../../repositories', () => ({ documentLifecycleRepo: { get: jest.fn() } }));
import { documentLifecycleRepo } from '../../repositories';
import { readDocumentWritePolicy } from '../documentWritePolicy';
import { DocumentLifecycleRepository } from '../../repositories/DocumentLifecycleRepository';
import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

it.each(['approved', 'finalized', 'final'])('keeps %s content locked using the existing lifecycle rule', async status => {
  jest.mocked(documentLifecycleRepo.get).mockResolvedValueOnce({ status, version: 7 } as any);
  expect(await readDocumentWritePolicy('doc')).toEqual({ documentId: 'doc', canWrite: false, version: 7 });
});
it('allows an implicit draft and does not create ownership', async () => {
  jest.mocked(documentLifecycleRepo.get).mockResolvedValueOnce(null);
  expect(await readDocumentWritePolicy('doc')).toEqual({ documentId: 'doc', canWrite: true, version: 0 });
});
it('does not turn repository outages into implicit drafts', async () => {
  jest.mocked(documentLifecycleRepo.get).mockRejectedValueOnce(new Error('DynamoDB unavailable'));
  await expect(readDocumentWritePolicy('doc')).rejects.toThrow('unavailable');
});
it('reads lifecycle strongly so acknowledged finalization cannot be missed', async () => {
  const send = jest.fn(async (_command: unknown) => ({ Item: { documentId: 'doc', status: 'finalized', version: 8 } }));
  const repo = new DocumentLifecycleRepository({ send } as unknown as DynamoDBDocumentClient);
  expect((await repo.get('doc'))?.status).toBe('finalized');
  expect(send.mock.calls[0]![0]).toEqual(expect.objectContaining({ input: expect.objectContaining({ ConsistentRead: true, Key: { documentId: 'doc' } }) }));
});
