import { documentLifecycleRepo } from '../repositories';
import { isLocked } from '../lib/document-lifecycle';

/** The same lifecycle authority used by workflow transitions, without loading
 * owner/review rosters for each transport mutation. Repository reads are strong. */
export async function readDocumentWritePolicy(documentId: string) {
  const row = await documentLifecycleRepo.get(documentId);
  return { documentId, canWrite: !isLocked(row?.status), version: row?.version ?? 0 };
}
export async function documentContentWritable(documentId: string): Promise<boolean> {
  return (await readDocumentWritePolicy(documentId)).canWrite;
}
