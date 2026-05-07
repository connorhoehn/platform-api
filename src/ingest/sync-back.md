# Sync-Back Action Integration

## Overview

The `sync-back` action node type enables pipelines to write document content back to the originating ingest source via the SourceAdapter.push() method. This creates bidirectional sync: changes made by pipelines (e.g., LLM edits, approval workflows) can be pushed back to external sources like Google Drive or S3.

## Architecture

```
Pipeline Action Node (sync-back)
  → Fetch TypedDocument from repository
  → Resolve source via document.metadata.sourceId
  → Create FileContent from document.values
  → Call SourceAdapter.push(config, documentId, content)
  → Update document.metadata.lastSyncedBack timestamp
```

## Action Config Schema

```typescript
{
  actionType: 'sync-back',
  sourceId: string,      // e.g., "{{document.metadata.sourceId}}"
  documentId: string,    // e.g., "{{document.id}}"
}
```

## SourceAdapter.push() Contract

```typescript
interface SourceAdapter {
  async push(
    config: SourceConfig,
    documentId: string,
    content: FileContent
  ): Promise<void>;
}
```

**MockDriveAdapter Implementation** (social-api/src/ingest/adapters/mock-drive.ts:178-196):
- Uses `config.pushPath` or falls back to `documentId` as target path
- Updates existing file's content and lastModified timestamp
- Creates new file if path doesn't exist

## Concurrency Control

Per task description, DistributedLock prevents concurrent push+pull on the same file:

```typescript
// Future implementation pattern:
const lockKey = `ingest:sync-back:${sourceId}:${filePath}`;
await distributedLock.acquire(lockKey, { ttl: 30_000 });
try {
  await adapter.push(config, documentId, content);
} finally {
  await distributedLock.release(lockKey);
}
```

This prevents race conditions where:
1. Pipeline pulls file, processes it, pushes changes back
2. Meanwhile, scheduled sync pulls the same file and detects a "change" (from step 1)
3. Two overlapping ingests for the same content

Lock scope is per-file (sourceId + filePath), so different files can sync concurrently.

## Pipeline Template Example

See `.planning/pipeline-templates/agentic-document-review.json`:
- Node "update-document" writes LLM edits to TypedDocument
- Node "sync-back" pushes updated content back to source via adapter.push()
- This closes the loop: external file → ingest → pipeline processing → write back

## Integration Points

1. **Frontend** (completed):
   - Added 'sync-back' to ActionType union (frontend/src/types/pipeline.ts)
   - Added UI dropdown option and field definitions (ActionConfig.tsx)

2. **Backend** (future work):
   - Pipeline action executor needs sync-back handler
   - Handler should:
     - Load document from TypedDocumentRepository
     - Load source from IngestSourceRepository
     - Create adapter via createAdapter()
     - Convert document.values → FileContent (reverse of materialization)
     - Call adapter.push()
     - Update document.metadata.lastSyncedBack

3. **Adapters**:
   - MockDriveAdapter.push() ✓ implemented and tested
   - Real Drive/S3 adapters need push() implementation (priority 5 tasks)

## Testing

Integration test (test/agentic-review-integration.test.ts) validates:
- Fetch original content via adapter.fetch()
- Push updated content via adapter.push()
- Verify file content updated in adapter storage
