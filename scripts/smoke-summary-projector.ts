// scripts/smoke-summary-projector.ts
//
// LIVE smoke for the Wave A summary→document projector chain WITHOUT
// needing audio/transcription/LLM: seeds a doc section + a recording row
// carrying a ready summaryMarkdown (with an Action Items table), then
// publishes `transcription:summary.ready` through the REAL catalog/MQ so
// the RUNNING platform-api pod's summary-projector consumes it, and
// polls for the projected section items.
//
// Run from the platform-api repo root (host):
//   LOCAL_DDB_ENDPOINT=http://localhost:18001 \
//   MESSAGE_QUEUE_URL=http://localhost:13910 \
//   npx tsx scripts/smoke-summary-projector.ts
//
// The host process PUBLISHES only — subscribing here would compete with
// the pod's consumer for queue messages.

import {
  callRecordingRepo,
  documentSectionRepo,
  sectionItemRepo,
} from '../src/repositories';
import { catalog, connectCatalog, disconnectCatalog } from '../src/lib/event-catalog';

const stamp = Date.now().toString(36);
const docId = `smoke-doc-${stamp}`;
const callId = `smoke-call-${stamp}`;
const recordingId = `smoke-rec-${stamp}`;

const SUMMARY = `## Summary
Smoke-test retro for the projector chain.

## Action Items
| Owner | Action | Deadline |
|-------|--------|----------|
| Hank | Verify the projector end-to-end (${stamp}) | 2026-09-01 |
| Carol | File the follow-up ticket (${stamp}) | TBD |
`;

async function main(): Promise<void> {
  const startedAt = new Date(Date.now() - 60_000).toISOString();
  const endedAt = new Date().toISOString();

  await documentSectionRepo.createSection({
    documentId: docId,
    title: 'Overview',
  } as never);
  console.log(`seeded section on document ${docId}`);

  await callRecordingRepo.putRecording({
    callId,
    recordingId,
    channelArn: `arn:smoke:${stamp}`,
    lobbyName: docId,
    startedAt,
    endedAt,
    durationMs: 60_000,
    totalBytes: 0,
    segments: [],
    destination: 'local',
    participants: [
      { userId: 'dev-hank', displayName: 'Hank' },
      { userId: 'dev-carol', displayName: 'Carol' },
    ],
  } as never);
  await callRecordingRepo.updateTranscriptStatus(callId, recordingId, {
    status: 'ready',
    transcriptKey: 'smoke/transcript.json',
    summaryMarkdown: SUMMARY,
    transcriptGeneratedAt: endedAt,
  });
  console.log(`seeded recording ${callId}/${recordingId} with summaryMarkdown`);

  await connectCatalog();
  await catalog.publish('transcription:summary.ready', {
    recordingId,
    callId,
    channelArn: `arn:smoke:${stamp}`,
    lobbyName: docId,
    summaryChars: SUMMARY.length,
  });
  console.log('published transcription:summary.ready — waiting for the pod projector…');

  const deadline = Date.now() + 60_000;
  for (;;) {
    const items = await sectionItemRepo.getItemsForDocument(docId);
    if (items.length >= 2) {
      console.log('PROJECTED ITEMS:');
      for (const item of items) {
        console.log(
          `  [${item.sectionId}] "${item.text}" assignee=${item.assignee} due=${item.dueDate ?? '-'} category=${item.category}`,
        );
      }
      // Redelivery-safety probe: publish AGAIN — the projector's
      // existing-text dedupe must not double the items.
      await catalog.publish('transcription:summary.ready', {
        recordingId,
        callId,
        channelArn: `arn:smoke:${stamp}`,
        lobbyName: docId,
        summaryChars: SUMMARY.length,
      });
      await new Promise((r) => setTimeout(r, 8_000));
      const after = await sectionItemRepo.getItemsForDocument(docId);
      console.log(`after duplicate publish: ${after.length} items (expected 2)`);
      if (after.length !== 2) {
        console.error('DEDUPE FAILED');
        process.exitCode = 1;
      } else {
        console.log('SMOKE PASS');
      }
      break;
    }
    if (Date.now() > deadline) {
      console.error(`TIMEOUT: only ${items.length} items projected`);
      process.exitCode = 1;
      break;
    }
    await new Promise((r) => setTimeout(r, 2_000));
  }
  await disconnectCatalog();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
