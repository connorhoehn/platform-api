// social-api/src/ingest/dlq-singleton.ts
//
// Lazy singleton for the ingest DeadLetterQueue. Mirrors engine-singleton:
// bootstrap calls setIngestDLQ(dlq) at startup; routes call getIngestDLQ()
// at request-time and 503 when the engine hasn't been wired yet.

import type { DeadLetterQueue, BusEvent } from 'distributed-core';
import type { IngestItem } from './types';

// We type the DLQ as `DeadLetterQueue<IngestItem>` because that's what the
// bootstrap actually creates. Keeping it narrow here so route handlers get
// IngestItem-shaped envelopes without casting.
export type IngestDLQ = DeadLetterQueue<IngestItem | BusEvent>;

let dlq: IngestDLQ | null = null;

export function setIngestDLQ(d: IngestDLQ | null): void {
  dlq = d;
}

export function getIngestDLQ(): IngestDLQ | null {
  return dlq;
}
