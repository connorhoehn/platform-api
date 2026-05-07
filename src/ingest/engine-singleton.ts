// Lazy singleton for the IngestEngine. The engine requires an event bus that
// comes from bootstrap — so we expose get/set helpers mirroring the pipeline
// bridge-types pattern. Routes call `getIngestEngine()` at request-time; it
// returns null if the engine hasn't been bootstrapped yet (non-fatal — routes
// that require the engine return 503).

import { IngestEngine } from './engine';

let engine: IngestEngine | null = null;

export function setIngestEngine(e: IngestEngine | null): void {
  engine = e;
}

export function getIngestEngine(): IngestEngine | null {
  return engine;
}
