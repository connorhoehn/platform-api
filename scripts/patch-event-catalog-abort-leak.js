#!/usr/bin/env node
// In-place patch of @connorhoehn/event-catalog v0.3.48 DurableTransport.sleep
// to fix the AbortSignal listener leak (hub#971; root cause confirmed via
// handoff #349 from distributed-core + blocker #28 option B from operator).
//
// The unpatched sleep() attaches addEventListener('abort', ..., { once: true })
// per call, but `once: true` only auto-removes on abort — on the normal
// timeout success path the listener stays attached to the long-lived
// poller signal. At default pollingInterval = 1000ms, listeners accumulate
// at ~3600/hour per subscribed queue, eventually triggering Node's
// MaxListenersExceededWarning. The websocket-gateway audit observed
// ~1857 accumulated listeners in production.
//
// Idempotent: detects the unpatched signature and replaces it; no-op when
// already patched OR when event-catalog ships its own fix and the unpatched
// signature no longer matches. Delete this script + the postinstall hook
// when bumping past v0.3.48 with the upstream fix.
//
// Safe to invoke directly:
//   node scripts/patch-event-catalog-abort-leak.js
//   node scripts/patch-event-catalog-abort-leak.js --check   (exit 1 if would patch)

'use strict';

const fs = require('node:fs');
const path = require('node:path');

const TARGET = path.resolve(
  __dirname,
  '..',
  'node_modules/@connorhoehn/event-catalog/dist/client/durable.js',
);

const UNPATCHED = `    sleep(ms, signal) {
        return new Promise(resolve => {
            if (signal.aborted) {
                resolve();
                return;
            }
            const timer = setTimeout(resolve, ms);
            signal.addEventListener('abort', () => { clearTimeout(timer); resolve(); }, { once: true });
        });
    }`;

const PATCHED = `    sleep(ms, signal) {
        return new Promise(resolve => {
            if (signal.aborted) {
                resolve();
                return;
            }
            // hub#971 platform-api workaround: remove the abort listener on
            // the normal-timeout success path. Without this, every poll
            // iteration leaks one listener onto the long-lived signal.
            let timer;
            const onAbort = () => { clearTimeout(timer); cleanup(); resolve(); };
            const cleanup = () => signal.removeEventListener('abort', onAbort);
            timer = setTimeout(() => { cleanup(); resolve(); }, ms);
            signal.addEventListener('abort', onAbort, { once: true });
        });
    }`;

function main() {
  const checkOnly = process.argv.includes('--check');

  if (!fs.existsSync(TARGET)) {
    // Allow postinstall to run before event-catalog has been installed.
    console.log('[event-catalog-patch] target not present yet, skipping');
    return 0;
  }

  const src = fs.readFileSync(TARGET, 'utf8');

  if (src.includes(PATCHED)) {
    console.log('[event-catalog-patch] already patched (hub#971)');
    return 0;
  }

  if (!src.includes(UNPATCHED)) {
    console.log(
      '[event-catalog-patch] unpatched signature not found — assuming upstream fix is live; remove this script when bumping past event-catalog v0.3.48',
    );
    return 0;
  }

  if (checkOnly) {
    console.error('[event-catalog-patch] would patch DurableTransport.sleep but --check was given');
    return 1;
  }

  fs.writeFileSync(TARGET, src.replace(UNPATCHED, PATCHED));
  console.log('[event-catalog-patch] patched DurableTransport.sleep (hub#971)');
  return 0;
}

process.exit(main());
