// Lets pptxgenjs render inside jest.
//
// pptxgenjs decides it is on Node by looking at `process.release.name`, and on
// that branch it fires `import('node:fs')` (for encoding media it may have to
// read from disk) without awaiting it. jest's CJS module registry cannot service
// a dynamic import without --experimental-vm-modules, so that un-awaited promise
// rejects and jest attributes the rejection to whichever test is running.
//
// A deck carries no media — every slide is text, shapes and native charts — so
// the branch is dead weight here. Hiding `process.release` for the duration of
// the suite takes pptxgenjs down its other path, which for a media-free deck
// does exactly the same work. Nothing in `src/deck` reads `process.release`.
//
// Production is unaffected: under plain Node the dynamic import resolves.

export function hidePptxNodeDetection(): () => void {
  const descriptor = Object.getOwnPropertyDescriptor(process, 'release');
  Object.defineProperty(process, 'release', { value: undefined, configurable: true, writable: true });
  return () => {
    if (descriptor) Object.defineProperty(process, 'release', descriptor);
    else delete (process as unknown as { release?: unknown }).release;
  };
}
