// What a failing `claude` CLI actually told us, in one place.
//
// THE TRAP THIS EXISTS FOR: the CLI explains its own failures on STDOUT, as a
// stream-json `result` event with `is_error: true` ("Not logged in · Please
// run /login"), and exits 1 with an EMPTY stderr. A caller that composes its
// error from stderr alone therefore reports `claude exited 1: (no stderr)` —
// technically true, completely useless, and it hid a stale-credentials
// failure behind "the planner is unavailable" for two live runs.
//
// So every caller builds its error from the same three inputs (exit code,
// stderr, and the stdout result text) and through the same classifier.

/** How much of a stream we put in an error message. */
export const MAX_ERROR_LINES = 8;
export const MAX_ERROR_CHARS = 2_000;

/** The tail of a stream — the last lines are the ones that say what went wrong. */
export function lastLines(text: string, maxLines = MAX_ERROR_LINES): string {
  const lines = String(text ?? '').split('\n').map((l) => l.trimEnd()).filter((l) => l.trim());
  if (!lines.length) return '';
  return lines.slice(-maxLines).join('\n').slice(-MAX_ERROR_CHARS);
}

/**
 * The CLI is running with credentials extracted from a host and mounted in.
 * When they go stale it says "Not logged in · Please run /login", which is
 * advice for a terminal that does not exist here — the person reading this is
 * looking at a failed run card in a chat.
 */
export function isClaudeAuthFailure(detail: string): boolean {
  return /not logged in|\/login|invalid api key|authentication_error|oauth token has expired/i.test(String(detail ?? ''));
}

export interface ClaudeCliFailure {
  code: number | null;
  /** Everything the child wrote to stderr. */
  stderr?: string;
  /** The `result` text of a stream-json event with `is_error: true`. */
  resultError?: string;
  /** Raw stdout, for a failure that never produced a parseable event at all. */
  stdout?: string;
}

/**
 * One error message, from whichever stream actually carried the reason.
 *
 * Order matters: the stdout `result` error is the CLI's own diagnosis and is
 * preferred over stderr noise; raw stdout is the last resort for a process
 * that died before emitting a single event.
 */
export function describeClaudeCliFailure(failure: ClaudeCliFailure): string {
  const resultError = lastLines(failure.resultError ?? '');
  const stderr = lastLines(failure.stderr ?? '');
  const stdout = lastLines(failure.stdout ?? '');
  const detail = resultError || stderr || stdout || '(no output)';

  if (isClaudeAuthFailure(detail)) {
    // The actionable instruction is the one that refreshes the MOUNTED
    // snapshot, not the CLI's own "/login".
    return `claude is not authenticated in this environment: ${detail} — the mounted credentials have gone stale; re-extract them (tilt trigger claude-creds-extract) and run this again`;
  }
  return `claude exited ${failure.code}: ${detail}`;
}

/**
 * A non-zero exit that said nothing at all, on any stream — the cold-start
 * signature, and the only kind worth retrying. An auth failure is explicitly
 * NOT one of these: it fails identically the second time, and retrying only
 * makes a person wait twice as long for the same answer.
 */
export function isSilentClaudeFailure(message: string): boolean {
  const text = String(message ?? '');
  if (isClaudeAuthFailure(text)) return false;
  return /\(no stderr\)|\(no output\)/.test(text) || /^claude exited \d+:\s*$/.test(text.trim());
}
