// The CLI explains itself on STDOUT and exits 1 with an empty stderr. Two
// live runs reported "claude exited 1: (no stderr)" while the real cause —
// stale mounted credentials — sat in a stdout result event nobody read.
import {
  describeClaudeCliFailure, isClaudeAuthFailure, isSilentClaudeFailure, lastLines,
} from '../claudeCliErrors';

describe('describeClaudeCliFailure', () => {
  it('reaches into stdout when stderr is empty', () => {
    expect(describeClaudeCliFailure({ code: 1, stderr: '', resultError: 'Error: model overloaded' }))
      .toBe('claude exited 1: Error: model overloaded');
  });

  it('prefers the CLI\'s own diagnosis over stderr noise', () => {
    expect(describeClaudeCliFailure({ code: 1, stderr: 'npm warn deprecated foo', resultError: 'Error: model overloaded' }))
      .toContain('model overloaded');
  });

  it('falls back to raw stdout when the process died before emitting an event', () => {
    expect(describeClaudeCliFailure({ code: 127, stdout: 'claude: command not found' }))
      .toBe('claude exited 127: claude: command not found');
  });

  it('says "(no output)" only when every stream really was empty', () => {
    expect(describeClaudeCliFailure({ code: 1 })).toBe('claude exited 1: (no output)');
  });

  // The CLI's own "/login" is advice for a terminal this pod does not have.
  it('turns stale credentials into the instruction that actually fixes them', () => {
    const message = describeClaudeCliFailure({ code: 1, stderr: '', resultError: 'Not logged in · Please run /login' });
    expect(message).toContain('claude is not authenticated in this environment');
    expect(message).toContain('Not logged in · Please run /login');
    expect(message).toContain('tilt trigger claude-creds-extract');
  });

  it('recognises the auth failure however the CLI words it', () => {
    expect([
      isClaudeAuthFailure('Not logged in · Please run /login'),
      isClaudeAuthFailure('invalid api key'),
      isClaudeAuthFailure('authentication_error'),
      isClaudeAuthFailure('OAuth token has expired'),
      isClaudeAuthFailure('model overloaded'),
    ]).toEqual([true, true, true, true, false]);
  });
});

describe('isSilentClaudeFailure', () => {
  it('is true only for a failure that said nothing at all', () => {
    expect([
      isSilentClaudeFailure('claude exited 1: (no stderr)'),
      isSilentClaudeFailure('claude exited 1: (no output)'),
      isSilentClaudeFailure('claude exited 1: '),
      isSilentClaudeFailure('claude exited 1: Error: ENOENT'),
    ]).toEqual([true, true, true, false]);
  });

  // Retrying stale credentials just makes a person wait twice as long.
  it('is NEVER true for an auth failure, whatever else the message says', () => {
    const authMessage = describeClaudeCliFailure({ code: 1, resultError: 'Not logged in · Please run /login' });
    expect(isSilentClaudeFailure(authMessage)).toBe(false);
    expect(isSilentClaudeFailure('claude exited 1: (no stderr) Not logged in')).toBe(false);
  });
});

describe('lastLines', () => {
  it('keeps the tail and drops the blanks', () => {
    expect(lastLines('a\n\n b \nc')).toBe('a\n b\nc');
    expect(lastLines(Array.from({ length: 20 }, (_, i) => `l${i}`).join('\n'), 3)).toBe('l17\nl18\nl19');
    expect(lastLines('')).toBe('');
  });
});
