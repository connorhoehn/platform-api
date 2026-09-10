import { callFeedChannelForLobby, chatChannelForLobby } from '../lobbyChannel';

// These three expected values were produced by the frontend's roomFeedChannel
// (realtime-examples/frontend/src/hooks/useShareToRoom.ts) on 2026-09-09. The
// rule is duplicated, not shared; this is the tripwire for drift.
describe('callFeedChannelForLobby — pinned against the frontend', () => {
  it('matches the frontend byte for byte', () => {
    expect(callFeedChannelForLobby('6ea7c8fd-1234-4abc-8def-0123456789ab')).toBe('chat:call:6ea7c8fd-1234-4abc-8def-0123456789ab');
    expect(callFeedChannelForLobby('room:design')).toBe('room:design');
    expect(callFeedChannelForLobby('x'.repeat(120))).toBe(`chat:call:${'x'.repeat(60)}-g55340`);
    expect(callFeedChannelForLobby(`a-very-long-lobby-${'q'.repeat(90)}`)).toBe(`chat:call:a-very-long-lobby-${'q'.repeat(42)}-bzuvnh`);
  });
});

describe('chatChannelForLobby', () => {
  it('still maps DMs, groups and rooms as before', () => {
    expect(chatChannelForLobby('dm:a:b')).toBe('chat:dm:a:b');
    expect(chatChannelForLobby('dmg:abc')).toBe('chat:dmg:abc');
    expect(chatChannelForLobby('room:design')).toBe('room:design');
  });
  // The gap: a document call's cards used to be dropped here.
  it('sends a document call to the document chat panel\'s thread', () => {
    expect(chatChannelForLobby('6EA7C8FD-1234-4ABC-8DEF-0123456789AB')).toBe('chat:call:6EA7C8FD-1234-4ABC-8DEF-0123456789AB');
  });
  it('still has no answer for a bare lobby', () => {
    expect(chatChannelForLobby('global-hangout')).toBeNull();
    expect(chatChannelForLobby(null)).toBeNull();
  });
});
