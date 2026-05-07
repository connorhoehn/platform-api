// Unit tests for requireRoomMembership middleware.
//
// Mocks roomRepo at module level and exercises the membership gate,
// SKIP_AUTH bypass, missing roomId, and error paths.

import type { Request, Response, NextFunction } from 'express';

const mockIsMember = jest.fn();

jest.mock('../../repositories', () => ({
  roomRepo: { isMember: mockIsMember },
}));

import { requireRoomMembership } from '../require-membership';

function mockReq(roomId?: string, userId = 'user-1'): Request {
  return {
    params: roomId !== undefined ? { roomId } : {},
    user: { sub: userId },
  } as unknown as Request;
}

interface TestRes {
  statusCode: number;
  body: unknown;
  status(code: number): TestRes;
  json(body: unknown): TestRes;
}

function mockRes(): TestRes {
  const r: TestRes = {
    statusCode: 200,
    body: undefined,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; return this; },
  };
  return r;
}

const originalEnv = process.env.SKIP_AUTH;

beforeEach(() => {
  mockIsMember.mockReset();
  delete process.env.SKIP_AUTH;
});

afterAll(() => {
  if (originalEnv !== undefined) process.env.SKIP_AUTH = originalEnv;
  else delete process.env.SKIP_AUTH;
});

describe('requireRoomMembership', () => {
  it('calls next when user is a member', async () => {
    mockIsMember.mockResolvedValue(true);
    const next = jest.fn() as unknown as NextFunction;
    await requireRoomMembership(mockReq('room-1'), mockRes() as unknown as Response, next);
    expect(next).toHaveBeenCalled();
    expect(mockIsMember).toHaveBeenCalledWith('room-1', 'user-1');
  });

  it('returns 403 when user is not a member', async () => {
    mockIsMember.mockResolvedValue(false);
    const next = jest.fn() as unknown as NextFunction;
    const res = mockRes();
    await requireRoomMembership(mockReq('room-1'), res as unknown as Response, next);
    expect(res.statusCode).toBe(403);
    expect(next).not.toHaveBeenCalled();
  });

  it('returns 400 when roomId is missing', async () => {
    const next = jest.fn() as unknown as NextFunction;
    const res = mockRes();
    await requireRoomMembership(mockReq(undefined), res as unknown as Response, next);
    expect(res.statusCode).toBe(400);
    expect(next).not.toHaveBeenCalled();
  });

  it('skips check when SKIP_AUTH=true', async () => {
    process.env.SKIP_AUTH = 'true';
    const next = jest.fn() as unknown as NextFunction;
    await requireRoomMembership(mockReq('room-1'), mockRes() as unknown as Response, next);
    expect(next).toHaveBeenCalled();
    expect(mockIsMember).not.toHaveBeenCalled();
  });

  it('returns 500 on repo error', async () => {
    mockIsMember.mockRejectedValue(new Error('DDB error'));
    const next = jest.fn() as unknown as NextFunction;
    const res = mockRes();
    const spy = jest.spyOn(console, 'error').mockImplementation(() => {});
    await requireRoomMembership(mockReq('room-1'), res as unknown as Response, next);
    expect(res.statusCode).toBe(500);
    expect(next).not.toHaveBeenCalled();
    spy.mockRestore();
  });
});
