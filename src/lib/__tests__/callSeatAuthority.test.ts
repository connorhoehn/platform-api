import { askCallSeatAuthority, callSeatAuthorityUrl } from '../callSeatAuthority';
const request = { callId: 'call-one', lobbyName: 'orgiq:initiative:p-observability', clientId: 'client-one' };
const proof = { allowed: true, ...request, userId: 'alice', tenant: 'orgiq', epoch: 2 };
const fetchImpl = jest.fn();
beforeEach(() => fetchImpl.mockReset());
const ask = () => askCallSeatAuthority('synthetic-bearer', request, { url: 'http://bff.invalid/api/realtime/call-seat', fetchImpl });
test('derives only the existing exact trusted lobby callback sibling', () => {
  expect(callSeatAuthorityUrl('http://bff:3001/api/realtime/lobby-access')).toBe('http://bff:3001/api/realtime/call-seat');
  for (const value of ['', 'file:///api/realtime/lobby-access', 'http://u:p@bff/lobby-access', 'http://bff/wrong', 'http://bff/lobby-access?q=1']) expect(callSeatAuthorityUrl(value)).toBe('');
});
test('forwards exact original caller token and identity tuple with timeout; accepts matching proof', async () => {
  fetchImpl.mockResolvedValue(new Response(JSON.stringify(proof)));
  expect(await ask()).toEqual(proof);
  expect(fetchImpl).toHaveBeenCalledWith('http://bff.invalid/api/realtime/call-seat', expect.objectContaining({ method: 'POST', redirect: 'error', headers: expect.objectContaining({ Authorization: 'Bearer synthetic-bearer' }), body: JSON.stringify(request), signal: expect.any(AbortSignal) }));
});
test.each([401, 403, 404])('native upstream denial %s remains denial', async status => {
  fetchImpl.mockResolvedValue(new Response('{}', { status })); await expect(ask()).rejects.toMatchObject({ status: 403 });
});
test.each([{}, { ...proof, allowed: false }, { ...proof, callId: 'other' }, { ...proof, clientId: 'other' }, { ...proof, lobbyName: 'other' }, { ...proof, epoch: -1 }, { ...proof, actor: { sub: 'admin', epoch: '2' } }])('malformed/mismatched proof %j refuses', async body => {
  fetchImpl.mockResolvedValue(new Response(JSON.stringify(body))); await expect(ask()).rejects.toMatchObject({ status: 503 });
});
test('oversized chunked body cancels and network/timeout fail closed', async () => {
  const cancel = jest.fn();
  fetchImpl.mockResolvedValue(new Response(new ReadableStream({ pull(c) { c.enqueue(new Uint8Array(5000)); }, cancel })));
  await expect(ask()).rejects.toMatchObject({ status: 503 }); expect(cancel).toHaveBeenCalled();
  fetchImpl.mockRejectedValue(new Error('timeout')); await expect(ask()).rejects.toMatchObject({ status: 503 });
});
test('redirect is refused rather than accepting proof from another endpoint', async () => {
  fetchImpl.mockResolvedValue(new Response('', { status: 302, headers: { Location: 'http://elsewhere.invalid/call-seat' } }));
  await expect(ask()).rejects.toMatchObject({ status: 503 });
});
