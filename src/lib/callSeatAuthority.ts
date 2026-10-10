// Only the owning BFF can attest a current accepted signaling seat. The
// caller's bearer is forwarded unchanged; this callback creates no access.
import { AppError } from '../middleware/error-handler';
import { lobbyAuthorityUrl, LOBBY_AUTHORITY_TIMEOUT_MS } from './lobbyAuthority';

export interface CallSeatProof {
  allowed: true; callId: string; lobbyName: string; clientId: string;
  userId: string; tenant: string; epoch: number;
  actor?: { sub: string; epoch: number };
}
export function callSeatAuthorityUrl(lobbyUrl = lobbyAuthorityUrl()): string {
  try {
    const url = new URL(lobbyUrl);
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash
      || !url.pathname.endsWith('/lobby-access')) return '';
    url.pathname = url.pathname.slice(0, -'/lobby-access'.length) + '/call-seat';
    return url.toString();
  } catch { return ''; }
}
const id = (value: unknown): value is string => typeof value === 'string' && /^[A-Za-z0-9:_-]{1,256}$/.test(value);
const epoch = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0;

export async function askCallSeatAuthority(bearer: string, request: { callId: string; lobbyName: string; clientId: string },
  options: { url?: string; fetchImpl?: typeof fetch; timeoutMs?: number } = {}): Promise<CallSeatProof> {
  const url = options.url ?? callSeatAuthorityUrl();
  if (!url || !bearer) throw new AppError(503, 'Call seat authority is unavailable');
  try {
    const response = await (options.fetchImpl ?? fetch)(url, { method: 'POST', redirect: 'error',
      headers: { Authorization: `Bearer ${bearer}`, 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify(request), signal: AbortSignal.timeout(options.timeoutMs ?? LOBBY_AUTHORITY_TIMEOUT_MS),
    });
    if (response.status === 401 || response.status === 403 || response.status === 404) throw new AppError(403, 'Accepted call seat required');
    if (response.status !== 200 || !response.body) throw new AppError(503, 'Call seat authority is unavailable');
    const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let size = 0;
    try {
      for (;;) {
        const part = await reader.read(); if (part.done) break;
        size += part.value.byteLength;
        if (size > 8_192) { await reader.cancel(); throw new AppError(503, 'Call seat authority is unavailable'); }
        chunks.push(part.value);
      }
    } finally { reader.releaseLock(); }
    const proof = JSON.parse(Buffer.concat(chunks).toString('utf8')) as CallSeatProof;
    if (proof?.allowed !== true || proof.callId !== request.callId || proof.clientId !== request.clientId || proof.lobbyName !== request.lobbyName
      || !id(proof.userId) || !id(proof.tenant) || !epoch(proof.epoch)
      || (proof.actor !== undefined && (!id(proof.actor?.sub) || !epoch(proof.actor?.epoch)))) throw new AppError(503, 'Call seat authority is unavailable');
    return proof;
  } catch (error) {
    if (error instanceof AppError) throw error;
    throw new AppError(503, 'Call seat authority is unavailable');
  }
}
