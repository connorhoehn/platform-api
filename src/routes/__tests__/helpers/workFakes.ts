// Shared fakes for the document-work / run-draft / estimate route tests:
// one in-memory table that honours exactly the condition expressions the two
// repositories write, an express app per "replica", and a JSON fetch helper.

import express, { type NextFunction, type Request, type Response, type Router } from 'express';
import type { AddressInfo } from 'net';
import type { Server } from 'http';

interface Cmd { constructor: { name: string }; input: Record<string, any> }

function condFail(): Error {
  const e = new Error('The conditional request failed');
  e.name = 'ConditionalCheckFailedException';
  return e;
}

/** A DynamoDB document client over one table, keyed by `hashKey` (+ `rangeKey`). */
export class FakeTable {
  rows = new Map<string, Record<string, any>>();
  puts = 0;
  constructor(private readonly hashKey: string, private readonly rangeKey?: string) {}

  private keyOf(item: Record<string, any>): string {
    return this.rangeKey ? `${item[this.hashKey]}\u0000${item[this.rangeKey]}` : String(item[this.hashKey]);
  }

  async send(cmd: Cmd): Promise<any> {
    const name = cmd.constructor.name;
    const input = cmd.input;
    if (name === 'GetCommand') {
      const row = this.rows.get(this.keyOf(input.Key));
      return { Item: row ? structuredClone(row) : undefined };
    }
    if (name === 'BatchGetCommand') {
      const table = Object.keys(input.RequestItems)[0]!;
      const keys: Array<Record<string, any>> = input.RequestItems[table].Keys;
      return { Responses: { [table]: keys.map((k) => this.rows.get(this.keyOf(k))).filter(Boolean).map((r) => structuredClone(r)) } };
    }
    if (name === 'QueryCommand') {
      const vals = input.ExpressionAttributeValues ?? {};
      const [attr] = String(input.KeyConditionExpression).split(' = ');
      const want = Object.values(vals)[0];
      const items = [...this.rows.values()].filter((r) => r[attr!.trim()] === want).map((r) => structuredClone(r));
      return { Items: input.Limit ? items.slice(0, input.Limit) : items };
    }
    if (name === 'PutCommand') {
      const item = input.Item as Record<string, any>;
      const cur = this.rows.get(this.keyOf(item));
      const cond: string | undefined = input.ConditionExpression;
      const vals = input.ExpressionAttributeValues ?? {};
      let ok = true;
      if (!cond) ok = true;
      else if (cond === `attribute_not_exists(${this.rangeKey ?? this.hashKey})` || cond === `attribute_not_exists(${this.hashKey})`) ok = cur === undefined;
      else if (cond === 'revision = :seen') ok = cur !== undefined && cur.revision === vals[':seen'];
      else if (/^revision = :rev AND #status IN \(/.test(cond)) {
        const allowed = Object.entries(vals).filter(([k]) => k.startsWith(':s')).map(([, v]) => v);
        ok = cur !== undefined && cur.revision === vals[':rev'] && allowed.includes(cur.status);
      } else throw new Error(`unexpected condition ${cond}`);
      if (!ok) throw condFail();
      this.puts += 1;
      this.rows.set(this.keyOf(item), structuredClone(item));
      return {};
    }
    throw new Error(`unexpected ${name}`);
  }
}

/** `requireAuth` stand-in: the caller is `x-test-user` in org `x-test-org` (default dev-org). */
export function testAuth(req: Request, res: Response, next: NextFunction): void {
  const sub = req.header('x-test-user');
  if (!sub) { res.status(401).json({ error: 'unauthorized' }); return; }
  req.user = { sub, orgId: req.header('x-test-org') ?? 'dev-org' } as Request['user'];
  next();
}

export interface Replica { url: string; close(): Promise<void> }

export async function serve(mounts: Array<[string, Router]>): Promise<Replica> {
  const app = express();
  app.use(express.json());
  for (const [path, router] of mounts) app.use(path, router);
  app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
    res.status(typeof err?.status === 'number' ? err.status : 500).json({ error: err?.message ?? 'error' });
  });
  const server: Server = await new Promise((resolve) => { const s = app.listen(0, () => resolve(s)); });
  const { port } = server.address() as AddressInfo;
  return { url: `http://127.0.0.1:${port}`, close: () => new Promise((r) => server.close(() => r())) };
}

export async function call(base: string, method: string, path: string, user: string | null, body?: unknown): Promise<{ status: number; body: any; headers: Headers }> {
  const res = await fetch(`${base}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...(user ? { 'x-test-user': user } : {}) },
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null, headers: res.headers };
}

/** Document rows: `doc-1` owned by dev-connor with editor dev-erin and viewer dev-vic. */
export const DOCS: Record<string, { createdBy: string; editors?: string[]; viewers?: string[] }> = {
  'doc-1': { createdBy: 'dev-connor', editors: ['dev-erin'], viewers: ['dev-vic'] },
  'doc-2': { createdBy: 'dev-connor' },
};
