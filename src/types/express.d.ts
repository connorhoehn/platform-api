declare namespace Express {
  interface Request {
    user?: { sub: string; email?: string; orgId?: string };
  }
}
