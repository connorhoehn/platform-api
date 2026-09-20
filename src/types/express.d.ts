declare namespace Express {
  interface Request {
    documentGrant?: import('@connorhoehn/realtime-modules/server').DocumentGrantClaims;
    user?: { sub: string; email?: string; orgId?: string };
  }
}
