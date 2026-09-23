declare namespace Express {
  interface Request {
    documentGrant?: import('@connorhoehn/realtime-modules/server').DocumentGrantClaims;
    user?: { sub: string; email?: string; orgId?: string };
    /** Set by serviceOnBehalfOf: the service whose signed call named `user` (NFR #72). */
    serviceActor?: string;
  }
}
