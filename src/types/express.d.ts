declare namespace Express {
  interface Request {
    documentGrant?: import('@connorhoehn/realtime-modules/server').DocumentGrantClaims;
    user?: {
      sub: string;
      email?: string;
      orgId?: string;
      /** An OrgIQ realtime token's `tenants` claim: every partition the caller may use (lib/partition.ts). */
      tenants?: string[];
      /** From an OrgIQ realtime token only (lib/orgiqRealtimeToken.ts). */
      displayName?: string;
      /** Set when the principal came from an OrgIQ realtime token; absent for Cognito/dev. */
      authKind?: 'orgiq-hs256';
    };
    /** Set by serviceOnBehalfOf: the service whose signed call named `user` (NFR #72). */
    serviceActor?: string;
  }
}
