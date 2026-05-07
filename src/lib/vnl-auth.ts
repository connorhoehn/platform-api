/**
 * VNL Auth — manages a Cognito service-account token for calling the
 * videonowandlater API. Caches the token and auto-refreshes before expiry.
 *
 * Env vars:
 *   VNL_COGNITO_USER_POOL_CLIENT_ID — Cognito app client ID
 *   VNL_SERVICE_USERNAME — pre-created service user
 *   VNL_SERVICE_PASSWORD — service user password
 *   VNL_API_URL — VNL API base URL (e.g. https://xxx.execute-api.us-east-1.amazonaws.com/prod)
 */

import {
  CognitoIdentityProviderClient,
  InitiateAuthCommand,
} from '@aws-sdk/client-cognito-identity-provider';

const REGION = process.env.AWS_REGION ?? 'us-east-1';
const CLIENT_ID = process.env.VNL_COGNITO_USER_POOL_CLIENT_ID ?? '';
const USERNAME = process.env.VNL_SERVICE_USERNAME ?? '';
const PASSWORD = process.env.VNL_SERVICE_PASSWORD ?? '';

const cognitoClient = new CognitoIdentityProviderClient({ region: REGION });

let cachedToken: string | null = null;
let tokenExpiresAt = 0;

/**
 * Get a valid Cognito ID token for the VNL service user.
 * Caches and refreshes automatically.
 */
export async function getVnlAuthToken(): Promise<string> {
  // Return cached token if still valid (with 5-minute buffer)
  if (cachedToken && Date.now() < tokenExpiresAt - 5 * 60 * 1000) {
    return cachedToken;
  }

  if (!CLIENT_ID || !USERNAME || !PASSWORD) {
    throw new Error(
      'VNL auth not configured. Set VNL_COGNITO_USER_POOL_CLIENT_ID, VNL_SERVICE_USERNAME, VNL_SERVICE_PASSWORD.',
    );
  }

  const result = await cognitoClient.send(
    new InitiateAuthCommand({
      AuthFlow: 'USER_PASSWORD_AUTH',
      ClientId: CLIENT_ID,
      AuthParameters: {
        USERNAME,
        PASSWORD,
      },
    }),
  );

  const idToken = result.AuthenticationResult?.IdToken;
  const expiresIn = result.AuthenticationResult?.ExpiresIn ?? 3600;

  if (!idToken) {
    throw new Error('VNL Cognito auth failed — no IdToken returned');
  }

  cachedToken = idToken;
  tokenExpiresAt = Date.now() + expiresIn * 1000;

  return idToken;
}

/** The configured VNL API base URL. */
export const VNL_API_URL = process.env.VNL_API_URL ?? '';
