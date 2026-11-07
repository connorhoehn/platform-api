// social-api/jest.setup.js
//
// Per-worker setup referenced from jest.config.js — mirrors the root-level
// test/jest.setup.js. Sets env vars that distributed-core's IS_TEST_ENV
// detection + PIPELINE_TEST_FAST_MODE read at bootstrap time.

if (!process.env.NODE_ENV) {
  process.env.NODE_ENV = 'test';
}

if (process.env.PIPELINE_TEST_FAST_MODE == null) {
  process.env.PIPELINE_TEST_FAST_MODE = 'true';
}

// Unit tests must not inherit an operator's AWS credentials or silently use
// cloud endpoints when a transitive repository was not mocked. A closed local
// port makes accidental I/O fail; live local integration fixtures can explicitly
// supply their own endpoint. These are intentionally unusable cloud credentials.
process.env.AWS_ACCESS_KEY_ID = 'unit-test';
process.env.AWS_SECRET_ACCESS_KEY = 'unit-test';
delete process.env.AWS_SESSION_TOKEN;
process.env.AWS_EC2_METADATA_DISABLED = 'true';
process.env.AWS_ENDPOINT_URL = 'http://127.0.0.1:9';
if (!process.env.LOCAL_DDB_ENDPOINT && !process.env.DYNAMODB_ENDPOINT && !process.env.LOCALSTACK_ENDPOINT) {
  process.env.LOCAL_DDB_ENDPOINT = 'http://127.0.0.1:9';
}

// SKIP_AUTH is honoured only with the explicit insecure dev flag
// (lib/insecureDevMode.ts). Unit tests that exercise the dev path set
// SKIP_AUTH=true; the flag is on here so they keep doing so, and the tests of
// the refusal delete it.
if (process.env.INSECURE_DEV_MODE == null) {
  process.env.INSECURE_DEV_MODE = 'true';
}
// Cognito ID tokens must name an allowed app client (middleware/auth.ts).
if (process.env.COGNITO_CLIENT_ID == null) {
  process.env.COGNITO_CLIENT_ID = 'test-client';
}
