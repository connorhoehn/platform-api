#!/usr/bin/env node
// social-api/scripts/raft-admin.js
//
// Admin CLI for Raft membership operations. Wraps distributed-core's
// RaftMembershipManager for add/remove operations.
//
// Usage:
//   node scripts/raft-admin.js add-member <nodeId>
//   node scripts/raft-admin.js remove-member <nodeId>
//   node scripts/raft-admin.js health
//
// Requires social-api to be running with PIPELINE_REGISTRY_MODE=raft.

const http = require('http');

const SOCIAL_API_HOST = process.env.SOCIAL_API_HOST || 'localhost';
const SOCIAL_API_PORT = process.env.SOCIAL_API_PORT || 3001;

function httpRequest(method, path, body) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: SOCIAL_API_HOST,
      port: SOCIAL_API_PORT,
      path,
      method,
      headers: body ? {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(JSON.stringify(body))
      } : {}
    };

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(data) });
        } catch {
          resolve({ status: res.statusCode, body: data });
        }
      });
    });

    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

async function addMember(nodeId) {
  console.log(`[raft-admin] Adding member: ${nodeId}`);

  const { status, body } = await httpRequest('POST', '/internal/raft/add-member', { nodeId });

  if (status === 200) {
    console.log(`[raft-admin] ✓ Member added successfully`);
    console.log(JSON.stringify(body, null, 2));
    process.exit(0);
  } else {
    console.error(`[raft-admin] ✗ Failed to add member (HTTP ${status})`);
    console.error(JSON.stringify(body, null, 2));
    process.exit(1);
  }
}

async function removeMember(nodeId) {
  console.log(`[raft-admin] Removing member: ${nodeId}`);

  const { status, body } = await httpRequest('POST', '/internal/raft/remove-member', { nodeId });

  if (status === 200) {
    console.log(`[raft-admin] ✓ Member removed successfully`);
    console.log(JSON.stringify(body, null, 2));
    process.exit(0);
  } else {
    console.error(`[raft-admin] ✗ Failed to remove member (HTTP ${status})`);
    console.error(JSON.stringify(body, null, 2));
    process.exit(1);
  }
}

async function health() {
  const { status, body } = await httpRequest('GET', '/internal/raft/health');

  if (status === 200) {
    console.log('[raft-admin] Cluster health:');
    console.log(JSON.stringify(body, null, 2));
    process.exit(0);
  } else {
    console.error(`[raft-admin] Health check failed (HTTP ${status})`);
    console.error(JSON.stringify(body, null, 2));
    process.exit(1);
  }
}

// Main
const command = process.argv[2];
const arg = process.argv[3];

switch (command) {
  case 'add-member':
    if (!arg) {
      console.error('Usage: raft-admin.js add-member <nodeId>');
      process.exit(1);
    }
    addMember(arg).catch((err) => {
      console.error('[raft-admin] Error:', err.message);
      process.exit(1);
    });
    break;

  case 'remove-member':
    if (!arg) {
      console.error('Usage: raft-admin.js remove-member <nodeId>');
      process.exit(1);
    }
    removeMember(arg).catch((err) => {
      console.error('[raft-admin] Error:', err.message);
      process.exit(1);
    });
    break;

  case 'health':
    health().catch((err) => {
      console.error('[raft-admin] Error:', err.message);
      process.exit(1);
    });
    break;

  default:
    console.error('Usage: raft-admin.js <add-member|remove-member|health> [nodeId]');
    console.error('');
    console.error('Commands:');
    console.error('  add-member <nodeId>     Add a node to the Raft cluster via joint consensus');
    console.error('  remove-member <nodeId>  Remove a node from the Raft cluster');
    console.error('  health                  Show Raft cluster health and membership');
    process.exit(1);
}
