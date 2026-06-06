# syntax=docker/dockerfile:1
# platform-api — standalone application platform service.
#
# Shared libraries (distributed-core, event-catalog, service-runtime,
# pipeline-runtime) are fetched from GitHub by tag via the github: pins
# in package.json. Private repos require SSH agent forwarding — invoke
# with BuildKit + `--ssh default` so the host's ssh-agent is forwarded
# into the RUN --mount=type=ssh steps below.
#
# Build invocation:
#   DOCKER_BUILDKIT=1 docker build \
#     --ssh default \
#     -f platform-api/Dockerfile -t platform-api .
#
# The gateway Tiltfile builds with the repo root as context; this
# Dockerfile assumes a `platform-api/` directory in the context.
#
# Detox 2026-05-28: dropped the rsync-as-sibling-dep pattern. Used to
# `COPY .distributed-core/ .event-catalog/ ...` from rsynced siblings
# at the context root, then `npm install --install-links=true`. That
# whole flow is gone — install happens straight from github: pins.

# Stage 1: Build TypeScript
FROM node:20-alpine AS build
RUN apk add --no-cache git openssh-client python3 make g++ \
    && mkdir -p -m 0700 /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts
WORKDIR /app
# Lockfile intentionally NOT copied into the build context. npm 10.8.2
# (node:20-alpine) crashes with "Cannot destructure property 'package'
# of 'node.target' as it is null" when reifying a lockfile that mixes
# github: deps with transitive `file:` peer references (service-runtime
# v0.2.4's published package.json declares distributed-core as
# `file:../distributed-core` in its peers). The gateway Dockerfile uses
# the same workaround — `src/package-lock.json` is git-ignored / deleted
# pre-build for the same reason. SSH agent forwarding is required for
# the private repo pins (--ssh default at the host).
COPY platform-api/package.json ./
RUN --mount=type=ssh npm install --ignore-scripts
COPY platform-api/tsconfig.json platform-api/tsconfig.build.json ./
COPY platform-api/src/ ./src/
RUN npx tsc --project tsconfig.build.json; test -f dist/index.js

# Stage 2: Runtime
FROM node:20-alpine
RUN apk add --no-cache curl git openssh-client python3 make g++ ripgrep \
    && mkdir -p -m 0700 /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts
RUN npm install -g @anthropic-ai/claude-code
# typescript installed globally so Tilt live_update's `npx tsc` step has a
# compiler to invoke. Without this, the runtime image only has the
# --omit=dev install below, which excludes typescript (devDep), and the
# live_update tsc step silently no-ops while the running pod keeps stale
# dist/. Pinned to the same major as devDeps to avoid lib drift.
RUN npm install -g typescript@~5.6.3
RUN addgroup -g 1001 -S nodejs && adduser -S platformapi -u 1001 -G nodejs
WORKDIR /app
COPY platform-api/package.json platform-api/tsconfig.json platform-api/tsconfig.build.json ./
# See build-stage comment for why the lockfile isn't copied.
RUN --mount=type=ssh npm install --omit=dev --ignore-scripts && npm cache clean --force
COPY --from=build /app/dist ./dist/
COPY platform-api/src/ ./src/
RUN chown -R platformapi:nodejs /app
USER platformapi
EXPOSE 3001
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:3001/health || exit 1
CMD ["node", "dist/index.js"]
