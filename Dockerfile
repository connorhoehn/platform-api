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
# Share ONE authenticated SSH connection across every git call npm makes.
#
# Load-bearing. npm resolves each github: pin with its own `git ls-remote`,
# and git-dep *preparation* spawns a nested `npm install` per dep that
# re-resolves the whole graph — hundreds of SSH handshakes to github.com in
# one RUN layer. GitHub's SSH rate limit cuts them off partway with
#   npm error An unknown git error occurred
#   npm error command git --no-replace-objects ls-remote ssh://git@github.com/...
#   npm error Connection closed by 140.82.112.3 port 22
# The named repo VARIES between runs, which is the tell that it is a
# connection budget and not an access problem — npm's "make sure you have the
# correct access rights" sends you hunting a key bug that isn't there.
# ControlMaster=auto multiplexes the rest over the first connection so the
# install authenticates once. ControlPath lives in /tmp because $HOME is not
# writable in every stage.
ENV GIT_SSH_COMMAND="ssh -o ControlMaster=auto -o ControlPath=/tmp/gh-%r@%h:%p -o ControlPersist=600"
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
# --legacy-peer-deps matches the project's resolution mode, the same as the
# gateway Dockerfile. Without it this image is the ONLY place that resolves
# peers strictly, so it fails on conflicts every local `npm install` accepts —
# a build break that cannot be reproduced outside Docker.
#
# The live one: @connorhoehn/pipeline-runtime declares its peer on
# distributed-core as an EXACT SHA, so any DC bump anywhere makes this install
# unsatisfiable even though the code is fine.
RUN --mount=type=ssh npm install --legacy-peer-deps --ignore-scripts
COPY platform-api/tsconfig.json platform-api/tsconfig.build.json ./
COPY platform-api/src/ ./src/
RUN npx tsc --project tsconfig.build.json; test -f dist/index.js

# Stage 2: Runtime
FROM node:20-alpine
RUN apk add --no-cache curl git openssh-client python3 make g++ ripgrep \
    && mkdir -p -m 0700 /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts
# SSH connection multiplexing — see the note in the build stage. This stage
# installs the SAME github: pins, so it hits the same rate limit without it.
# Each stage needs its own ENV; they don't inherit.
ENV GIT_SSH_COMMAND="ssh -o ControlMaster=auto -o ControlPath=/tmp/gh-%r@%h:%p -o ControlPersist=600"
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
RUN --mount=type=ssh npm install --omit=dev --legacy-peer-deps --ignore-scripts && npm cache clean --force
COPY --from=build /app/dist ./dist/
COPY platform-api/src/ ./src/
RUN chown -R platformapi:nodejs /app
USER platformapi
EXPOSE 3001
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:3001/health || exit 1
# A one-line supervisor, for local dev's live update: Tilt syncs src, runs
# tsc, then asks for a restart by killing the node PID this writes — the
# PROCESS restarts and the CONTAINER does not. It used to `kill 1`, which
# exited the container; each exit counted as a restart, a handful put the
# pod in CrashLoopBackOff, Tilt read the restart as a crash and replayed the
# same update on the next container — a 25s build per start, indefinitely
# (2026-09-15). A non-zero exit still exits the container, so a real crash
# still surfaces as one.
CMD ["sh", "-c", "while :; do node dist/index.js & echo $! > /tmp/platform-api.pid; wait $!; code=$?; [ \"$code\" -eq 0 ] || exit \"$code\"; echo \"[supervisor] node exited 0; starting it again\"; done"]
