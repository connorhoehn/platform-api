# syntax=docker/dockerfile:1
# platform-api — standalone application platform service.
# Shared libraries (distributed-core, event-catalog, service-runtime,
# pipeline-runtime) are fetched from GitHub by tag via the pins in
# package.json. The github: shorthand requires git auth — we pass a
# token through a BuildKit build secret and tell git to use HTTPS
# with the token wherever the package.json refers to github.com.
#
# Build invocation (BuildKit required):
#   docker build \
#     --secret id=github-token,src=$HOME/.github-token \
#     -f platform-api/Dockerfile -t platform-api .
#
# The build context root contains a `platform-api/` directory (gateway
# Tiltfile pattern). The token file should hold a PAT with `repo` scope
# for the four private sibling repos.

# Stage 1: Build TypeScript
FROM node:20-alpine AS build
RUN apk add --no-cache git python3 make g++
WORKDIR /app
# Only the manifest, not the lockfile — npm 10.8.2 in node:20-alpine has a
# "Cannot destructure property 'package'" bug when installing git: deps
# against a pre-generated lockfile. Skipping the lockfile lets npm resolve
# fresh, matching the pre-swap practice (commit 54f11b5 also dropped the
# lockfile from the build for similar reasons).
COPY platform-api/package.json ./
RUN --mount=type=secret,id=github-token \
    TOKEN=$(cat /run/secrets/github-token) && \
    git config --global url."https://oauth2:${TOKEN}@github.com/".insteadOf "https://github.com/" && \
    git config --global url."https://oauth2:${TOKEN}@github.com/".insteadOf "git@github.com:" && \
    git config --global url."https://oauth2:${TOKEN}@github.com/".insteadOf "ssh://git@github.com/" && \
    npm install && \
    git config --global --remove-section url."https://oauth2:${TOKEN}@github.com/"
COPY platform-api/tsconfig.json platform-api/tsconfig.build.json ./
COPY platform-api/src/ ./src/
RUN npx tsc --project tsconfig.build.json; test -f dist/index.js

# Stage 2: Runtime
FROM node:20-alpine
RUN apk add --no-cache curl git python3 make g++ ripgrep
RUN npm install -g @anthropic-ai/claude-code
RUN addgroup -g 1001 -S nodejs && adduser -S platformapi -u 1001 -G nodejs
WORKDIR /app
COPY platform-api/package.json platform-api/tsconfig.json platform-api/tsconfig.build.json ./
RUN --mount=type=secret,id=github-token \
    TOKEN=$(cat /run/secrets/github-token) && \
    git config --global url."https://oauth2:${TOKEN}@github.com/".insteadOf "https://github.com/" && \
    git config --global url."https://oauth2:${TOKEN}@github.com/".insteadOf "git@github.com:" && \
    git config --global url."https://oauth2:${TOKEN}@github.com/".insteadOf "ssh://git@github.com/" && \
    npm install --omit=dev && \
    git config --global --remove-section url."https://oauth2:${TOKEN}@github.com/"
COPY --from=build /app/dist ./dist/
COPY platform-api/src/ ./src/
RUN chown -R platformapi:nodejs /app
USER platformapi
EXPOSE 3001
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:3001/health || exit 1
CMD ["node", "dist/index.js"]
