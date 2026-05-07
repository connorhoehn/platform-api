# syntax=docker/dockerfile:1
# platform-api — standalone application platform service.
# distributed-core is a file: dep (sibling directory in local dev,
# or published package in CI).

# Stage 1: Build TypeScript
FROM node:20-alpine AS build
RUN apk add --no-cache git openssh-client python3 make g++
WORKDIR /app
COPY package*.json ./
# In CI, distributed-core is fetched from GitHub Packages.
# In local dev, it's mounted via Tilt live_update.
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN --mount=type=ssh \
    npm config set strict-ssl false && \
    npm ci
COPY tsconfig.json tsconfig.build.json ./
COPY src/ ./src/
RUN npx tsc --project tsconfig.build.json; test -f dist/index.js

# Stage 2: Runtime
FROM node:20-alpine
RUN apk add --no-cache curl git openssh-client python3 make g++ ripgrep
RUN addgroup -g 1001 -S nodejs && adduser -S platformapi -u 1001 -G nodejs
WORKDIR /app
COPY package*.json tsconfig.json tsconfig.build.json ./
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN --mount=type=ssh \
    npm config set strict-ssl false && \
    npm ci && npm cache clean --force
COPY --from=build /app/dist ./dist/
COPY src/ ./src/
RUN chown -R platformapi:nodejs /app
USER platformapi
EXPOSE 3001
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:3001/health || exit 1
CMD ["node", "dist/index.js"]
