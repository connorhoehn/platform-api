# syntax=docker/dockerfile:1
# platform-api — standalone application platform service.
# Dependencies (distributed-core, event-catalog) are fetched from GitHub
# during npm install. Private repos require SSH agent forwarding via BuildKit.

# Stage 1: Build TypeScript
FROM node:20-alpine AS build
RUN apk add --no-cache git openssh-client python3 make g++
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan -p 443 ssh.github.com >> ~/.ssh/known_hosts \
    && printf 'Host github.com\n  Hostname ssh.github.com\n  Port 443\n  User git\n' > ~/.ssh/config
WORKDIR /app
COPY platform-api/package*.json ./
RUN --mount=type=ssh npm config set strict-ssl false && npm install
COPY platform-api/tsconfig.json platform-api/tsconfig.build.json ./
COPY platform-api/src/ ./src/
RUN npx tsc --project tsconfig.build.json; test -f dist/index.js

# Stage 2: Runtime
FROM node:20-alpine
RUN apk add --no-cache curl git openssh-client python3 make g++ ripgrep
RUN npm install -g @anthropic-ai/claude-code
RUN addgroup -g 1001 -S nodejs && adduser -S platformapi -u 1001 -G nodejs
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan -p 443 ssh.github.com >> ~/.ssh/known_hosts \
    && printf 'Host github.com\n  Hostname ssh.github.com\n  Port 443\n  User git\n' > ~/.ssh/config
WORKDIR /app
COPY platform-api/package*.json platform-api/tsconfig.json platform-api/tsconfig.build.json ./
RUN --mount=type=ssh npm config set strict-ssl false && npm install --omit=dev
COPY --from=build /app/dist ./dist/
COPY platform-api/src/ ./src/
RUN chown -R platformapi:nodejs /app
USER platformapi
EXPOSE 3001
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:3001/health || exit 1
CMD ["node", "dist/index.js"]
