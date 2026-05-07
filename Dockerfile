# syntax=docker/dockerfile:1
# platform-api — standalone application platform service.
# Dependencies (distributed-core, event-catalog) are fetched from GitHub
# during npm install — no sibling directory COPY needed.

# Stage 1: Build TypeScript
FROM node:20-alpine AS build
RUN apk add --no-cache git openssh-client python3 make g++
WORKDIR /app
COPY platform-api/package*.json ./
RUN npm config set strict-ssl false && npm install
COPY platform-api/tsconfig.json platform-api/tsconfig.build.json ./
COPY platform-api/src/ ./src/
RUN npx tsc --project tsconfig.build.json; test -f dist/index.js

# Stage 2: Runtime
FROM node:20-alpine
RUN apk add --no-cache curl git openssh-client python3 make g++ ripgrep
RUN npm install -g @anthropic-ai/claude-code
RUN addgroup -g 1001 -S nodejs && adduser -S platformapi -u 1001 -G nodejs
WORKDIR /app
COPY platform-api/package*.json platform-api/tsconfig.json platform-api/tsconfig.build.json ./
RUN npm config set strict-ssl false && npm install --omit=dev
COPY --from=build /app/dist ./dist/
COPY platform-api/src/ ./src/
RUN chown -R platformapi:nodejs /app
USER platformapi
EXPOSE 3001
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:3001/health || exit 1
CMD ["node", "dist/index.js"]
