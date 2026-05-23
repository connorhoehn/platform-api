# syntax=docker/dockerfile:1
# platform-api — standalone application platform service.
# Siblings (distributed-core, event-catalog, service-runtime, pipeline-runtime)
# are rsynced from ~/Projects/<name> into platform-api/.<name>/ by the gateway
# Tiltfile before each build (mirrors the .ui-components pattern). file:
# pins in package.json resolve to those rsynced copies — no git+ssh needed.

# Stage 1: Build TypeScript
FROM node:20-alpine AS build
RUN apk add --no-cache git python3 make g++
# typescript installed globally so file: dep prepare scripts that run `tsc`
# (e.g. pipeline-runtime) can resolve the binary. Their dist/ is already
# rsynced; the prepare rebuild is harmless but tsc must exist on PATH.
RUN npm install -g typescript@5.6.3
WORKDIR /app
COPY platform-api/.distributed-core/  /.distributed-core/
COPY platform-api/.event-catalog/     /.event-catalog/
COPY platform-api/.service-runtime/   /.service-runtime/
COPY platform-api/.pipeline-runtime/  /.pipeline-runtime/
COPY platform-api/.message-queue/     /.message-queue/
COPY platform-api/package*.json ./
# --install-links=true: copy file: deps as real subdirs under
# /app/node_modules/<name>/ so Node's require walk from inside the dep's
# dist/ finds bare packages (ws, eventemitter3) in /app/node_modules.
RUN npm install --install-links=true
COPY platform-api/tsconfig.json platform-api/tsconfig.build.json ./
COPY platform-api/src/ ./src/
RUN npx tsc --project tsconfig.build.json; test -f dist/index.js

# Stage 2: Runtime
FROM node:20-alpine
RUN apk add --no-cache curl git python3 make g++ ripgrep
RUN npm install -g @anthropic-ai/claude-code typescript@5.6.3
RUN addgroup -g 1001 -S nodejs && adduser -S platformapi -u 1001 -G nodejs
WORKDIR /app
COPY platform-api/.distributed-core/  /.distributed-core/
COPY platform-api/.event-catalog/     /.event-catalog/
COPY platform-api/.service-runtime/   /.service-runtime/
COPY platform-api/.pipeline-runtime/  /.pipeline-runtime/
COPY platform-api/.message-queue/     /.message-queue/
COPY platform-api/package*.json platform-api/tsconfig.json platform-api/tsconfig.build.json ./
RUN npm install --omit=dev --install-links=true
COPY --from=build /app/dist ./dist/
COPY platform-api/src/ ./src/
RUN chown -R platformapi:nodejs /app
USER platformapi
# distributed-core is a file: dep at /.distributed-core (not /app/node_modules/
# real copy). When its dist/ requires bare packages like 'ws', Node walks up
# from /.distributed-core/... and never reaches /app/node_modules. NODE_PATH
# adds /app/node_modules as a fallback search root.
ENV NODE_PATH=/app/node_modules
EXPOSE 3001
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:3001/health || exit 1
CMD ["node", "dist/index.js"]
