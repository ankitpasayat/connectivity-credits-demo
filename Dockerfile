# Build the TypeScript client separately; only its built assets enter the app.
FROM node:24-bookworm-slim AS frontend-build
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM node:24-bookworm-slim AS server-build
WORKDIR /app/server
RUN apt-get update && apt-get install -y --no-install-recommends python3 make g++ pkg-config libssl-dev libsasl2-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
COPY server/package.json server/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY server/ ./
RUN npm run build && npm prune --omit=dev

FROM node:24-bookworm-slim AS app
ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=3000
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 libsasl2-2 libstdc++6 zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 demo \
    && mkdir -p /app/.data \
    && chown -R demo:demo /app/.data
COPY --from=server-build --chown=demo:demo /app/server/package.json /app/server/package.json
COPY --from=server-build --chown=demo:demo /app/server/node_modules/ /app/server/node_modules/
COPY --from=server-build --chown=demo:demo /app/server/dist/ /app/server/dist/
COPY --from=frontend-build --chown=demo:demo /app/frontend/dist/ /app/frontend/dist/
USER demo
EXPOSE 3000
HEALTHCHECK --interval=15s --timeout=15s --start-period=30s --retries=4 \
    CMD node -e "fetch('http://127.0.0.1:3000/api/health',{signal:AbortSignal.timeout(12000)}).then(r=>r.json()).then(r=>{if(r.status!=='ok'||r.broker==='unavailable')process.exit(1)}).catch(()=>process.exit(1))"
CMD ["node", "server/dist/index.js"]

