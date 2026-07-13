# ── Stage 1: build the admin console SPA (web/) ──────────────────────────────
FROM oven/bun:1 AS web-builder
WORKDIR /web
COPY web/package.json web/bun.lock* ./
RUN bun install --frozen-lockfile 2>/dev/null || bun install
COPY web/ ./
RUN bun run build

# ── Stage 2: the Barrelman API ───────────────────────────────────────────────
FROM oven/bun:1 AS base
WORKDIR /app

# Install dependencies
COPY package.json bun.lock* ./
RUN bun install --frozen-lockfile 2>/dev/null || bun install

# Copy source
COPY . .

# Bring in the built admin console so the API can serve it at /console
COPY --from=web-builder /web/dist ./web/dist

EXPOSE 3001
CMD ["bun", "run", "src/index.ts"]
