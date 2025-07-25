# ------------------------------------------
# Stage 1: Base image (Node 20 on Alpine)
# ------------------------------------------
FROM node:20-alpine AS base

# Install build tools and any needed packages
# Note: we don't install corepack as a package, we enable it via Node
RUN apk add --no-cache \
    python3 \
    make \
    g++ \
    && corepack enable

WORKDIR /app

# -----------------------------
# Stage 2: Install dependencies
# -----------------------------
FROM base AS deps

# Copy package info first for caching
COPY package.json yarn.lock ./

# Install all dependencies (including dev deps for building)
RUN yarn install --frozen-lockfile

# ---------------------
# Stage 3: Build the app
# ---------------------
FROM deps AS builder

COPY tsconfig.json ./
COPY esbuild.js ./
COPY src ./src

RUN yarn build

# -----------------------------
# Stage 4: Production deps only
# -----------------------------
FROM base AS prod-deps

COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile --production=true

# ----------------------------------------------
# Stage 5: Final Alpine image with pg_dump etc.
# ----------------------------------------------
FROM node:20-alpine AS runner

# Add PostgreSQL 15 client
RUN apk add --no-cache postgresql15-client

# Use a non-root user for security (Alpine's default "node" user in the Node image)
USER node

WORKDIR /app

# Copy the production node_modules and compiled output
COPY --chown=node:node --from=prod-deps /app/node_modules ./node_modules
COPY --chown=node:node --from=builder /app/dist ./dist

EXPOSE 3000

ENV NODE_ENV=production

CMD ["node", "dist/index.js"]