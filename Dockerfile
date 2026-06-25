# syntax=docker/dockerfile:1

# ---- Builder: install all deps, generate Prisma client, compile ----
FROM node:22-alpine AS builder
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .

# `prisma generate` never connects; the placeholder only satisfies env('DATABASE_URL')
# resolution in prisma.config.ts. The real URL is injected at runtime via fly secrets.
RUN DATABASE_URL="postgresql://placeholder" npx prisma generate
RUN npm run build

# Inject debug IDs and upload source maps so production stack traces are de-minified in Sentry.
# Runs only when the auth token is provided as a build secret (`--build-secret SENTRY_AUTH_TOKEN=...`);
# without it the step is skipped, so local/CI image builds never fail or upload. @sentry/cli is a dev
# dependency, so this must run before `npm prune`. Org/project default to the app's Sentry slugs.
ARG SENTRY_ORG=tradetrack
ARG SENTRY_PROJECT=tradetrack-api
ENV SENTRY_ORG=$SENTRY_ORG SENTRY_PROJECT=$SENTRY_PROJECT
RUN --mount=type=secret,id=SENTRY_AUTH_TOKEN \
	if [ -f /run/secrets/SENTRY_AUTH_TOKEN ]; then \
		export SENTRY_AUTH_TOKEN="$(cat /run/secrets/SENTRY_AUTH_TOKEN)" && npm run sentry:sourcemaps; \
	else \
		echo 'SENTRY_AUTH_TOKEN not provided — skipping source map upload.'; \
	fi

# Strip dev dependencies; the generated client (@prisma/client + .prisma) is a prod dep and stays.
RUN npm prune --omit=dev

# ---- Runner: minimal runtime image ----
FROM node:22-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=8080

COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package.json ./package.json

USER node
EXPOSE 8080
CMD ["node", "dist/main.js"]
