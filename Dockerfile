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
