# tradetrack-api

Always-on real-time backend for **TradeTrack**. The main app (Next.js on Vercel) is
serverless and can't hold a persistent TCP socket, so this service runs 24/7 on Fly.io,
keeps live connections to the cTrader Open API, and writes deals into the **same** Postgres
(Neon) the main app uses.

- **Stack:** NestJS 11 · TypeScript · Prisma 7 (`@prisma/adapter-pg`) · Node 22
- **Host:** Fly.io, region `fra` (always-on, `min_machines_running = 1`)
- **DB:** the main app's Neon Postgres (shared schema, vendored as `prisma/schema.prisma`)

Migrations are **owned by the main app** — this service only runs `prisma generate`.

## Local development

```bash
npm install
cp .env.example .env        # fill DATABASE_URL (the same pooled Neon URL as the main app)
npm run start:dev           # http://localhost:3001  (frontend stays on 3000)
```

### Health endpoints

| Route            | Meaning                                                              |
| ---------------- | ------------------------------------------------------------------- |
| `GET /health`    | Liveness — 200 while the process is up (DB-independent; Fly's probe) |
| `GET /health/ready` | Readiness — `SELECT 1`; 200 when DB is up, 503 when down          |

## Scripts

```bash
npm run start:dev    # watch mode
npm run build        # nest build → dist/
npm run start:prod   # node dist/main.js
npm run lint         # eslint --fix
npm test             # unit tests
npm run test:e2e     # e2e (needs DATABASE_URL)
```

## Deploy to Fly.io

One-time setup:

```bash
fly auth login
fly apps create tradetrack-api                 # name matches fly.toml
fly secrets set -a tradetrack-api \
  DATABASE_URL="postgresql://USER:PASSWORD@HOST-pooler.REGION.aws.neon.tech/DB?sslmode=require"
```

Deploy (uses the local `fly.toml` + `Dockerfile`):

```bash
fly deploy -a tradetrack-api
# No local Docker daemon? Build on Fly's servers instead:
fly deploy -a tradetrack-api --remote-only
```

Verify:

```bash
fly logs -a tradetrack-api
curl https://tradetrack-api.fly.dev/health
curl https://tradetrack-api.fly.dev/health/ready
```

`NODE_ENV` and `PORT=8080` come from `fly.toml`; everything secret goes through `fly secrets`
(never committed). Later phases add `ENCRYPTION_KEY` (must equal the main app's key),
`CTRADER_CLIENT_ID/SECRET/REDIRECT_URI`.
