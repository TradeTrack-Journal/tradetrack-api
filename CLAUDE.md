# CLAUDE.md — tradetrack-api

Guidance for Claude Code when working in this repository.

**tradetrack-api** is the always-on real-time backend for TradeTrack (NestJS on Fly.io,
region `fra`). It keeps live cTrader Open API connections and writes deals into the same
Neon Postgres as the main app (`traders-notetaker`).

## Testing

- **Do not write tests.** No unit tests, no e2e tests — do not add new test files or expand
  the scaffold tests, and do not gate work on a test suite. Verify changes by building
  (`npm run build`) and running the app (curl the endpoints) instead.

## Conventions

- TypeScript, **tabs** for indentation, **single quotes**, `index.ts` barrel exports per module.
- Package manager is **npm**.
- Default dev port **3001** (the frontend runs on 3000); Fly injects `PORT=8080`.

## Prisma / database

- `prisma/schema.prisma` is **vendored** from the main app; this service only runs
  `prisma generate`. **Never** run migrations here — migrations are owned by the main app.
- Runtime DB access goes through `PrismaService` (`@prisma/adapter-pg`, one small pool).

## Verification

- After substantive edits: `npm run lint` and `npm run build`, then confirm the app boots and
  the relevant endpoints respond. There is no test suite to run.
