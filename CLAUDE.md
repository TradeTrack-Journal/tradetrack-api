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

## Project structure

Layout is **feature/module-based** — one folder per module under `src/`, each self-contained.
Never layer-based: there is no top-level `controllers/` or `services/`. Generate with the CLI to
stay conforming: `nest g module <name>`, `nest g service <name>/<thing>`, `nest g controller <name>`.

### Two kinds of folders

- **Nest modules** — own a `<name>.module.ts` and are imported in `app.module.ts`
  (`prisma`, `health`, `ctrader`, `tradelocker`). All providers and controllers live here.
- **Plain utility folders** — pure functions, types and constants with **no `@Module`**,
  imported directly (`config`, `crypto`, `trades`). Use these for shared logic that isn't a Nest
  provider; don't wrap pure helpers in a module just to share them.

### File naming — canonical NestJS `name.role.ts`

Lowercase kebab-case; the **role suffix** says what the file is. **No feature-name prefix** — the
folder already namespaces it: `ctrader/connection-manager.service.ts`, _not_
`ctrader/ctrader-connection-manager.ts`.

| Role | File | Class |
| --- | --- | --- |
| Module | `<name>.module.ts` | `XModule` |
| Controller | `<name>.controller.ts` | `XController` |
| Provider / service | `<thing>.service.ts` | `ThingService` |
| Guard / Pipe / Interceptor / Filter | `<n>.guard.ts` … | `NGuard` … |
| DTO | `<verb-noun>.dto.ts` | `VerbNounDto` |
| Entity | `<name>.entity.ts` | `XEntity` |
| Constants / types / zod schema | `constants.ts` · `types.ts` · `<name>.schema.ts` | — |
| Pure helper (no decorator) | plain name: `client.ts`, `metrics.ts` | — |

Hard rules:

- **Every `@Injectable()` provider lives in a `*.service.ts` and its class name ends in `Service`.**
  This is the rule the current `ctrader-connection-manager.ts` / `ctrader-trade-writer.ts` break —
  new code must not.
- Class name mirrors the file: `connection-manager.service.ts` ↔ `ConnectionManagerService`,
  `health.controller.ts` ↔ `HealthController`.
- One responsibility per file. If a service grows several concerns, split it into collaborating
  services in the same folder rather than one fat file.

### Inside a feature module

```
src/<feature>/
  <feature>.module.ts        # wires providers/controllers/exports
  <feature>.controller.ts    # only if it exposes HTTP routes
  <thing>.service.ts         # one provider per file
  dto/                       # request/response DTOs (only when there are routes)
  entities/                  # persistence types (only when needed)
  types.ts                   # shared internal types/interfaces
  constants.ts               # tunables, magic numbers
  index.ts                   # barrel: public surface only
```

Create `dto/`, `entities/`, `interfaces/` subfolders only when there is more than one file — don't
scaffold empty folders. A single shared-types file stays `types.ts` at the module root.

### Barrels & imports

- Each module has an `index.ts` re-exporting **only its public surface** (the module class + the
  types other modules import). Don't export internal providers that nothing else consumes.
- Import across modules via the barrel (`from '../prisma'`), never via a deep path
  (`from '../prisma/prisma.service'`).

### Cross-cutting code

- `config/` — env validation + typed config (`validateEnv`, `Env`).
- `common/` — create when first needed, for shared guards / interceptors / pipes / filters /
  decorators.
- Shared pure logic that isn't a provider → a plain utility folder (like `trades/`).
- `main.ts` = bootstrap only · `app.module.ts` = compose modules + global providers ·
  `instrument.ts` = Sentry init, imported first. No business logic in any of the three.

### Known debt — follow the rule above, don't copy these

`ctrader/` and `tradelocker/` still use the old `<feature>-<thing>.ts` names with no `.service`
suffix. They are scheduled for a rename pass; until then, **match the canonical convention for all
new files** and don't add more `<feature>-`-prefixed files.

## Prisma / database

- `prisma/schema.prisma` is **vendored** from the main app; this service only runs
  `prisma generate`. **Never** run migrations here — migrations are owned by the main app.
- Runtime DB access goes through `PrismaService` (`@prisma/adapter-pg`, one small pool).

## Verification

- After substantive edits: `npm run lint` and `npm run build`, then confirm the app boots and
  the relevant endpoints respond. There is no test suite to run.
