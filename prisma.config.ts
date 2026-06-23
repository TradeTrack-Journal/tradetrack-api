import { defineConfig, env } from 'prisma/config';

// This service only ever runs `prisma generate` — migrations are owned by the main app
// (traders-notetaker). The runtime DB connection is wired in src/prisma/prisma.service.ts
// via @prisma/adapter-pg, which takes its own connection string from DATABASE_URL.
// The datasource url below is only consulted by CLI commands that connect (studio/db pull);
// `generate` never resolves it.
export default defineConfig({
	schema: 'prisma/schema.prisma',
	datasource: {
		url: env('DATABASE_URL'),
	},
});
