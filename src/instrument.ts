import * as Sentry from '@sentry/nestjs';

/**
 * Sentry must be initialised before anything else is imported, so this file is the very first
 * import in `main.ts` (before `@nestjs/*`, `pg`, etc.) — that's what lets the SDK auto-instrument
 * HTTP, Postgres and the Nest internals.
 *
 * It runs before ConfigModule loads `.env`, so it reads `process.env` directly and falls back to a
 * literal DSN. The DSN is not a secret: it only identifies which project to ingest events into. On
 * Fly the env vars are real `process.env`, so `SENTRY_DSN` / `NODE_ENV` override the defaults there.
 */
Sentry.init({
	// Only report from real deployments — locally `NODE_ENV` is `development`, so nothing is sent.
	// The SDK still initialises (instrumentation stays harmless), it just drops every event.
	enabled: process.env.NODE_ENV === 'production',

	dsn:
		process.env.SENTRY_DSN ??
		'https://fae1f1b080c9e406ad4b519f10b69019@o4510533522685952.ingest.de.sentry.io/4511617112146000',

	// Tag events so local/dev noise stays separate from production in the Sentry UI.
	environment: process.env.NODE_ENV ?? 'development',

	// Send structured logs to Sentry.
	enableLogs: true,

	// Tracing — capture 100% of transactions. Tune down (e.g. 0.1) if this gets noisy/expensive.
	tracesSampleRate: 1.0,

	dataCollection: {
		// To disable sending user data and HTTP bodies, uncomment the lines below. For more info visit:
		// https://docs.sentry.io/platforms/javascript/guides/node/configuration/options/#dataCollection
		// userInfo: false,
		// httpBodies: [],
	},
});
