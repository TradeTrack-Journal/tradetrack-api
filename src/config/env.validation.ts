import { z } from 'zod';

/**
 * Runtime environment contract. Validated once at boot by ConfigModule, so a missing or
 * malformed value fails fast with a readable message instead of surfacing as an obscure
 * error deep inside a request. Later phases extend this (ENCRYPTION_KEY, CTRADER_*, ...).
 */
const envSchema = z.object({
	NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
	PORT: z.coerce.number().int().positive().default(3001),
	DATABASE_URL: z.string().min(1, 'DATABASE_URL is required'),

	// cTrader (Phase 1+). Optional so the service degrades gracefully (warn + skip) when absent,
	// instead of blocking boot. ENCRYPTION_KEY must equal the main app's key to decrypt tokens.
	ENCRYPTION_KEY: z.string().optional(),
	CTRADER_CLIENT_ID: z.string().optional(),
	CTRADER_CLIENT_SECRET: z.string().optional(),

	// Staged-rollout / test scope: comma-separated user ids whose cTrader accounts the backend
	// manages exclusively. Read via process.env in the connection manager — it MUST be declared
	// here, otherwise Zod strips it from the validated config and it never reaches process.env.
	CTRADER_ONLY_USER_IDS: z.string().optional(),

	// PoC-only account override: connect a single hard-provided account instead of reading the DB.
	CTRADER_POC_ACCESS_TOKEN: z.string().optional(),
	CTRADER_POC_ACCOUNT_ID: z.string().optional(),
	CTRADER_POC_ENVIRONMENT: z.enum(['demo', 'live']).optional(),

	// Master switch for the TradeLocker Streams live-sync. The manager stays idle unless this is
	// exactly 'true'. Keep it off until our production Streams API key is approved by TradeLocker.
	TRADELOCKER_LIVE_SYNC_ENABLED: z.string().optional(),
	// Staged-rollout / test scope: comma-separated user ids whose TradeLocker accounts the worker
	// manages exclusively (mirrors CTRADER_ONLY_USER_IDS). Empty/unset = all tradelocker accounts.
	TRADELOCKER_ONLY_USER_IDS: z.string().optional(),
	// TradeLocker live-sync (Streams API). Optional so the service degrades gracefully (warn + idle)
	// when absent. The developer-api-key is sent ONLY on the socket handshake (never on auth).
	TRADELOCKER_DEVELOPER_API_KEY: z.string().optional(),
	// Override for the auth host that issues stream JWTs (/auth/jwt/accounts/tokens). Defaults match
	// the main app: demo -> demo.tradelocker.com, live -> live.tradelocker.com.
	TRADELOCKER_AUTH_BASE_URL: z.string().optional(),
	// Override for the streams socket host (e.g. wss://api.tradelocker.com or wss://api-dev...).
	// The developer-api-key is environment-scoped, so this must pair with the right key.
	TRADELOCKER_STREAM_BASE_URL: z.string().optional(),

	// Telegram notifications (reuses the main app's bot/chat). When both are set, the worker posts a
	// one-off "live close captured" message to the same feedback chat. Unset = notifications skipped.
	TELEGRAM_BOT_TOKEN: z.string().optional(),
	TELEGRAM_FEEDBACK_CHAT_ID: z.string().optional(),

	// PoC-only single-account override: connect one account from env instead of reading the DB.
	TRADELOCKER_POC_EMAIL: z.string().optional(),
	TRADELOCKER_POC_PASSWORD: z.string().optional(),
	TRADELOCKER_POC_SERVER: z.string().optional(),
	TRADELOCKER_POC_ENVIRONMENT: z.enum(['demo', 'live']).optional(),
	TRADELOCKER_POC_ACCOUNT_ID: z.string().optional(),
});

export type Env = z.infer<typeof envSchema>;

export function validateEnv(config: Record<string, unknown>): Env {
	const parsed = envSchema.safeParse(config);

	if (!parsed.success) {
		const issues = parsed.error.issues
			.map((issue) => `  - ${issue.path.join('.') || '(root)'}: ${issue.message}`)
			.join('\n');
		throw new Error(`Invalid environment variables:\n${issues}`);
	}

	return parsed.data;
}
