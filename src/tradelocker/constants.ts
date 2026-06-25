/**
 * Streams API socket hosts. CRITICAL — the developer-api-key, the account (= auth host that issues
 * the JWT), and the socket must ALL be the SAME TradeLocker environment, or SUBSCRIBE is rejected:
 *   - PRODUCTION: auth demo|live.tradelocker.com + socket wss://api.tradelocker.com + a PRODUCTION
 *     developer-api-key. Real, frontend-connected accounts live here.
 *   - DEV/STAGING: auth stg.tradelocker.com + socket wss://api-dev.tradelocker.com + a DEV key +
 *     accounts that exist on stg. This is the streams example's setup and TradeLocker's integration
 *     sandbox.
 * Mixing them — e.g. a DEV key (accepted on api-dev) with PRODUCTION accounts (JWT from
 * demo.tradelocker.com) — passes the developer-key check but the SUBSCRIBE is rejected because the
 * account/JWT is unknown to that environment. Defaults are production; to target dev override BOTH
 * TRADELOCKER_AUTH_BASE_URL (stg) and TRADELOCKER_STREAM_BASE_URL (api-dev) together.
 */
export const TRADELOCKER_STREAM_HOSTS = {
	demo: 'wss://api.tradelocker.com',
	live: 'wss://api.tradelocker.com',
} as const;

/**
 * Host that issues the per-account JWT the SUBSCRIBE action accepts, via
 * POST /backend-api/auth/jwt/accounts/tokens.
 *
 * These MUST match where the account actually lives — the SAME hosts the main app's REST importer
 * authenticates against (demo = paper trading, live = real money). The streams example used
 * stg.tradelocker.com, but that is staging; real accounts are rejected there ("Incorrect email or
 * password"). Override per deploy with TRADELOCKER_AUTH_BASE_URL only to target staging.
 */
export const TRADELOCKER_AUTH_HOSTS = {
	demo: 'https://demo.tradelocker.com',
	live: 'https://live.tradelocker.com',
} as const;

export const STREAM_NAMESPACE = '/streams-api';
export const STREAM_HANDSHAKE_PATH = '/streams-api/socket.io';

export const STREAM_EVENTS = {
	connection: 'connection',
	subscriptions: 'subscriptions',
	stream: 'stream',
} as const;

export const SUBSCRIPTION_ACTIONS = {
	subscribe: 'SUBSCRIBE',
	unsubscribe: 'UNSUBSCRIBE',
} as const;

export const TERMINAL_NAME = 'tradelocker';

/** SUBSCRIBE ack must arrive within this window or the attempt is treated as failed. */
export const SUBSCRIBE_ACK_TIMEOUT_MS = 15_000;

/** engine.io handshake timeout — without it a black-holed socket hangs open() forever. */
export const CONNECT_TIMEOUT_MS = 15_000;

export const RECONNECT_BASE_DELAY_MS = 1_000;
export const RECONNECT_MAX_DELAY_MS = 60_000;
