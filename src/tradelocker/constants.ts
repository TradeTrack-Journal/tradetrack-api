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

/** How often to reconcile live sessions with the DB (pick up newly connected / removed accounts). */
export const RECONCILE_INTERVAL_MS = 30_000;

/**
 * How many times to re-attempt a close-history top-up when the report lagged the live `ClosePosition`
 * (retried once per reconcile tick). After this the SyncEnd backfill on the next reconnect is the
 * final backstop.
 */
export const CLOSE_TOPUP_MAX_RETRIES = 12;

/** REST top-up (close-trades-history) tunables. */
export const REST_TIMEOUT_MS = 20_000;
/** Page size for the close-trades-history cursor (the report's own default is 200). */
export const HISTORY_PAGE_LIMIT = 200;
/** Bound the cursor walk so a huge history can't stall a reconnect or single-position top-up. */
export const HISTORY_MAX_PAGES = 10;
/**
 * close-trades-history is rate-limited to ~5 requests / 10s, and ALL of a login's accounts share it.
 * A global min-interval pacer (one gate across every account + page) keeps us safely under that.
 */
export const REST_MIN_INTERVAL_MS = 2_200;
/** On HTTP 429, retry this many times honoring Retry-After (falls back to the delay below). */
export const HISTORY_RATELIMIT_RETRIES = 2;
export const HISTORY_RATELIMIT_DELAY_MS = 30_000;
/** How long a cached user-level REST token is reused before re-logging in (TradeLocker JWTs outlive this). */
export const USER_TOKEN_TTL_MS = 10 * 60_000;
