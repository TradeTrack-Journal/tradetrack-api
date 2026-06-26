export const CTRADER_HOSTS = {
	demo: 'demo.ctraderapi.com',
	live: 'live.ctraderapi.com',
} as const;

export const CTRADER_PORT = 5035;

/** Outgoing heartbeat cadence. cTrader drops a silent socket after ~30s. */
export const HEARTBEAT_INTERVAL_MS = 10_000;

/** Hard cap on any single command. The library never rejects a pending command when the
 *  socket dies, so without this a dropped connection hangs auth/probes forever. */
export const COMMAND_TIMEOUT_MS = 15_000;

/** Active round-trip liveness probe (ProtoOAVersionReq). Detects a silently dropped socket. */
export const LIVENESS_PROBE_INTERVAL_MS = 20_000;

export const RECONNECT_BASE_DELAY_MS = 1_000;
export const RECONNECT_MAX_DELAY_MS = 60_000;

/** How often to reconcile live connections with the DB so newly connected / removed cTrader
 *  accounts are picked up without a process restart. A cheap probe gates the full reload. */
export const RECONCILE_INTERVAL_MS = 30_000;

/** On (re)connect, backfill closed history from this far back when there is no prior watermark. */
export const BACKFILL_LOOKBACK_MS = 90 * 24 * 60 * 60 * 1000;

/** Safety overlap subtracted from the trade watermark so a deal near the boundary isn't missed. */
export const BACKFILL_WATERMARK_BUFFER_MS = 15 * 24 * 60 * 60 * 1000;

/** Cap on deals returned by a single ProtoOADealListReq. */
export const BACKFILL_MAX_ROWS = 10_000;

/** Refresh an access token this long before it actually expires. */
export const TOKEN_REFRESH_SKEW_MS = 60_000;

/** Fallback access-token lifetime when cTrader's refresh response omits expiresIn (avoids a
 *  zero-lifetime token that would otherwise refresh in a hot loop). */
export const DEFAULT_TOKEN_LIFETIME_MS = 30 * 24 * 60 * 60 * 1000;

/** Written to CtraderToken.deactivatedReason when this backend kills a token whose refresh token
 *  cTrader rejected. The main app reads it to keep the returning user OFF auto-reactivation (the
 *  token is dead — only a fresh OAuth reconnect revives it). Keep the literal in sync with the main
 *  app's CtraderDeactivationReason.TOKEN_REJECTED. */
export const DEAD_TOKEN_DEACTIVATION_REASON = 'TOKEN_REJECTED';
