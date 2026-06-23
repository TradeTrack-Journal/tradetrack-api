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

/** Refresh an access token this long before it actually expires. */
export const TOKEN_REFRESH_SKEW_MS = 60_000;

/** Fallback access-token lifetime when cTrader's refresh response omits expiresIn (avoids a
 *  zero-lifetime token that would otherwise refresh in a hot loop). */
export const DEFAULT_TOKEN_LIFETIME_MS = 30 * 24 * 60 * 60 * 1000;
