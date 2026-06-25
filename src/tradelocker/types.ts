export type TradeLockerEnvironment = 'demo' | 'live';

/**
 * One account-scoped JWT issued by /auth/jwt/accounts/tokens. A single credential set returns one
 * token per account it can access; `accountId` + `brandId` are required by SUBSCRIBE/UNSUBSCRIBE.
 */
export interface TradeLockerAccountToken {
	accessToken: string;
	accountId: string;
	brandId: string;
	/** Null when the auth response omits an expiry (not used in the PoC). */
	expiresAt: Date | null;
	/** `host` claim decoded from the JWT (the account's backend cluster, e.g. bsb.tradelocker.com).
	 *  The socket must belong to the same environment as this host or SUBSCRIBE returns invalidJwt. */
	host?: string;
}

/** PoC single-account config, sourced from TRADELOCKER_POC_* env vars (Phase 2; no DB). */
export interface PocAccountConfig {
	email: string;
	password: string;
	server: string;
	environment: TradeLockerEnvironment;
	/** Optional: pick this account's token from the issued list; falls back to the first. */
	accountId?: string;
}
