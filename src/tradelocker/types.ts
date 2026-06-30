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

export type TradeSide = 'BUY' | 'SELL';

/**
 * Resolved persistence context for one streamed account — the DB identity the writer needs to key a
 * Trade row. Sourced from the linked TradingAccount (Phase 3 manager); `nominal` drives the realized
 * profit-% / R-multiple math exactly as the main app's cron does.
 */
export interface TradeLockerAccountContext {
	userId: string;
	tradingAccountId: string;
	/** Account nominal (deposit) in account currency; 0 when unknown — money math degrades gracefully. */
	nominal: number;
}

/**
 * A normalized OPEN position taken from a live `Position` stream message. `quantity` is in UNITS (not
 * lots) to match the main app's importer, which the stream provides directly as `units`.
 */
export interface OpenTrade {
	positionId: string;
	symbol: string;
	side: TradeSide;
	quantity: number;
	entryDate: Date;
}

/**
 * A normalized fully-realized CLOSED trade, sourced from REST `close-trades-history` (the live
 * `ClosePosition` message carries no realized money). All monetary fields are absolute totals for the
 * position, in account currency — the writer SETs them (idempotent), never accumulates.
 */
export interface ClosedTrade {
	positionId: string;
	symbol: string;
	side: TradeSide;
	quantity: number;
	entryDate: Date;
	exitDate: Date;
	grossProfit: number;
	commission: number;
	swap: number;
	/** Native net profit when the report provides it; else pnl falls back to gross + commission + swap. */
	netProfit?: number;
}
