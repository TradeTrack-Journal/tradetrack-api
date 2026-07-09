/**
 * Value written to Trade.terminalName. Deliberately distinct from the EA importer's 'mt5' so the
 * @@unique([userId, terminalName, terminalTradeId]) key can never collide with EA-written rows.
 * The main app maps this back onto the mt5 bucket in terminal-normalize.ts.
 */
export const TERMINAL_NAME = 'metaapi';

/** Trade.terminalTradeId = `${POSITION_KEY_PREFIX}${positionId}` — mirrors cTrader's `ctrader_pos_`. */
export const POSITION_KEY_PREFIX = 'metaapi_pos_';

/**
 * Marker stored at TradingAccount.terminalSyncState.syncMethod. This is what distinguishes a
 * MetaApi-synced MT5 account from an EA-synced one — terminalType is 'mt5' for both.
 */
export const SYNC_METHOD = 'metaapi';

/** terminalIntegrationStatus values. `null` means active/healthy. */
export const STATUS = {
	/** The app captured credentials; the worker has not finished creating + connecting yet. */
	connecting: 'connecting',
	/** The app asked for teardown. INCLUDED in the worker's selection so teardown is reachable. */
	disconnecting: 'disconnecting',
	/** Terminal state. Only ever set by the worker, and only AFTER a confirmed undeploy. */
	deactivated: 'deactivated',
	/** Set by the main app's idle cron. MetaApi rows are excluded from that cron for now. */
	pausedInactive: 'paused_inactive',
} as const;

/** How often to reconcile live sessions with the DB (pick up newly connected / removed accounts). */
export const RECONCILE_INTERVAL_MS = 30_000;

export const RECONNECT_BASE_DELAY_MS = 1_000;
export const RECONNECT_MAX_DELAY_MS = 60_000;

/**
 * Deterministic cloud-account name. This — not login+server — is how we recognise an account as ours.
 * Matching on login+server would adopt an account belonging to another TradingAccount row (two rows can
 * carry the same MT5 login) or to another app sharing the MetaApi token.
 */
export const ACCOUNT_NAME_PREFIX = 'tradetrack-';

export function accountName(tradingAccountId: string): string {
	return `${ACCOUNT_NAME_PREFIX}${tradingAccountId}`;
}

/** Backfill window on every (re)connect — mirrors cTrader's constants. */
export const BACKFILL_LOOKBACK_DAYS = 90;
export const BACKFILL_WATERMARK_BUFFER_DAYS = 15;

/** MT5 deal discriminators. Balance operations (deposits/withdrawals) are not trades. */
export const DEAL_TYPE_BALANCE = 'DEAL_TYPE_BALANCE';
export const DEAL_TYPE_BUY = 'DEAL_TYPE_BUY';
export const DEAL_ENTRY_IN = 'DEAL_ENTRY_IN';
export const DEAL_ENTRY_OUT = 'DEAL_ENTRY_OUT';

/** MT5 position direction discriminator. */
export const POSITION_TYPE_BUY = 'POSITION_TYPE_BUY';

/**
 * MetaApi account states we branch on.
 *
 * `createAccount` resolves with state DEPLOYING, not DEPLOYED (the SDK re-GETs the account right after
 * the REST create). So "is it already running?" must accept both, or we would re-issue deploy() on an
 * account that is merely still spinning up. Only UNDEPLOYED genuinely needs a deploy() — that is the
 * pause/resume case.
 */
export const STATE_DEPLOYED = 'DEPLOYED';
export const STATE_DEPLOYING = 'DEPLOYING';
export const STATE_UNDEPLOYED = 'UNDEPLOYED';

/** States in which a cloud terminal is running (or about to) and therefore billing. */
export const LIVE_STATES: readonly string[] = [STATE_DEPLOYED, STATE_DEPLOYING];
