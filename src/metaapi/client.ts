/**
 * How a MetaApi failure must be handled. Getting this wrong is the most expensive bug in this module.
 *
 * - `fatal-account` — THIS account's credentials or server are wrong, proven by an error code MetaApi
 *                     only emits while adding an account. Deactivate it. Never retry: MetaApi may
 *                     charge for each failed attempt to add an account.
 * - `global-auth`   — OUR METAAPI_TOKEN expired or was revoked (401). It hits every account at once.
 *                     NEVER deactivate or delete anything: that would destroy healthy, live cloud
 *                     accounts because a secret rotted. Log, alert, back off, wait for rotation.
 *                     cTrader draws the same line in ctrader-client.ts, separating a dead refresh token
 *                     (invalid_grant -> deactivate) from a global outage (retry).
 * - `not-found`     — 404: the cloud account is already gone. For teardown that is SUCCESS, not failure;
 *                     without this a crash between remove() and the DB write leaves the row stuck in
 *                     'disconnecting' forever.
 * - `transient`     — network blip, 5xx, rate limit, a ValidationError from a data call. Retry.
 */
export type MetaApiErrorKind = 'fatal-account' | 'global-auth' | 'not-found' | 'transient';

/**
 * Codes MetaApi returns when the TRADING ACCOUNT itself is unusable. Classification keys off these
 * codes alone.
 *
 * It deliberately does NOT key off HTTP 400. The SDK raises ValidationError(status=400) for any
 * server-side validation complaint, including ones from data calls (`getDealsByTimeRange`,
 * `getPositions`) and from the account-listing lookup. Treating a bare 400 as this account's fault
 * would send a perfectly healthy, streaming terminal down the teardown path and delete it.
 */
const FATAL_ACCOUNT_CODES = new Set([
	'E_AUTH',
	'E_SRV_NOT_FOUND',
	'ERR_OTP_REQUIRED',
	'E_PASSWORD_CHANGE_REQUIRED',
	'E_TRADING_ACCOUNT_DISABLED',
	'E_SERVER_TIMEZONE',
]);

interface MetaApiErrorShape {
	status?: number;
	code?: string;
	error?: string;
	message?: string;
	/** ValidationError.details is typed `object` — it is whatever the server sent. Never assume an array. */
	details?: unknown;
}

function asShape(error: unknown): MetaApiErrorShape {
	return error ?? {};
}

function collectCodes(shape: MetaApiErrorShape): string[] {
	const detailCodes = Array.isArray(shape.details)
		? shape.details.map((detail: unknown) => (detail as { code?: unknown })?.code)
		: [];
	return [shape.code, shape.error, ...detailCodes].filter(
		(value): value is string => typeof value === 'string'
	);
}

/**
 * Never throws. A throw here would escape handleFailure and leave a freshly created — and billing —
 * cloud terminal with no owner.
 */
export function classifyMetaApiError(error: unknown): MetaApiErrorKind {
	try {
		const shape = asShape(error);

		// 401 is about OUR token, not the trading account. It must never cascade into teardown.
		if (shape.status === 401) return 'global-auth';
		if (shape.status === 404) return 'not-found';

		if (collectCodes(shape).some((code) => FATAL_ACCOUNT_CODES.has(code))) return 'fatal-account';

		return 'transient';
	} catch {
		// An unexpected error shape must degrade to a retry, never to a deletion.
		return 'transient';
	}
}
