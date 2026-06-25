const CTRADER_TOKEN_URL = 'https://openapi.ctrader.com/apps/token';

export interface CtraderTokens {
	accessToken: string;
	refreshToken: string;
	/** Seconds until the new access token expires. */
	expiresIn: number;
}

interface RefreshParams {
	clientId: string;
	clientSecret: string;
	refreshToken: string;
}

/**
 * Thrown when cTrader definitively rejects the refresh token (HTTP 400/401 or an errorCode in the
 * body, e.g. invalid_grant) — the token is dead and retrying won't help, so the caller deactivates
 * it. Transient failures (429, 5xx, network) throw a plain Error and are left to retry.
 */
export class CtraderTokenRejectedError extends Error {
	constructor(message: string) {
		super(message);
		this.name = 'CtraderTokenRejectedError';
	}
}

/**
 * OAuth/cTrader error codes that mean the refresh TOKEN itself is dead — only these deactivate the
 * account. Everything else (wrong app credentials invalid_client / unauthorized_client /
 * invalid_request, rate limits, server or network errors) is transient or a GLOBAL config fault and
 * must NOT deactivate — otherwise a single bad CTRADER_CLIENT_SECRET would reap every account at once.
 */
const DEAD_REFRESH_TOKEN_CODES = new Set(['invalid_grant']);

function isDeadRefreshToken(code: string | null | undefined): boolean {
	return code != null && DEAD_REFRESH_TOKEN_CODES.has(code.toLowerCase());
}

/**
 * Exchange a refresh token for a fresh access/refresh token pair (cTrader rotates both).
 * Ported from the main app's CtraderClient.refreshAccessToken — cTrader returns camelCase keys.
 */
export async function refreshAccessToken(params: RefreshParams): Promise<CtraderTokens> {
	const query = new URLSearchParams({
		grant_type: 'refresh_token',
		refresh_token: params.refreshToken,
		client_id: params.clientId,
		client_secret: params.clientSecret,
	});

	const response = await fetch(`${CTRADER_TOKEN_URL}?${query.toString()}`, {
		method: 'GET',
		headers: { Accept: 'application/json' },
	});

	if (!response.ok) {
		// Truncate the upstream body — it is unvetted and the request carried secrets in its query.
		const raw = (await response.text()).slice(0, 300);
		let code: string | undefined;
		try {
			const parsed = JSON.parse(raw) as { error?: string; errorCode?: string };
			code = parsed.error ?? parsed.errorCode;
		} catch {
			// non-JSON error body — leave code undefined so it is treated as transient (retry)
		}
		const message = `cTrader token refresh failed: HTTP ${response.status} ${code ?? raw}`;
		// Only a dead refresh token deactivates; a 400 invalid_client (wrong app secret) must retry.
		if (isDeadRefreshToken(code)) {
			throw new CtraderTokenRejectedError(message);
		}
		throw new Error(message);
	}

	const data = (await response.json()) as {
		accessToken?: string;
		refreshToken?: string;
		expiresIn?: number;
		errorCode?: string;
		description?: string;
	};

	if (data.errorCode) {
		const message = `cTrader token refresh error: ${data.errorCode} — ${data.description ?? ''}`;
		// Same allow-list as the HTTP branch: deactivate only on a dead refresh token, not on
		// transient/global codes (the main app's ctrader-errors.ts keeps these distinct too).
		if (isDeadRefreshToken(data.errorCode)) {
			throw new CtraderTokenRejectedError(message);
		}
		throw new Error(message);
	}
	if (!data.accessToken || !data.refreshToken) {
		throw new Error('cTrader token refresh returned an incomplete token pair');
	}

	return {
		accessToken: data.accessToken,
		refreshToken: data.refreshToken,
		expiresIn: data.expiresIn ?? 0,
	};
}
