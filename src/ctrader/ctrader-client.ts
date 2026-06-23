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
		const body = (await response.text()).slice(0, 200);
		throw new Error(`cTrader token refresh failed: HTTP ${response.status} ${body}`);
	}

	const data = (await response.json()) as {
		accessToken?: string;
		refreshToken?: string;
		expiresIn?: number;
		errorCode?: string;
		description?: string;
	};

	if (data.errorCode) {
		throw new Error(`cTrader token refresh error: ${data.errorCode} — ${data.description ?? ''}`);
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
