import type { TradeLockerAccountToken } from './types';

interface AuthParams {
	/** Auth host, e.g. https://stg.tradelocker.com (NOT the socket host). */
	baseUrl: string;
	email: string;
	password: string;
	server: string;
}

interface RawAccountToken {
	accessToken?: string;
	accountId?: string | number;
	brandId?: string | number;
	expireDate?: number | string;
}

/**
 * Issue per-account stream JWTs via POST /backend-api/auth/jwt/accounts/tokens.
 *
 * Differences from the main app's REST importer (intentional, confirmed against the streams example):
 *  - endpoint is /auth/jwt/accounts/tokens, NOT /auth/jwt/token;
 *  - it returns one token PER account the credentials can access (`data.data[]`), each carrying the
 *    accountId + brandId that SUBSCRIBE/UNSUBSCRIBE require;
 *  - the `developer-api-key` header MUST NOT be sent here — that key belongs only on the socket
 *    handshake.
 */
export async function authenticateAccounts(params: AuthParams): Promise<TradeLockerAccountToken[]> {
	const response = await fetch(`${params.baseUrl}/backend-api/auth/jwt/accounts/tokens`, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
		body: JSON.stringify({
			email: params.email,
			password: params.password,
			server: params.server,
		}),
	});

	const text = await response.text();

	if (!response.ok) {
		// Body is unvetted and the request carried credentials — truncate.
		throw new Error(`TradeLocker auth failed: HTTP ${response.status} ${text.slice(0, 200)}`);
	}

	if (text.trim().startsWith('<')) {
		throw new Error('TradeLocker auth returned HTML — check the auth host, server and environment.');
	}

	let parsed: unknown;
	try {
		parsed = JSON.parse(text);
	} catch {
		throw new Error('TradeLocker auth returned an invalid JSON response.');
	}

	// Axios in the example reads response.data.data, i.e. the body is { data: [ ... ] }. Accept a
	// double-nested { data: { data: [...] } } too, defensively.
	const container = parsed as { data?: unknown };
	const inner = (container.data as { data?: unknown })?.data;
	const rows: RawAccountToken[] = Array.isArray(container.data)
		? (container.data as RawAccountToken[])
		: Array.isArray(inner)
			? (inner as RawAccountToken[])
			: [];

	return rows
		.filter(
			(row): row is RawAccountToken & { accessToken: string } =>
				typeof row.accessToken === 'string' && row.accessToken.length > 0
		)
		.map((row) => ({
			accessToken: row.accessToken,
			accountId: row.accountId !== undefined && row.accountId !== null ? String(row.accountId) : '',
			brandId: row.brandId !== undefined && row.brandId !== null ? String(row.brandId) : '',
			expiresAt: toDate(row.expireDate),
			host: decodeJwtHost(row.accessToken),
		}));
}

interface UserAuthParams {
	/** Auth host, e.g. https://demo.tradelocker.com (NOT the socket host). */
	baseUrl: string;
	email: string;
	password: string;
	server: string;
	/** developer-api-key — sent here (unlike the streams /accounts/tokens auth, which forbids it). */
	developerApiKey?: string;
}

/**
 * Issue a USER-LEVEL JWT via POST /backend-api/auth/jwt/token — the token the REST /trade/* endpoints
 * accept. The per-account stream JWT from /accounts/tokens is rejected there with HTTP 400; the REST
 * layer wants this user token plus an `accNum` header to pick the account, exactly as the main app's
 * importer does. Returns the access token only (refresh isn't used — re-login is the refresh).
 */
export async function authenticateUser(params: UserAuthParams): Promise<string> {
	const headers: Record<string, string> = { 'Content-Type': 'application/json' };
	if (params.developerApiKey) {
		headers['developer-api-key'] = params.developerApiKey;
	}

	const response = await fetch(`${params.baseUrl}/backend-api/auth/jwt/token`, {
		method: 'POST',
		headers,
		body: JSON.stringify({ email: params.email, password: params.password, server: params.server }),
	});

	const text = await response.text();
	if (!response.ok) {
		// Body is unvetted and the request carried credentials — truncate.
		throw new Error(`TradeLocker REST auth failed: HTTP ${response.status} ${text.slice(0, 200)}`);
	}
	if (text.trim().startsWith('<')) {
		throw new Error(
			'TradeLocker REST auth returned HTML — check the auth host, server and environment.'
		);
	}

	let parsed: unknown;
	try {
		parsed = JSON.parse(text);
	} catch {
		throw new Error('TradeLocker REST auth returned an invalid JSON response.');
	}

	const body = parsed as { accessToken?: unknown; data?: { accessToken?: unknown } };
	const token =
		typeof body.accessToken === 'string' && body.accessToken
			? body.accessToken
			: typeof body.data?.accessToken === 'string'
				? body.data.accessToken
				: '';
	if (!token) {
		throw new Error('TradeLocker REST auth response had no accessToken.');
	}
	return token;
}

/**
 * Best-effort decode of the `host` claim from a Streams JWT (no signature check). The host names the
 * account's backend cluster (e.g. bsb.tradelocker.com). The streams socket MUST belong to the same
 * environment as this host, or SUBSCRIBE fails with `invalidJwt — Host: <host> not recognized`.
 */
function decodeJwtHost(jwt: string): string | undefined {
	const payload = jwt.split('.')[1];
	if (!payload) {
		return undefined;
	}
	try {
		const json = Buffer.from(payload, 'base64url').toString('utf8');
		const claims = JSON.parse(json) as { host?: unknown };
		return typeof claims.host === 'string' ? claims.host : undefined;
	} catch {
		return undefined;
	}
}

function toDate(value: number | string | undefined): Date | null {
	if (value === undefined || value === null) {
		return null;
	}
	const ms = Number(value);
	if (!Number.isFinite(ms) || ms <= 0) {
		return null;
	}
	const date = new Date(ms);
	return Number.isNaN(date.getTime()) ? null : date;
}
