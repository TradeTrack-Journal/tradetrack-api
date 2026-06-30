import { z } from 'zod';

import {
	HISTORY_MAX_PAGES,
	HISTORY_PAGE_LIMIT,
	HISTORY_RATELIMIT_DELAY_MS,
	HISTORY_RATELIMIT_RETRIES,
	REST_TIMEOUT_MS,
} from './constants';
import type { ClosedTrade, TradeSide } from './types';

/**
 * REST top-up for realized P&L. The live `ClosePosition` stream message carries no money, so closed
 * trades are finalized from `GET /backend-api/trade/reports/close-trades-history` — the same report
 * the main app's cron reads, so realized rows come out numerically identical.
 *
 * Each row is self-contained (positionId, instrument, side, amounts, open/close ms, gross `profit`,
 * `netProfit`, commission, swap), so no ordersHistory pairing is needed. Responses are cursor
 * paginated by `lastTradeId`; we walk pages (bounded) newest-first and stop at the cursor end.
 */

/** Numeric fields arrive as decimal strings (sometimes numbers); coerce leniently, 0 on garbage. */
const numeric = z.union([z.string(), z.number()]).optional();

const ClosedTradeRowSchema = z.object({
	positionId: z.union([z.string(), z.number()]).transform((v) => String(v)),
	instrument: z.string().optional(),
	positionSide: z.string().optional(),
	openAmount: numeric,
	closeAmount: numeric,
	openMilliseconds: numeric,
	closeMilliseconds: numeric,
	/** Gross profit before commission/swap. */
	profit: numeric,
	/** Net profit after commission/swap. */
	netProfit: numeric,
	commission: numeric,
	swap: numeric,
});

export interface FetchClosedTradesParams {
	/** Auth host base, e.g. https://demo.tradelocker.com — `/backend-api/...` is appended. */
	baseUrl: string;
	/** Bearer JWT scoped to the account (the per-account stream token works for these REST reads). */
	accessToken: string;
	/** TradeLocker account number — required `accNum` header for every /trade/* request. */
	accNum: string;
	/** developer-api-key header — the /trade/* gateway rejects the request with HTTP 400 without it. */
	developerApiKey?: string;
	/** Awaited before every HTTP request to pace calls under TradeLocker's ~5-req/10s limit. */
	throttle?: () => Promise<void>;
	/** Stop once a row with this positionId is seen (early-out for a single-position top-up). */
	untilPositionId?: string;
	limit?: number;
	maxPages?: number;
}

function parseNumber(value: string | number | undefined): number {
	if (value === undefined || value === null) {
		return 0;
	}
	const n = typeof value === 'number' ? value : Number(value);
	return Number.isFinite(n) ? n : 0;
}

function toClosedTrade(row: z.infer<typeof ClosedTradeRowSchema>): ClosedTrade | null {
	if (!row.positionId) {
		return null;
	}
	const side: TradeSide = (row.positionSide ?? '').toUpperCase() === 'SELL' ? 'SELL' : 'BUY';
	const openAmount = parseNumber(row.openAmount);
	const closeAmount = parseNumber(row.closeAmount);
	// Mirror the main app's Math.min(openQty, closeQty); fall back to whichever side is present.
	const quantity =
		openAmount > 0 && closeAmount > 0 ? Math.min(openAmount, closeAmount) : closeAmount || openAmount;

	const openMs = parseNumber(row.openMilliseconds);
	const closeMs = parseNumber(row.closeMilliseconds);
	// A realized row must have a close time; without it we can't finalize a trade — skip rather than
	// write a bogus epoch date.
	if (closeMs <= 0) {
		return null;
	}

	return {
		positionId: row.positionId,
		symbol: row.instrument ?? `INSTRUMENT_${row.positionId}`,
		side,
		quantity,
		entryDate: openMs > 0 ? new Date(openMs) : new Date(closeMs),
		exitDate: new Date(closeMs),
		grossProfit: parseNumber(row.profit),
		commission: parseNumber(row.commission),
		swap: parseNumber(row.swap),
		netProfit: row.netProfit === undefined ? undefined : parseNumber(row.netProfit),
	};
}

/**
 * Fetch recent realized closed trades for one account, newest-first, mapped to ClosedTrade. Bounded
 * by maxPages so a huge history can't stall a reconnect; pass `untilPositionId` to stop as soon as a
 * specific just-closed position is found.
 */
export async function fetchClosedTrades(params: FetchClosedTradesParams): Promise<ClosedTrade[]> {
	const limit = params.limit ?? HISTORY_PAGE_LIMIT;
	const maxPages = params.maxPages ?? HISTORY_MAX_PAGES;
	const url = `${params.baseUrl}/backend-api/trade/reports/close-trades-history`;

	const headers: Record<string, string> = {
		Authorization: `Bearer ${params.accessToken}`,
		accNum: params.accNum,
		'Content-Type': 'application/json',
	};
	if (params.developerApiKey) {
		headers['developer-api-key'] = params.developerApiKey;
	}

	const out: ClosedTrade[] = [];
	let lastTradeId: string | undefined;

	for (let page = 0; page < maxPages; page += 1) {
		const query = new URLSearchParams({ limit: String(limit) });
		if (lastTradeId) {
			query.set('lastTradeId', lastTradeId);
		}

		const res = await getPage(`${url}?${query.toString()}`, headers, params.throttle);
		if (!res.ok) {
			throw new Error(`close-trades-history HTTP ${res.status} ${res.statusText}`);
		}

		const raw: unknown = await res.json();
		const rows = extractRows(raw);
		if (rows === undefined) {
			// Fail LOUD on an envelope we don't recognise — a permissive parse would look like an empty
			// page and silently drop a real close. A genuinely empty history returns an empty array.
			throw new Error('close-trades-history: unrecognized response shape');
		}

		let hitTarget = false;
		for (const rawRow of rows) {
			const parsed = ClosedTradeRowSchema.safeParse(rawRow);
			if (!parsed.success) {
				continue; // tolerate a single malformed row rather than dropping the whole page
			}
			const trade = toClosedTrade(parsed.data);
			if (trade) {
				out.push(trade);
				if (params.untilPositionId && trade.positionId === params.untilPositionId) {
					hitTarget = true;
				}
			}
		}

		if (hitTarget) {
			break; // found the position we were topping up — no need to page further back
		}
		const next = extractNextCursor(raw);
		if (!next) {
			break; // cursor exhausted
		}
		lastTradeId = next;
	}

	return out;
}

/** GET one page: pace via the shared throttle, then retry HTTP 429 honoring Retry-After (bounded). */
async function getPage(
	url: string,
	headers: Record<string, string>,
	throttle?: () => Promise<void>
): Promise<Response> {
	for (let attempt = 0; ; attempt += 1) {
		if (throttle) {
			await throttle();
		}
		const res = await fetch(url, {
			method: 'GET',
			headers,
			signal: AbortSignal.timeout(REST_TIMEOUT_MS),
		});
		if (res.status === 429 && attempt < HISTORY_RATELIMIT_RETRIES) {
			await sleep(parseRetryAfterMs(res.headers.get('retry-after')));
			continue;
		}
		return res;
	}
}

function parseRetryAfterMs(header: string | null): number {
	const seconds = header ? Number(header) : Number.NaN;
	return Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : HISTORY_RATELIMIT_DELAY_MS;
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Pull the rows array out of the envelope shapes /trade/* may use: a bare array, `{ data: [...] }`,
 * the double-nested `{ data: { data: [...] } }`, or any of those behind a `{ d: ... }` wrapper.
 * Returns undefined for a shape we don't recognise so the caller can fail loud instead of silently
 * treating it as empty.
 */
function extractRows(raw: unknown): unknown[] | undefined {
	if (Array.isArray(raw)) {
		return raw as unknown[];
	}
	if (raw && typeof raw === 'object') {
		const obj = raw as Record<string, unknown>;
		if (Array.isArray(obj.data)) {
			return obj.data as unknown[];
		}
		const nested = obj.data as Record<string, unknown> | undefined;
		if (nested && Array.isArray(nested.data)) {
			return nested.data as unknown[];
		}
		if (obj.d && typeof obj.d === 'object') {
			return extractRows(obj.d);
		}
	}
	return undefined;
}

/** Next-page cursor (links.next.params.lastTradeId), tolerating a `{ d: ... }` wrapper. */
function extractNextCursor(raw: unknown): string | undefined {
	if (!raw || typeof raw !== 'object') {
		return undefined;
	}
	const obj = raw as Record<string, unknown>;
	const container = obj.d && typeof obj.d === 'object' ? (obj.d as Record<string, unknown>) : obj;
	const links = container.links as { next?: { params?: { lastTradeId?: unknown } } } | undefined;
	const cursor = links?.next?.params?.lastTradeId;
	return typeof cursor === 'string' && cursor.length > 0 ? cursor : undefined;
}
