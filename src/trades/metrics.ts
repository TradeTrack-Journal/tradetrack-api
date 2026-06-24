// Vendored from the main app so terminal trades written here are numerically identical to the
// ones its sync writes: money rounding (money-precision.ts), trade result (trade-result.ts) and
// the realized R-multiple / profit-% helpers (lib/trade-rr.ts).

/** Default risk % for imported/terminal trades — terminals don't report risk. */
export const DEFAULT_IMPORTED_RISK_PERCENTAGE = 1;

export function roundMoney(value: number): number {
	return Math.round(value * 100) / 100;
}

export function roundPercent(value: number): number {
	return Math.round(value * 10000) / 10000;
}

/**
 * Classify trade outcome from PnL vs account nominal.
 * BE when |pnl as % of nominal| < 0.1%; nominal 0 + zero pnl → BE; else TP/SL by sign.
 */
export function getTradeResult(
	pnl: number | null | undefined,
	nominal: number
): 'TP' | 'SL' | 'BE' | null {
	if (pnl === null || pnl === undefined) {
		return null;
	}
	if (nominal > 0 && Math.abs((pnl / nominal) * 100) < 0.1) {
		return 'BE';
	}
	if (nominal === 0 && pnl === 0) {
		return 'BE';
	}
	return pnl > 0 ? 'TP' : 'SL';
}

/** profit % from money PnL and account nominal */
export function computeProfitPercentageFromMoney(
	profitMoney: number | null | undefined,
	nominal: number | null | undefined
): number | null {
	if (profitMoney === null || profitMoney === undefined || !Number.isFinite(profitMoney)) {
		return null;
	}
	if (nominal === null || nominal === undefined || !Number.isFinite(nominal) || nominal <= 0) {
		return null;
	}
	return roundPercent((profitMoney / nominal) * 100);
}

/**
 * Realized R-multiple: account-currency profit divided by 1R (nominal × risk%).
 * Falls back to profit% / risk% when nominal is missing.
 */
export function computeTradeRr(params: {
	profitMoney?: number | null;
	profitPercentage?: number | null;
	nominal?: number | null;
	riskPercentage?: number | null;
}): number | null {
	const riskPct = params.riskPercentage;
	if (riskPct === null || riskPct === undefined || !Number.isFinite(riskPct) || riskPct <= 0) {
		return null;
	}

	const { nominal, profitMoney } = params;
	if (
		profitMoney !== null &&
		profitMoney !== undefined &&
		Number.isFinite(profitMoney) &&
		nominal != null &&
		nominal > 0
	) {
		const riskMoney = (nominal * riskPct) / 100;
		if (!Number.isFinite(riskMoney) || riskMoney === 0) {
			return null;
		}
		return profitMoney / riskMoney;
	}

	const profitPct = params.profitPercentage;
	if (profitPct !== null && profitPct !== undefined && Number.isFinite(profitPct)) {
		return profitPct / riskPct;
	}

	return null;
}
