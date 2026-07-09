/**
 * Phase 0 spike — throwaway validation, imported by nothing and excluded from `nest build`.
 *
 * Answers the questions the design spec parked as "open":
 *   1. Does a read-only (investor password) connection to OUR broker work at all?
 *   2. How long does `createAccount` (which IS the deploy) take to reach CONNECTED?
 *   3. What do this broker's raw deals actually look like, and how deep does history go?
 *   4. Is `deal.profit` really gross (i.e. excludes commission and swap)?
 *   5. What does one account, deployed, actually cost?
 *
 * BILLING WARNING
 * `createAccount` starts a cloud terminal and bills a **6-hour minimum**. Failed add attempts may
 * also be charged. Run this ONCE, with real credentials. Never probe a wrong password "to see the
 * error" — that costs money and proves nothing the docs don't already state.
 */
import { setTimeout as sleep } from 'node:timers/promises';

import MetaApi, { SynchronizationListener } from 'metaapi.cloud-sdk';
import type { MetatraderDeal, MetatraderPosition } from 'metaapi.cloud-sdk';

const token = process.env.METAAPI_TOKEN;
const login = process.env.METAAPI_POC_LOGIN;
const server = process.env.METAAPI_POC_SERVER;
const password = process.env.METAAPI_POC_PASSWORD;

if (!token || !login || !server || !password) {
	throw new Error(
		'Set METAAPI_TOKEN, METAAPI_POC_LOGIN, METAAPI_POC_SERVER and METAAPI_POC_PASSWORD in .env'
	);
}

const STREAM_LISTEN_MS = 120_000;
const HISTORY_LOOKBACK_DAYS = 90;
const DAY_MS = 24 * 60 * 60 * 1_000;

/**
 * MetatraderPosition.id is a NUMBER while MetatraderDeal.positionId is a STRING. Comparing them
 * directly always fails, which would silently misclassify every position. Normalize to string.
 */
function positionKey(value: number | string): string {
	return String(value);
}

class SpikeListener extends SynchronizationListener {
	override async onDealAdded(_instanceIndex: string, deal: MetatraderDeal): Promise<void> {
		console.log('[spike] onDealAdded', JSON.stringify(deal));
	}

	override async onPositionUpdated(
		_instanceIndex: string,
		position: MetatraderPosition
	): Promise<void> {
		console.log(
			`[spike] onPositionUpdated id=${positionKey(position.id)} unrealized=${position.unrealizedProfit}`
		);
	}

	override async onPositionRemoved(_instanceIndex: string, positionId: string): Promise<void> {
		console.log('[spike] onPositionRemoved', positionId);
	}
}

async function main(): Promise<void> {
	const api = new MetaApi(token as string);
	const startedAt = Date.now();

	console.log('[spike] createAccount — this DEPLOYS the account and starts billing...');
	const account = await api.metatraderAccountApi.createAccount({
		name: 'tradetrack-spike',
		type: 'cloud-g2',
		login: login as string,
		password: password as string,
		server: server as string,
		platform: 'mt5',
		magic: 0,
	});
	console.log(`[spike] created id=${account.id} state=${account.state}`);

	try {
		console.log('[spike] waiting for the broker connection...');
		await account.waitConnected();
		console.log(`[spike] CONNECTED after ${((Date.now() - startedAt) / 1_000).toFixed(1)}s`);

		const rpc = account.getRPCConnection();
		await rpc.connect();
		await rpc.waitSynchronized();

		const now = new Date();
		const from = new Date(now.getTime() - HISTORY_LOOKBACK_DAYS * DAY_MS);
		const history = await rpc.getDealsByTimeRange(from, now);
		const positions = await rpc.getPositions();
		const deals = history.deals ?? [];

		console.log(`[spike] deals=${deals.length} openPositions=${positions.length}`);
		console.log('[spike] --- first 3 raw deals ---');
		console.log(JSON.stringify(deals.slice(0, 3), null, 2));
		console.log('[spike] --- history depth ---');
		const times = deals.map((deal) => new Date(deal.time).getTime()).filter(Number.isFinite);
		if (times.length) {
			console.log(`oldest=${new Date(Math.min(...times)).toISOString()}`);
			console.log(`newest=${new Date(Math.max(...times)).toISOString()}`);
		} else {
			console.log('no deals in the window');
		}
		console.log('[spike] --- first 3 open positions ---');
		console.log(JSON.stringify(positions.slice(0, 3), null, 2));

		// Reconcile a few closed positions by hand: compare `net` against the MT5 terminal's Profit
		// column. If they match, `deal.profit` is gross and our money semantics are correct.
		const openIds = new Set(positions.map((position) => positionKey(position.id)));
		const groups = new Map<string, MetatraderDeal[]>();
		for (const deal of deals) {
			if (deal.type === 'DEAL_TYPE_BALANCE' || !deal.positionId) continue;
			const bucket = groups.get(deal.positionId);
			if (bucket) bucket.push(deal);
			else groups.set(deal.positionId, [deal]);
		}
		console.log('[spike] --- money reconciliation (compare `net` with MT5 Profit column) ---');
		for (const [positionId, group] of [...groups].slice(0, 5)) {
			const gross = group.reduce((sum, deal) => sum + (deal.profit ?? 0), 0);
			const commission = group.reduce((sum, deal) => sum + (deal.commission ?? 0), 0);
			const swap = group.reduce((sum, deal) => sum + (deal.swap ?? 0), 0);
			const state = openIds.has(positionId) ? 'OPEN' : 'CLOSED';
			console.log(
				`position ${positionId} [${state}] deals=${group.length} gross=${gross} commission=${commission} swap=${swap} net=${gross + commission + swap}`
			);
		}

		console.log(`[spike] streaming for ${STREAM_LISTEN_MS / 1_000}s...`);
		const stream = account.getStreamingConnection();
		stream.addSynchronizationListener(new SpikeListener());
		await stream.connect();
		await stream.waitSynchronized();
		await sleep(STREAM_LISTEN_MS);
		await stream.close();
	} finally {
		// Always stop the meter, even if the run above threw.
		console.log('[spike] undeploy + remove (stops billing, deletes the cloud account)...');
		await account.undeploy();
		await account.remove();
		console.log('[spike] done — now check the actual charge at app.metaapi.cloud.');
	}
}

main().catch((error: unknown) => {
	console.error('[spike] FAILED', error);
	process.exitCode = 1;
});
