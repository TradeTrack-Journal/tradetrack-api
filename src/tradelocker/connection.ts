import { Logger } from '@nestjs/common';
import { io, type Socket } from 'socket.io-client';

import {
	CONNECT_TIMEOUT_MS,
	STREAM_EVENTS,
	STREAM_HANDSHAKE_PATH,
	STREAM_NAMESPACE,
	SUBSCRIBE_ACK_TIMEOUT_MS,
	SUBSCRIPTION_ACTIONS,
} from './constants';
import {
	StatusMessageSchema,
	StreamMessageSchema,
	TypedMessageSchema,
	type AccountStatusMessage,
	type ClosePositionMessage,
	type OpenOrderMessage,
	type PositionMessage,
} from './stream.schema';
import type { TradeLockerEnvironment } from './types';

export interface TradeLockerConnectionHandlers {
	onAccountStatus: (msg: AccountStatusMessage) => void;
	onOpenOrder: (msg: OpenOrderMessage) => void;
	onPosition: (msg: PositionMessage) => void;
	onClosePosition: (msg: ClosePositionMessage) => void;
	/** Initial snapshot finished — subsequent messages are realtime deltas. */
	onSyncEnd: () => void;
	/** SUBSCRIBE rejected because the JWT is invalid/expired — caller should re-auth + resubscribe. */
	onJwtInvalid: (reason: string) => void;
	/** Fatal: socket dropped, connect_error, or a server `Disconnected` property. Fires at most once. */
	onLost: (reason: string) => void;
}

/** A SUBSCRIBE failure carrying enough context for the manager to decide deactivate vs back-off. */
export class TradeLockerSubscribeError extends Error {
	constructor(
		message: string,
		readonly code: string,
		/** true → don't reconnect (bad account / permission); false → transient, may retry. */
		readonly fatal: boolean
	) {
		super(message);
		this.name = 'TradeLockerSubscribeError';
	}
}

/**
 * One socket.io connection to the TradeLocker Streams API for a single account. Wraps the
 * socket.io-client with the safety the example lacks: a bounded connect, a bounded SUBSCRIBE ack,
 * validated dispatch, the SyncEnd snapshot boundary, and a single onLost(). Reconnect/backoff is
 * owned by the manager (reconnection is disabled on the client), mirroring CtraderConnection.
 */
export class TradeLockerConnection {
	private socket: Socket | null = null;
	private lost = false;
	private accountId?: string;
	private brandId?: string;

	constructor(
		private readonly environment: TradeLockerEnvironment,
		/** Resolved socket base URL, e.g. wss://api-dev.tradelocker.com (the manager applies the env override). */
		private readonly streamHost: string,
		private readonly developerApiKey: string,
		private readonly logger: Logger,
		private readonly handlers: TradeLockerConnectionHandlers
	) {}

	/** Open the socket (engine.io handshake) and start listening. Rejects on connect error/timeout. */
	async open(): Promise<void> {
		const url = `${this.streamHost}${STREAM_NAMESPACE}`;
		const socket = io(url, {
			path: STREAM_HANDSHAKE_PATH,
			// Prefer websocket but fall back to polling when a host/proxy refuses a direct WS upgrade
			// (some TradeLocker hosts answer a bare "websocket error" otherwise). tryAllTransports makes
			// socket.io try the next transport instead of giving up on the first.
			transports: ['websocket', 'polling'],
			tryAllTransports: true,
			extraHeaders: { 'developer-api-key': this.developerApiKey },
			// The manager owns reconnect/backoff, so a drop surfaces as a single onLost().
			reconnection: false,
			autoConnect: false,
		});
		this.socket = socket;
		this.wire(socket);

		await new Promise<void>((resolve, reject) => {
			let settled = false;
			const timer = setTimeout(() => {
				if (settled) return;
				settled = true;
				reject(new Error(`socket connect timed out after ${CONNECT_TIMEOUT_MS}ms`));
			}, CONNECT_TIMEOUT_MS);

			socket.once('connect', () => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				resolve();
			});
			socket.once('connect_error', (error) => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				reject(new Error(`connect_error: ${describe(error)}`));
			});
			socket.connect();
		});
	}

	/** Subscribe to the account stream. Resolves on `{status:'ok'}`, throws TradeLockerSubscribeError. */
	async subscribe(jwt: string, accountId?: string, brandId?: string): Promise<void> {
		const socket = this.socket;
		if (!socket) {
			throw new Error('subscribe() called before open()');
		}
		this.accountId = accountId;
		this.brandId = brandId;

		const ack = await this.emitWithAck(socket, {
			action: SUBSCRIPTION_ACTIONS.subscribe,
			token: jwt,
		});

		const parsed = StatusMessageSchema.safeParse(ack);
		if (!parsed.success) {
			// Unknown ack shape — treat as transient so the manager retries rather than giving up.
			throw new TradeLockerSubscribeError(
				`unrecognized SUBSCRIBE ack: ${JSON.stringify(ack).slice(0, 200)}`,
				'unknown',
				false
			);
		}

		const { status, code, message } = parsed.data;
		if (status === 'ok') {
			this.logger.log(`[${this.environment}] subscribed (code=${code ?? 'ok'})`);
			return;
		}

		const reason = `${code ?? 'error'}${message ? `: ${message}` : ''}`;
		switch (code) {
			case 'invalidJwt':
				// "Host: <h> not recognized" is an ENVIRONMENT mismatch (account vs socket), not an
				// expiry — reconnecting can't fix a config error, so make it fatal. A genuine expiry
				// stays transient (refresh + resubscribe).
				if (/not recognized|host/i.test(message ?? '')) {
					throw new TradeLockerSubscribeError(reason, 'invalidJwt', true);
				}
				this.handlers.onJwtInvalid(reason);
				throw new TradeLockerSubscribeError(reason, 'invalidJwt', false);
			case 'accountNotFound':
			case 'unauthorized':
				// Bad/forbidden account — reconnecting can only hot-loop.
				throw new TradeLockerSubscribeError(reason, code, true);
			case 'rateLimit':
			case 'alreadyConnectedError':
				// Transient: back off (don't hot-loop) and retry.
				throw new TradeLockerSubscribeError(reason, code, false);
			default:
				throw new TradeLockerSubscribeError(reason, code ?? 'error', false);
		}
	}

	/** Idempotent teardown — best-effort UNSUBSCRIBE, then drop the socket. */
	close(): void {
		this.lost = true;
		const socket = this.socket;
		this.socket = null;
		if (!socket) {
			return;
		}
		try {
			if (socket.connected && this.accountId && this.brandId) {
				socket.emit(STREAM_EVENTS.subscriptions, {
					action: SUBSCRIPTION_ACTIONS.unsubscribe,
					brandId: this.brandId,
					accountId: this.accountId,
				});
			}
		} catch {
			// best-effort
		}
		try {
			socket.removeAllListeners();
			socket.disconnect();
		} catch {
			// ignore close errors
		}
	}

	private wire(socket: Socket): void {
		socket.on(STREAM_EVENTS.stream, (raw: unknown) => this.dispatch(raw));
		socket.on(STREAM_EVENTS.subscriptions, (data: unknown) =>
			this.logger.debug(`[${this.environment}] subscriptions: ${preview(data)}`)
		);
		socket.on(STREAM_EVENTS.connection, (data: unknown) =>
			this.logger.warn(`[${this.environment}] connection event: ${preview(data)}`)
		);
		// Post-connect drops arrive as 'disconnect' (reconnection is disabled).
		socket.on('disconnect', (reason: string) => this.fail(`disconnect: ${reason}`));
	}

	private dispatch(raw: unknown): void {
		const typed = TypedMessageSchema.safeParse(raw);
		if (!typed.success) {
			this.logger.warn(`[${this.environment}] stream frame without a type — ignored`);
			return;
		}

		const parsed = StreamMessageSchema.safeParse(raw);
		if (!parsed.success) {
			this.logger.warn(
				`[${this.environment}] unparseable '${typed.data.type}' frame: ${parsed.error.message.slice(0, 200)}`
			);
			return;
		}

		const msg = parsed.data;
		switch (msg.type) {
			case 'AccountStatus':
				this.handlers.onAccountStatus(msg);
				break;
			case 'OpenOrder':
				this.handlers.onOpenOrder(msg);
				break;
			case 'Position':
				this.handlers.onPosition(msg);
				break;
			case 'ClosePosition':
				this.handlers.onClosePosition(msg);
				break;
			case 'Property':
				if (msg.name === 'SyncEnd') {
					this.handlers.onSyncEnd();
				} else {
					// 'Disconnected' — server is dropping us (session takeover / forced). Treat as lost;
					// the manager re-authenticates on the next reconnect, so it self-heals either way.
					this.fail('server Disconnected');
				}
				break;
		}
	}

	private emitWithAck(socket: Socket, payload: Record<string, unknown>): Promise<unknown> {
		return new Promise((resolve, reject) => {
			let settled = false;
			const timer = setTimeout(() => {
				if (settled) return;
				settled = true;
				reject(new Error(`SUBSCRIBE ack timed out after ${SUBSCRIBE_ACK_TIMEOUT_MS}ms`));
			}, SUBSCRIBE_ACK_TIMEOUT_MS);

			socket.emit(STREAM_EVENTS.subscriptions, payload, (ack: unknown) => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				resolve(ack);
			});
		});
	}

	private fail(reason: string): void {
		if (this.lost) {
			return;
		}
		this.lost = true;
		this.handlers.onLost(reason);
	}
}

function preview(value: unknown): string {
	try {
		return JSON.stringify(value).slice(0, 300);
	} catch {
		return String(value);
	}
}

function describe(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
