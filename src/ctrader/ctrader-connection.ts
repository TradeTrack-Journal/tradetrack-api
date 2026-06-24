import { Logger } from '@nestjs/common';
import { CTraderConnection } from '@reiryoku/ctrader-layer';

import {
	COMMAND_TIMEOUT_MS,
	CTRADER_HOSTS,
	CTRADER_PORT,
	HEARTBEAT_INTERVAL_MS,
	LIVENESS_PROBE_INTERVAL_MS,
} from './ctrader.constants';
import type { CtraderEnvironment, ExecutionEventPayload } from './types';

export interface CtraderConnectionHandlers {
	onExecution: (ctidTraderAccountId: number, payload: ExecutionEventPayload) => void;
	onTokenInvalidated: (ctidTraderAccountIds: number[], reason: string) => void;
	/** Fatal: server disconnect or a failed liveness probe. Fires at most once. */
	onLost: (reason: string) => void;
}

/**
 * One TLS socket to cTrader for a single environment, multiplexing many accounts. Wraps the
 * @reiryoku/ctrader-layer connection with the safety the library lacks: every command has a
 * timeout (the library never rejects a pending command when the socket drops), a heartbeat keeps
 * the link open, and a periodic liveness probe plus the server's ProtoOAClientDisconnectEvent
 * surface a dead connection as a single onLost(). Reconnect policy lives in the owner (manager).
 */
export class CtraderConnection {
	private readonly connection: CTraderConnection;
	private heartbeat?: NodeJS.Timeout;
	private probe?: NodeJS.Timeout;
	private lost = false;

	constructor(
		private readonly environment: CtraderEnvironment,
		private readonly logger: Logger,
		private readonly handlers: CtraderConnectionHandlers
	) {
		this.connection = new CTraderConnection({
			host: CTRADER_HOSTS[environment],
			port: CTRADER_PORT,
		});
	}

	/** Open the socket, subscribe to push events, and start heartbeat + liveness timers. */
	async open(): Promise<void> {
		await this.command(() => this.connection.open());
		this.subscribe();
		this.startHeartbeat();
		this.startLivenessProbe();
	}

	async applicationAuth(clientId: string, clientSecret: string): Promise<void> {
		await this.command(() =>
			this.connection.sendCommand('ProtoOAApplicationAuthReq', { clientId, clientSecret })
		);
	}

	async accountAuth(accessToken: string, ctidTraderAccountId: number): Promise<void> {
		await this.command(() =>
			this.connection.sendCommand('ProtoOAAccountAuthReq', { accessToken, ctidTraderAccountId })
		);
	}

	/** Run an arbitrary request/response command (e.g. ProtoOASymbolsListReq) with a timeout. */
	async request(payloadName: string, data?: Record<string, unknown>): Promise<unknown> {
		return this.command(() => this.connection.sendCommand(payloadName, data));
	}

	/** Idempotent teardown — clears timers and destroys the socket. */
	close(): void {
		this.lost = true;
		this.clearTimers();
		try {
			this.connection.close();
		} catch {
			// ignore close errors
		}
	}

	private subscribe(): void {
		this.connection.on('ProtoOAExecutionEvent', (event: unknown) => {
			const payload = this.descriptor<ExecutionEventPayload>(event);
			this.handlers.onExecution(Number(payload.ctidTraderAccountId ?? 0), payload);
		});

		this.connection.on('ProtoOAClientDisconnectEvent', (event: unknown) => {
			const payload = this.descriptor<{ reason?: string }>(event);
			this.fail(`server disconnect: ${payload.reason ?? 'unknown'}`);
		});

		this.connection.on('ProtoOAAccountsTokenInvalidatedEvent', (event: unknown) => {
			const payload = this.descriptor<{
				ctidTraderAccountIds?: Array<string | number>;
				reason?: string;
			}>(event);
			const ids = (payload.ctidTraderAccountIds ?? []).map(Number).filter((id) => Number.isFinite(id));
			this.handlers.onTokenInvalidated(ids, payload.reason ?? 'unknown');
		});
	}

	private startHeartbeat(): void {
		this.heartbeat = setInterval(() => {
			try {
				this.connection.sendHeartbeat();
			} catch (error) {
				this.logger.warn(`[${this.environment}] heartbeat failed: ${describe(error)}`);
			}
		}, HEARTBEAT_INTERVAL_MS);
	}

	private startLivenessProbe(): void {
		this.probe = setInterval(() => {
			void this.command(() => this.connection.sendCommand('ProtoOAVersionReq', {})).catch((error) =>
				this.fail(`liveness probe failed: ${describe(error)}`)
			);
		}, LIVENESS_PROBE_INTERVAL_MS);
	}

	private fail(reason: string): void {
		if (this.lost) {
			return;
		}
		this.lost = true;
		this.clearTimers();
		this.handlers.onLost(reason);
	}

	private clearTimers(): void {
		if (this.heartbeat) {
			clearInterval(this.heartbeat);
			this.heartbeat = undefined;
		}
		if (this.probe) {
			clearInterval(this.probe);
			this.probe = undefined;
		}
	}

	/** Race a library call against a timeout — the library never rejects on a dropped socket. */
	private async command<T>(run: () => Promise<T>, timeoutMs = COMMAND_TIMEOUT_MS): Promise<T> {
		let timer: NodeJS.Timeout | undefined;
		const timeout = new Promise<never>((_, reject) => {
			timer = setTimeout(
				() => reject(new Error(`cTrader command timed out after ${timeoutMs}ms`)),
				timeoutMs
			);
		});

		try {
			return await Promise.race([run(), timeout]);
		} finally {
			if (timer) {
				clearTimeout(timer);
			}
		}
	}

	/** Push listeners receive a CTraderLayerEvent wrapper; the real payload is on .descriptor. */
	private descriptor<T>(event: unknown): T {
		const wrapper = event as { descriptor?: unknown };
		return (wrapper?.descriptor ?? event ?? {}) as T;
	}
}

function describe(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
