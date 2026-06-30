/**
 * Minimal Telegram sender — mirrors the main app's telegram.service so the worker can post to the
 * SAME bot/chat (set TELEGRAM_BOT_TOKEN + TELEGRAM_FEEDBACK_CHAT_ID as Fly secrets, same values as the
 * main app). Best-effort: never throws, returns false on any failure so a notify can't break a flow.
 */
export async function sendTelegramMessage(params: {
	botToken: string;
	chatId: string;
	text: string;
}): Promise<boolean> {
	try {
		const res = await fetch(`https://api.telegram.org/bot${params.botToken}/sendMessage`, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ chat_id: params.chatId, text: params.text, parse_mode: 'HTML' }),
			signal: AbortSignal.timeout(10_000),
		});
		return res.ok;
	} catch {
		return false;
	}
}
