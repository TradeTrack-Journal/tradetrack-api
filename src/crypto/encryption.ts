import crypto from 'crypto';

// Vendored from the main app (src/server/utils/encryption.ts). The algorithm, salt and key
// derivation MUST stay byte-for-byte identical to the main app, otherwise tokens written there
// cannot be decrypted here. Format: `${ivHex}:${authTagHex}:${cipherHex}`, AES-256-GCM,
// key = scrypt(ENCRYPTION_KEY, 'salt', 32).
const ALGORITHM = 'aes-256-gcm';

function getKey(): Buffer {
	const key = process.env.ENCRYPTION_KEY;
	if (!key) {
		throw new Error('ENCRYPTION_KEY is not set — cannot decrypt/encrypt cTrader tokens');
	}
	return crypto.scryptSync(key, 'salt', 32);
}

export function encrypt(text: string): string {
	const key = getKey();
	const iv = crypto.randomBytes(16);
	const cipher = crypto.createCipheriv(ALGORITHM, key, iv);

	let encrypted = cipher.update(text, 'utf8', 'hex');
	encrypted += cipher.final('hex');

	const authTag = cipher.getAuthTag();

	return `${iv.toString('hex')}:${authTag.toString('hex')}:${encrypted}`;
}

export function decrypt(encryptedText: string): string {
	try {
		const [ivHex, authTagHex, encrypted] = encryptedText.split(':');

		if (!ivHex || !authTagHex || !encrypted) {
			throw new Error('Invalid encrypted data format');
		}

		const key = getKey();
		const iv = Buffer.from(ivHex, 'hex');
		const authTag = Buffer.from(authTagHex, 'hex');

		const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
		decipher.setAuthTag(authTag);

		let decrypted = decipher.update(encrypted, 'hex', 'utf8');
		decrypted += decipher.final('utf8');

		return decrypted;
	} catch (error) {
		throw new Error(
			`Decryption failed: ${error instanceof Error ? error.message : 'Unknown error'}. This usually means ENCRYPTION_KEY differs from the main app or the data was encrypted with a different key.`
		);
	}
}
