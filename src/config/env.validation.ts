import { z } from 'zod';

/**
 * Runtime environment contract. Validated once at boot by ConfigModule, so a missing or
 * malformed value fails fast with a readable message instead of surfacing as an obscure
 * error deep inside a request. Later phases extend this (ENCRYPTION_KEY, CTRADER_*, ...).
 */
const envSchema = z.object({
	NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
	PORT: z.coerce.number().int().positive().default(3001),
	DATABASE_URL: z.string().min(1, 'DATABASE_URL is required'),
});

export type Env = z.infer<typeof envSchema>;

export function validateEnv(config: Record<string, unknown>): Env {
	const parsed = envSchema.safeParse(config);

	if (!parsed.success) {
		const issues = parsed.error.issues
			.map((issue) => `  - ${issue.path.join('.') || '(root)'}: ${issue.message}`)
			.join('\n');
		throw new Error(`Invalid environment variables:\n${issues}`);
	}

	return parsed.data;
}
