/**
 * Minimal typing for the pure-JS LZMA decoder dukascopy-node itself bundles for .bi5 files
 * (synchronous, binary-safe — unlike the popular `lzma` package, which guesses UTF-8 and can
 * silently return a string). We only use decompressFile.
 */
declare module 'lzma-purejs-requirejs' {
	export function decompressFile(input: Buffer | Uint8Array): Buffer;
}
