import path from 'node:path';
import fs from 'fs-extra';
import { Command } from 'commander';

/**
 * Placeholder: chunks → embeddings.
 *
 * Expected input: output/<bookSlug>/chunks/chunks.jsonl produced by 02_chunk_markdown.ts
 * Expected output: output/<bookSlug>/embeddings/embeddings.jsonl or a vector DB.
 */

async function main() {
  const program = new Command();
  program
    .name('03_embed_chunks')
    .description('Embed chunks into vectors (placeholder).')
    .option('-i, --in <file>', 'Input chunks.jsonl', '')
    .option('-o, --out <dir>', 'Output directory', '')
    .option('--provider <name>', 'Embedding provider (placeholder)', 'openai')
    .option('--model <name>', 'Embedding model (placeholder)', 'text-embedding-3-large')
    .parse(process.argv);

  const opts = program.opts<{ in: string; out: string; provider: string; model: string }>();
  if (!opts.in) {
    throw new Error('Missing --in. Example: --in output/my-book/chunks/chunks.jsonl');
  }

  const inPath = path.resolve(opts.in);
  const outDir = opts.out
    ? path.resolve(opts.out)
    : path.join(path.dirname(inPath), '..', 'embeddings');
  await fs.ensureDir(outDir);

  const outPath = path.join(outDir, 'embeddings.jsonl');

  // TODO: implement real embedding.
  // - Read JSONL chunks
  // - Call embedding provider
  // - Store vectors + metadata
  await fs.writeFile(
    outPath,
    [
      JSON.stringify({
        placeholder: true,
        message:
          'Embedding not implemented yet. See scripts/03_embed_chunks.ts for TODOs.',
        provider: opts.provider,
        model: opts.model,
      }),
      '',
    ].join('\n'),
    'utf8',
  );

  console.log(`⚠️  Wrote placeholder embeddings file: ${outPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
