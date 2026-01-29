import path from 'node:path';
import fs from 'fs-extra';
import { Command } from 'commander';

/**
 * Placeholder: Markdown → chunks.
 *
 * Expected input: output/<bookSlug>/chapters/*.md produced by 01_extract_epub.ts
 * Expected output: output/<bookSlug>/chunks/chunks.jsonl (one JSON per chunk)
 */

async function main() {
  const program = new Command();
  program
    .name('02_chunk_markdown')
    .description('Chunk Markdown chapters into smaller passages (placeholder).')
    .option('-i, --in <dir>', 'Book directory (e.g., output/my-book)', '')
    .option('-o, --out <dir>', 'Output dir (defaults to <in>/chunks)', '')
    .option('--maxTokens <n>', 'Target max tokens per chunk (placeholder)', (v) => parseInt(v, 10), 800)
    .parse(process.argv);

  const opts = program.opts<{ in: string; out: string; maxTokens: number }>();
  if (!opts.in) {
    throw new Error('Missing --in. Example: --in output/my-book');
  }

  const bookDir = path.resolve(opts.in);
  const outDir = opts.out ? path.resolve(opts.out) : path.join(bookDir, 'chunks');
  await fs.ensureDir(outDir);

  const chunksPath = path.join(outDir, 'chunks.jsonl');

  // TODO: implement real chunking.
  // - Read chapters in order
  // - Split into overlapping windows (by tokens or characters)
  // - Emit JSON lines: { chunkId, chapter, start, end, text, metadata }
  await fs.writeFile(
    chunksPath,
    [
      JSON.stringify({
        placeholder: true,
        message:
          'Chunking not implemented yet. See scripts/02_chunk_markdown.ts for TODOs.',
        maxTokens: opts.maxTokens,
      }),
      '',
    ].join('\n'),
    'utf8',
  );

  console.log(`⚠️  Wrote placeholder chunks file: ${chunksPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
