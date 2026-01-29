import path from 'node:path';
import crypto from 'node:crypto';
import fs from 'fs-extra';
import { Command } from 'commander';

/**
 * Markdown → chunks.
 *
 * Expected input: <bookDir>/metadata.json + <bookDir>/chapters/*.md produced by 01_extract_epub.ts
 * Default output: <bookDir>/chunks/chunks.jsonl (one JSON per chunk)
 *
 * Notes:
 * - We don't have a true tokenizer here, so we approximate tokens ~= words (good enough for sizing).
 * - Chunk boundaries are paragraph-based (split on blank lines), with a sliding-window overlap.
 */

type ExtractMetadata = {
  source: {
    epubPath: string;
    extractedAt: string;
  };
  book: {
    title?: string;
    author?: string;
    language?: string;
    publisher?: string;
    rights?: string;
    description?: string;
    isbn?: string;
    coverId?: string;
  };
  toc: Array<{
    id: string;
    href?: string;
    order: number;
    title?: string;
    level?: number;
  }>;
  chapters: Array<{
    order: number;
    id: string;
    title: string;
    file: string;
    href?: string;
    wordCount: number;
  }>;
};

type ChunkRecord = {
  chunkId: string;
  book: {
    slug: string;
    title?: string;
    author?: string;
    language?: string;
    publisher?: string;
    isbn?: string;
    sourceEpubPath: string;
    extractedAt: string;
  };
  chapter: {
    order: number;
    id: string;
    title: string;
    file: string;
    href?: string;
  };
  chunk: {
    index: number;
    strategy: 'paragraph_window_v1';
    approxTokens: number;
    maxTokens: number;
    overlapTokens: number;
    startParagraph: number;
    endParagraphExclusive: number;
    sha256: string;
  };
  text: string;
};

function approxTokenCount(text: string): number {
  // Simple approximation: 1 token ~= 0.75 words (varies by language), but
  // using words directly is stable and easy to reason about. We'll return words.
  const trimmed = text.trim();
  if (!trimmed) return 0;
  return trimmed.split(/\s+/).length;
}

function sha256Hex(text: string): string {
  return crypto.createHash('sha256').update(text).digest('hex');
}

function splitIntoParagraphs(md: string): string[] {
  // Normalize newlines and split on blank lines.
  const normalized = md.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const paras = normalized
    .split(/\n\s*\n+/)
    .map((p) => p.trim())
    .filter((p) => p.length > 0);
  return paras;
}

function buildOverlappedWindows(params: {
  paragraphs: string[];
  maxTokens: number;
  overlapTokens: number;
}): Array<{ start: number; endExclusive: number; text: string; approxTokens: number }> {
  const { paragraphs, maxTokens, overlapTokens } = params;

  if (maxTokens <= 0) throw new Error('--maxTokens must be > 0');
  if (overlapTokens < 0) throw new Error('--overlapTokens must be >= 0');
  if (overlapTokens >= maxTokens)
    throw new Error('--overlapTokens must be < --maxTokens (otherwise windows cannot advance)');

  const out: Array<{ start: number; endExclusive: number; text: string; approxTokens: number }> = [];

  let start = 0;
  while (start < paragraphs.length) {
    let endExclusive = start;
    let combined = '';

    while (endExclusive < paragraphs.length) {
      const next = combined ? `${combined}\n\n${paragraphs[endExclusive]}` : paragraphs[endExclusive];
      const t = approxTokenCount(next);

      // Always include at least one paragraph.
      if (t > maxTokens && endExclusive > start) break;

      combined = next;
      endExclusive += 1;

      if (t >= maxTokens) break;
    }

    const approxTokens = approxTokenCount(combined);
    out.push({ start, endExclusive, text: combined, approxTokens });

    if (endExclusive >= paragraphs.length) break;

    // Slide forward but keep an overlap measured by tokens.
    if (overlapTokens === 0) {
      start = endExclusive;
      continue;
    }

    // Walk backward from endExclusive until we have >= overlapTokens.
    let overlapStart = endExclusive;
    let overlapAccum = 0;
    while (overlapStart > start) {
      overlapStart -= 1;
      overlapAccum += approxTokenCount(paragraphs[overlapStart]);
      if (overlapAccum >= overlapTokens) break;
    }

    // Ensure progress.
    start = Math.max(overlapStart, start + 1);
  }

  return out;
}

async function main() {
  const program = new Command();
  program
    .name('02_chunk_markdown')
    .description('Chunk Markdown chapters into smaller passages (JSONL)')
    .option('-i, --in <dir>', 'Book directory (e.g., output/my-book)', '')
    .option('-o, --out <dir>', 'Output dir (defaults to <in>/chunks)', '')
    .option(
      '--maxTokens <n>',
      'Approx token budget per chunk (currently approximated as words)',
      (v) => parseInt(v, 10),
      450,
    )
    .option(
      '--overlapTokens <n>',
      'Approx tokens of overlap between adjacent chunks (currently approximated as words)',
      (v) => parseInt(v, 10),
      80,
    )
    .option(
      '--includeChapterHeader',
      'Include `# <chapter title>` as the first paragraph in chunk text',
      true,
    )
    .parse(process.argv);

  const opts = program.opts<{
    in: string;
    out: string;
    maxTokens: number;
    overlapTokens: number;
    includeChapterHeader: boolean;
  }>();

  if (!opts.in) {
    throw new Error('Missing --in. Example: --in output/my-book');
  }

  const bookDir = path.resolve(opts.in);
  const outDir = opts.out ? path.resolve(opts.out) : path.join(bookDir, 'chunks');
  await fs.ensureDir(outDir);

  const metadataPath = path.join(bookDir, 'metadata.json');
  if (!(await fs.pathExists(metadataPath))) {
    throw new Error(`Missing metadata.json at: ${metadataPath}`);
  }

  const metadata = (await fs.readJson(metadataPath)) as ExtractMetadata;
  const bookSlug = path.basename(bookDir);

  const chunksPath = path.join(outDir, 'chunks.jsonl');

  const lines: string[] = [];
  let chunkIndex = 0;

  const chaptersSorted = [...metadata.chapters].sort((a, b) => a.order - b.order);

  for (const ch of chaptersSorted) {
    const chapterPath = path.join(bookDir, ch.file);
    if (!(await fs.pathExists(chapterPath))) {
      // Skip missing chapters rather than failing the whole book.
      // (We still want the rest of the pipeline to run.)
      // eslint-disable-next-line no-console
      console.warn(`⚠️  missing chapter file; skipping: ${chapterPath}`);
      continue;
    }

    const md = await fs.readFile(chapterPath, 'utf8');
    const paragraphs = splitIntoParagraphs(md);

    // Optionally force the chapter header into the text for retrieval context.
    const chapterHeader = `# ${ch.title}`;
    const parasWithHeader = opts.includeChapterHeader
      ? [chapterHeader, ...paragraphs.filter((p) => p !== chapterHeader)]
      : paragraphs;

    const windows = buildOverlappedWindows({
      paragraphs: parasWithHeader,
      maxTokens: opts.maxTokens,
      overlapTokens: opts.overlapTokens,
    });

    for (const w of windows) {
      const text = w.text.trim();
      if (!text) continue;

      const chunkId = `chunk_${bookSlug}_${String(chunkIndex).padStart(6, '0')}`;

      const record: ChunkRecord = {
        chunkId,
        book: {
          slug: bookSlug,
          title: metadata.book.title,
          author: metadata.book.author,
          language: metadata.book.language,
          publisher: metadata.book.publisher,
          isbn: metadata.book.isbn,
          sourceEpubPath: metadata.source.epubPath,
          extractedAt: metadata.source.extractedAt,
        },
        chapter: {
          order: ch.order,
          id: ch.id,
          title: ch.title,
          file: ch.file,
          href: ch.href,
        },
        chunk: {
          index: chunkIndex,
          strategy: 'paragraph_window_v1',
          approxTokens: w.approxTokens,
          maxTokens: opts.maxTokens,
          overlapTokens: opts.overlapTokens,
          startParagraph: w.start,
          endParagraphExclusive: w.endExclusive,
          sha256: sha256Hex(text),
        },
        text,
      };

      lines.push(JSON.stringify(record));
      chunkIndex += 1;
    }
  }

  await fs.writeFile(chunksPath, lines.join('\n') + (lines.length ? '\n' : ''), 'utf8');

  // eslint-disable-next-line no-console
  console.log(`✅ Wrote ${lines.length} chunks: ${chunksPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
