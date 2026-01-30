import path from 'node:path';
import fs from 'fs-extra';
import { Command } from 'commander';
import dotenv from 'dotenv';
import pg from 'pg';

const { Pool } = pg;

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
    strategy: string;
    approxTokens: number;
    maxTokens: number;
    overlapTokens: number;
    startParagraph: number;
    endParagraphExclusive: number;
    sha256: string;
  };
  text: string;
};

type IngestStats = {
  startedAt: string;
  finishedAt?: string;
  sourceRoot: string;
  filesScanned: number;
  booksUpserted: number;
  chunksUpserted: number;
  chunksFailed: number;
  notes: string[];
};

function nowIso() {
  return new Date().toISOString();
}

async function findChunkFiles(root: string): Promise<string[]> {
  const out: string[] = [];

  async function walk(dir: string) {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    for (const e of entries) {
      const p = path.join(dir, e.name);
      if (e.isDirectory()) {
        await walk(p);
      } else if (e.isFile() && e.name.toLowerCase().endsWith('.jsonl')) {
        // We only want chunk JSONL files. Heuristic: must contain at least one line with "chunkId" and "text".
        out.push(p);
      }
    }
  }

  await walk(root);

  // Prefer files named chunks.jsonl first (stable convention).
  out.sort((a, b) => {
    const aScore = path.basename(a) === 'chunks.jsonl' ? 0 : 1;
    const bScore = path.basename(b) === 'chunks.jsonl' ? 0 : 1;
    if (aScore !== bScore) return aScore - bScore;
    return a.localeCompare(b);
  });

  return out;
}

function buildConnStringFromEnv(params: {
  host: string;
  port: number;
  db: string;
  user: string;
  password: string;
  sslmode?: string;
}): string {
  const { host, port, db, user, password, sslmode } = params;
  // pg uses ssl: boolean/object. We'll map sslmode=require to ssl=true.
  const base = `postgresql://${encodeURIComponent(user)}:${encodeURIComponent(password)}@${host}:${port}/${encodeURIComponent(db)}`;
  if (!sslmode) return base;
  return `${base}?sslmode=${encodeURIComponent(sslmode)}`;
}

async function upsertBook(pool: pg.Pool, book: ChunkRecord['book']) {
  const q = {
    text: `
      INSERT INTO rag.books (
        book_slug, title, author, language, publisher, isbn, source_epub_path, extracted_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      ON CONFLICT (book_slug) DO UPDATE SET
        title = EXCLUDED.title,
        author = EXCLUDED.author,
        language = EXCLUDED.language,
        publisher = EXCLUDED.publisher,
        isbn = EXCLUDED.isbn,
        source_epub_path = EXCLUDED.source_epub_path,
        extracted_at = EXCLUDED.extracted_at
    `,
    values: [
      book.slug,
      book.title ?? null,
      book.author ?? null,
      book.language ?? null,
      book.publisher ?? null,
      book.isbn ?? null,
      book.sourceEpubPath ?? null,
      book.extractedAt ? new Date(book.extractedAt).toISOString() : null,
    ],
  };
  await pool.query(q);
}

async function upsertChunkBatch(pool: pg.Pool, rows: ChunkRecord[]) {
  if (rows.length === 0) return;

  // Build a multi-values insert.
  // 16 columns (excluding embedding cols).
  const cols = [
    'chunk_id',
    'book_slug',
    'chapter_order',
    'chapter_id',
    'chapter_title',
    'chapter_file',
    'chapter_href',
    'chunk_index',
    'chunk_strategy',
    'approx_tokens',
    'max_tokens',
    'overlap_tokens',
    'start_paragraph',
    'end_paragraph_exclusive',
    'text_sha256',
    'text',
  ];

  const values: unknown[] = [];
  const tuples: string[] = [];

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const baseIdx = i * cols.length;
    const placeholders = cols.map((_, j) => `$${baseIdx + j + 1}`);
    tuples.push(`(${placeholders.join(',')})`);
    values.push(
      r.chunkId,
      r.book.slug,
      r.chapter.order,
      r.chapter.id,
      r.chapter.title,
      r.chapter.file,
      r.chapter.href ?? null,
      r.chunk.index,
      r.chunk.strategy,
      r.chunk.approxTokens,
      r.chunk.maxTokens,
      r.chunk.overlapTokens,
      r.chunk.startParagraph,
      r.chunk.endParagraphExclusive,
      r.chunk.sha256,
      r.text,
    );
  }

  const sql = `
    INSERT INTO rag.chunks (${cols.join(', ')})
    VALUES ${tuples.join(',\n')}
    ON CONFLICT (chunk_id) DO UPDATE SET
      book_slug = EXCLUDED.book_slug,
      chapter_order = EXCLUDED.chapter_order,
      chapter_id = EXCLUDED.chapter_id,
      chapter_title = EXCLUDED.chapter_title,
      chapter_file = EXCLUDED.chapter_file,
      chapter_href = EXCLUDED.chapter_href,
      chunk_index = EXCLUDED.chunk_index,
      chunk_strategy = EXCLUDED.chunk_strategy,
      approx_tokens = EXCLUDED.approx_tokens,
      max_tokens = EXCLUDED.max_tokens,
      overlap_tokens = EXCLUDED.overlap_tokens,
      start_paragraph = EXCLUDED.start_paragraph,
      end_paragraph_exclusive = EXCLUDED.end_paragraph_exclusive,
      text_sha256 = EXCLUDED.text_sha256,
      text = EXCLUDED.text
  `;

  await pool.query({ text: sql, values });
}

async function main() {
  dotenv.config();

  const program = new Command();
  program
    .name('04_ingest_pg')
    .description('Ingest chunk JSONL files into Postgres (rag schema: books + chunks)')
    .option(
      '--chunksRoot <dir>',
      'Root directory to scan for chunk JSONL files (default: /home/pi/rag-library/chunks_w450_o80)',
      '/home/pi/rag-library/chunks_w450_o80',
    )
    .option('--batchSize <n>', 'Chunk rows per INSERT batch', (v) => parseInt(v, 10), 200)
    .option(
      '--runLog <path>',
      'Write a JSON run log to this path (default: output/ingest_runs/<timestamp>.json)',
      '',
    )
    .parse(process.argv);

  const opts = program.opts<{ chunksRoot: string; batchSize: number; runLog: string }>();

  const host = process.env.RAG_DB_HOST;
  const port = parseInt(process.env.RAG_DB_PORT ?? '5432', 10);
  const db = process.env.RAG_DB_NAME;
  const user = process.env.RAG_DB_USER;
  const password = process.env.RAG_DB_PASSWORD;
  const sslmode = process.env.RAG_DB_SSLMODE;

  if (!host || !db || !user || !password) {
    throw new Error(
      'Missing DB env vars. Ensure .env contains RAG_DB_HOST, RAG_DB_NAME, RAG_DB_USER, RAG_DB_PASSWORD (and optional port/sslmode).',
    );
  }

  const conn = buildConnStringFromEnv({ host, port, db, user, password, sslmode });

  const stats: IngestStats = {
    startedAt: nowIso(),
    sourceRoot: path.resolve(opts.chunksRoot),
    filesScanned: 0,
    booksUpserted: 0,
    chunksUpserted: 0,
    chunksFailed: 0,
    notes: [],
  };

  const logPath =
    opts.runLog.trim() ||
    path.resolve('output', 'ingest_runs', `${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
  await fs.ensureDir(path.dirname(logPath));

  const pool = new Pool({ connectionString: conn });

  try {
    // Basic connectivity.
    await pool.query('SELECT 1');

    const root = path.resolve(opts.chunksRoot);
    if (!(await fs.pathExists(root))) {
      throw new Error(`chunksRoot not found: ${root}`);
    }

    const files = await findChunkFiles(root);
    stats.filesScanned = files.length;

    const seenBooks = new Set<string>();

    for (const filePath of files) {
      const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
      let buffer = '';
      const batch: ChunkRecord[] = [];

      const flush = async () => {
        if (batch.length === 0) return;
        await upsertChunkBatch(pool, batch);
        stats.chunksUpserted += batch.length;
        batch.length = 0;
      };

      for await (const chunk of stream as any as AsyncIterable<string>) {
        buffer += chunk;
        while (true) {
          const idx = buffer.indexOf('\n');
          if (idx === -1) break;
          const line = buffer.slice(0, idx).trim();
          buffer = buffer.slice(idx + 1);
          if (!line) continue;

          let rec: ChunkRecord;
          try {
            rec = JSON.parse(line) as ChunkRecord;
          } catch {
            continue;
          }

          if (!rec.chunkId || !rec.book?.slug || !rec.text) {
            continue;
          }

          // Upsert book once per slug.
          if (!seenBooks.has(rec.book.slug)) {
            await upsertBook(pool, rec.book);
            seenBooks.add(rec.book.slug);
            stats.booksUpserted += 1;
          }

          batch.push(rec);
          if (batch.length >= opts.batchSize) {
            try {
              await flush();
            } catch (err) {
              stats.chunksFailed += batch.length;
              stats.notes.push(`Failed batch insert for ${filePath}: ${String(err)}`);
              batch.length = 0;
            }
          }
        }
      }

      // Last buffered line.
      const last = buffer.trim();
      if (last) {
        try {
          const rec = JSON.parse(last) as ChunkRecord;
          if (rec.chunkId && rec.book?.slug && rec.text) {
            if (!seenBooks.has(rec.book.slug)) {
              await upsertBook(pool, rec.book);
              seenBooks.add(rec.book.slug);
              stats.booksUpserted += 1;
            }
            batch.push(rec);
          }
        } catch {
          // ignore
        }
      }

      try {
        await flush();
      } catch (err) {
        stats.chunksFailed += batch.length;
        stats.notes.push(`Failed final batch insert for ${filePath}: ${String(err)}`);
      }
    }

    stats.finishedAt = nowIso();
    await fs.writeJson(logPath, stats, { spaces: 2 });

    // eslint-disable-next-line no-console
    console.log(`✅ Ingest complete. booksUpserted=${stats.booksUpserted} chunksUpserted=${stats.chunksUpserted} failed=${stats.chunksFailed}`);
    // eslint-disable-next-line no-console
    console.log(`📝 Run log: ${logPath}`);
  } finally {
    await pool.end().catch(() => undefined);
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
