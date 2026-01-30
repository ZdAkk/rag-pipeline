import path from 'node:path';
import fs from 'fs-extra';
import { Command } from 'commander';
import dotenv from 'dotenv';
import pg from 'pg';

const { Pool } = pg;

function nowStamp(): string {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function buildPgConnFromEnv(): string {
  const host = requireEnv('RAG_DB_HOST');
  const port = parseInt(process.env.RAG_DB_PORT ?? '5432', 10);
  const db = requireEnv('RAG_DB_NAME');
  const user = requireEnv('RAG_DB_USER');
  const password = requireEnv('RAG_DB_PASSWORD');
  const sslmode = process.env.RAG_DB_SSLMODE;

  const base = `postgresql://${encodeURIComponent(user)}:${encodeURIComponent(password)}@${host}:${port}/${encodeURIComponent(db)}`;
  return sslmode ? `${base}?sslmode=${encodeURIComponent(sslmode)}` : base;
}

function vectorLiteral(embedding: number[]): string {
  // pgvector text input format: '[1,2,3]'
  // We'll limit precision to reduce payload size.
  const body = embedding.map((x) => (Number.isFinite(x) ? x.toFixed(8) : '0')).join(',');
  return `[${body}]`;
}

async function openaiEmbed(params: { apiKey: string; model: string; input: string }): Promise<number[]> {
  const res = await fetch('https://api.openai.com/v1/embeddings', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${params.apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ model: params.model, input: params.input }),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`OpenAI embeddings failed: ${res.status} ${res.statusText} ${text}`);
  }

  const json = (await res.json()) as any;
  const emb = json?.data?.[0]?.embedding;
  if (!Array.isArray(emb)) throw new Error('OpenAI embeddings response missing data[0].embedding');
  return emb as number[];
}

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function main() {
  dotenv.config();

  const program = new Command();
  program
    .name('03_embed_chunks')
    .description('Embed chunks using the OpenAI embeddings endpoint and write vectors back to Postgres.')
    .option('--model <name>', 'Embedding model', 'text-embedding-3-large')
    .option('--schema <name>', 'DB schema', 'rag')
    .option('--table <name>', 'Chunks table', 'chunks')
    .option('--where <sql>', 'Optional SQL WHERE clause (without the word WHERE)', '')
    .option('--limit <n>', 'Limit number of chunks', (v) => parseInt(v, 10), 0)
    .option('--batchSize <n>', 'Rows fetched per loop iteration', (v) => parseInt(v, 10), 50)
    .option('--sleepMs <n>', 'Sleep between API calls (ms)', (v) => parseInt(v, 10), 0)
    .option('--dryRun', 'Do not call OpenAI or update DB; just show what would run', false)
    .option(
      '--runLog <path>',
      'Write a JSON run log (default: output/embeddings_runs/<timestamp>.json)',
      '',
    )
    .parse(process.argv);

  const opts = program.opts<{
    model: string;
    schema: string;
    table: string;
    where: string;
    limit: number;
    batchSize: number;
    sleepMs: number;
    dryRun: boolean;
    runLog: string;
  }>();

  const conn = buildPgConnFromEnv();
  const pool = new Pool({ connectionString: conn });

  const runId = nowStamp();
  const logPath =
    opts.runLog.trim() ||
    `${process.cwd()}/output/embeddings_runs/${runId}.json`;
  await fs.ensureDir(path.dirname(logPath));

  const log: any = {
    runId,
    model: opts.model,
    startedAt: new Date().toISOString(),
    dryRun: opts.dryRun,
    where: opts.where || null,
    limit: opts.limit || null,
    batchSize: opts.batchSize,
    sleepMs: opts.sleepMs,
    embedded: 0,
    failed: 0,
    sample: null,
  };

  try {
    await pool.query('SELECT 1');

    const apiKey = opts.dryRun ? null : requireEnv('OPENAI_API_KEY');

    let remaining = opts.limit && opts.limit > 0 ? opts.limit : Number.POSITIVE_INFINITY;

    while (remaining > 0) {
      const fetchN = Math.min(opts.batchSize, remaining);
      const where = opts.where?.trim();
      const whereSql = where ? ` AND (${where})` : '';

      const sql = `
        SELECT chunk_id, text
        FROM ${opts.schema}.${opts.table}
        WHERE embedding IS NULL${whereSql}
        ORDER BY chunk_id
        LIMIT ${fetchN}
      `;

      const result = await pool.query(sql);
      const rows = result.rows as Array<{ chunk_id: string; text: string }>;
      if (rows.length === 0) break;

      for (const row of rows) {
        if (remaining <= 0) break;

        if (opts.dryRun) {
          log.embedded += 1;
          if (!log.sample) {
            log.sample = {
              chunk_id: row.chunk_id,
              text_preview: row.text.slice(0, 200),
              request: { model: opts.model, input_preview: row.text.slice(0, 80) },
            };
          }
          remaining -= 1;
          continue;
        }

        try {
          const emb = await openaiEmbed({ apiKey: apiKey!, model: opts.model, input: row.text });
          const vec = vectorLiteral(emb);

          await pool.query(
            `
              UPDATE ${opts.schema}.${opts.table}
              SET embedding = $1::vector,
                  embedding_model = $2,
                  embedding_created_at = NOW()
              WHERE chunk_id = $3
            `,
            [vec, opts.model, row.chunk_id],
          );

          log.embedded += 1;
          if (!log.sample) {
            log.sample = {
              chunk_id: row.chunk_id,
              embedding_dims: emb.length,
              embedding_preview: emb.slice(0, 5),
            };
          }
        } catch (err) {
          log.failed += 1;
          // eslint-disable-next-line no-console
          console.error(`❌ Failed embedding ${row.chunk_id}: ${String(err)}`);
        }

        remaining -= 1;
        if (opts.sleepMs > 0) await sleep(opts.sleepMs);
      }
    }

    log.finishedAt = new Date().toISOString();
    await fs.writeJson(logPath, log, { spaces: 2 });

    // eslint-disable-next-line no-console
    console.log(`✅ Embed run complete. embedded=${log.embedded} failed=${log.failed}`);
    // eslint-disable-next-line no-console
    console.log(`📝 Run log: ${logPath}`);
    if (log.sample) {
      // eslint-disable-next-line no-console
      console.log('Sample:', JSON.stringify(log.sample, null, 2));
    }
  } finally {
    await pool.end().catch(() => undefined);
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
