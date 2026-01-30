import path from 'node:path';
import fs from 'fs-extra';
import { Command } from 'commander';
import dotenv from 'dotenv';
import pg from 'pg';

const { Pool } = pg;

type OpenAIBatchRequest = {
  custom_id: string;
  method: 'POST';
  url: '/v1/embeddings';
  body: {
    model: string;
    input: string;
  };
};

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

async function openaiUploadBatchFile(params: {
  apiKey: string;
  filePath: string;
}): Promise<string> {
  const { apiKey, filePath } = params;
  const buf = await fs.readFile(filePath);

  const form = new FormData();
  form.append('purpose', 'batch');
  form.append('file', new Blob([buf]), path.basename(filePath));

  const res = await fetch('https://api.openai.com/v1/files', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
    },
    body: form,
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`OpenAI file upload failed: ${res.status} ${res.statusText} ${text}`);
  }

  const json = (await res.json()) as any;
  const fileId = json?.id;
  if (!fileId) throw new Error('OpenAI file upload returned no id');
  return String(fileId);
}

async function openaiCreateBatch(params: {
  apiKey: string;
  inputFileId: string;
  endpoint: '/v1/embeddings';
  completionWindow: '24h';
}): Promise<string> {
  const { apiKey, inputFileId, endpoint, completionWindow } = params;

  const res = await fetch('https://api.openai.com/v1/batches', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      input_file_id: inputFileId,
      endpoint,
      completion_window: completionWindow,
    }),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`OpenAI batch create failed: ${res.status} ${res.statusText} ${text}`);
  }

  const json = (await res.json()) as any;
  const batchId = json?.id;
  if (!batchId) throw new Error('OpenAI batch create returned no id');
  return String(batchId);
}

async function main() {
  dotenv.config();

  const program = new Command();
  program
    .name('03_embed_chunks')
    .description('Create OpenAI embedding requests for chunks (and optionally submit as a Batch job).')
    .option('--model <name>', 'Embedding model', 'text-embedding-3-large')
    .option('--schema <name>', 'DB schema', 'rag')
    .option('--table <name>', 'Chunks table', 'chunks')
    .option('--where <sql>', 'Optional SQL WHERE clause (without the word WHERE)', '')
    .option('--limit <n>', 'Limit number of chunks', (v) => parseInt(v, 10), 0)
    .option(
      '--out <dir>',
      'Output directory for batch input + logs',
      path.resolve('output', 'embeddings'),
    )
    .option('--submit', 'Actually submit a Batch job to OpenAI (requires OPENAI_API_KEY)', false)
    .option('--dryRun', 'Generate files but do not submit batch (default)', true)
    .parse(process.argv);

  const opts = program.opts<{
    model: string;
    schema: string;
    table: string;
    where: string;
    limit: number;
    out: string;
    submit: boolean;
    dryRun: boolean;
  }>();

  const outDir = path.resolve(opts.out);
  await fs.ensureDir(outDir);

  // Default behavior: if submit is true, dryRun should be false.
  const doSubmit = Boolean(opts.submit);

  const conn = buildPgConnFromEnv();
  const pool = new Pool({ connectionString: conn });

  const runId = nowStamp();
  const inputJsonl = path.join(outDir, `openai-batch-input.${runId}.jsonl`);
  const runLog = path.join(outDir, `openai-batch-run.${runId}.json`);

  try {
    await pool.query('SELECT 1');

    const where = opts.where?.trim();
    const whereSql = where ? ` AND (${where})` : '';
    const limitSql = opts.limit && opts.limit > 0 ? ` LIMIT ${opts.limit}` : '';

    // Only embed rows missing an embedding.
    const sql = `
      SELECT chunk_id, text
      FROM ${opts.schema}.${opts.table}
      WHERE embedding IS NULL${whereSql}
      ORDER BY chunk_id
      ${limitSql}
    `;

    const result = await pool.query(sql);
    const rows = result.rows as Array<{ chunk_id: string; text: string }>;

    const reqs: OpenAIBatchRequest[] = rows.map((r) => ({
      custom_id: r.chunk_id,
      method: 'POST',
      url: '/v1/embeddings',
      body: {
        model: opts.model,
        input: r.text,
      },
    }));

    await fs.writeFile(inputJsonl, reqs.map((r) => JSON.stringify(r)).join('\n') + (reqs.length ? '\n' : ''), 'utf8');

    const log: any = {
      runId,
      model: opts.model,
      rowsSelected: rows.length,
      inputJsonl,
      submitted: false,
      batchId: null,
      fileId: null,
      createdAt: new Date().toISOString(),
    };

    if (doSubmit) {
      const apiKey = requireEnv('OPENAI_API_KEY');
      const fileId = await openaiUploadBatchFile({ apiKey, filePath: inputJsonl });
      const batchId = await openaiCreateBatch({
        apiKey,
        inputFileId: fileId,
        endpoint: '/v1/embeddings',
        completionWindow: '24h',
      });
      log.submitted = true;
      log.fileId = fileId;
      log.batchId = batchId;
    }

    await fs.writeJson(runLog, log, { spaces: 2 });

    // eslint-disable-next-line no-console
    console.log(`✅ Prepared ${rows.length} embedding requests: ${inputJsonl}`);
    // eslint-disable-next-line no-console
    console.log(`📝 Run log: ${runLog}`);
    if (doSubmit) {
      // eslint-disable-next-line no-console
      console.log(`🚀 Submitted Batch job: ${log.batchId}`);
    } else {
      // eslint-disable-next-line no-console
      console.log('ℹ️  Not submitted (dry run). Use --submit to create a Batch job.');
    }

    // Print one sample line for inspection.
    if (reqs.length > 0) {
      // eslint-disable-next-line no-console
      console.log('\nSample request JSONL line:');
      // eslint-disable-next-line no-console
      console.log(JSON.stringify(reqs[0]));
    }
  } finally {
    await pool.end().catch(() => undefined);
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
