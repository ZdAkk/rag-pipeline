import path from 'node:path';
import os from 'node:os';
import fs from 'fs-extra';
import { Command } from 'commander';
import { spawn } from 'node:child_process';

function nowStamp(): string {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

function run(cmd: string, args: string[], opts?: { cwd?: string; env?: NodeJS.ProcessEnv }) {
  return new Promise<void>((resolve, reject) => {
    const child = spawn(cmd, args, {
      stdio: 'inherit',
      cwd: opts?.cwd,
      env: { ...process.env, ...opts?.env },
    });
    child.on('error', reject);
    child.on('exit', (code) => {
      if (code === 0) return resolve();
      reject(new Error(`Command failed (${code}): ${cmd} ${args.join(' ')}`));
    });
  });
}

async function listEpubs(inputPath: string): Promise<string[]> {
  const p = path.resolve(inputPath);
  const stat = await fs.stat(p);
  if (stat.isFile()) return [p];
  const entries = await fs.readdir(p);
  return entries
    .filter((x) => x.toLowerCase().endsWith('.epub'))
    .map((x) => path.join(p, x))
    .sort();
}

async function findSingleSubdir(root: string): Promise<string> {
  const entries = await fs.readdir(root, { withFileTypes: true });
  const dirs = entries.filter((e) => e.isDirectory()).map((e) => path.join(root, e.name));
  if (dirs.length !== 1) {
    throw new Error(`Expected exactly 1 output book directory in ${root}, found ${dirs.length}`);
  }
  return dirs[0];
}

async function pipelineOne(params: {
  epubPath: string;
  repoRoot: string;
  workDir: string;
  maxTokens: number;
  overlapTokens: number;
  batchSize: number;
}) {
  const { epubPath, repoRoot, workDir, maxTokens, overlapTokens, batchSize } = params;

  const extractRoot = path.join(workDir, 'extract');
  const chunkRoot = path.join(workDir, 'chunks');
  await fs.ensureDir(extractRoot);
  await fs.ensureDir(chunkRoot);

  // 1) EPUB -> Markdown
  await run('npm', ['run', '-s', 'extract:epub', '--', epubPath, '--out', extractRoot], {
    cwd: repoRoot,
  });

  const bookDir = await findSingleSubdir(extractRoot);

  // 2) Markdown -> Chunks
  // Write chunks into workdir (not inside bookDir) so cleanup is simple.
  const slug = path.basename(bookDir);
  const outChunks = path.join(chunkRoot, slug);
  await fs.ensureDir(outChunks);

  await run(
    'npm',
    [
      'run',
      '-s',
      'chunk',
      '--',
      '--in',
      bookDir,
      '--out',
      outChunks,
      '--maxTokens',
      String(maxTokens),
      '--overlapTokens',
      String(overlapTokens),
    ],
    { cwd: repoRoot },
  );

  // 3) Ingest chunks into Postgres
  // Point ingest script at chunkRoot (it scans recursively for chunks.jsonl).
  await run(
    'npm',
    ['run', '-s', 'ingest:pg', '--', '--chunksRoot', chunkRoot, '--batchSize', String(batchSize)],
    {
      cwd: repoRoot,
      // Reduce dotenv noise if dotenv prints.
      env: { DOTENV_CONFIG_QUIET: 'true' },
    },
  );

  // Note: embeddings step will be added later.
}

async function main() {
  const program = new Command();
  program
    .name('00_pipeline')
    .description('One-command RAG pipeline: EPUB -> Markdown -> chunks -> Postgres ingest (embeddings later)')
    .argument('<input>', 'Path to an .epub file OR a directory containing .epub files')
    .option('--maxTokens <n>', 'Chunk size (approx words)', (v) => parseInt(v, 10), 450)
    .option('--overlapTokens <n>', 'Chunk overlap (approx words)', (v) => parseInt(v, 10), 80)
    .option('--batchSize <n>', 'DB ingest batch size', (v) => parseInt(v, 10), 200)
    .option('--keepWorkdir', 'Keep temporary workdir (for debugging)', false)
    .parse(process.argv);

  const opts = program.opts<{ maxTokens: number; overlapTokens: number; batchSize: number; keepWorkdir: boolean }>();
  const inputPath = program.args[0];

  const repoRoot = path.resolve(process.cwd());

  const epubs = await listEpubs(inputPath);
  if (epubs.length === 0) {
    throw new Error(`No .epub files found at: ${path.resolve(inputPath)}`);
  }

  const workDir = path.join(os.tmpdir(), `rag-pipeline-${nowStamp()}`);
  await fs.ensureDir(workDir);

  try {
    for (const epubPath of epubs) {
      // Make a per-epub work subdir so partial failures don't poison the rest.
      const name = path.parse(epubPath).name.replace(/[^a-zA-Z0-9._-]+/g, '_').slice(0, 80);
      const oneWork = path.join(workDir, name);
      await fs.ensureDir(oneWork);

      console.log(`\n=== Processing EPUB: ${epubPath} ===`);
      await pipelineOne({
        epubPath,
        repoRoot,
        workDir: oneWork,
        maxTokens: opts.maxTokens,
        overlapTokens: opts.overlapTokens,
        batchSize: opts.batchSize,
      });
    }
  } finally {
    if (!opts.keepWorkdir) {
      await fs.remove(workDir);
    } else {
      console.log(`⚠️  Kept workdir: ${workDir}`);
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
