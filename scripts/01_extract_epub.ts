import path from 'node:path';
import fs from 'fs-extra';
import { Command } from 'commander';
import EPub from 'epub2';
import slugifyImport from 'slugify';
import TurndownService from 'turndown';

const slugify = slugifyImport as unknown as (input: string, opts: any) => string;

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

function toSafeSlug(input: string): string {
  const s = slugify(input, { lower: true, strict: true, trim: true });
  return s.length ? s : 'book';
}

function stripMarkdownFrontMatter(md: string): string {
  if (!md.startsWith('---')) return md;
  const end = md.indexOf('\n---', 3);
  if (end === -1) return md;
  return md.slice(end + '\n---'.length).trimStart();
}

function estimateWordCount(text: string): number {
  const cleaned = text
    .replace(/`[^`]*`/g, ' ')
    .replace(/\[[^\]]*\]\([^)]*\)/g, ' ')
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .trim();
  if (!cleaned) return 0;
  return cleaned.split(/\s+/).length;
}

async function epubParse(epub: any): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    epub.on('end', () => resolve());
    epub.on('error', (err: any) => reject(err));
    epub.parse();
  });
}

async function getChapterHtml(epub: any, id: string): Promise<string> {
  return await new Promise<string>((resolve, reject) => {
    epub.getChapter(id, (err: any, text?: string) => {
      if (err) return reject(err);
      resolve(text ?? '');
    });
  });
}

function buildTurndown(): TurndownService {
  const td = new TurndownService({
    headingStyle: 'atx',
    codeBlockStyle: 'fenced',
    emDelimiter: '_',
    bulletListMarker: '-',
  });

  // Drop scripts/styles and common nav/footnote clutter by stripping their contents first.
  td.remove(['script', 'style']);

  // Prefer turning <br> into newlines.
  td.addRule('lineBreaks', {
    filter: ['br'],
    replacement: () => '\n',
  });

  // Keep images as markdown with alt; src is often relative inside EPUB.
  td.addRule('images', {
    filter: (node: any) => node.nodeName === 'IMG',
    replacement: (_content: string, node: any) => {
      const alt = (node?.getAttribute?.('alt') ?? '').trim();
      const src = (node?.getAttribute?.('src') ?? '').trim();
      if (!src) return '';
      return `![${alt}](${src})`;
    },
  });

  return td;
}

async function main() {
  const program = new Command();
  program
    .name('01_extract_epub')
    .description('Extract an EPUB into per-chapter Markdown plus a metadata JSON file.')
    .argument('<epubPath>', 'Path to input .epub')
    .option('-o, --out <dir>', 'Output directory', 'output')
    .option('--bookSlug <slug>', 'Override output folder name (default: derived from title/file)')
    .option('--maxChapters <n>', 'Safety limit for number of spine items to extract', (v) => parseInt(v, 10))
    .parse(process.argv);

  const epubPathArg = program.args[0];
  const opts = program.opts<{
    out: string;
    bookSlug?: string;
    maxChapters?: number;
  }>();

  const epubPath = path.resolve(epubPathArg);
  const outRoot = path.resolve(opts.out);
  await fs.ensureDir(outRoot);

  if (!(await fs.pathExists(epubPath))) {
    throw new Error(`EPUB not found: ${epubPath}`);
  }

  const EPubAny: any = EPub;
  const epub = new EPubAny(epubPath);
  await epubParse(epub);

  const title: string | undefined = epub?.metadata?.title;
  const author: string | undefined = epub?.metadata?.creator;
  const language: string | undefined = epub?.metadata?.language;
  const publisher: string | undefined = epub?.metadata?.publisher;
  const rights: string | undefined = epub?.metadata?.rights;
  const description: string | undefined = epub?.metadata?.description;
  const isbn: string | undefined = epub?.metadata?.ISBN;
  const coverId: string | undefined = epub?.metadata?.cover;

  const slugBase = opts.bookSlug ?? toSafeSlug(title ?? path.parse(epubPath).name);
  const bookDir = path.join(outRoot, slugBase);
  const chaptersDir = path.join(bookDir, 'chapters');
  await fs.ensureDir(chaptersDir);

  const turndown = buildTurndown();

  // TOC: epub.toc is populated when present.
  const toc = (epub.toc ?? []).map((t: any, idx: number) => ({
    id: String(t.id ?? t.href ?? idx),
    href: t.href,
    order: idx,
    title: t.title,
    level: t.level,
  }));

  // Spine: epub.spine.contents maps ids to entries. The iteration order of `epub.spine.contents`
  // is not guaranteed, but epub2 also provides `epub.flow` / `epub.spine` structures.
  // We'll use epub.flow when available (ordered array) and fall back to spine.contents.
  const orderedIds: string[] = Array.isArray((epub as any).flow)
    ? ((epub as any).flow as any[]).map((x) => x.id).filter(Boolean)
    : Object.keys((epub as any).spine?.contents ?? {});

  if (orderedIds.length === 0) {
    throw new Error('No chapters/spine items found in EPUB.');
  }

  if (opts.maxChapters && orderedIds.length > opts.maxChapters) {
    throw new Error(
      `Refusing to extract ${orderedIds.length} chapters (maxChapters=${opts.maxChapters}).`,
    );
  }

  const chapters: ExtractMetadata['chapters'] = [];

  for (let i = 0; i < orderedIds.length; i++) {
    const id = orderedIds[i];
    const spineEntry = (epub as any).spine?.contents?.[id] ?? (epub as any).manifest?.[id];

    const rawHtml = await getChapterHtml(epub, id);
    const mdRaw = turndown.turndown(rawHtml);
    const md = stripMarkdownFrontMatter(mdRaw).trim();

    // Best-effort title: TOC title matching href/id, else spine entry title, else fallback.
    const tocMatch = (epub.toc ?? []).find((t: any) => t.id === id || t.href === spineEntry?.href);
    const chapterTitle =
      (tocMatch?.title as string | undefined) ||
      (spineEntry?.title as string | undefined) ||
      `Chapter ${i + 1}`;

    const safeTitleSlug = toSafeSlug(chapterTitle).slice(0, 60);
    const file = `${String(i + 1).padStart(3, '0')}_${safeTitleSlug || 'chapter'}.md`;
    const filePath = path.join(chaptersDir, file);

    const header = `# ${chapterTitle}\n\n`;
    const contents = header + (md.length ? md + '\n' : '');
    await fs.writeFile(filePath, contents, 'utf8');

    const wordCount = estimateWordCount(contents);

    chapters.push({
      order: i,
      id,
      title: chapterTitle,
      file: path.relative(bookDir, filePath).replace(/\\/g, '/'),
      href: spineEntry?.href,
      wordCount,
    });
  }

  const metadata: ExtractMetadata = {
    source: {
      epubPath,
      extractedAt: new Date().toISOString(),
    },
    book: {
      title,
      author,
      language,
      publisher,
      rights,
      description,
      isbn,
      coverId,
    },
    toc,
    chapters,
  };

  await fs.writeJson(path.join(bookDir, 'metadata.json'), metadata, { spaces: 2 });

  // Small index file for convenience.
  const indexMd = [
    `# ${title ?? slugBase}`,
    '',
    author ? `- Author: ${author}` : undefined,
    language ? `- Language: ${language}` : undefined,
    '',
    '## Chapters',
    '',
    ...chapters.map((c) => `- [${c.title}](./${c.file})`),
    '',
  ]
    .filter((x): x is string => Boolean(x))
    .join('\n');

  await fs.writeFile(path.join(bookDir, 'README.md'), indexMd, 'utf8');

  console.log(`✅ Extracted ${chapters.length} chapters to: ${bookDir}`);
  console.log(`- metadata: ${path.join(bookDir, 'metadata.json')}`);
  console.log(`- chapters:  ${chaptersDir}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
