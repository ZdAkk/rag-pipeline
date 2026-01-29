# rag-pipeline

Local preprocessing pipeline for RAG (retrieval augmented generation):

1. **EPUB → Markdown** (per chapter)
2. **Markdown → chunks** (placeholder)
3. **Chunks → embeddings** (placeholder)

> This repo is intentionally "local-first". You can run it on a single machine and later wire the chunking/embedding outputs into your vector DB of choice.

## Requirements

- Node.js >= 20

## Setup

```bash
cd ~/clawd/rag-pipeline
npm install
```

## 1) Extract an EPUB into Markdown

```bash
npm run extract:epub -- \
  /path/to/book.epub \
  --out output
```

This creates:

- `output/<bookSlug>/metadata.json`
- `output/<bookSlug>/README.md` (chapter index)
- `output/<bookSlug>/chapters/*.md`

### Small sample command

```bash
npm run extract:epub -- ./data/example.epub --out output
```

## 2) Chunk Markdown (placeholder)

```bash
npm run chunk -- --in output/<bookSlug>
```

Writes a placeholder file at `output/<bookSlug>/chunks/chunks.jsonl`.

## 3) Embed chunks (placeholder)

```bash
npm run embed -- --in output/<bookSlug>/chunks/chunks.jsonl
```

Writes a placeholder file at `output/<bookSlug>/embeddings/embeddings.jsonl`.

## Notes

- `scripts/01_extract_epub.ts` uses:
  - `epub2` to read the EPUB container
  - `turndown` to convert chapter HTML to Markdown
- Images are left as Markdown image links with the original `src` (often relative inside EPUB). If you want image extraction, add a pass to copy referenced assets from the EPUB into an `assets/` folder.

## License

MIT
