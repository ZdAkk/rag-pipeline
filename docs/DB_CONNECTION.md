# DB connection (local notes)

This project reads Postgres connection details from `.env` (gitignored).

## Required env vars

- `RAG_DB_HOST`
- `RAG_DB_PORT` (default: 5432)
- `RAG_DB_NAME` (current: `postgres`)
- `RAG_DB_USER` (current: `postgres`)
- `RAG_DB_PASSWORD`
- `RAG_DB_SSLMODE` (optional; e.g. `require`)

## Example

```bash
# in .env (NOT COMMITTED)
RAG_DB_HOST=1.2.3.4
RAG_DB_PORT=5432
RAG_DB_NAME=postgres
RAG_DB_USER=postgres
RAG_DB_PASSWORD=***
RAG_DB_SSLMODE=require
```

## Security

- Never commit `.env`.
- If you need to share config, use `.env.example` with placeholders.
