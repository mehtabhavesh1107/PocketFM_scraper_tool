# Vercel Deployment

This repo is configured for a single Vercel project with a static Vite dashboard and a Python FastAPI serverless function mounted at `/api`.

## Project Setup

1. Import `Navya123445/PocketFM_scraper_tool` into Vercel.
2. Keep the root directory as `./`.
3. Use the normal Vercel project import flow. Do not use the Services preset.
4. Leave build settings controlled by `vercel.json`.
5. Do not set `VITE_API_BASE_URL` for the normal Vercel deployment. The frontend defaults to `/api`.

## Recommended Environment Variables

Set these in Vercel Project Settings when you want persistent cloud data:

```text
COMMISSIONING_DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DATABASE
```

Without `COMMISSIONING_DATABASE_URL`, the backend uses SQLite under `/tmp/pocketfm`. That lets the app boot on Vercel, but data and generated exports can disappear after cold starts or redeploys.

Useful scrape tuning variables:

```text
AMAZON_DETAIL_WORKERS=4
AMAZON_DETAIL_RETRY_ROUNDS=1
COMMISSIONING_JOB_WORKERS=2
```

## Notes

- The local-only contact discovery helper is optional in this deployment. If it is not present, the backend still runs and skips contact enrichment instead of crashing.
- For long production scrape workloads, a durable queue and persistent object storage should replace in-process jobs and `/tmp` export files.
