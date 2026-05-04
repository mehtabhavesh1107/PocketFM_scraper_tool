# Pocket FM Commissioning Tool

A local-only commissioning workflow for finding, enriching, evaluating, and exporting title candidates for Pocket FM.

The tool is split into two local apps:

- `backend/`: FastAPI API, background jobs, SQLite persistence, scraper/enrichment services, data quality checks, and exports.
- `frontend/`: React + Vite dashboard that talks to the backend through the local `/api` proxy.

## What The Tool Does

The app helps you turn source links into a curated commissioning shortlist.

1. Create or select a run.
2. Add Amazon and Goodreads source URLs.
3. Scrape candidates from those sources.
4. Enrich rows with Amazon details, Goodreads metadata, and optional contact research.
5. Review data quality issues and accept manual Goodreads candidates when needed.
6. Apply tier and benchmark rules.
7. Draft outreach copy.
8. Export CSV, diagnostic CSV, JSON, XLSX, or PDF files.

## Main Workflow

### Dashboard

Shows the current run, high-level counts, recent run state, and navigation into the pipeline.

### Scraping

Use this page to enter source URLs and start background jobs.

Supported source types:

- Amazon source URLs, including bestseller/category/search/detail/ASIN style URLs handled by the Amazon parser.
- Goodreads list and shelf URLs handled by the Goodreads parser.

Job buttons:

- `Fast scrape`: fetches Amazon details and skips Goodreads mapping for speed.
- `Full scrape`: fetches source data and auto-runs Goodreads enrichment.
- `Enrich Goodreads`: runs Goodreads enrichment on existing books.
- `Find contacts`: runs optional author contact enrichment.

The UI polls job status from the backend until the job completes or fails.

### Data & Genre Mapping

Review scraped rows, search/filter the book table, inspect source/provenance fields, and fix row-level fields manually.

This page is also where low-confidence Goodreads matches can be reviewed and accepted.

### Tier Mapping

Applies final Goodreads rating and length rules into export columns.

Current rule set:

- Tier 1: GR ratings 20,000+ and length 80+ hours, MG 10k-15k.
- Tier 2: GR ratings 20,000+ and length 50+ hours, MG 10k-12.5k.
- Tier 3: GR ratings 5,000+ and length 80+ hours, MG 7.5k-10k.
- Tier 4: GR ratings 5,000+ and length 50+ hours, MG 3k-5k.
- Tier 5: all other rows, no MG.

### Benchmark Filters

Tune shortlist filters such as minimum rating, review count, word count, series size, audio score, genres, and book type.

The backend stores whether each book matched the current benchmark and the UI shows the resulting shortlist.

### Author Outreach

Review reachable books, edit contact fields, generate local outreach drafts, and mark outreach state.

Contact discovery is optional and depends on local helper scripts being available.

### Export & Share

Create downloadable files from the current run.

Common export profiles:

- Final CSV: sample-compatible commissioning output with tier, length, MG, benchmark, and contact columns.
- Full diagnostic CSV: includes provenance and data-quality columns for audit/debugging.
- JSON diagnostic: structured diagnostic rows.
- XLSX/PDF: available through backend export support.

Generated files are stored locally under `backend/generated/`.

## Requirements

- Windows PowerShell
- Python 3.12
- Node.js 22
- npm

Install Python dependencies:

```text
py -m pip install -r backend/requirements.txt
```

Install Node dependencies:

```text
npm install
npm --prefix frontend install
```

Optional browser fallback for Playwright-based scraping:

```text
py -m playwright install chromium
```

## Running Locally

Run both backend and frontend:

```text
npm run dev
```

Open:

```text
http://127.0.0.1:5173
```

The frontend dev server proxies `/api` to:

```text
http://127.0.0.1:8000
```

Run each side separately:

```text
npm run dev:backend
npm run dev:frontend
```

Backend health check:

```text
http://127.0.0.1:8000/api/health
```

## Scripts

Root scripts:

```text
npm run dev             # backend + frontend together
npm run dev:backend     # FastAPI on 127.0.0.1:8000
npm run dev:frontend    # Vite on 127.0.0.1:5173
npm run build           # frontend build check
npm run test            # backend unittest suite
npm run check           # backend tests + frontend build
```

Frontend scripts live in `frontend/package.json`.

## Local Data

Default backend storage:

```text
backend/backend_data/commissioning.db
```

Generated exports:

```text
backend/generated/
```

Both folders are ignored by git. Deleting them resets local stored runs and generated files.

The UI also stores an anonymous workspace id in browser local storage:

```text
pocketfm_workspace_id
```

Changing or clearing that value creates a separate local workspace view.

## Environment Variables

Copy `.env.example` only when you need to override defaults.

Common settings:

```text
VITE_API_BASE_URL=/api
COMMISSIONING_JOB_WORKERS=4
AMAZON_DETAIL_WORKERS=4
AMAZON_REQUEST_TIMEOUT_SECONDS=25
AMAZON_REQUEST_RETRIES=2
GOODREADS_LOOKUP_WORKERS=4
GOODREADS_REQUEST_ATTEMPTS=3
GOODREADS_REQUEST_TIMEOUT=25
GOODREADS_REQUEST_DELAY_SECONDS=1.0
```

Database override:

```text
COMMISSIONING_DATABASE_URL=sqlite:///backend/backend_data/commissioning.db
```

If `COMMISSIONING_DATABASE_URL` is blank, the app uses the default local SQLite path.

## Optional Local Integrations

### Google Sheets Sync

The API includes a Google Sheets sync endpoint. It depends on the local `sheets_handler.py` helper and Google credentials being available in the surrounding workspace.

If the helper is missing, the sync endpoint returns a clear unavailable message and the rest of the app still works.

### Contact Research

Contact enrichment depends on the local `contact_info_pipeline.py` helper. If the helper is missing, contact jobs safely skip enrichment and keep the rest of the run intact.

## Project Structure

```text
PocketFM/
  backend/
    app.py                         # FastAPI app
    requirements.txt               # Python dependencies
    commissioning/
      api/routes.py                # API routes
      db.py                        # SQLAlchemy engine/session setup
      jobs/                        # background job orchestration
      models.py                    # database models
      schemas.py                   # request/response schemas
      services/                    # scraping, mapping, exports, quality checks
    tests/                         # backend tests
  frontend/
    src/App.tsx                    # main React UI
    src/App.css                    # app styles
    vite.config.ts                 # local proxy to FastAPI
    package.json                   # frontend dependencies/scripts
  package.json                     # root orchestration scripts
  README.md
```

## API Overview

Core API groups:

- `/api/bootstrap`: load or create the current workspace run.
- `/api/batches`: create/list runs.
- `/api/batches/{batch_id}/sources`: add, replace, and list source URLs.
- `/api/batches/{batch_id}/jobs/...`: queue scrape/enrichment jobs.
- `/api/jobs/{job_id}` and `/api/jobs/{job_id}/events`: inspect job status/events.
- `/api/batches/{batch_id}/books`: list books in a run.
- `/api/books/{book_id}`: patch book fields.
- `/api/batches/{batch_id}/data-quality`: data-quality report.
- `/api/batches/{batch_id}/benchmark/apply`: apply shortlist filters.
- `/api/batches/{batch_id}/tier-mapping/apply`: apply tier columns.
- `/api/books/{book_id}/goodreads/accept`: accept a Goodreads candidate.
- `/api/books/{book_id}/evaluation`: save subjective evaluation.
- `/api/books/{book_id}/outreach`: save outreach state.
- `/api/batches/{batch_id}/exports`: create exports.
- `/api/exports/{export_id}/download`: download generated export files.

## Testing

Run backend tests:

```text
npm run test
```

Run backend tests and verify the frontend build:

```text
npm run check
```

The test suite covers:

- Amazon URL normalization and parsing.
- Goodreads matching behavior.
- API workspace isolation.
- Fast scrape job queueing.
- Export profiles.
- Pipeline smoke flows.

## Troubleshooting

### The UI cannot connect to the backend

Make sure both local servers are running:

```text
npm run dev
```

Then check:

```text
http://127.0.0.1:8000/api/health
```

### A scrape job is stuck after a restart

Restart the backend. On startup, interrupted queued/running jobs are marked failed so the UI does not poll forever. Start the job again from the Scraping page.

### Amazon returns empty or incomplete detail fields

Amazon changes markup and may challenge automated requests. Try:

- Fewer workers.
- A smaller source URL.
- Longer request timeouts.
- Playwright fallback enabled with Chromium installed.

Relevant env vars:

```text
AMAZON_DETAIL_WORKERS=1
AMAZON_REQUEST_TIMEOUT_SECONDS=25
AMAZON_REQUEST_RETRIES=2
COMMISSIONING_DISABLE_PLAYWRIGHT_FALLBACK=0
```

### Goodreads matching is slow

Reduce worker count and increase delay:

```text
GOODREADS_LOOKUP_WORKERS=1
GOODREADS_REQUEST_DELAY_SECONDS=1.5
```

### Reset all local runs

Stop the backend, then delete:

```text
backend/backend_data/
backend/generated/
```

Start the app again with:

```text
npm run dev
```

## Development Notes

- Keep generated exports and local databases out of git.
- Keep root `README.md` as the source of truth for running the whole tool.
- Keep `frontend/README.md` focused on the Vite UI only.
- Add tests for backend behavior changes, especially parsers, job flow, exports, and API contracts.
