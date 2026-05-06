# Pocket FM Commissioning Tool

A commissioning workflow for finding, enriching, evaluating, and exporting title candidates for Pocket FM. It runs locally for development and can be deployed to Google Cloud Run with Cloud SQL Postgres, Cloud Storage exports, and a separate queue worker.

The tool is split into two local apps:

- `backend/`: FastAPI API, background jobs, SQLite/Postgres persistence, scraper/enrichment services, data quality checks, and local or Cloud Storage exports.
- `frontend/`: React + Vite dashboard that talks to the backend through the local `/api` proxy.

## Current Status

The repo is cloud-ready, but the permanent Google Cloud deployment is not complete until billing is linked and a real Cloud Run smoke test passes.

What is already in place:

- Local app and packaged Docker image were tested successfully.
- Google Cloud deployment files are present: `Dockerfile`, `.dockerignore`, `cloudbuild.yaml`, and `scripts/deploy_cloud_run.ps1`.
- The backend supports SQLite locally and Cloud SQL Postgres in cloud mode.
- Exports can be stored locally or in Cloud Storage.
- Long-running jobs can run in-process locally or through a database-backed Cloud Run worker.
- Amazon blocked/deferred states and manual CSV fallback are available in the UI/API.
- Goodreads matching was tightened to avoid accepting title/author mismatches blindly.

Current Google Cloud project prepared during setup:

```text
pocketfm-jc8ah9
```

Billing still needs to be linked to that project before Cloud Run, Cloud SQL, Cloud Build, Artifact Registry, Secret Manager, and Cloud Storage can be enabled. Temporary `trycloudflare.com` links are only laptop tunnels. They are useful for a quick preview, but they are not the Google Cloud deployment and they stop working when the laptop, Docker, tunnel process, or internet connection stops.

## Component Overview

The production shape is:

```text
User browser
  -> Cloud Run public service
      -> FastAPI API
      -> built React/Vite UI served as static files
      -> Cloud SQL Postgres for runs, books, sources, jobs, and exports
      -> Cloud Storage for generated export files
      -> Secret Manager for database password
  -> Cloud Run worker service
      -> claims queued jobs from Cloud SQL
      -> runs Amazon scraping, Goodreads enrichment, tier mapping, exports, and contact enrichment
```

Local development uses the same application code with lighter state:

```text
Vite dev server on 127.0.0.1:5173
  -> FastAPI on 127.0.0.1:8000
      -> SQLite database in backend/backend_data/
      -> generated files in backend/generated/
      -> thread-backed background jobs by default
```

The frontend never talks directly to Google Cloud services. It calls the backend API. The backend decides whether to use local disk/SQLite or Cloud Storage/Cloud SQL based on environment variables.

## What The Tool Does

The app helps you turn source links into a curated commissioning shortlist.

1. Create or select a run.
2. Add Amazon and Goodreads source URLs.
3. Scrape candidates from those sources.
4. Enrich rows with Amazon details and Goodreads metadata.
5. Review data quality issues and accept manual Goodreads candidates when needed.
6. Apply benchmark filters and tier rules.
7. Run contact details mapping for author, agent, publisher, website, form, and social contact fields.
8. Draft outreach copy.
9. Export CSV, diagnostic CSV, JSON, XLSX, or PDF files.

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
- `Find contacts`: runs author contact enrichment on existing books.

The UI polls job status from the backend until the job completes or fails.

### Data & Genre Mapping

Review scraped rows, search/filter the book table, inspect source/provenance fields, and fix row-level fields manually.

This page is also where low-confidence Goodreads matches can be reviewed and accepted.

The mapping table shows the full operational row set used by exports, including author, publisher, Amazon rating/review count, Goodreads average rating, Goodreads rating count, tier fields, series fields, series book ratings, contact fields, and notes. Each visible column has a dropdown filter in its header so rows can be narrowed by the values already present in the run.

### Tier Mapping

Applies final Goodreads rating-count and length rules into export columns after benchmark filters are set.

Default rule set:

- Tier 1: GR ratings 20,000+ and length 80+ hours, MG 10k-15k.
- Tier 2: GR ratings 20,000+ and length 50+ hours, MG 10k-12.5k.
- Tier 3: GR ratings 5,000+ and length 80+ hours, MG 7.5k-10k.
- Tier 4: GR ratings 5,000+ and length 50+ hours, MG 3k-5k.
- Tier 5: all other rows, no MG.

The UI lets these tier names, GR rating-count thresholds, length thresholds, and MG values be edited before applying the mapping. The backend receives the edited rules and stamps the final tier columns onto the books. After this step, continue to Contact Details Mapping before creating the outreach CSV.

Column meanings:

- `Rating`: Amazon star rating when available.
- `no. of rating`: Amazon review/rating count when available.
- `Goodreads rating`: Goodreads average star rating for the resolved Goodreads book.
- `Goodreads no of rating`: Goodreads rating/review count for the resolved Goodreads book.
- `GR Ratings`: the Goodreads rating/review count used by the tier mapping rules.
- `GR Book 1 Rating` onward: Goodreads average star ratings for every detected primary book in the series when Goodreads exposes the series rows. The export keeps the legacy sample header `GR Book 1O Rating` for book 10 and adds `GR Book 11 Rating`, `GR Book 12 Rating`, etc. for longer series.
- `Book 1 No Of Rating` through `Book 10 No Of Rating`: Goodreads rating/review counts for those series books when available.

### Benchmark Filters

Tune shortlist filters such as minimum rating, review count, word count, series size, audio score, genres, and book type.

The backend stores whether each book matched the current benchmark and the UI shows the resulting shortlist.

### Contact Details Mapping

Runs contact discovery after tier mapping and exposes the final outreach contact columns in one editable table:

- `Email ID`
- `Email ID source`
- `Email type`
- `Author Email`
- `Agent Email`
- `Website`
- `Contact Forms`
- `Facebook link`
- `Publisher's details`

Each contact column has a dropdown filter in the header. Inline edits are saved back to the backend through `/api/books/{book_id}/contact`, and `Create Final CSV` exports the final commissioning file with contact details included.

### Author Outreach

Review reachable books, generate local outreach drafts, and mark outreach state.

### Export & Share

Create downloadable files from the current run.

Common export profiles:

- Final CSV: sample-compatible commissioning output with Amazon, Goodreads, dynamic series rating columns, tier mapping, minimum guarantee, rev-share, benchmark, and contact columns, including author email, agent email, email type/source, website, forms, Facebook link, and publisher details.
- Full diagnostic CSV: includes provenance and data-quality columns for audit/debugging.
- JSON diagnostic: structured diagnostic rows.
- XLSX/PDF: available through backend export support.

Generated files are stored locally under `backend/generated/` by default. When `COMMISSIONING_GCS_BUCKET` is set, exports are uploaded to Cloud Storage, the export record stores the `gs://` URI, and downloads stream back through the API.

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

## Deploying To Google Cloud Run

Cloud Build can build this repo into a container and deploy it to Cloud Run. Cloud Build is the CI/CD builder; Cloud Run is the public hosting runtime.

### Prerequisites

- A Google Cloud project with billing linked.
- Google Cloud SDK installed and authenticated.
- Permission to enable APIs, create Cloud SQL, create Cloud Storage buckets, create Secret Manager secrets, and deploy Cloud Run services.

Authenticate:

```text
gcloud auth login
gcloud config set project PROJECT_ID
```

For the new project created during setup, use:

```text
gcloud config set project pocketfm-jc8ah9
```

Check billing before deploying:

```text
gcloud billing projects describe PROJECT_ID
```

If `billingEnabled` is `false`, link billing in the Google Cloud console first.

### One-command deploy helper

From the repo root:

```text
.\scripts\deploy_cloud_run.ps1 -ProjectId PROJECT_ID -DbPassword "A_STRONG_DATABASE_PASSWORD"
```

The helper script:

- Sets the active Google Cloud project.
- Enables the required APIs.
- Grants the Cloud Build and Cloud Run service accounts the roles needed by this app.
- Creates the Artifact Registry Docker repository if missing.
- Creates the Cloud SQL Postgres instance, database, and user if missing.
- Stores or rotates the database password in Secret Manager.
- Creates the Cloud Storage export bucket if missing.
- Runs Cloud Build with `cloudbuild.yaml`.
- Prints the public Cloud Run UI/API URL.

The default cloud resources are:

```text
Region: us-central1
Artifact Registry repository: pocketfm
Cloud Run UI/API service: pocketfm-commissioning
Cloud Run worker service: pocketfm-commissioning-worker
Cloud SQL instance: pocketfm-postgres
Cloud SQL database/user: pocketfm
Secret Manager secret: pocketfm-db-password
Cloud Storage bucket: PROJECT_ID-pocketfm-exports
```

The build deploys two Cloud Run services:

- `pocketfm-commissioning`: public UI/API. It queues work in the database.
- `pocketfm-commissioning-worker`: background worker. It claims queued jobs from Cloud SQL and executes them with concurrency 1 per instance. Amazon product detail fetching is split into per-book child jobs, so two worker instances can make large source runs modestly faster without increasing per-instance request pressure.

The Cloud Run deploy uses these durable state variables:

```text
COMMISSIONING_JOB_BACKEND=database
CLOUD_SQL_CONNECTION_NAME=PROJECT_ID:us-central1:pocketfm-postgres
CLOUD_SQL_DATABASE=pocketfm
CLOUD_SQL_USER=pocketfm
CLOUD_SQL_PASSWORD=from Secret Manager
COMMISSIONING_GCS_BUCKET=PROJECT_ID-pocketfm-exports
```

Amazon scraping note: Cloud Run deploys with `COMMISSIONING_DISABLE_PLAYWRIGHT_FALLBACK=1`, `COMMISSIONING_DISTRIBUTED_AMAZON_DETAILS=1`, `AMAZON_DETAIL_WORKERS=1`, `AMAZON_DETAIL_ITEM_DELAY_SECONDS=1.0`, and a slower Amazon page delay. Source discovery remains cautious; only product detail pages are split into per-book child jobs. When Amazon blocks or rate-limits a source, the job marks that source blocked/deferred instead of attempting manual CAPTCHA clearing.

### Manual deploy commands

The helper script wraps these resources. If a manual deploy is needed, create the resources first:

```text
gcloud services enable cloudbuild.googleapis.com run.googleapis.com artifactregistry.googleapis.com sqladmin.googleapis.com secretmanager.googleapis.com storage.googleapis.com
gcloud artifacts repositories create pocketfm --repository-format=docker --location=us-central1
gcloud sql instances create pocketfm-postgres --database-version=POSTGRES_16 --region=us-central1 --tier=db-custom-1-3840 --storage-size=20GB
gcloud sql databases create pocketfm --instance=pocketfm-postgres
gcloud sql users create pocketfm --instance=pocketfm-postgres --password=YOUR_DB_PASSWORD
gcloud secrets create pocketfm-db-password --data-file=-
gcloud storage buckets create gs://PROJECT_ID-pocketfm-exports --location=us-central1
```

Then deploy:

```text
gcloud builds submit --config cloudbuild.yaml --substitutions="_REGION=us-central1,_REPOSITORY=pocketfm,_SERVICE=pocketfm-commissioning,_WORKER_SERVICE=pocketfm-commissioning-worker,_CLOUD_SQL_INSTANCE=pocketfm-postgres,_CLOUD_SQL_DATABASE=pocketfm,_CLOUD_SQL_USER=pocketfm,_DB_PASSWORD_SECRET=pocketfm-db-password,_GCS_EXPORT_BUCKET_SUFFIX=pocketfm-exports"
```

### Smoke test after deploy

Run the Cloud Run smoke test after deploy with one small Amazon URL and one Goodreads URL:

```text
py scripts/cloud_run_smoke.py --base-url https://YOUR_SERVICE_URL --amazon-url "https://www.amazon.com/..." --goodreads-url "https://www.goodreads.com/..."
```

At minimum, verify:

```text
https://YOUR_SERVICE_URL/api/health
```

### Cost and sizing notes

The current `cloudbuild.yaml` favors reliability for a presentation or light production trial:

- UI/API service: 2 CPU, 2Gi memory, `min-instances=1`.
- Worker service: 2 CPU, 2Gi memory, `min-instances=2`, `max-instances=2`.
- Cloud SQL: `db-custom-1-3840`, 20GB storage.

That avoids cold starts and keeps a worker alive, but it has an always-on cost. For a cheaper demo, reduce Cloud Run CPU/memory and set `min-instances` to `0`; for heavier production use, keep the current sizing or scale up after real job timings are known.

## Scripts

Root scripts:

```text
npm run dev             # backend + frontend together
npm run dev:backend     # FastAPI on 127.0.0.1:8000
npm run dev:frontend    # Vite on 127.0.0.1:5173
npm run build           # frontend build check
npm run test            # backend unittest suite
npm run check           # backend tests + frontend build
npm run start:worker    # process queued DB jobs when COMMISSIONING_JOB_BACKEND=database
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
COMMISSIONING_JOB_BACKEND=thread
COMMISSIONING_DISABLE_PLAYWRIGHT_FALLBACK=1
COMMISSIONING_DISTRIBUTED_AMAZON_DETAILS=1
COMMISSIONING_CHILD_JOB_FALLBACK_AFTER_SECONDS=3.0
AMAZON_DETAIL_WORKERS=1
AMAZON_DETAIL_ITEM_DELAY_SECONDS=1.0
AMAZON_PAGE_DELAY_SECONDS=2.0
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

Cloud SQL Postgres shortcut:

```text
CLOUD_SQL_CONNECTION_NAME=PROJECT_ID:REGION:INSTANCE
CLOUD_SQL_DATABASE=pocketfm
CLOUD_SQL_USER=pocketfm
CLOUD_SQL_PASSWORD=...
```

Export storage override:

```text
COMMISSIONING_GCS_BUCKET=PROJECT_ID-pocketfm-exports
COMMISSIONING_GCS_PREFIX=commissioning-exports
```

## Optional Local Integrations

### Google Sheets Sync

The API includes a Google Sheets sync endpoint. It depends on the local `sheets_handler.py` helper and Google credentials being available in the surrounding workspace.

If the helper is missing, the sync endpoint returns a clear unavailable message and the rest of the app still works.

### Contact Research

Contact enrichment first uses the local `contact_info_pipeline.py` helper when that larger research pipeline is available. If the helper is missing or fails, the backend falls back to its built-in public-web contact discovery using DuckDuckGo-style search (`ddgs`), page fetches, email extraction, contact-form detection, Facebook link detection, and author/agent/publisher email classification.

Contact jobs save results into the `contacts` table and expose them in Contact Details Mapping. Manual corrections can be made through the UI and are preserved by later enrichment runs unless the user edits a value directly.

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
- `/api/batches/{batch_id}/imports/csv`: import a manual CSV fallback when Amazon blocks a source.
- `/api/batches/{batch_id}/jobs/...`: queue scrape/enrichment jobs.
- `/api/jobs/{job_id}` and `/api/jobs/{job_id}/events`: inspect job status/events.
- `/api/batches/{batch_id}/books`: list books in a run.
- `/api/books/{book_id}`: patch book fields.
- `/api/books/{book_id}/contact`: patch contact fields.
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

## Present Limitations

### Scraping and source reliability

- Amazon and Goodreads can block or rate-limit automated traffic. This tool detects those cases and defers the source instead of trying to bypass CAPTCHA or access controls.
- Cloud hosting does not make scraping unblockable. It only makes the app public and independent of the laptop.
- Large Amazon category/search links can still take time because source discovery remains low-concurrency to reduce blocking. Product detail pages are distributed as per-book child jobs so a second worker can improve throughput modestly while using the same detail parser and retry logic.
- Some Amazon pages may expose incomplete metadata because Amazon markup changes frequently.
- Goodreads matching is stricter than before, but no title-matching system should be treated as perfect. The app now prefers exact title/author evidence, keeps low-confidence candidates for review, and avoids misleading cross-title matches where possible.
- Contact discovery uses public web search and public pages. It can still miss authors with no public email, blocked websites, JavaScript-only contact forms, or ambiguous pen names, so the Contact Details Mapping tab keeps all results editable and filterable.
- For blocked or high-risk sources, use the blocked/deferred dashboard and CSV import fallback. The cleanest long-term solution is an authorized data feed or user-provided export.

### Cloud and operations

- Permanent Google Cloud deployment requires billing to be linked to the project.
- The current Cloud Run configuration allows unauthenticated public access. Add Cloud Run authentication, Identity-Aware Proxy, or another access gate before sharing sensitive data broadly.
- Cloud SQL is the main recurring cost. Even low traffic costs money because the database instance is provisioned.
- The worker currently runs with `min-instances=2` and `max-instances=2` in cloud mode. That gives one parent scrape orchestrator plus one detail worker most of the time; the parent can also run child detail jobs after a short fallback delay if no worker is available.
- Database schema management is lightweight. Before a multi-team production rollout, add formal migrations and a backup/restore procedure.
- The app does not include a full admin/user permission system yet. Workspaces are isolated by workspace id, not by authenticated user accounts.

### Temporary public links

- `trycloudflare.com` links are laptop tunnels. They are not stable deployment URLs.
- A tunnel link stops working if the laptop sleeps, Docker stops, the tunnel process exits, or the network drops.
- The proper public URL after cloud deployment will be the Cloud Run service URL printed by the deploy script.

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
- Retry later or use an authorized Amazon data source / user-provided CSV when the source is blocked.

Relevant env vars:

```text
AMAZON_DETAIL_WORKERS=1
AMAZON_PAGE_DELAY_SECONDS=2.0
AMAZON_REQUEST_TIMEOUT_SECONDS=25
AMAZON_REQUEST_RETRIES=2
COMMISSIONING_DISABLE_PLAYWRIGHT_FALLBACK=1
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
