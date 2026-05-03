# Pocket FM Commissioning Tool

A full-stack commissioning workflow for discovering, enriching, evaluating, and exporting title candidates for Pocket FM.

The app is now set up as one clean Vite + FastAPI project:

- `frontend/`: React + Vite dashboard.
- `backend/`: FastAPI API, background jobs, SQLAlchemy persistence, Amazon/Goodreads/contact enrichment, exports, and optional Google Sheets sync.
- `Dockerfile`: production full-stack image that builds the UI and serves it from FastAPI.
- `render.yaml`: Render Blueprint for the full app plus Postgres.

## Local Development

Install dependencies:

```text
npm install
npm --prefix frontend install
py -m pip install -r backend/requirements.txt
```

Run the UI and backend together:

```text
npm run dev
```

Open:

```text
http://127.0.0.1:5173
```

The Vite dev server proxies `/api` to FastAPI on port `8000`.

## Production-Style Local Run

```text
npm run serve
```

This builds `frontend/dist` and starts FastAPI at:

```text
http://127.0.0.1:8000
```

FastAPI serves both:

- UI: `/`
- API: `/api`
- Health check: `/api/health`

## Render Deployment

Use the Blueprint in `render.yaml`.

```text
services:
pocketfm-scraper-tool  # Docker web service
databases:
pocketfm-scraper-db    # Postgres
```

Render builds the frontend inside Docker, installs backend dependencies, installs Chromium for Playwright, and runs:

```text
uvicorn app:app --host 0.0.0.0 --port $PORT
```

See [docs/RENDER.md](docs/RENDER.md) for the exact setup notes.

## Environment Variables

The main optional values are:

```text
COMMISSIONING_DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DATABASE
COMMISSIONING_ALLOWED_ORIGINS=*
COMMISSIONING_JOB_WORKERS=1
AMAZON_DETAIL_WORKERS=1
GOODREADS_LOOKUP_WORKERS=1
```

Google Sheets sync is optional and depends on local Google credentials / sheet access.

## Verification

Run:

```text
npm run check
```

That runs the backend test suite and the Vite production build.
