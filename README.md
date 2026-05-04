# Pocket FM Commissioning Tool

A local commissioning workflow for discovering, enriching, evaluating, and exporting title candidates for Pocket FM.

The project is split into two local apps:

- `backend/`: FastAPI API, background jobs, SQLAlchemy persistence, Amazon/Goodreads/contact enrichment, exports, and optional Google Sheets sync.
- `frontend/`: React + Vite dashboard that calls the backend through the local Vite `/api` proxy.

## Local Setup

Install dependencies:

```text
npm install
npm --prefix frontend install
py -m pip install -r backend/requirements.txt
```

Run both apps together:

```text
npm run dev
```

Open the UI:

```text
http://127.0.0.1:5173
```

The frontend proxies `/api` requests to FastAPI at:

```text
http://127.0.0.1:8000
```

## Separate Runs

Backend only:

```text
npm run dev:backend
```

Frontend only:

```text
npm run dev:frontend
```

## Local Data

By default, the backend uses SQLite under:

```text
backend/backend_data/
```

Generated CSV/XLSX/JSON/PDF exports are written under:

```text
backend/generated/
```

Both folders are local working data and are ignored by git.

## Environment Variables

Copy `.env.example` only when you need to override local defaults. The most common options are:

```text
VITE_API_BASE_URL=/api
COMMISSIONING_JOB_WORKERS=4
AMAZON_DETAIL_WORKERS=4
GOODREADS_LOOKUP_WORKERS=4
```

Google Sheets sync is optional and depends on local Google credentials / sheet access.

## Verification

Run:

```text
npm run check
```

That runs the backend test suite and verifies the frontend build.
