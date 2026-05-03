# Render Full-Stack Deployment

This repo is configured as a single full-stack Render service. The Docker build compiles the Vite React UI, installs the FastAPI backend with Playwright/Chromium, and serves the built UI from FastAPI.

## Recommended Free Setup

Use Render's free web service for the app and a Postgres database for persistence.

- App host: Render Web Service, free plan, Docker runtime.
- UI: Vite React, built into the Docker image.
- API: FastAPI under `/api`.
- Database: Render Postgres free plan for short pilots, or Neon/Supabase Postgres for longer-lived free data.

Render's free Postgres database expires after 30 days, so use it for demos and short pilots only. For ongoing free use, create a Neon or Supabase Postgres database and set this backend environment variable manually in the Render web service:

```text
COMMISSIONING_DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DATABASE
```

## Deploy From GitHub

1. Push this repo to GitHub.
2. In Render, choose **New > Blueprint**.
3. Select this repo and let Render read `render.yaml`.
4. Deploy the `pocketfm-backend` service.
5. Open `https://YOUR-SERVICE.onrender.com/api/health`; it should return `{"status":"ok"}`.
6. Open `https://YOUR-SERVICE.onrender.com/`; the Vite UI should load from the same service.

## Local Development

Run both dev servers:

```text
npm run dev
```

The UI runs on `http://127.0.0.1:5173` and proxies `/api` to the backend on `http://127.0.0.1:8000`.

For a local production-style run:

```text
npm run serve
```

That builds `frontend/dist` and starts FastAPI on `http://127.0.0.1:8000`, serving both UI and API from the backend.

## Free Tier Notes

- The backend uses Docker because Playwright needs Chromium and Linux system dependencies.
- Worker counts are intentionally set to `1` in `render.yaml` to fit the free instance.
- Free Render web services sleep after idle periods, so the first request after sleep can be slow.
- Local SQLite and generated exports are not durable on free Render; the database should be Postgres.
- Generated CSV/XLSX/PDF files are still local ephemeral files. Download exports soon after generating them.

## Useful Manual Settings

If the UI is hosted on a fixed domain, replace `COMMISSIONING_ALLOWED_ORIGINS=*` with that domain:

```text
COMMISSIONING_ALLOWED_ORIGINS=https://your-frontend-domain.example
```

If you later move to a paid Render instance, increase workers carefully:

```text
COMMISSIONING_JOB_WORKERS=2
AMAZON_DETAIL_WORKERS=2
GOODREADS_LOOKUP_WORKERS=2
```
