FROM node:22-bookworm-slim AS frontend-build
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
ENV VITE_API_BASE_URL=/api
RUN npm run build

FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV COMMISSIONING_DATA_DIR=/tmp/commissioning_data
ENV COMMISSIONING_GENERATED_DIR=/tmp/commissioning_generated
ENV COMMISSIONING_DISABLE_PLAYWRIGHT_FALLBACK=1
ENV COMMISSIONING_JOB_BACKEND=thread
ENV COMMISSIONING_GCS_PREFIX=commissioning-exports
ENV AMAZON_DETAIL_WORKERS=1
ENV AMAZON_PAGE_DELAY_SECONDS=2.0

WORKDIR /app
COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

COPY backend/ ./backend/
COPY --from=frontend-build /app/frontend/dist ./backend/static

WORKDIR /app/backend
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080}"]
