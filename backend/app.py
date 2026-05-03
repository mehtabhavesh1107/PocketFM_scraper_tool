from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import HTTPException
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from commissioning.api.routes import router
from commissioning.db import init_db
from commissioning.jobs.manager import job_manager
from commissioning.settings import ALLOWED_ORIGINS, ensure_directories


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_directories()
    init_db()
    yield
    job_manager.shutdown()


app = FastAPI(
    title="Pocket FM Commissioning Backend",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


FRONTEND_DIST_DIR = Path(__file__).resolve().parents[1] / "frontend" / "dist"


def _mount_frontend() -> None:
    if not FRONTEND_DIST_DIR.exists():
        return

    assets_dir = FRONTEND_DIST_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="frontend-assets")

    @app.get("/", include_in_schema=False)
    def serve_frontend_index():
        return FileResponse(FRONTEND_DIST_DIR / "index.html")

    @app.get("/{path:path}", include_in_schema=False)
    def serve_frontend_path(path: str):
        if path.startswith("api/"):
            raise HTTPException(status_code=404, detail="API route not found")
        requested = (FRONTEND_DIST_DIR / path).resolve()
        try:
            requested.relative_to(FRONTEND_DIST_DIR.resolve())
        except ValueError as exc:
            raise HTTPException(status_code=404, detail="Frontend asset not found") from exc
        if requested.is_file():
            return FileResponse(requested)
        return FileResponse(FRONTEND_DIST_DIR / "index.html")


_mount_frontend()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
