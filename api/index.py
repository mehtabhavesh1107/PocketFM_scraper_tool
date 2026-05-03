from __future__ import annotations

import sys
from pathlib import Path

from fastapi import HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"
FRONTEND_DIST_DIR = ROOT_DIR / "frontend" / "dist"

if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app import app  # noqa: E402


if FRONTEND_DIST_DIR.exists():
    assets_dir = FRONTEND_DIST_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="frontend-assets")

    @app.get("/", include_in_schema=False)
    def serve_frontend_index():
        return FileResponse(FRONTEND_DIST_DIR / "index.html")

    @app.get("/{path:path}", include_in_schema=False)
    def serve_frontend_path(path: str):
        requested = (FRONTEND_DIST_DIR / path).resolve()
        if not str(requested).startswith(str(FRONTEND_DIST_DIR.resolve())):
            raise HTTPException(status_code=404)
        if requested.is_file():
            return FileResponse(requested)
        return FileResponse(FRONTEND_DIST_DIR / "index.html")
