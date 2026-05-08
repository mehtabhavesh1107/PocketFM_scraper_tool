"""Drop-in Google OAuth for FastAPI/Starlette using authlib.

Required env:
    GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET
    SESSION_SECRET   (random ≥32 chars)
    APP_BASE_URL     (e.g. https://scraper.49.13.95.229.sslip.io)

Sign-in restricted to @pocketfm.com Google accounts.

Usage:
    from google_auth import attach_oauth, require_auth

    attach_oauth(app)                          # mounts SessionMiddleware + /auth/* routes
    @app.get('/protected', dependencies=[Depends(require_auth)])
    async def protected_route(): ...
"""

from __future__ import annotations

import os
import re
from typing import Any

from authlib.integrations.starlette_client import OAuth, OAuthError
from fastapi import FastAPI, HTTPException, Request, status
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import RedirectResponse


ALLOWED_EMAIL = re.compile(r"^[^@]+@pocketfm\.com$")

_oauth = OAuth()


def _safe_relative(path: str | None) -> str:
    """Return path iff it is a same-origin relative URL; otherwise '/'.
    Blocks open-redirect via // (protocol-relative), absolute URLs, and CRLF injection.
    """
    if not path or not isinstance(path, str):
        return "/"
    if not path.startswith("/"):
        return "/"
    if path.startswith("//") or path.startswith("/\\"):
        return "/"
    if "\r" in path or "\n" in path:
        return "/"
    return path


def attach_oauth(app: FastAPI) -> None:
    """Add SessionMiddleware + /auth/google, /auth/google/callback, /auth/logout, /auth/me."""
    base_url = (os.environ.get("APP_BASE_URL", "") or "").rstrip("/")
    if not base_url:
        raise RuntimeError("APP_BASE_URL env is required for google-auth")

    session_secret = os.environ.get("SESSION_SECRET", "")
    if len(session_secret) < 32:
        raise RuntimeError(
            "SESSION_SECRET env is required and must be ≥32 random chars "
            "(generate with `openssl rand -hex 32`)"
        )

    app.add_middleware(
        SessionMiddleware,
        secret_key=session_secret,
        session_cookie="scraper_sid",
        same_site="lax",
        https_only=True,
        max_age=12 * 60 * 60,
    )

    _oauth.register(
        name="google",
        client_id=os.environ.get("GOOGLE_CLIENT_ID", ""),
        client_secret=os.environ.get("GOOGLE_CLIENT_SECRET", ""),
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )

    @app.get("/auth/google", include_in_schema=False)
    async def google_login(request: Request, next: str | None = None):
        request.session["return_to"] = _safe_relative(next)
        redirect_uri = base_url + "/auth/google/callback"
        return await _oauth.google.authorize_redirect(
            request,
            redirect_uri,
            hd="pocketfm.com",
            prompt="select_account",
        )

    @app.get("/auth/google/callback", include_in_schema=False)
    async def google_callback(request: Request):
        try:
            token = await _oauth.google.authorize_access_token(request)
        except OAuthError as exc:
            return RedirectResponse(f"/login?error={exc.error}")

        userinfo: dict[str, Any] = token.get("userinfo") or {}
        email = (userinfo.get("email") or "").lower()
        if not email or not ALLOWED_EMAIL.match(email):
            request.session.clear()
            return RedirectResponse("/login?error=domain")

        request.session["user"] = {
            "email": email,
            "name":  userinfo.get("name") or email,
            "sub":   userinfo.get("sub"),
        }
        dest = _safe_relative(request.session.pop("return_to", "/"))
        return RedirectResponse(dest)

    @app.get("/auth/logout", include_in_schema=False)
    async def google_logout(request: Request):
        request.session.clear()
        return RedirectResponse("/login")

    @app.get("/auth/me", include_in_schema=False)
    async def google_me(request: Request):
        user = request.session.get("user")
        if not user:
            return {"ok": False}
        return {"ok": True, "user": user}


def _is_browser_navigation(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    if "text/html" in accept:
        return True
    return not request.url.path.startswith("/api/")


def require_auth(request: Request) -> dict[str, Any]:
    """FastAPI dependency. Use as `Depends(require_auth)`.
    Auth source: signed session cookie only. The X-Auth-Request-Email header is
    NOT trusted — there is no longer a forward-auth gateway in front of this app,
    so any internet client could otherwise spoof it and bypass OAuth.
    """
    user = request.session.get("user") if hasattr(request, "session") else None
    if user and ALLOWED_EMAIL.match(user.get("email", "")):
        return user

    if _is_browser_navigation(request):
        # 302 to /auth/google with ?next=
        next_ = _safe_relative(str(request.url.path))
        if request.url.query and next_ != "/":
            next_ += "?" + request.url.query
        from urllib.parse import quote
        raise HTTPException(
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
            headers={"Location": f"/auth/google?next={quote(next_, safe='/')}"},
        )

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={"ok": False, "error": "Not authenticated"},
    )
