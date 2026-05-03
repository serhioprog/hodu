"""
Double-submit-cookie CSRF.

On every GET: if the CSRF cookie isn't set, we set it to a fresh
token. Jinja can read it via request.state.csrf_token and render as a
hidden <input> in every form.

On every non-safe method (POST/PUT/PATCH/DELETE): we require that the
form field `csrf_token` matches the cookie. Mismatch → 403.

Why double-submit vs synchronizer-token:
  * No server-side session store required (we're stateless aside from
    the long-lived hodu_session cookie).
  * Attacker on a different origin cannot read our cookie (same-origin
    policy), so they cannot forge the form field even knowing the URL.
"""
import hmac
import secrets
from typing import Awaitable, Callable

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from src.core.config import settings


_CSRF_SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
_CSRF_FIELD_NAME = "csrf_token"


def _new_token() -> str:
    return secrets.token_urlsafe(32)


def _tokens_match(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    return hmac.compare_digest(a, b)


class CSRFMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        cookie_token = request.cookies.get(settings.CSRF_COOKIE_NAME)

        # Make it available to Jinja templates.
        request.state.csrf_token = cookie_token or _new_token()

        # --- enforcement for unsafe methods ------------------------
        if request.method not in _CSRF_SAFE_METHODS:
            # /auth/{token} is an exception — it's the magic-link endpoint
            # that's reached via email, there's no CSRF threat model here
            # because the token itself is a secret.
            path = request.url.path
            is_auth_callback = path.startswith("/auth/")

            if not is_auth_callback:
                # Pull the form token. Only parse if it's actually form data.
                form_token: str | None = None
                ctype = request.headers.get("content-type", "")
                if ctype.startswith("application/x-www-form-urlencoded") \
                   or ctype.startswith("multipart/form-data"):
                    form = await request.form()
                    form_token = form.get(_CSRF_FIELD_NAME)

                if not _tokens_match(cookie_token, form_token):
                    return JSONResponse(
                        {"detail": "CSRF token missing or invalid"},
                        status_code=403,
                    )

        response = await call_next(request)

        # --- set / refresh cookie ---------------------------------
        if cookie_token is None:
            response.set_cookie(
                key=settings.CSRF_COOKIE_NAME,
                value=request.state.csrf_token,
                httponly=False,   # MUST be readable for double-submit
                samesite="lax",
                secure=settings.COOKIE_SECURE,
                max_age=30 * 24 * 3600,
            )

        return response