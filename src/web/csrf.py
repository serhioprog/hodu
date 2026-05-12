"""
Double-submit-cookie CSRF.

On every GET: if the CSRF cookie isn't set, we set it to a fresh
token. Jinja can read it via request.state.csrf_token and render as a
hidden <input> in every form.

On every non-safe method (POST/PUT/PATCH/DELETE): we require that
*either*:
  • header X-CSRF-Token matches the cookie  (preferred for fetch/AJAX)
  • form field `csrf_token` matches the cookie  (fallback for classic
    <form method=post> submissions where we cannot add a header)

Mismatch → 403.

Why prefer the header path:
  Reading `await request.form()` in middleware consumes the request
  body. With the BaseHTTPMiddleware/Request model in our stack, that
  body cannot be replayed for the route handler — handlers that use
  `Form(...)` parameters then hang forever waiting for body bytes that
  will never arrive. Headers are zero-cost, the body is left untouched.

  The form fallback exists only because some endpoints still use
  classic <form method=post> where we cannot inject an XHR header.
  For those routes, the same body-consumption issue applies — but we
  don't have a practical alternative, so we accept the trade-off and
  that path stays the original behaviour.

Why double-submit vs synchronizer-token:
  - No server-side session store required.
  - Attacker on a different origin cannot read our cookie (same-origin
    policy), so they cannot forge the form field/header even knowing
    the URL.
"""
import hmac
import secrets
from typing import Awaitable, Callable

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from src.core.config import settings


_CSRF_SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
_CSRF_FIELD_NAME   = "csrf_token"
_CSRF_HEADER_NAME  = "x-csrf-token"  # case-insensitive in HTTP


def _new_token() -> str:
    return secrets.token_urlsafe(32)


def _tokens_match(a, b) -> bool:
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
            # /auth/{token} is an exception — magic-link endpoint reached
            # from email; the token in the URL itself is the secret.
            path = request.url.path
            is_auth_callback = path.startswith("/auth/")

            if not is_auth_callback:
                # 1) Header path — preferred. Doesn't touch body, so
                #    handlers with Form(...) work normally afterwards.
                header_token = request.headers.get(_CSRF_HEADER_NAME)

                if _tokens_match(cookie_token, header_token):
                    pass  # OK, fall through to handler.
                else:
                    # 2) Form path — fallback for classic form submissions
                    #    that cannot send custom headers. Note: this DOES
                    #    consume the body, which means the route handler
                    #    cannot read Form(...) parameters reliably. Use
                    #    only for endpoints whose handler does NOT depend
                    #    on Form(...) — e.g. admin/scrapers/run.
                    form_token = None
                    ctype = request.headers.get("content-type", "")
                    if ctype.startswith("application/x-www-form-urlencoded") \
                       or ctype.startswith("multipart/form-data"):
                        try:
                            form = await request.form()
                            form_token = form.get(_CSRF_FIELD_NAME)
                        except Exception:
                            form_token = None

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