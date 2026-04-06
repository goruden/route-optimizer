# ============================================================
#  auth.py  –  JWT authentication for VRP system
#
#  Flow:
#    1. POST /api/auth/login  → returns {access_token, expires_in}
#    2. Client stores token in localStorage
#    3. Every request: Authorization: Bearer <token>
#    4. GET /api/auth/refresh → new token (call when < 5 min left)
#    5. Token expires after 30 min of no refresh
#
#  Install: pip install PyJWT
# ============================================================

import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

# ── Config ────────────────────────────────────────────────────
# Set JWT_SECRET in your .env / environment variables.
# A random one is generated per-process if not set (fine for dev,
# but tokens won't survive server restarts).
SECRET_KEY: str = os.getenv("JWT_SECRET", secrets.token_urlsafe(32))
ALGORITHM = "HS256"
TOKEN_EXPIRE_MINUTES: int = int(os.getenv("TOKEN_EXPIRE_MINUTES", "30"))

# ── User store ─────────────────────────────────────────────────
# Replace with a MongoDB lookup in production.
# Passwords should be bcrypt-hashed; kept plain here for clarity.
_USERS: dict[str, str] = {
    "admin": os.getenv("ADMIN_PASSWORD", "admin"),
    "test":  os.getenv("TEST_PASSWORD",  "test1234!"),
    "user":  os.getenv("USER_PASSWORD",  "user1234!"),
}

# ── Pydantic schemas ───────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int          # seconds until expiry
    username: str

class MeResponse(BaseModel):
    username: str

# ── Token helpers ──────────────────────────────────────────────

def _make_token(username: str) -> TokenResponse:
    """Create a signed JWT for the given user."""
    now    = datetime.now(timezone.utc)
    expiry = now + timedelta(minutes=TOKEN_EXPIRE_MINUTES)
    payload = {
        "sub": username,
        "iat": now,
        "exp": expiry,
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    return TokenResponse(
        access_token = token,
        expires_in   = TOKEN_EXPIRE_MINUTES * 60,
        username     = username,
    )


def _decode_token(token: str) -> dict:
    """
    Decode and validate a JWT.
    Raises HTTPException on any failure so FastAPI returns 401.
    """
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED,
            detail      = "Token expired — please log in again",
            headers     = {"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED,
            detail      = f"Invalid token: {exc}",
            headers     = {"WWW-Authenticate": "Bearer"},
        )

# ── FastAPI dependency ─────────────────────────────────────────

_bearer = HTTPBearer(auto_error=False)

def get_current_user(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
) -> str:
    """
    FastAPI dependency — add to any route that needs authentication:

        @app.get("/api/protected")
        async def protected(user: str = Depends(get_current_user)):
            return {"hello": user}
    """
    if not creds:
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED,
            detail      = "Not authenticated — provide Bearer token",
            headers     = {"WWW-Authenticate": "Bearer"},
        )
    payload = _decode_token(creds.credentials)
    return payload["sub"]

# ── Service functions (called from route handlers) ─────────────

def login_user(req: LoginRequest) -> TokenResponse:
    stored = _USERS.get(req.username)
    if not stored or stored != req.password:
        # Use a constant-time comparison in production (e.g. secrets.compare_digest)
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED,
            detail      = "Invalid username or password",
        )
    return _make_token(req.username)


def refresh_token(username: str) -> TokenResponse:
    """Issue a fresh 30-min token for an already-authenticated user."""
    return _make_token(username)