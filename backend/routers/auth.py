import logging
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from app.core.security import (
    create_access_token,
    decode_token,
    hash_password,
    verify_password,
)
from app.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])


# ── Schemas ────────────────────────────────────────────────────────────────────

class LoginBody(BaseModel):
    email: str
    password: str


class RegisterBody(BaseModel):
    email: str
    password: str
    first_name: str
    last_name: str
    gsu: Optional[str] = None
    role: str = "delegue_medical"


# ── Auth Helpers ───────────────────────────────────────────────────────────────

def get_current_user(authorization: Optional[str] = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")

    token = authorization.split(" ", 1)[1]

    payload = decode_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    # ✅ IMPORTANT: convert back to int
    try:
        user_id = int(payload["sub"])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    db = get_db()
    row = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    db.close()

    if not row:
        raise HTTPException(status_code=401, detail="User not found")

    if not row["is_active"]:
        raise HTTPException(status_code=403, detail="Account disabled")

    return dict(row)


def require_admin(current_user: dict = Depends(get_current_user)) -> dict:
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


# ── Utils ──────────────────────────────────────────────────────────────────────

def _user_payload(row: dict) -> dict:
    """Remove sensitive fields"""
    return {k: v for k, v in row.items() if k != "hashed_password"}


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.post("/login")
def login(body: LoginBody):
    db = get_db()

    row = db.execute(
        "SELECT * FROM users WHERE email = ?", (body.email,)
    ).fetchone()

    db.close()

    if not row or not verify_password(body.password, row["hashed_password"]):
        raise HTTPException(
            status_code=400,
            detail="Email ou mot de passe incorrect",
        )

    if not row["is_active"]:
        raise HTTPException(
            status_code=403,
            detail="Compte désactivé. Contactez l'administrateur.",
        )

    # ✅ FIX: sub MUST be string
    token = create_access_token({"sub": str(row["id"])})

    return {
        "token": token,
        "user": _user_payload(dict(row)),
    }


@router.post("/register")
def register(body: RegisterBody):
    db = get_db()

    # ── Whitelist check ─────────────────────────────
    allowed = db.execute(
        "SELECT * FROM allowed_emails WHERE email = ?",
        (body.email,),
    ).fetchone()

    if not allowed:
        db.close()
        raise HTTPException(
            status_code=403,
            detail="Email non autorisé. Contactez votre administrateur.",
        )

    # ── Duplicate check ─────────────────────────────
    exists = db.execute(
        "SELECT id FROM users WHERE email = ?",
        (body.email,),
    ).fetchone()

    if exists:
        db.close()
        raise HTTPException(
            status_code=400,
            detail="Cet email est déjà enregistré.",
        )

    # ── Insert user ─────────────────────────────────
    db.execute(
        """
        INSERT INTO users (email, first_name, last_name, hashed_password, role, gsu)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            body.email,
            body.first_name,
            body.last_name,
            hash_password(body.password),
            body.role,
            body.gsu,
        ),
    )

    db.commit()

    row = db.execute(
        "SELECT * FROM users WHERE email = ?",
        (body.email,),
    ).fetchone()

    db.close()

    # ✅ FIX: sub MUST be string
    token = create_access_token({"sub": str(row["id"])})

    return {
        "token": token,
        "user": _user_payload(dict(row)),
    }


@router.get("/me")
def me(current_user: dict = Depends(get_current_user)):
    return _user_payload(current_user)