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


def _load_gamme_permissions(db, user_id: int) -> list:
    rows = db.execute(
        "SELECT gamme, sous_gamme FROM user_gamme_permissions "
        "WHERE user_id = ? ORDER BY gamme",
        (user_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_current_user(authorization: Optional[str] = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")

    token = authorization.split(" ", 1)[1]
    payload = decode_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    try:
        user_id = int(payload["sub"])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    db  = get_db()
    row = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()

    if not row:
        db.close()
        raise HTTPException(status_code=401, detail="User not found")
    if not row["is_active"]:
        db.close()
        raise HTTPException(status_code=403, detail="Account disabled")

    user = dict(row)
    user["gamme_permissions"] = (
        [] if user["role"] == "admin"
        else _load_gamme_permissions(db, user_id)
    )
    db.close()
    return user


def require_admin(current_user: dict = Depends(get_current_user)) -> dict:
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


def _user_payload(row: dict) -> dict:
    return {k: v for k, v in row.items() if k != "hashed_password"}


@router.post("/login")
def login(body: LoginBody):
    db  = get_db()
    row = db.execute("SELECT * FROM users WHERE email = ?", (body.email,)).fetchone()

    if not row or not verify_password(body.password, row["hashed_password"]):
        db.close()
        raise HTTPException(status_code=400, detail="Email ou mot de passe incorrect")

    if not row["is_active"]:
        db.close()
        raise HTTPException(
            status_code=403,
            detail="Compte désactivé. Contactez l'administrateur.",
        )

    user = dict(row)
    user["gamme_permissions"] = (
        [] if user["role"] == "admin"
        else _load_gamme_permissions(db, user["id"])
    )
    db.close()

    token = create_access_token({"sub": str(user["id"])})
    return {"token": token, "user": _user_payload(user)}


@router.post("/register")
def register(body: RegisterBody):
    db = get_db()

    allowed = db.execute(
        "SELECT * FROM allowed_emails WHERE email = ?", (body.email,)
    ).fetchone()
    if not allowed:
        db.close()
        raise HTTPException(
            status_code=403,
            detail="Email non autorisé. Contactez votre administrateur.",
        )

    if db.execute("SELECT id FROM users WHERE email = ?", (body.email,)).fetchone():
        db.close()
        raise HTTPException(status_code=400, detail="Cet email est déjà enregistré.")

    db.execute(
        "INSERT INTO users (email,first_name,last_name,hashed_password,role,gsu) "
        "VALUES (?,?,?,?,?,?)",
        (body.email, body.first_name, body.last_name,
         hash_password(body.password), body.role, body.gsu),
    )
    db.commit()

    row  = db.execute("SELECT * FROM users WHERE email = ?", (body.email,)).fetchone()
    user = dict(row)
    user["gamme_permissions"] = _load_gamme_permissions(db, user["id"])
    db.close()

    token = create_access_token({"sub": str(user["id"])})
    return {"token": token, "user": _user_payload(user)}


@router.get("/me")
def me(current_user: dict = Depends(get_current_user)):
    return _user_payload(current_user)