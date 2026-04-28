import io
import logging
import re
from collections import Counter
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.core.security import hash_password
from app.database import get_db
from app.routers.auth import get_current_user, require_admin
from app.core.prompt_builder import invalidate_catalog_cache

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])

# ── French / English stop words for product extraction ─────────────────────────
_STOP = {
    "le","la","les","de","du","des","et","en","un","une","dans","pour","que","qui",
    "par","sur","avec","est","sont","this","that","the","and","for","with","from",
    "are","was","has","have","not","but","what","how","which","their","more",
    "quel","quelle","quels","quelles","taux","réalisation","ventes","objectif",
    "mars","juin","janvier","fevrier","avril","mai","juillet","aout","septembre",
    "octobre","novembre","decembre","2024","2025","2023","hikma","tunisie",
}

# ── Schemas ────────────────────────────────────────────────────────────────────

class UserCreateBody(BaseModel):
    email: str
    password: str
    first_name: str
    last_name: str
    gsu: Optional[str] = None
    role: str = "delegue_medical"


class UserUpdateBody(BaseModel):
    email: Optional[str] = None
    password: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    gsu: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None


class AllowedEmailBody(BaseModel):
    email: str
    role: str = "delegue_medical"
    gsu: Optional[str] = None


# ── Users ──────────────────────────────────────────────────────────────────────

@router.get("/users")
def list_users(admin: dict = Depends(require_admin)):
    db = get_db()
    rows = db.execute(
        """
        SELECT id, email, first_name, last_name, role, gsu,
               is_active, query_count, created_at
        FROM   users
        ORDER  BY created_at ASC
        """
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


@router.post("/users")
def create_user(body: UserCreateBody, admin: dict = Depends(require_admin)):
    db = get_db()
    if db.execute("SELECT id FROM users WHERE email = ?", (body.email,)).fetchone():
        db.close()
        raise HTTPException(status_code=400, detail="Email already registered.")
    db.execute(
        """
        INSERT INTO users (email, first_name, last_name, hashed_password, role, gsu)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (body.email, body.first_name, body.last_name,
         hash_password(body.password), body.role, body.gsu),
    )
    db.commit()
    row = db.execute("SELECT id, email, first_name, last_name, role, gsu, is_active, query_count FROM users WHERE email = ?", (body.email,)).fetchone()
    db.close()
    return dict(row)


@router.patch("/users/{user_id}")
def update_user(user_id: int, body: UserUpdateBody, admin: dict = Depends(require_admin)):
    db = get_db()
    row = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    if not row:
        db.close()
        raise HTTPException(status_code=404, detail="User not found")

    fields, values = [], []
    if body.email is not None:       fields.append("email = ?");        values.append(body.email)
    if body.first_name is not None:  fields.append("first_name = ?");   values.append(body.first_name)
    if body.last_name is not None:   fields.append("last_name = ?");    values.append(body.last_name)
    if body.gsu is not None:         fields.append("gsu = ?");          values.append(body.gsu)
    if body.role is not None:        fields.append("role = ?");         values.append(body.role)
    if body.is_active is not None:   fields.append("is_active = ?");    values.append(int(body.is_active))
    if body.password:                fields.append("hashed_password = ?"); values.append(hash_password(body.password))

    if fields:
        values.append(user_id)
        db.execute(f"UPDATE users SET {', '.join(fields)} WHERE id = ?", values)
        db.commit()

    updated = db.execute(
        "SELECT id, email, first_name, last_name, role, gsu, is_active, query_count FROM users WHERE id = ?",
        (user_id,)
    ).fetchone()
    db.close()
    return dict(updated)


@router.delete("/users/{user_id}")
def delete_user(user_id: int, admin: dict = Depends(require_admin)):
    db = get_db()
    row = db.execute("SELECT role FROM users WHERE id = ?", (user_id,)).fetchone()
    if not row:
        db.close()
        raise HTTPException(status_code=404, detail="User not found")
    if row["role"] == "admin":
        db.close()
        raise HTTPException(status_code=403, detail="Cannot delete admin account")
    db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    db.commit()
    db.close()
    return {"deleted": True}


# ── Allowed emails (whitelist) ─────────────────────────────────────────────────

@router.get("/allowed-emails")
def list_allowed(admin: dict = Depends(require_admin)):
    db = get_db()
    rows = db.execute("SELECT * FROM allowed_emails ORDER BY created_at DESC").fetchall()
    db.close()
    return [dict(r) for r in rows]


@router.post("/allowed-emails")
def add_allowed(body: AllowedEmailBody, admin: dict = Depends(require_admin)):
    db = get_db()
    if db.execute("SELECT id FROM allowed_emails WHERE email = ?", (body.email,)).fetchone():
        db.close()
        raise HTTPException(status_code=400, detail="Email already in whitelist.")
    db.execute(
        "INSERT INTO allowed_emails (email, role, gsu) VALUES (?, ?, ?)",
        (body.email, body.role, body.gsu),
    )
    db.commit()
    row = db.execute("SELECT * FROM allowed_emails WHERE email = ?", (body.email,)).fetchone()
    db.close()
    return dict(row)


@router.delete("/allowed-emails/{entry_id}")
def remove_allowed(entry_id: int, admin: dict = Depends(require_admin)):
    db = get_db()
    db.execute("DELETE FROM allowed_emails WHERE id = ?", (entry_id,))
    db.commit()
    db.close()
    return {"deleted": True}


# ── Stats ──────────────────────────────────────────────────────────────────────

@router.get("/stats")
def get_stats(admin: dict = Depends(require_admin)):
    db = get_db()

    total_users      = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    active_users     = db.execute("SELECT COUNT(*) FROM users WHERE is_active = 1").fetchone()[0]
    active_delegues  = db.execute("SELECT COUNT(*) FROM users WHERE is_active = 1 AND role = 'delegue_medical'").fetchone()[0]
    active_superviseurs = db.execute("SELECT COUNT(*) FROM users WHERE is_active = 1 AND role = 'superviseur'").fetchone()[0]
    total_queries    = db.execute("SELECT COUNT(*) FROM messages WHERE role = 'user'").fetchone()[0]

    avg_rt_row = db.execute(
        "SELECT AVG(response_time) FROM messages WHERE role = 'assistant' AND response_time IS NOT NULL"
    ).fetchone()
    avg_response_s = round(avg_rt_row[0], 2) if avg_rt_row[0] else None

    # Top users
    top_rows = db.execute(
        """
        SELECT first_name || ' ' || last_name AS name, role, query_count AS count
        FROM   users
        WHERE  is_active = 1 AND query_count > 0
        ORDER  BY query_count DESC
        LIMIT  5
        """
    ).fetchall()
    top_users = [dict(r) for r in top_rows]

    # Least active
    least_rows = db.execute(
        """
        SELECT first_name || ' ' || last_name AS name, role, query_count AS count
        FROM   users
        WHERE  is_active = 1
        ORDER  BY query_count ASC
        LIMIT  5
        """
    ).fetchall()
    least_users = [dict(r) for r in least_rows]

    # Repeated questions
    rep_rows = db.execute(
        """
        SELECT content AS question, COUNT(*) AS count
        FROM   messages
        WHERE  role = 'user'
        GROUP  BY LOWER(TRIM(content))
        HAVING COUNT(*) > 1
        ORDER  BY count DESC
        LIMIT  10
        """
    ).fetchall()
    repeated_questions = [dict(r) for r in rep_rows]

    # Top questions
    top_q_rows = db.execute(
        """
        SELECT content AS question, COUNT(*) AS count
        FROM   messages
        WHERE  role = 'user'
        GROUP  BY LOWER(TRIM(content))
        ORDER  BY count DESC
        LIMIT  10
        """
    ).fetchall()
    top_questions = [dict(r) for r in top_q_rows]

    # Top mentioned products (word frequency, ignoring stop words)
    all_msgs = db.execute(
        "SELECT content FROM messages WHERE role = 'user'"
    ).fetchall()
    words: List[str] = []
    for m in all_msgs:
        tokens = re.findall(r'\b[A-Za-zÀ-ÿ]{4,}\b', m[0])
        words.extend(t.lower() for t in tokens if t.lower() not in _STOP)
    top_products = [
        {"product": w, "count": c}
        for w, c in Counter(words).most_common(10)
    ]

    # Users over time (daily new registrations)
    uot_rows = db.execute(
        """
        SELECT DATE(created_at) AS date, COUNT(*) AS total
        FROM   users
        GROUP  BY DATE(created_at)
        ORDER  BY date ASC
        """
    ).fetchall()
    users_over_time = [dict(r) for r in uot_rows]

    db.close()
    return {
        "total_users":        total_users,
        "active_users":       active_users,
        "avg_response_s":     avg_response_s,
        "total_queries":      total_queries,
        "active_delegues":    active_delegues,
        "active_superviseurs":active_superviseurs,
        "top_users":          top_users,
        "least_users":        least_users,
        "repeated_questions": repeated_questions,
        "top_products":       top_products,
        "top_questions":      top_questions,
        "users_over_time":    users_over_time,
    }


# ── Export Excel ───────────────────────────────────────────────────────────────

@router.get("/export-excel")
def export_excel(admin: dict = Depends(require_admin)):
    db = get_db()

    users_rows = db.execute(
        "SELECT id, email, first_name, last_name, role, gsu, is_active, query_count, created_at FROM users"
    ).fetchall()
    msgs_rows = db.execute(
        """
        SELECT u.first_name || ' ' || u.last_name AS user, m.role, m.content,
               m.response_time, m.created_at
        FROM   messages m
        JOIN   chat_sessions s ON s.id = m.session_id
        JOIN   users u         ON u.id = s.user_id
        ORDER  BY m.created_at DESC
        LIMIT  5000
        """
    ).fetchall()
    db.close()

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame([dict(r) for r in users_rows]).to_excel(
            writer, sheet_name="Utilisateurs", index=False
        )
        pd.DataFrame([dict(r) for r in msgs_rows]).to_excel(
            writer, sheet_name="Messages", index=False
        )

    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=hikma_stats.xlsx"},
    )


# ── Data ingestion ─────────────────────────────────────────────────────────────

# Column name normalization maps
_IMS_COLS = {
    "date": "date", "product": "product", "produit": "product",
    "forme": "forme", "gsu": "gsu",
    "gouvernorat": "gouvernorat", "governorat": "gouvernorat",
    "region": "region", "région": "region",
    "market": "market", "marché": "market", "marche": "market",
    "ourproduct": "ourproduct", "our_product": "ourproduct", "hikma": "ourproduct",
    "mr": "mr", "sv": "sv",
    "sales_value": "sales_value", "valeur": "sales_value", "chiffre_affaires": "sales_value",
    "sales_quantity": "sales_quantity", "quantite": "sales_quantity", "quantité": "sales_quantity",
    "gamme": "gamme", "sous_gamme": "sous_gamme", "sous-gamme": "sous_gamme",
}
_TARGET_COLS = {
    "date": "date", "product": "product", "produit": "product",
    "forme": "forme", "gsu": "gsu",
    "value_objectif": "value_objectif", "objectif_valeur": "value_objectif",
    "unit_objectif": "unit_objectif", "objectif_unite": "unit_objectif",
    "gamme": "gamme", "sous_gamme": "sous_gamme", "sous-gamme": "sous_gamme",
}


def _normalize(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    df.columns = [
        c.strip().lower()
         .replace(" ", "_")
         .replace("-", "_")      # ← fixes Sous-Gamme → sous_gamme
         .replace("é", "e")
         .replace("è", "e")
        for c in df.columns
    ]
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    return df


def _read_file(file: UploadFile) -> pd.DataFrame:
    content = file.file.read()
    name = (file.filename or "").lower()
    if name.endswith(".csv"):
        for sep in [";", ",", "\t"]:
            try:
                df = pd.read_csv(io.BytesIO(content), sep=sep)
                if df.shape[1] > 1:
                    return df
            except Exception:
                continue
        raise ValueError("Cannot parse CSV file")
    else:
        return pd.read_excel(io.BytesIO(content))


@router.post("/ingest/{doc_type}")
async def ingest_data(
    doc_type: str,
    files: List[UploadFile] = File(...),
    admin: dict = Depends(require_admin),
):
    if doc_type not in ("ims", "target"):
        raise HTTPException(status_code=400, detail="doc_type must be 'ims' or 'target'")

    results = []
    db = get_db()

    # Clear existing data for this type
    table = "ims_data" if doc_type == "ims" else "target_data"
    db.execute(f"DELETE FROM {table}")
    db.commit()

    for f in files:
        try:
            df = _read_file(f)
            col_map = _IMS_COLS if doc_type == "ims" else _TARGET_COLS
            df = _normalize(df, col_map)

            if doc_type == "ims":
                wanted = ["date","product","forme","gsu","gouvernorat","region",
                          "market","ourproduct","mr","sv","sales_value","sales_quantity",
                          "gamme","sous_gamme"]
                for col in wanted:
                    if col not in df.columns:
                        df[col] = None
                df = df[wanted]
                # Normalize boolean
                if "ourproduct" in df.columns:
                    df["ourproduct"] = df["ourproduct"].map(
                        lambda x: 1 if str(x).strip().lower() in ("true","1","oui","yes","hikma") else 0
                    )
                df.to_sql("ims_data", db, if_exists="append", index=False)

            else:
                wanted = ["date","product","forme","gsu","value_objectif","unit_objectif","gamme","sous_gamme"]
                for col in wanted:
                    if col not in df.columns:
                        df[col] = None
                df = df[wanted]
                df.to_sql("target_data", db, if_exists="append", index=False)

            results.append({"filename": f.filename, "success": True,
                            "message": f"{len(df):,} lignes importées"})
        except Exception as exc:
            logger.exception("Ingest error for %s", f.filename)
            results.append({"filename": f.filename, "success": False, "message": str(exc)})

    db.close()

    # Invalidate catalog cache so next query picks up fresh values
    invalidate_catalog_cache()

    return {"results": results}


# ── Data status ────────────────────────────────────────────────────────────────

@router.get("/data-status")
def data_status(admin: dict = Depends(require_admin)):
    db = get_db()
    status = {}

    for doc_type, table, date_col in [
        ("ims",    "ims_data",    "date"),
        ("target", "target_data", "date"),
    ]:
        count = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if count > 0:
            dr = db.execute(
                f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}"
            ).fetchone()
            cols_raw = db.execute(f"PRAGMA table_info({table})").fetchall()
            cols = [r[1] for r in cols_raw if r[1] != "id"]
            status[doc_type] = {
                "loaded": True,
                "rows": count,
                "date_range": {"min": dr[0], "max": dr[1]},
                "columns": cols,
            }
        else:
            status[doc_type] = {"loaded": False, "rows": 0, "date_range": None, "columns": []}

    db.close()
    return status


# ── Data delete ────────────────────────────────────────────────────────────────

@router.delete("/data/{doc_type}")
def delete_data(doc_type: str, admin: dict = Depends(require_admin)):
    if doc_type not in ("ims", "target"):
        raise HTTPException(status_code=400, detail="doc_type must be 'ims' or 'target'")
    table = "ims_data" if doc_type == "ims" else "target_data"
    db = get_db()
    db.execute(f"DELETE FROM {table}")
    db.commit()
    db.close()

    # Invalidate catalog cache so stale values are not served
    invalidate_catalog_cache()

    return {"deleted": True, "table": table}