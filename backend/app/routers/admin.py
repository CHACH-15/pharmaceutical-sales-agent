import io, json, logging, re
from collections import Counter
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.core.security import hash_password
from app.database import get_db
from app.routers.auth import get_current_user, require_admin
from app.core.prompt_builder import invalidate_catalog_cache

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])

_STOP = {
    "le","la","les","de","du","des","et","en","un","une","dans","pour","que","qui",
    "par","sur","avec","est","sont","this","that","the","and","for","with","from",
    "are","was","has","have","not","but","what","how","which","quel","quelle","taux",
    "réalisation","ventes","objectif","mars","juin","janvier","fevrier","avril","mai",
    "juillet","aout","septembre","octobre","novembre","decembre","2024","2025","2023",
    "hikma","tunisie",
}


class UserCreateBody(BaseModel):
    email:      str
    password:   str
    first_name: str
    last_name:  str
    gsu:        Optional[str] = None
    role:       str = "delegue_medical"


class UserUpdateBody(BaseModel):
    email:      Optional[str]  = None
    password:   Optional[str]  = None
    first_name: Optional[str]  = None
    last_name:  Optional[str]  = None
    gsu:        Optional[str]  = None
    role:       Optional[str]  = None
    is_active:  Optional[bool] = None


class AllowedEmailBody(BaseModel):
    email: str
    role:  str           = "delegue_medical"
    gsu:   Optional[str] = None


class PermissionBody(BaseModel):
    gamme:      Optional[str] = None
    sous_gamme: Optional[str] = None


# ── Users ──────────────────────────────────────────────────────────────────────

@router.get("/users")
def list_users(admin: dict = Depends(require_admin)):
    db   = get_db()
    rows = db.execute(
        "SELECT id,email,first_name,last_name,role,gsu,is_active,query_count,created_at "
        "FROM users ORDER BY created_at ASC"
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


@router.post("/users")
def create_user(body: UserCreateBody, admin: dict = Depends(require_admin)):
    db = get_db()
    if db.execute("SELECT id FROM users WHERE email=?", (body.email,)).fetchone():
        db.close()
        raise HTTPException(400, "Email already registered.")
    db.execute(
        "INSERT INTO users (email,first_name,last_name,hashed_password,role,gsu) "
        "VALUES (?,?,?,?,?,?)",
        (body.email, body.first_name, body.last_name,
         hash_password(body.password), body.role, body.gsu),
    )
    db.commit()
    row = db.execute(
        "SELECT id,email,first_name,last_name,role,gsu,is_active,query_count "
        "FROM users WHERE email=?", (body.email,)
    ).fetchone()
    db.close()
    return dict(row)


@router.patch("/users/{user_id}")
def update_user(user_id: int, body: UserUpdateBody, admin: dict = Depends(require_admin)):
    db = get_db()
    if not db.execute("SELECT id FROM users WHERE id=?", (user_id,)).fetchone():
        db.close()
        raise HTTPException(404, "User not found")
    fields, values = [], []
    if body.email      is not None: fields.append("email=?");           values.append(body.email)
    if body.first_name is not None: fields.append("first_name=?");      values.append(body.first_name)
    if body.last_name  is not None: fields.append("last_name=?");       values.append(body.last_name)
    if body.gsu        is not None: fields.append("gsu=?");             values.append(body.gsu)
    if body.role       is not None: fields.append("role=?");            values.append(body.role)
    if body.is_active  is not None: fields.append("is_active=?");       values.append(int(body.is_active))
    if body.password:               fields.append("hashed_password=?"); values.append(hash_password(body.password))
    if fields:
        values.append(user_id)
        db.execute(f"UPDATE users SET {', '.join(fields)} WHERE id=?", values)
        db.commit()
    row = db.execute(
        "SELECT id,email,first_name,last_name,role,gsu,is_active,query_count "
        "FROM users WHERE id=?", (user_id,)
    ).fetchone()
    db.close()
    return dict(row)


@router.delete("/users/{user_id}")
def delete_user(user_id: int, admin: dict = Depends(require_admin)):
    db  = get_db()
    row = db.execute("SELECT role FROM users WHERE id=?", (user_id,)).fetchone()
    if not row:
        db.close()
        raise HTTPException(404, "User not found")
    if row["role"] == "admin":
        cnt = db.execute("SELECT COUNT(*) FROM users WHERE role='admin'").fetchone()[0]
        if cnt <= 1:
            db.close()
            raise HTTPException(403, "Cannot delete the last admin.")
    db.execute("DELETE FROM users WHERE id=?", (user_id,))
    db.commit()
    db.close()
    return {"deleted": True}


# ── User permissions ───────────────────────────────────────────────────────────

@router.get("/users/{user_id}/permissions")
def get_perms(user_id: int, admin: dict = Depends(require_admin)):
    db   = get_db()
    rows = db.execute(
        "SELECT id,gamme,sous_gamme,created_at FROM user_gamme_permissions "
        "WHERE user_id=? ORDER BY id", (user_id,)
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


@router.post("/users/{user_id}/permissions")
def add_perm(user_id: int, body: PermissionBody, admin: dict = Depends(require_admin)):
    db = get_db()
    if not db.execute("SELECT id FROM users WHERE id=?", (user_id,)).fetchone():
        db.close()
        raise HTTPException(404, "User not found")
    db.execute(
        "INSERT INTO user_gamme_permissions (user_id,gamme,sous_gamme) VALUES (?,?,?)",
        (user_id, body.gamme, body.sous_gamme),
    )
    db.commit()
    row = db.execute(
        "SELECT id,gamme,sous_gamme,created_at FROM user_gamme_permissions "
        "WHERE user_id=? ORDER BY id DESC LIMIT 1", (user_id,)
    ).fetchone()
    db.close()
    return dict(row)


@router.delete("/users/{user_id}/permissions/{perm_id}")
def del_perm(user_id: int, perm_id: int, admin: dict = Depends(require_admin)):
    db = get_db()
    db.execute(
        "DELETE FROM user_gamme_permissions WHERE id=? AND user_id=?", (perm_id, user_id)
    )
    db.commit()
    db.close()
    return {"deleted": True}


@router.delete("/users/{user_id}/permissions")
def clear_perms(user_id: int, admin: dict = Depends(require_admin)):
    db = get_db()
    db.execute("DELETE FROM user_gamme_permissions WHERE user_id=?", (user_id,))
    db.commit()
    db.close()
    return {"deleted": True}


@router.get("/available-gammes")
def available_gammes(admin: dict = Depends(require_admin)):
    db      = get_db()
    gammes  = [r[0] for r in db.execute(
        "SELECT DISTINCT gamme FROM ims_data WHERE gamme IS NOT NULL ORDER BY gamme"
    ).fetchall()]
    sgammes = [r[0] for r in db.execute(
        "SELECT DISTINCT sous_gamme FROM ims_data WHERE sous_gamme IS NOT NULL ORDER BY sous_gamme"
    ).fetchall()]
    if not gammes:
        gammes  = [r[0] for r in db.execute(
            "SELECT DISTINCT gamme FROM kpi_cache WHERE gamme IS NOT NULL AND gamme!='' ORDER BY gamme"
        ).fetchall()]
        sgammes = [r[0] for r in db.execute(
            "SELECT DISTINCT sous_gamme FROM kpi_cache WHERE sous_gamme IS NOT NULL AND sous_gamme!='' ORDER BY sous_gamme"
        ).fetchall()]
    db.close()
    return {"gammes": gammes, "sous_gammes": sgammes}


# ── Whitelist ──────────────────────────────────────────────────────────────────

@router.get("/allowed-emails")
def list_allowed(admin: dict = Depends(require_admin)):
    db   = get_db()
    rows = db.execute("SELECT * FROM allowed_emails ORDER BY created_at DESC").fetchall()
    db.close()
    return [dict(r) for r in rows]


@router.post("/allowed-emails")
def add_allowed(body: AllowedEmailBody, admin: dict = Depends(require_admin)):
    db = get_db()
    if db.execute("SELECT id FROM allowed_emails WHERE email=?", (body.email,)).fetchone():
        db.close()
        raise HTTPException(400, "Email already in whitelist.")
    db.execute(
        "INSERT INTO allowed_emails (email,role,gsu) VALUES (?,?,?)",
        (body.email, body.role, body.gsu),
    )
    db.commit()
    row = db.execute("SELECT * FROM allowed_emails WHERE email=?", (body.email,)).fetchone()
    db.close()
    return dict(row)


@router.delete("/allowed-emails/{entry_id}")
def remove_allowed(entry_id: int, admin: dict = Depends(require_admin)):
    db = get_db()
    db.execute("DELETE FROM allowed_emails WHERE id=?", (entry_id,))
    db.commit()
    db.close()
    return {"deleted": True}


# ── ETL ────────────────────────────────────────────────────────────────────────

@router.post("/etl")
async def run_etl_endpoint(
    ims_file:     UploadFile = File(...),
    marche_file:  UploadFile = File(...),
    sectori_file: UploadFile = File(...),
    target_file:  UploadFile = File(...),
    admin: dict = Depends(require_admin),
):
    from app.core.etl import run_ims_etl, run_target_etl

    ims_c     = await ims_file.read()
    marche_c  = await marche_file.read()
    sectori_c = await sectori_file.read()
    target_c  = await target_file.read()

    db = get_db()

    ims_result = run_ims_etl(
        ims_c,     ims_file.filename     or "ims.xlsx",
        marche_c,  marche_file.filename  or "marche.xlsx",
        sectori_c, sectori_file.filename or "sectori.xlsx",
        admin["id"], db,
    )
    if not ims_result.get("success"):
        db.close()
        return {"success": False, "error": ims_result.get("error")}

    target_result = run_target_etl(
        target_c,  target_file.filename  or "target.xlsx",
        sectori_c, sectori_file.filename or "sectori.xlsx",
        marche_c,  marche_file.filename  or "marche.xlsx",
        admin["id"], db,
    )
    db.close()
    invalidate_catalog_cache()

    if not target_result.get("success"):
        return {"success": False, "error": target_result.get("error")}

    return {
        "success":     True,
        "batch":       target_result["batch"],
        "ims_rows":    ims_result["ims_rows"],
        "target_rows": target_result["target_rows"],
        "kpi_rows":    target_result["kpi_rows"],
    }


# ── Map data ───────────────────────────────────────────────────────────────────

@router.get("/map-data")
def map_data(
    mr: Optional[str] = Query(None),
    sv: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user),
):
    from app.core.etl import get_map_data
    db   = get_db()
    role = current_user.get("role")

    # For delegue_medical: scope to their GSU brick code (stored in users.gsu)
    # users.gsu is a territory brick code e.g. "Sfax 1A2" — matches kpi_cache.gsu
    # NOT their MR name, so we filter by gsu column, not mr column.
    # For superviseur: scope by their full name (stored in users.gsu as zone/name).
    # Admin: pass through any explicit mr/sv query params, or show all.
    gsu_filter = None
    if role == "delegue_medical":
        gsu_filter = current_user.get("gsu")
        mr = None
        sv = None
    elif role == "superviseur":
        sv = current_user.get("gsu")
        mr = None

    result = get_map_data(db, mr_name=mr, sv_name=sv, gsu_filter=gsu_filter)
    db.close()
    return result


# ── Uploaded files ─────────────────────────────────────────────────────────────

@router.get("/uploaded-files")
def list_uploaded(admin: dict = Depends(require_admin)):
    db   = get_db()
    rows = db.execute("""
        SELECT uf.id, uf.filename, uf.file_type, uf.file_size, uf.row_count,
               uf.columns, uf.date_range_min, uf.date_range_max,
               uf.uploaded_at, uf.upload_batch,
               u.first_name || ' ' || u.last_name AS uploaded_by_name
        FROM   uploaded_files uf
        LEFT JOIN users u ON u.id = uf.uploaded_by
        ORDER  BY uf.upload_batch DESC, uf.id ASC
    """).fetchall()
    db.close()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["columns"] = json.loads(d["columns"]) if d["columns"] else []
        except Exception:
            d["columns"] = []
        out.append(d)
    return out


@router.delete("/uploaded-files/batch/{batch_id}")
def del_batch(batch_id: int, admin: dict = Depends(require_admin)):
    db = get_db()
    db.execute("DELETE FROM uploaded_files WHERE upload_batch=?", (batch_id,))
    db.commit()
    db.close()
    return {"deleted": True, "batch": batch_id}


# ── KPI preview ────────────────────────────────────────────────────────────────

@router.get("/kpi-preview")
def kpi_preview(
    admin:       dict          = Depends(require_admin),
    date:        Optional[str] = Query(None),
    product:     Optional[str] = Query(None),
    gsu:         Optional[str] = Query(None),
    gamme:       Optional[str] = Query(None),
    sous_gamme:  Optional[str] = Query(None),
    gouvernorat: Optional[str] = Query(None),
    mr:          Optional[str] = Query(None),
    sv:          Optional[str] = Query(None),
    group_by:    str           = Query("date"),
):
    db = get_db()
    conds, params = [], []
    if date:        conds.append("date=?");        params.append(date)
    if product:     conds.append("product=?");     params.append(product)
    if gsu:         conds.append("gsu=?");         params.append(gsu)
    if gamme:       conds.append("gamme=?");       params.append(gamme)
    if sous_gamme:  conds.append("sous_gamme=?");  params.append(sous_gamme)
    if gouvernorat: conds.append("gouvernorat=?"); params.append(gouvernorat)
    if mr:          conds.append("mr=?");          params.append(mr)
    if sv:          conds.append("sv=?");          params.append(sv)

    where = ("WHERE " + " AND ".join(conds)) if conds else ""

    valid = {
        "date", "product", "forme", "gsu", "gamme", "sous_gamme",
        "gouvernorat", "region", "mr", "sv",
    }
    g = group_by if group_by in valid else "date"

    q = f"""
        SELECT {g} AS dimension,
            ROUND(SUM(actual_value),2)    AS actual_value,
            ROUND(SUM(target_value),2)    AS target_value,
            ROUND(SUM(actual_quantity),0) AS actual_quantity,
            ROUND(SUM(target_quantity),0) AS target_quantity,
            ROUND(SUM(actual_value)*100.0/NULLIF(SUM(target_value),0),1)
                AS taux_realisation_value_pct,
            ROUND(SUM(actual_quantity)*100.0/NULLIF(SUM(target_quantity),0),1)
                AS taux_realisation_unit_pct,
            COUNT(*) AS nb_rows
        FROM kpi_cache {where}
        GROUP BY {g} ORDER BY {g} ASC LIMIT 500
    """
    rows = db.execute(q, params).fetchall()

    opts = {}
    for col in ("date", "product", "gsu", "gamme", "sous_gamme", "gouvernorat", "mr", "sv"):
        opts[col] = [
            r[0] for r in db.execute(
                f"SELECT DISTINCT {col} FROM kpi_cache "
                f"WHERE {col} IS NOT NULL AND {col}!='' ORDER BY {col}"
            ).fetchall()
        ]
    db.close()
    return {"rows": [dict(r) for r in rows], "group_by": g, "filters": opts}


# ── Stats ──────────────────────────────────────────────────────────────────────

@router.get("/stats")
def get_stats(admin: dict = Depends(require_admin)):
    db = get_db()
    total_users         = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    active_users        = db.execute("SELECT COUNT(*) FROM users WHERE is_active=1").fetchone()[0]
    active_delegues     = db.execute("SELECT COUNT(*) FROM users WHERE is_active=1 AND role='delegue_medical'").fetchone()[0]
    active_superviseurs = db.execute("SELECT COUNT(*) FROM users WHERE is_active=1 AND role='superviseur'").fetchone()[0]
    total_admins        = db.execute("SELECT COUNT(*) FROM users WHERE role='admin'").fetchone()[0]
    total_queries       = db.execute("SELECT COUNT(*) FROM messages WHERE role='user'").fetchone()[0]
    avg = db.execute(
        "SELECT AVG(response_time) FROM messages WHERE role='assistant' AND response_time IS NOT NULL"
    ).fetchone()
    avg_response_s = round(avg[0], 2) if avg[0] else None

    top_users   = [dict(r) for r in db.execute(
        "SELECT first_name||' '||last_name AS name, role, query_count AS count "
        "FROM users WHERE is_active=1 AND query_count>0 ORDER BY query_count DESC LIMIT 5"
    ).fetchall()]
    least_users = [dict(r) for r in db.execute(
        "SELECT first_name||' '||last_name AS name, role, query_count AS count "
        "FROM users WHERE is_active=1 ORDER BY query_count ASC LIMIT 5"
    ).fetchall()]

    all_msgs = db.execute("SELECT content FROM messages WHERE role='user'").fetchall()
    words = []
    for m in all_msgs:
        tokens = re.findall(r'\b[A-Za-zÀ-ÿ]{4,}\b', m[0])
        words.extend(t.lower() for t in tokens if t.lower() not in _STOP)
    top_products = [{"product": w, "count": c} for w, c in Counter(words).most_common(10)]

    uot = [dict(r) for r in db.execute(
        "SELECT DATE(created_at) AS date, COUNT(*) AS total "
        "FROM users GROUP BY DATE(created_at) ORDER BY date ASC"
    ).fetchall()]

    db.close()
    return {
        "total_users":        total_users,
        "active_users":       active_users,
        "avg_response_s":     avg_response_s,
        "total_queries":      total_queries,
        "active_delegues":    active_delegues,
        "active_superviseurs":active_superviseurs,
        "total_admins":       total_admins,
        "top_users":          top_users,
        "least_users":        least_users,
        "top_products":       top_products,
        "users_over_time":    uot,
    }


@router.get("/export-excel")
def export_excel(admin: dict = Depends(require_admin)):
    db    = get_db()
    users = [dict(r) for r in db.execute(
        "SELECT id,email,first_name,last_name,role,gsu,is_active,query_count,created_at FROM users"
    ).fetchall()]
    msgs  = [dict(r) for r in db.execute("""
        SELECT u.first_name||' '||u.last_name AS user,
               m.role, m.content, m.response_time, m.created_at
        FROM   messages m
        JOIN   chat_sessions s ON s.id = m.session_id
        JOIN   users u ON u.id = s.user_id
        ORDER  BY m.created_at DESC LIMIT 5000
    """).fetchall()]
    db.close()

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        pd.DataFrame(users).to_excel(w, sheet_name="Utilisateurs", index=False)
        pd.DataFrame(msgs).to_excel(w,  sheet_name="Messages",     index=False)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=hikma_stats.xlsx"},
    )


@router.get("/data-status")
def data_status(admin: dict = Depends(require_admin)):
    db     = get_db()
    status = {}
    for dtype, table, dcol in [("ims", "ims_data", "date"), ("target", "target_data", "date")]:
        cnt = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if cnt > 0:
            dr   = db.execute(f"SELECT MIN({dcol}), MAX({dcol}) FROM {table}").fetchone()
            cols = [r[1] for r in db.execute(f"PRAGMA table_info({table})").fetchall() if r[1] != "id"]
            status[dtype] = {
                "loaded": True, "rows": cnt,
                "date_range": {"min": dr[0], "max": dr[1]}, "columns": cols,
            }
        else:
            status[dtype] = {"loaded": False, "rows": 0, "date_range": None, "columns": []}
    db.close()
    return status


@router.delete("/data/{doc_type}")
def delete_data(doc_type: str, admin: dict = Depends(require_admin)):
    if doc_type not in ("ims", "target"):
        raise HTTPException(400, "doc_type must be 'ims' or 'target'")
    table = "ims_data" if doc_type == "ims" else "target_data"
    db    = get_db()
    db.execute(f"DELETE FROM {table}")
    db.commit()
    db.close()
    invalidate_catalog_cache()
    return {"deleted": True, "table": table}