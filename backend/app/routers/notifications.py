"""
notifications.py  (FastAPI router)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Provides two sub-routers:

  /notifications  — CRUD for in-app notifications
  /reports        — PDF report listing, manual generation, download

Mount both in main.py:
    from app.routers.notifications import notifications_router, reports_router
    app.include_router(notifications_router)
    app.include_router(reports_router)
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.database import get_db
from app.routers.auth import get_current_user   # reuse existing auth dependency

# ── Sub-routers ────────────────────────────────────────────────────────────────

notifications_router = APIRouter(prefix="/notifications", tags=["notifications"])
reports_router       = APIRouter(prefix="/reports",       tags=["reports"])


# ═══════════════════════════════════════════════════════════════════════════════
# NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════════════════════

@notifications_router.get("")
def list_notifications(current_user: dict = Depends(get_current_user)):
    """Return the 50 most recent notifications + unread count."""
    db  = get_db()
    rows = db.execute(
        """
        SELECT id, type, title, message, is_read, data, created_at
        FROM   notifications
        WHERE  user_id = ?
        ORDER  BY created_at DESC
        LIMIT  50
        """,
        (current_user["id"],),
    ).fetchall()
    unread = db.execute(
        "SELECT COUNT(*) FROM notifications WHERE user_id = ? AND is_read = 0",
        (current_user["id"],),
    ).fetchone()[0]
    db.close()
    return {
        "notifications": [dict(r) for r in rows],
        "unread_count":  unread,
    }


@notifications_router.put("/{notif_id}/read")
def mark_one_read(
    notif_id: int,
    current_user: dict = Depends(get_current_user),
):
    """Mark a single notification as read."""
    db = get_db()
    db.execute(
        "UPDATE notifications SET is_read = 1 WHERE id = ? AND user_id = ?",
        (notif_id, current_user["id"]),
    )
    db.commit()
    db.close()
    return {"ok": True}


@notifications_router.put("/read-all")
def mark_all_read(current_user: dict = Depends(get_current_user)):
    """Mark every notification as read for the current user."""
    db = get_db()
    db.execute(
        "UPDATE notifications SET is_read = 1 WHERE user_id = ?",
        (current_user["id"],),
    )
    db.commit()
    db.close()
    return {"ok": True}


@notifications_router.delete("/{notif_id}")
def delete_notification(
    notif_id: int,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    db.execute(
        "DELETE FROM notifications WHERE id = ? AND user_id = ?",
        (notif_id, current_user["id"]),
    )
    db.commit()
    db.close()
    return {"deleted": True}


# ─── Admin: broadcast a notification to all users ──────────────────────────────

class BroadcastBody(BaseModel):
    title:   str
    message: str
    type:    str = "info"   # "info" | "alert" | "report_ready"
    roles:   list[str] = []  # [] = all users


@notifications_router.post("/broadcast")
def broadcast(
    body: BroadcastBody,
    current_user: dict = Depends(get_current_user),
):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    db = get_db()
    if body.roles:
        placeholders = ",".join("?" * len(body.roles))
        users = db.execute(
            f"SELECT id FROM users WHERE is_active=1 AND role IN ({placeholders})",
            body.roles,
        ).fetchall()
    else:
        users = db.execute(
            "SELECT id FROM users WHERE is_active = 1"
        ).fetchall()

    for u in users:
        db.execute(
            "INSERT INTO notifications (user_id, type, title, message) VALUES (?,?,?,?)",
            (u[0], body.type, body.title, body.message),
        )
    db.commit()
    db.close()
    return {"sent": len(users)}


# ═══════════════════════════════════════════════════════════════════════════════
# REPORTS
# ═══════════════════════════════════════════════════════════════════════════════

@reports_router.get("")
def list_reports(current_user: dict = Depends(get_current_user)):
    """Return all reports belonging to the current user."""
    db   = get_db()
    rows = db.execute(
        """
        SELECT id, title, report_type, period, file_path, generated_at
        FROM   reports
        WHERE  user_id = ?
        ORDER  BY generated_at DESC
        """,
        (current_user["id"],),
    ).fetchall()
    db.close()
    result = []
    for r in rows:
        d = dict(r)
        d["available"] = bool(r["file_path"] and os.path.exists(r["file_path"]))
        del d["file_path"]   # never expose server paths to the frontend
        result.append(d)
    return result


class GenerateBody(BaseModel):
    period: Optional[str] = None    # "YYYY-MM"; None = last calendar month


@reports_router.post("/generate")
async def generate_report(
    body: GenerateBody = GenerateBody(),
    current_user: dict = Depends(get_current_user),
):
    """
    Manually trigger report generation for the current user.
    Available to delegue_medical and superviseur; admin can pass user_id as query param.
    """
    if current_user["role"] not in ("delegue_medical", "superviseur"):
        raise HTTPException(
            status_code=403,
            detail="Les rapports sont disponibles pour les Délégués et Superviseurs.",
        )

    period = body.period
    if not period:
        today  = date.today()
        first  = today.replace(day=1)
        prev   = first - timedelta(days=1)
        period = prev.strftime("%Y-%m")

    # Validate format
    try:
        yr, mo = int(period[:4]), int(period[5:7])
        assert 2000 <= yr <= 2099 and 1 <= mo <= 12
    except Exception:
        raise HTTPException(status_code=400, detail="Format de période invalide (YYYY-MM)")

    from app.services.report_generator import generate_report_for_user
    filepath = generate_report_for_user(current_user["id"], period)

    if not filepath:
        raise HTTPException(
            status_code=404,
            detail=f"Aucune donnée disponible pour la période {period}. "
                   "Vérifiez que vos données IMS et objectifs ont été importées.",
        )

    # Upsert report record + notification
    db = get_db()
    existing = db.execute(
        "SELECT id FROM reports WHERE user_id = ? AND period = ?",
        (current_user["id"], period),
    ).fetchone()

    if existing:
        db.execute(
            "UPDATE reports SET file_path = ?, generated_at = datetime('now') WHERE id = ?",
            (filepath, existing["id"]),
        )
        report_id = existing["id"]
    else:
        cur = db.execute(
            "INSERT INTO reports (user_id, title, report_type, period, file_path) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                current_user["id"],
                f"Rapport de Performance — {period}",
                f"{current_user['role']}_monthly",
                period,
                filepath,
            ),
        )
        report_id = cur.lastrowid
        db.execute(
            "INSERT INTO notifications (user_id, type, title, message, data) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                current_user["id"],
                "report_ready",
                f"📊 Rapport {period} généré",
                "Votre rapport de performance est prêt. Cliquez pour le télécharger.",
                json.dumps({"report_id": report_id}),
            ),
        )

    db.commit()
    db.close()
    return {"report_id": report_id, "period": period}


@reports_router.get("/{report_id}/download")
def download_report(
    report_id: int,
    current_user: dict = Depends(get_current_user),
):
    """Stream the PDF to the browser."""
    db  = get_db()
    row = db.execute(
        "SELECT file_path, title, period FROM reports WHERE id = ? AND user_id = ?",
        (report_id, current_user["id"]),
    ).fetchone()
    db.close()

    if not row:
        raise HTTPException(status_code=404, detail="Rapport introuvable.")
    if not row["file_path"] or not os.path.exists(row["file_path"]):
        raise HTTPException(
            status_code=404,
            detail="Le fichier rapport n'existe plus sur le serveur. "
                   "Veuillez le régénérer.",
        )

    fname = f"Hikma_Rapport_{row['period']}_{current_user['last_name']}.pdf"
    return FileResponse(
        path=row["file_path"],
        media_type="application/pdf",
        filename=fname,
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


# ─── Admin: trigger report generation for all users (bulk) ────────────────────

@reports_router.post("/admin/generate-all")
async def admin_generate_all(
    period: Optional[str] = Query(default=None),
    current_user: dict = Depends(get_current_user),
):
    """Admin only: generate reports for ALL active field users."""
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    from scheduler import trigger_now
    result = await trigger_now(period)
    return result


# ─── Profile stats endpoint ───────────────────────────────────────────────────

@notifications_router.get("/profile-stats")
def profile_stats(current_user: dict = Depends(get_current_user)):
    """
    Returns enriched profile data for the profile modal:
    query count, last activity, achievement stats, report count.
    """
    db  = get_db()
    uid = current_user["id"]

    # Base user info
    user = db.execute(
        "SELECT id, email, first_name, last_name, role, gsu, "
        "       is_active, query_count, created_at "
        "FROM users WHERE id = ?",
        (uid,),
    ).fetchone()

    # Last message timestamp
    last_msg = db.execute(
        """
        SELECT MAX(m.created_at)
        FROM   messages m
        JOIN   chat_sessions cs ON cs.id = m.session_id
        WHERE  cs.user_id = ? AND m.role = 'user'
        """,
        (uid,),
    ).fetchone()[0]

    # Session count
    session_count = db.execute(
        "SELECT COUNT(*) FROM chat_sessions WHERE user_id = ?", (uid,)
    ).fetchone()[0]

    # Report count
    report_count = db.execute(
        "SELECT COUNT(*) FROM reports WHERE user_id = ?", (uid,)
    ).fetchone()[0]

    # Unread notifications
    unread = db.execute(
        "SELECT COUNT(*) FROM notifications WHERE user_id = ? AND is_read = 0",
        (uid,),
    ).fetchone()[0]

    # KPI summary (last available month in kpi_cache for this user)
    name      = f"{user['first_name']} {user['last_name']}"
    role      = user["role"]
    kpi_field = "mr" if role == "delegue_medical" else "sv" if role == "superviseur" else None
    kpi_data  = None
    if kpi_field:
        row = db.execute(
            f"""
            SELECT
                strftime('%Y-%m', date)  AS mo,
                ROUND(SUM(actual_value)*100.0/NULLIF(SUM(target_value),0),1) AS tr,
                ROUND(SUM(actual_value),0)  AS av,
                ROUND(SUM(target_value),0)  AS tv
            FROM kpi_cache
            WHERE {kpi_field} = ?
            GROUP BY mo
            ORDER BY mo DESC
            LIMIT 1
            """,
            (name,),
        ).fetchone()
        if row:
            kpi_data = {
                "period":      row["mo"],
                "taux":        row["tr"],
                "actual":      row["av"],
                "target":      row["tv"],
            }

    db.close()
    return {
        "id":            user["id"],
        "email":         user["email"],
        "first_name":    user["first_name"],
        "last_name":     user["last_name"],
        "role":          user["role"],
        "gsu":           user["gsu"],
        "is_active":     bool(user["is_active"]),
        "query_count":   user["query_count"],
        "session_count": session_count,
        "report_count":  report_count,
        "unread_count":  unread,
        "last_activity": last_msg,
        "created_at":    user["created_at"],
        "latest_kpi":    kpi_data,
    }