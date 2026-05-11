import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.database import get_db
from app.routers.auth import get_current_user

router = APIRouter(prefix="/chat", tags=["chat"])


class CreateSessionBody(BaseModel):
    title: Optional[str] = "Nouvelle conversation"


# ── Sessions ───────────────────────────────────────────────────────────────────

@router.get("/sessions")
def list_sessions(current_user: dict = Depends(get_current_user)):
    db = get_db()
    rows = db.execute(
        """
        SELECT id, title, created_at, updated_at
        FROM   chat_sessions
        WHERE  user_id = ?
        ORDER  BY updated_at DESC
        """,
        (current_user["id"],),
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


@router.post("/sessions")
def create_session(
    body: CreateSessionBody = CreateSessionBody(),
    current_user: dict = Depends(get_current_user),
):
    session_id = str(uuid.uuid4())
    db = get_db()
    db.execute(
        "INSERT INTO chat_sessions (id, user_id, title) VALUES (?, ?, ?)",
        (session_id, current_user["id"], body.title or "Nouvelle conversation"),
    )
    db.commit()
    db.close()
    return {"id": session_id}


@router.delete("/sessions/{session_id}")
def delete_session(session_id: str, current_user: dict = Depends(get_current_user)):
    db = get_db()
    row = db.execute(
        "SELECT id FROM chat_sessions WHERE id = ? AND user_id = ?",
        (session_id, current_user["id"]),
    ).fetchone()
    if not row:
        db.close()
        raise HTTPException(status_code=404, detail="Session not found")
    db.execute("DELETE FROM chat_sessions WHERE id = ?", (session_id,))
    db.commit()
    db.close()
    return {"deleted": True}


# ── Messages ───────────────────────────────────────────────────────────────────

@router.get("/sessions/{session_id}/messages")
def get_messages(session_id: str, current_user: dict = Depends(get_current_user)):
    db = get_db()
    session = db.execute(
        "SELECT id FROM chat_sessions WHERE id = ? AND user_id = ?",
        (session_id, current_user["id"]),
    ).fetchone()
    if not session:
        db.close()
        raise HTTPException(status_code=404, detail="Session not found")
    rows = db.execute(
        "SELECT id, session_id, role, content, created_at FROM messages WHERE session_id = ? ORDER BY created_at ASC",
        (session_id,),
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]