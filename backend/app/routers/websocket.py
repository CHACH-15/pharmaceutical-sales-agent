"""
websocket.py
~~~~~~~~~~~~
WebSocket endpoint — single connection per chat session.

Responsibilities
─────────────────
  • Authenticate the user from the JWT query parameter.
  • Create an ephemeral in-memory LLM session (separate from the DB session).
  • Forward user messages to stream_response() with full user context.
  • Persist both turns (user + assistant) to the DB chat_sessions / messages tables.
  • Auto-title the session from the first user message.
  • Clean up the in-memory LLM session on disconnect.

WebSocket message protocol
───────────────────────────
  Client → Server : JSON  { "message": "<user text>" }
  Server → Client :
    { "type": "stream_start" }
    { "type": "token",      "content": "<chunk>" }  (repeated)
    { "type": "stream_end" }
    { "type": "error",      "content": "<message>" }  (on failure)
"""

import json
import logging
import re
import time
from typing import Optional

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from app.core.security import decode_token
from app.database import get_db
from app.services.groq_service import stream_response
from app.services.session_manager import session_manager

logger = logging.getLogger(__name__)
router = APIRouter()

_SESSION_TITLE_MAX_LEN = 60


def _make_title(text: str) -> str:
    """Truncate the first user message to use as a session title."""
    text = text.strip()
    return (text[:_SESSION_TITLE_MAX_LEN] + "…") if len(text) > _SESSION_TITLE_MAX_LEN else text


async def _resolve_user(token: Optional[str]) -> tuple[Optional[int], Optional[dict]]:
    """
    Decode the JWT and fetch the full user row from the DB.
    Returns (user_id, user_dict) or (None, None) on failure.
    """
    if not token:
        return None, None

    payload = decode_token(token)
    if not payload:
        return None, None

    user_id = payload.get("sub")
    if not user_id:
        return None, None

    try:
        user_id = int(user_id)
        db  = get_db()
        row = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        db.close()
        if row:
            return user_id, dict(row)
    except Exception as exc:
        logger.warning("Could not resolve user from token: %s", exc)

    return None, None


async def _persist_turn(
    session_id: str,
    user_id: int,
    user_message: str,
    assistant_response: str,
    response_time: float,
) -> None:
    """Write both conversation turns to the DB and update session metadata."""
    try:
        db = get_db()

        db.execute(
            "UPDATE users SET query_count = query_count + 1 WHERE id = ?",
            (user_id,),
        )
        db.execute(
            "INSERT INTO messages (session_id, role, content) VALUES (?, 'user', ?)",
            (session_id, user_message),
        )
        db.execute(
            "INSERT INTO messages (session_id, role, content, response_time) VALUES (?, 'assistant', ?, ?)",
            (session_id, assistant_response, round(response_time, 2)),
        )

        # Auto-title: use first user message as session title
        msg_count = db.execute(
            "SELECT COUNT(*) FROM messages WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0]

        if msg_count <= 2:
            db.execute(
                "UPDATE chat_sessions SET title = ?, updated_at = datetime('now') WHERE id = ?",
                (_make_title(user_message), session_id),
            )
        else:
            db.execute(
                "UPDATE chat_sessions SET updated_at = datetime('now') WHERE id = ?",
                (session_id,),
            )

        db.commit()
        db.close()

    except Exception as exc:
        logger.warning("DB persist failed for session %s: %s", session_id, exc)


@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token:      Optional[str] = Query(None),
    session_id: Optional[str] = Query(None),
) -> None:
    await websocket.accept()

    user_id, user_context = await _resolve_user(token)

    # Ephemeral in-memory LLM session (separate from DB session)
    llm_session_id = session_manager.create_session()

    logger.info(
        "WS connected | llm_session=%s | db_session=%s | user_id=%s",
        llm_session_id, session_id, user_id,
    )

    try:
        while True:
            raw = await websocket.receive_text()

            # Parse incoming message
            try:
                user_message: str = json.loads(raw).get("message", "").strip()
            except (json.JSONDecodeError, AttributeError):
                user_message = raw.strip()

            if not user_message:
                await websocket.send_json({"type": "error", "content": "Empty message."})
                continue

            await websocket.send_json({"type": "stream_start"})

            start_time = time.perf_counter()
            chunks: list[str] = []
            _SQL_RE = re.compile(
                r"__SQL_START__(.*?)__SQL_END__\n?", re.DOTALL
            )

            async for token_chunk in stream_response(
                session_id=llm_session_id,
                user_message=user_message,
                user_context=user_context,
            ):
                # Intercept the SQL sentinel before it reaches the UI.
                # groq_service emits __SQL_START__[...]__SQL_END__ as one chunk
                # before the answer text. Extract it, send as dedicated message,
                # and suppress from the token stream so it never renders as text.
                m = _SQL_RE.search(token_chunk)
                if m:
                    try:
                        sql_list = json.loads(m.group(1))
                        await websocket.send_json({
                            "type": "sql_queries",
                            "queries": sql_list if isinstance(sql_list, list) else [sql_list],
                        })
                    except Exception:
                        pass
                    # Remove sentinel from token before forwarding
                    token_chunk = _SQL_RE.sub("", token_chunk)
                    if not token_chunk:
                        chunks.append("")  # keep chunk count consistent
                        continue

                await websocket.send_json({"type": "token", "content": token_chunk})
                chunks.append(token_chunk)

            response_time    = time.perf_counter() - start_time
            # Strip any residual sentinel from persisted text
            full_response    = _SQL_RE.sub("", "".join(chunks))

            # Persist to DB if we have a linked session and authenticated user
            if session_id and user_id:
                await _persist_turn(
                    session_id=session_id,
                    user_id=user_id,
                    user_message=user_message,
                    assistant_response=full_response,
                    response_time=response_time,
                )

            await websocket.send_json({"type": "stream_end"})

    except WebSocketDisconnect:
        logger.info("WS disconnected | llm_session=%s", llm_session_id)

    except Exception as exc:
        logger.exception("WS unexpected error | llm_session=%s: %s", llm_session_id, exc)
        try:
            await websocket.send_json({"type": "error", "content": "Internal server error."})
        except Exception:
            pass

    finally:
        session_manager.delete_session(llm_session_id)