"""
session_manager.py
~~~~~~~~~~~~~~~~~~
In-memory conversation history store, keyed by ephemeral session ID.

Each WebSocket connection creates one LLM session via create_session() and
destroys it via delete_session() on disconnect.  DB persistence of the full
conversation is handled separately in websocket.py.

This store intentionally lives in memory (not Redis, not DB) because:
  • History only needs to survive for the duration of one WebSocket connection.
  • DB persistence is handled by the messages table; this is the live context window.
  • Restarting the server clears all in-progress sessions (acceptable trade-off).
"""

from __future__ import annotations

import uuid
from typing import Any


class SessionManager:
    """
    Thread-safe (single-process) in-memory store for per-connection
    conversation histories.
    """

    def __init__(self) -> None:
        self._sessions: dict[str, list[dict[str, Any]]] = {}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def create_session(self) -> str:
        """Create a new empty session and return its ID."""
        session_id = str(uuid.uuid4())
        self._sessions[session_id] = []
        return session_id

    def delete_session(self, session_id: str) -> None:
        """Remove a session from memory (called on WebSocket disconnect)."""
        self._sessions.pop(session_id, None)

    # ── History access ────────────────────────────────────────────────────────

    def get_history(self, session_id: str) -> list[dict[str, Any]]:
        """Return the full history for a session (may be empty)."""
        return self._sessions.get(session_id, [])

    def get_trimmed_history(
        self,
        session_id: str,
        max_turns: int = 8,
    ) -> list[dict[str, Any]]:
        """
        Return at most `max_turns` user+assistant pairs (= max_turns * 2 messages).
        Drops the oldest messages first to keep the most recent context.
        This prevents context overflow on long sessions.
        """
        history      = self._sessions.get(session_id, [])
        max_messages = max_turns * 2
        return history[-max_messages:] if len(history) > max_messages else history

    def append(self, session_id: str, role: str, content: str) -> None:
        """Append a single message to a session's history."""
        if session_id not in self._sessions:
            self._sessions[session_id] = []
        self._sessions[session_id].append({"role": role, "content": content})

    # ── Diagnostics ───────────────────────────────────────────────────────────

    @property
    def active_count(self) -> int:
        """Number of live WebSocket sessions currently in memory."""
        return len(self._sessions)


# ── Singleton ─────────────────────────────────────────────────────────────────

session_manager = SessionManager()