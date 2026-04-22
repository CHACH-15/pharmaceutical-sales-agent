import logging
import sqlite3
from app.config import settings

logger = logging.getLogger(__name__)


# ── Connection ─────────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(settings.db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL") 
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ── Schema ─────────────────────────────────────────────────────────────────────

def init_db() -> None:
    conn = get_db()
    c = conn.cursor()

    # ── users ──────────────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            email         TEXT    UNIQUE NOT NULL,
            first_name    TEXT    NOT NULL,
            last_name     TEXT    NOT NULL,
            hashed_password TEXT  NOT NULL,
            role          TEXT    NOT NULL DEFAULT 'delegue_medical',
            gsu           TEXT,
            is_active     INTEGER NOT NULL DEFAULT 1,
            query_count   INTEGER NOT NULL DEFAULT 0,
            created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # ── allowed_emails (whitelist) ─────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS allowed_emails (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            email      TEXT    UNIQUE NOT NULL,
            role       TEXT    NOT NULL DEFAULT 'delegue_medical',
            gsu        TEXT,
            created_at TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # ── chat_sessions ──────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS chat_sessions (
            id         TEXT    PRIMARY KEY,
            user_id    INTEGER NOT NULL,
            title      TEXT    NOT NULL DEFAULT 'Nouvelle conversation',
            created_at TEXT    NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT    NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    """)

    # ── messages ───────────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id    TEXT    NOT NULL,
            role          TEXT    NOT NULL,
            content       TEXT    NOT NULL,
            response_time REAL,
            created_at    TEXT    NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (session_id) REFERENCES chat_sessions(id) ON DELETE CASCADE
        )
    """)

    # ── ims_data ───────────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS ims_data (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            date           TEXT,
            product        TEXT,
            forme          TEXT,
            gsu            TEXT,
            gouvernorat    TEXT,
            region         TEXT,
            market         TEXT,
            ourproduct     Boolean,
            mr             TEXT,
            sv             TEXT,
            sales_value    REAL,
            sales_quantity INTEGER,
            gamme          TEXT,
            sous_gamme     TEXT
        )
    """)

    # ── target_data ────────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS target_data (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            date           TEXT,
            product        TEXT,
            forme          TEXT,
            gsu            TEXT,
            value_objectif REAL,
            unit_objectif  INTEGER,
            gamme          TEXT,
            sous_gamme     TEXT
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_ims_date ON ims_data(date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ims_product ON ims_data(product)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ims_ourproduct ON ims_data(ourproduct)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ims_mr ON ims_data(mr)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ims_gsu ON ims_data(gsu)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_target_date ON target_data(date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_target_product ON target_data(product)")

    conn.commit()

    # ── Seed admin account ─────────────────────────────────────────────────────
    existing = c.execute(
        "SELECT id FROM users WHERE email = ?", (settings.admin_email,)
    ).fetchone()

    if not existing:
        if not settings.admin_password:
            logger.warning(
                "ADMIN_PASSWORD is not set in .env — admin account will NOT be created."
            )
        else:
            from app.core.security import hash_password
            c.execute(
                """
                INSERT INTO users
                    (email, first_name, last_name, hashed_password, role, is_active)
                VALUES (?, 'Admin', 'Hikma', ?, 'admin', 1)
                """,
                (settings.admin_email, hash_password(settings.admin_password)),
            )
            conn.commit()
            logger.info("Admin account created: %s", settings.admin_email)
    else:
        logger.info("Admin account already exists.")

    conn.close()