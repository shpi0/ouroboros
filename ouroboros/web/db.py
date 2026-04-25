"""SQLite chat storage for Ouroboros web interface."""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional

DB_PATH = Path("/home/ouroboros/state/chats.db")

_conn: Optional[sqlite3.Connection] = None


def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
    return _conn


def init_db() -> None:
    c = get_conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS chats (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL DEFAULT 'Новый чат',
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            is_main INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            chat_id TEXT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
            role TEXT NOT NULL,
            text TEXT NOT NULL,
            ts REAL NOT NULL,
            is_progress INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id, ts);
    """)
    # Ensure main chat exists
    c.execute("""
        INSERT OR IGNORE INTO chats (id, title, created_at, updated_at, is_main)
        VALUES ('main', 'Telegram / Main', ?, ?, 1)
    """, (time.time(), time.time()))
    c.commit()


def list_chats() -> List[Dict]:
    rows = get_conn().execute(
        "SELECT id, title, created_at, updated_at, is_main FROM chats ORDER BY is_main DESC, updated_at DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def create_chat(title: str = "Новый чат") -> Dict:
    chat_id = uuid.uuid4().hex[:12]
    now = time.time()
    get_conn().execute(
        "INSERT INTO chats (id, title, created_at, updated_at, is_main) VALUES (?,?,?,?,0)",
        (chat_id, title, now, now),
    )
    get_conn().commit()
    return {"id": chat_id, "title": title, "created_at": now, "updated_at": now, "is_main": 0}


def delete_chat(chat_id: str) -> bool:
    if chat_id == "main":
        return False
    c = get_conn()
    c.execute("DELETE FROM messages WHERE chat_id=?", (chat_id,))
    c.execute("DELETE FROM chats WHERE id=? AND is_main=0", (chat_id,))
    c.commit()
    return True


def rename_chat(chat_id: str, title: str) -> bool:
    get_conn().execute(
        "UPDATE chats SET title=?, updated_at=? WHERE id=?", (title, time.time(), chat_id)
    )
    get_conn().commit()
    return True


def add_message(
    chat_id: str,
    role: str,
    text: str,
    is_progress: bool = False,
    msg_id: Optional[str] = None,
    ts: Optional[float] = None,
) -> Dict:
    now = ts or time.time()
    mid = msg_id or uuid.uuid4().hex[:16]
    c = get_conn()
    c.execute(
        "INSERT OR IGNORE INTO messages (id, chat_id, role, text, ts, is_progress) VALUES (?,?,?,?,?,?)",
        (mid, chat_id, role, text, now, 1 if is_progress else 0),
    )
    c.execute("UPDATE chats SET updated_at=? WHERE id=?", (now, chat_id))
    c.commit()
    return {
        "id": mid, "chat_id": chat_id, "role": role,
        "text": text, "ts": now, "is_progress": is_progress,
    }


def get_messages(chat_id: str, limit: int = 200, offset: int = 0) -> List[Dict]:
    rows = get_conn().execute(
        """SELECT id, chat_id, role, text, ts, is_progress
           FROM messages WHERE chat_id=?
           ORDER BY ts DESC LIMIT ? OFFSET ?""",
        (chat_id, limit, offset),
    ).fetchall()
    return list(reversed([dict(r) for r in rows]))


def get_messages_since(chat_id: str, since_ts: float) -> List[Dict]:
    rows = get_conn().execute(
        """SELECT id, chat_id, role, text, ts, is_progress
           FROM messages WHERE chat_id=? AND ts>?
           ORDER BY ts ASC""",
        (chat_id, since_ts),
    ).fetchall()
    return [dict(r) for r in rows]
