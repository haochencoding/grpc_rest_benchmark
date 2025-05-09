# shared/db_utils.py
"""
db_utils.py – tiny SQLite helper layer reused by the FastAPI and gRPC servers.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

# ──────────────────────────────────────────────
# Config – pick up the same env var the servers use
# ──────────────────────────────────────────────
BASE_DIR   = Path(__file__).resolve().parent.parent   # repo root
DEFAULT_DB = BASE_DIR / "db" / "data.db"
DB_PATH    = os.getenv("SQLITE_DB", str(DEFAULT_DB))

# ──────────────────────────────────────────────
# Low-level plumbing
# ──────────────────────────────────────────────
def dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> Dict[str, Any]:
    """Return every row as a plain dict keyed by column name."""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def get_conn(db_path: str | None = None) -> sqlite3.Connection:
    """
    Open a *short-lived* connection with the dict row_factory applied.

    Let callers override the DB file when they need to.
    """
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = dict_factory
    return conn

# ──────────────────────────────────────────────
# Query helpers – keep them generic so all callers
# (REST & gRPC) can reuse the same code.
# ──────────────────────────────────────────────
def select_rows(
    *,
    hostname: Optional[str] = None,
    region:   Optional[str] = None,
    limit:    int = 50,
    offset:   int = 0,
    db_path:  str | None = None,
    table:    str = "host_metrics",
) -> List[Dict[str, Any]]:
    """
    Fetch a slice of rows sorted by *id*.

    Arguments mirror both servers’ query parameters so they can
    delegate the DB work without extra glue code.
    """
    sql     = f"SELECT * FROM {table}"
    params: list[Any] = []
    clauses: list[str] = []

    if hostname:
        clauses.append("hostname = ?"); params.append(hostname)
    if region:
        clauses.append("region   = ?"); params.append(region)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)

    sql += " ORDER BY id LIMIT ? OFFSET ?"
    params += [limit, offset]

    with get_conn(db_path) as conn:
        return conn.execute(sql, params).fetchall()


def count_rows(
    *,
    hostname: Optional[str] = None,
    region:   Optional[str] = None,
    db_path:  str | None = None,
    table:    str = "host_metrics",
) -> int:
    """Return how many rows match the optional filters."""
    sql = f"SELECT COUNT(*) FROM {table}"
    params: list[Any] = []
    clauses: list[str] = []

    if hostname:
        clauses.append("hostname = ?"); params.append(hostname)
    if region:
        clauses.append("region   = ?"); params.append(region)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)

    with get_conn(db_path) as conn:
        return conn.execute(sql, params).fetchone()["COUNT(*)"]