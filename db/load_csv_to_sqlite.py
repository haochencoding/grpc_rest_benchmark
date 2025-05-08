#!/usr/bin/env python3
"""load_csv_to_sqlite.py

Populate a local SQLite database with the sample datasets that ship with
amazon‑timestream‑tools /sample_apps/.

* user_data.csv          →  table **user_data**          (lookup / dimension)
* sample_unload.csv      →  table **events**             (behavioural stream)
* sample‑multi.csv       →  table **host_metrics**       (multi‑measure time series)

Usage (default database name is data.db and CSVs are in current dir):

    python load_csv_to_sqlite.py

You can override paths:

    python load_csv_to_sqlite.py --dir csv/ --db data.db

Requires: pandas ≥ 1.0.
"""
from __future__ import annotations
import argparse
import csv
import os
import sqlite3
from typing import List, Dict

import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# Schema helpers
# ──────────────────────────────────────────────────────────────────────────────


def create_tables(conn: sqlite3.Connection) -> None:
    """Create destination tables if they don’t already exist."""
    cur = conn.cursor()
    # Dimension table: user profile
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_data (
            user_id       INTEGER PRIMARY KEY,
            first_name    TEXT,
            last_name     TEXT,
            zip           TEXT,
            job           TEXT,
            age           INTEGER
        );
        """
    )

    # Click‑stream / business events
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            channel       TEXT,
            ip_address    TEXT,
            session_id    TEXT,
            user_id       INTEGER,
            event         TEXT,
            user_group    TEXT,
            current_time  INTEGER,   -- epoch‑milliseconds
            query         TEXT,
            product_id    INTEGER,
            product       TEXT,
            quantity      INTEGER
        );
        """
    )

    # Host telemetry (multi‑measure row)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS host_metrics (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            region               TEXT,
            az                   TEXT,
            hostname             TEXT,
            ts                   TEXT,   -- original timestamp string
            cpu_utilization      REAL,
            memory_utilization   REAL
        );
        """
    )

    conn.commit()

# ──────────────────────────────────────────────────────────────────────────────
# Loaders for each file
# ──────────────────────────────────────────────────────────────────────────────


def load_user_data(conn: sqlite3.Connection, path: str) -> None:
    """Load ~95k rows of user profiles via pandas; this fits comfortably in RAM."""
    df = pd.read_csv(path)
    df.to_sql("user_data", conn, if_exists="append", index=False)
    print(f"✓ Inserted {len(df):,} rows into user_data")


def load_events(conn: sqlite3.Connection, path: str, chunk: int = 10_000) -> None:
    """Load the click‑stream in streamed chunks to keep memory usage low."""
    total = 0
    for chunk_df in pd.read_csv(path, chunksize=chunk):
        chunk_df.to_sql("events", conn, if_exists="append", index=False)
        total += len(chunk_df)
    print(f"✓ Inserted {total:,} rows into events")


# --- host_metrics (sample‑multi.csv) -----------------------------------------

_COLS = [
    "region",
    "az",
    "hostname",
    "ts",
    "cpu_utilization",
    "memory_utilization",
]


def _parse_host_metrics_row(row: List[str]) -> Dict[str, str | float]:
    """Transform the weird key,value,key,value line into a flat dict."""
    try:
        return {
            "region": row[1],
            "az": row[3],
            "hostname": row[5],
            "ts": row[6],  # e.g. '2020-03-18 02:56:02.342000000'
            "cpu_utilization": float(row[9]),
            "memory_utilization": float(row[12]),
        }
    except (IndexError, ValueError):
        raise ValueError(f"Unrecognised host_metrics row layout: {row}") from None


def load_host_metrics(conn: sqlite3.Connection, path: str, batch: int = 5_000) -> None:
    """Stream‑load the multi‑measure sample into host_metrics."""
    cur = conn.cursor()
    buf: List[Dict[str, str | float]] = []
    inserted = 0
    with open(path, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            buf.append(_parse_host_metrics_row(row))
            if len(buf) >= batch:
                _flush_host_metrics(cur, buf)
                inserted += len(buf)
                buf.clear()
        if buf:
            _flush_host_metrics(cur, buf)
            inserted += len(buf)
    conn.commit()
    print(f"✓ Inserted {inserted:,} rows into host_metrics")


def _flush_host_metrics(cur: sqlite3.Cursor, buf: List[Dict[str, str | float]]):
    cur.executemany(
        """
        INSERT INTO host_metrics (
            region, az, hostname, ts, cpu_utilization, memory_utilization
        ) VALUES (
            :region, :az, :hostname, :ts, :cpu_utilization, :memory_utilization
        );
        """,
        buf,
    )

# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────


def main():
    ap = argparse.ArgumentParser(description="Load amazon‑timestream sample CSVs into SQLite")
    ap.add_argument("--db", default="data.db", help="SQLite file to create/update (default: %(default)s)")
    ap.add_argument("--dir", default=os.getcwd(), help="Directory containing the CSV files (default: cwd)")
    args = ap.parse_args()

    csv_dir = os.path.abspath(args.dir)

    paths = {
        "user_data": os.path.join(csv_dir, "user_data.csv"),
        "events": os.path.join(csv_dir, "sample_unload.csv"),
        "host_metrics": os.path.join(csv_dir, "sample-multi.csv"),
    }

    missing = [name for name, p in paths.items() if not os.path.exists(p)]
    if missing:
        raise SystemExit(f"Missing CSV file(s): {', '.join(missing)} in {csv_dir}")

    conn = sqlite3.connect(args.db)
    create_tables(conn)

    load_user_data(conn, paths["user_data"])
    load_events(conn, paths["events"])
    load_host_metrics(conn, paths["host_metrics"])

    conn.close()
    print(f"✔ SQLite database ready → {args.db}")


if __name__ == "__main__":
    main()
