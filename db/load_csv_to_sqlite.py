#!/usr/bin/env python3
"""load_csv_to_sqlite.py

Populate a local SQLite database with the sample datasets

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

# ──────────────────────────────────────────────────────────────────────────────
# Schema helpers
# ──────────────────────────────────────────────────────────────────────────────


def create_tables(conn: sqlite3.Connection) -> None:
    """Create destination tables if they don’t already exist."""
    cur = conn.cursor()

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
# CSV → dict helpers
# ──────────────────────────────────────────────────────────────────────────────


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
        "host_metrics": os.path.join(csv_dir, "sample-multi.csv"),
    }

    missing = [name for name, p in paths.items() if not os.path.exists(p)]
    if missing:
        raise SystemExit(f"Missing CSV file(s): {', '.join(missing)} in {csv_dir}")

    conn = sqlite3.connect(args.db)
    create_tables(conn)

    load_host_metrics(conn, paths["host_metrics"])

    conn.close()
    print(f"✔ SQLite database ready → {args.db}")


if __name__ == "__main__":
    main()
