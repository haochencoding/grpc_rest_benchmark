#!/usr/bin/env python3
"""grpc_server.py – gRPC façade for the SQLite demo database

* Depends only on the generated files `sample_api_pb2.py` and
  `sample_api_pb2_grpc.py`, which must sit in the **same folder**
* SQLite file defaults to ../db/data.db but can be overridden with the
  `SQLITE_DB` env var.

Run locally:

    # default 1 MiB batches, maximum 4 MiB on the wire
    python grpc_server.py

    # 5 MiB max batch, maximum 20 MiB on the wire
    python grpc_server.py --max-batch-kb 5120 --grpc-max-mb 20
"""
from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List

import time
import json
import logging

import grpc

import sample_api_pb2 as pb2               # generated messages
import sample_api_pb2_grpc as pb2_grpc     # generated service stubs

# ──────────────────────────────────────────────────────────────────────────────
# Configuration (CLI flags parsed in main())
# ──────────────────────────────────────────────────────────────────────────────
DB_CHUNK_ROWS = 1_000                   # rows fetched per SQL query

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))       # ensure generated stubs import correctly
DEFAULT_DB = BASE_DIR.parent / "db" / "data.db"

# ──────────────────────────────────────────────────────────────────────────────
# Configuration (Logging)
# ──────────────────────────────────────────────────────────────────────────────


logging.basicConfig(                      # ❶ root logger setup
    level=logging.INFO,                   # show INFO and higher
    format="%(message)s",                 # print the raw JSON only
    handlers=[logging.StreamHandler(sys.stdout)],
)

log = logging.getLogger("api.metrics")    # ❷ your scoped logger

# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------


def _dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> Dict[str, Any]:
    """Return rows as dicts so we can unpack into Protobuf easily."""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = _dict_factory
    return conn

# ──────────────────────────────────────────────────────────────────────────────
# Service implementation
# ──────────────────────────────────────────────────────────────────────────────

class SampleApiService(pb2_grpc.SampleApiServicer):
    """Implements SampleApi defined in sample.proto."""

    def __init__(self, db_path: str, max_batch_bytes: int):
        self._db_path = db_path
        self._max_batch_bytes = max_batch_bytes

    # ———————————————————  HELPERS  ————————————————————————————————
    def _select_rows(
        self,
        req: pb2.MetricListRequest,
        limit: int,
        offset: int,
    ) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM host_metrics"
        params: List[Any] = []
        clauses: List[str] = []

        if req.hostname:
            clauses.append("hostname = ?")
            params.append(req.hostname)
        if req.region:
            clauses.append("region = ?")
            params.append(req.region)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)

        sql += " ORDER BY id LIMIT ? OFFSET ?"
        params += [limit, offset]

        with get_conn(self._db_path) as conn:
            return conn.execute(sql, params).fetchall()

    # ———————————————————  UNARY  ————————————————————————————————
    async def MetricsListUnaryResponse(           # type: ignore[override]
        self,
        request: pb2.MetricListRequest,
        context: grpc.aio.ServicerContext,
    ) -> pb2.MetricListResponse:
        t_in = time.time_ns()

        limit = request.limit or 50
        offset = request.offset

        # ───── query DB
        rows = self._select_rows(request, limit, offset)
        t_query_done = time.time_ns()

        # ───── serialize protobuf
        resp = pb2.MetricListResponse(metrics=[pb2.Metric(**r) for r in rows])
        t_serialized = time.time_ns()
        size_bytes = resp.ByteSize()

        # ---------- logging helper -----------------------------------
        def _log_line() -> None:
            t_out = time.time_ns()
            log.info(
                json.dumps(
                    {
                        "rpc": "MetricsListUnaryResponse",
                        "params": {
                            "limit":   limit,
                            "offset":  offset,
                            "hostname": request.hostname,
                            "region":   request.region,
                        },
                        "t_in":         t_in,
                        "t_query_done": t_query_done,
                        "t_serialized": t_serialized,
                        "t_out":        t_out,
                        "size_bytes":   size_bytes,
                    },
                    separators=(",", ":"),
                )
            )

        # ---------- register callback so we capture *t_out* ----------
        # async-API (grpc.aio) ----------------------------------------
        print("add_done_callback")
        context.add_done_callback(lambda _: _log_line())

        return resp

    # ———————————————————  STREAMING  ——————————————————————————————
    async def MetricsListStreamResponse(          # type: ignore[override]
        self,
        request: pb2.MetricListRequest,
        context: grpc.aio.ServicerContext,
    ):
        remaining = request.limit or None         # None ⇒ stream all
        offset = request.offset

        batch: List[pb2.Metric] = []
        bytes_used = 0

        while remaining is None or remaining > 0:
            page_size = DB_CHUNK_ROWS
            if remaining is not None:
                page_size = min(page_size, remaining)

            rows = self._select_rows(request, page_size, offset)
            if not rows:
                break

            for r in rows:
                metric = pb2.Metric(**r)
                m_size = metric.ByteSize()

                if batch and bytes_used + m_size > self._max_batch_bytes:
                    await context.write(pb2.MetricListResponse(metrics=batch))
                    batch, bytes_used = [], 0

                batch.append(metric)
                bytes_used += m_size
                offset += 1

                if remaining is not None:
                    remaining -= 1
                    if remaining == 0:
                        break

        if batch:
            await context.write(pb2.MetricListResponse(metrics=batch))

    # ———————————————————  COUNT  ————————————————————————————————
    async def CountMetrics(                      # type: ignore[override]
        self,
        request: pb2.MetricCountRequest,
        context: grpc.aio.ServicerContext,
    ) -> pb2.MetricCountResponse:
        sql = "SELECT COUNT(*) AS cnt FROM host_metrics"
        params, clauses = [], []
        if request.hostname:
            clauses.append("hostname = ?"); params.append(request.hostname)
        if request.region:
            clauses.append("region   = ?"); params.append(request.region)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)

        with get_conn(self._db_path) as conn:
            row = conn.execute(sql, params).fetchone()
        return pb2.MetricCountResponse(count=row["cnt"])

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


async def serve(
        host: str,
        port: int,
        db_path: str,
        max_batch_kb: int,
        grpc_max_mb: int
        ):
    grpc_max_bytes = grpc_max_mb * 1024 * 1024
    server = grpc.aio.server(
        options=[
            ('grpc.max_send_message_length',     grpc_max_bytes),
            ('grpc.max_receive_message_length',  grpc_max_bytes),
            ]
    )
    pb2_grpc.add_SampleApiServicer_to_server(
        SampleApiService(db_path, max_batch_kb * 1024), server
    )
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    print(
        f"gRPC server on {host}:{port} | DB={db_path} | batch≤{max_batch_kb} KB | message length≤{grpc_max_mb} MB "
    )
    await server.wait_for_termination()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="SampleApi gRPC server")
    ap.add_argument("--host", default="0.0.0.0", help="Bind address")
    ap.add_argument("--port", default=50051, type=int, help="Bind port")
    ap.add_argument("--db", default=str(DEFAULT_DB), help="SQLite file")
    ap.add_argument(
        "--max-batch-kb",
        default=1024,
        type=int,
        help="Max gRPC payload per message in **KB** (default: 1024)",
    )
    ap.add_argument(
        "--grpc-max-mb",
        default=4,
        type=int,
        help="Override gRPC send/receive limit in **MB** (default: 4 – gRPC built-in)",
    )
    args = ap.parse_args()

    try:
        asyncio.run(
            serve(args.host,
                  args.port,
                  args.db,
                  args.max_batch_kb,
                  args.grpc_max_mb)
        )
    except (KeyboardInterrupt, SystemExit):
        print("Shutting down gRPC server")