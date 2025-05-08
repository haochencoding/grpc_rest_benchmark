#!/usr/bin/env python3
"""grpc_server.py – gRPC façade for the SQLite demo database

* Depends only on the generated files `sample_api_pb2.py` and
  `sample_api_pb2_grpc.py`, which must sit in the **same folder**
* SQLite file defaults to ../db/data.db but can be overridden with the
  `SQLITE_DB` env var.

Run locally:

    python grpc_server.py            # port 50051

Test client (Python REPL):

    import grpc, sample_api_pb2 as pb2, sample_api_pb2_grpc as stubs
    ch   = grpc.insecure_channel('localhost:50051')
    cli  = stubs.SampleApiStub(ch)
    res  = cli.GetUser(pb2.UserRequest(user_id=432337))
    print(res)
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List

import grpc

import sample_api_pb2 as pb2               # generated messages
import sample_api_pb2_grpc as pb2_grpc     # generated service stubs

# ---------------------------------------------------------------------------
# Add ./grpc (where this file and the generated modules live) to sys.path
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))


DB_PATH = os.getenv("SQLITE_DB", str(BASE_DIR.parent / "db" / "data.db"))

# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------


def _dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> Dict[str, Any]:
    """Return rows as dicts so we can unpack into Protobuf easily."""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = _dict_factory
    return conn

# ---------------------------------------------------------------------------
# Service implementation
# ---------------------------------------------------------------------------


class SampleApiService(pb2_grpc.SampleApiServicer):
    """Concrete implementation of the protobuf service."""
    # METRICS ──────────────────────────────────────────────────────────────
    async def ListMetrics(
            self,
            request: pb2.MetricListRequest,
            context: grpc.aio.ServicerContext
    ) -> pb2.MetricListResponse:  # type: ignore[override]
        sql = "SELECT * FROM host_metrics"
        params: List[Any] = []
        clauses: List[str] = []
        if request.hostname:
            clauses.append("hostname = ?")
            params.append(request.hostname)
        if request.region:
            clauses.append("region = ?")
            params.append(request.region)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id LIMIT ? OFFSET ?"
        params += [request.limit or 50, request.offset]

        with get_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return pb2.MetricListResponse(metrics=[pb2.Metric(**row) for row in rows])
    
    async def CountMetrics(
        self,
        request: pb2.MetricCountRequest,
        context: grpc.aio.ServicerContext,
    ) -> pb2.MetricCountResponse:                                       # type: ignore[override]
        sql = "SELECT COUNT(*) AS cnt FROM host_metrics"
        params, clauses = [], []
        if request.hostname:
            clauses.append("hostname = ?")
            params.append(request.hostname)
        if request.region:
            clauses.append("region = ?")
            params.append(request.region)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)

        with get_conn() as conn:
            row = conn.execute(sql, params).fetchone()
        return pb2.MetricCountResponse(count=row["cnt"])

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


async def serve(host: str = "0.0.0.0", port: int = 50051):  # noqa: S104
    server = grpc.aio.server()
    pb2_grpc.add_SampleApiServicer_to_server(SampleApiService(), server)
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    print(f"gRPC server started on {host}:{port} (DB: {DB_PATH})")
    await server.wait_for_termination()


if __name__ == "__main__":
    try:
        asyncio.run(serve())
    except (KeyboardInterrupt, SystemExit):
        print("Shutting down gRPC server")