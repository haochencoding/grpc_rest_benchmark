#!/usr/bin/env python3
"""
fastapi_server.py – REST façade for the SQLite demo database

You can now pass a custom SQLite file **either** via the `--db` CLI option
when starting the embedded Uvicorn server or via the `SQLITE_DB` environment
variable when you launch the service through an external Uvicorn command.

Examples
--------
Run with the built‑in launcher (CLI wins over env var):

    python fastapi_server.py --host 0.0.0.0 --port 8000 --db ../db/alt.db

Use an external Uvicorn process (env var only):

    export SQLITE_DB=../db/alt.db
    uvicorn fastapi_server:app --host 0.0.0.0 --port 8000

The default is shared.db_utils.DEFAULT_DB (../db/data.db).
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
from pathlib import Path

from io import BytesIO

import uvicorn
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import StreamingResponse

from schemas import Metric, MetricsListResponse, MetricsCountResponse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.db_utils import DEFAULT_DB, select_rows, count_rows  # noqa: E402
from shared.logger_utils import setup_metrics_logger, log_rpc  # noqa: E402

# ─────────────────────────────────────────────────────────────────────────────
# Set up logging
# ─────────────────────────────────────────────────────────────────────────────

log = setup_metrics_logger(name='rest.api.metric')

# ─────────────────────────────────────────────────────────────────────────────
# FastAPI application
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Metrics REST API",
    description="HTTP wrapper around the SQLite demo database",
)

# Store the *startup* choice so worker processes can access it cheaply
app.state.db_path = os.getenv("SQLITE_DB", str(DEFAULT_DB))


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────


@app.get(
    "/metrics",
    response_model=MetricsListResponse,
    summary="List metrics (paged)",
)
async def list_metrics(
    limit: int = Query(50, ge=1),
    offset: int = Query(0, ge=0),
    hostname: str | None = Query(None),
    region: str | None = Query(None),
    background_tasks: BackgroundTasks = None,
):
    """Return a page of metrics rows."""
    t_in = time.time_ns()

    try:
        rows = select_rows(
            hostname=hostname or None,
            region=region or None,
            limit=limit,
            offset=offset,
            db_path=app.state.db_path,
        )
    except Exception as exc:  # pragma: no cover – DB errors are integration‑tested elsewhere
        raise HTTPException(500, str(exc)) from exc
    t_query_done = time.time_ns()

    resp_model = MetricsListResponse(metrics=[Metric(**r) for r in rows])
    t_serialized = time.time_ns()

    def _log_line() -> None:
        log_rpc(
                log=log,
                rpc="GET /metrics",
                params={
                    "limit":   limit,
                    "offset":  offset,
                    "hostname": hostname,
                    "region":   region,
                    },
                t_in=t_in,
                t_query_done=t_query_done,
                t_serialized=t_serialized,
                size_bytes=len(json.dumps(resp_model.model_dump(by_alias=True), separators=(",", ":")).encode())
            )

    background_tasks.add_task(_log_line)

    return resp_model


@app.get(
    "/metrics-gz",
    summary="List metrics (always gzip-compressed)",
)
async def list_metrics_gz(
    limit: int = Query(50, ge=1),
    offset: int = Query(0, ge=0),
    hostname: str | None = Query(None),
    region: str | None = Query(None),
    background_tasks: BackgroundTasks = None,
):
    """Return a page of metrics rows, always gzip-compressed."""
    t_in = time.time_ns()

    try:
        rows = select_rows(
            hostname=hostname or None,
            region=region or None,
            limit=limit,
            offset=offset,
            db_path=app.state.db_path,
        )
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc
    
    t_query_done = time.time_ns()

    # 1. Build the regular response body (dict → JSON bytes, already minified)
    payload = MetricsListResponse(metrics=[Metric(**r) for r in rows]).model_dump(
        by_alias=True
    )

    t_serialized = time.time_ns()

    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    # 2. Compress it into an in-memory buffer
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(raw)
    buf.seek(0)

    def _log_line() -> None:
        log_rpc(
            log=log,
            rpc="GET /metrics-gz",
            params={"limit": limit, "offset": offset, "hostname": hostname, "region": region},
            t_in=t_in,
            t_query_done=t_query_done,
            t_serialized=t_serialized,
            size_bytes=str(len(buf.getbuffer())),
        )

    background_tasks.add_task(_log_line)
    
    # 3. Stream it back with proper headers
    return StreamingResponse(
        buf,
        media_type="application/json",
        headers={
            "Content-Encoding": "gzip",
            "Content-Length": str(len(buf.getbuffer())),
        },
    )


@app.get(
    "/metrics/count",
    response_model=MetricsCountResponse,
    summary="Count matching metrics rows",
)
async def count_metrics(
    hostname: str | None = Query(None),
    region: str | None = Query(None),
):
    """Return the number of matching rows without returning the rows themselves."""
    try:
        cnt = count_rows(
            hostname=hostname or None,
            region=region or None,
            db_path=app.state.db_path,
        )
    except Exception as exc:  # pragma: no cover
        raise HTTPException(500, str(exc)) from exc
    return MetricsCountResponse(count=cnt)


# ─────────────────────────────────────────────────────────────────────────────
# Embedded CLI launcher (optional)
# ─────────────────────────────────────────────────────────────────────────────


def _parse_cli() -> argparse.Namespace:  # pragma: no cover
    ap = argparse.ArgumentParser(description="Metrics FastAPI server")
    ap.add_argument("--host", default="0.0.0.0", help="Bind address")
    ap.add_argument("--port", default=8000, type=int, help="Bind port")
    ap.add_argument("--reload", action="store_true", help="Enable Uvicorn --reload")
    ap.add_argument("--db", default=str(DEFAULT_DB), help="SQLite file to use")
    return ap.parse_args()


if __name__ == "__main__":  # pragma: no cover
    args = _parse_cli()

    # Make the override visible to *all* workers spawned by Uvicorn
    app.state.db_path = args.db

    uvicorn.run(
        "fastapi_server:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )
