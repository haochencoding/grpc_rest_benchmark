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
import os
import sys
import time
from io import BytesIO
from pathlib import Path
from typing import Any, Dict

import json
import logging
from logging.handlers import RotatingFileHandler

import uvicorn
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks

from schemas import Metric, MetricsListResponse, MetricsCountResponse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.db_utils import DEFAULT_DB, select_rows, count_rows  # noqa: E402

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _effective_db_path() -> str:
    """Return the SQLite file to use for the current request.

    Precedence (highest → lowest):
        1. `app.state.db_path` – set by the embedded CLI launcher
        2. $SQLITE_DB environment variable
        3. shared.db_utils.DEFAULT_DB
    """
    if getattr(app.state, "db_path", None):
        return str(app.state.db_path)
    return os.getenv("SQLITE_DB", str(DEFAULT_DB))

def _log_request(
    rpc: str,
    params: Dict[str, Any],
    t_in: int,
    t_query_done: int,
    t_serialized: int,
    size_bytes: int,
) -> None:
    """Emit a structured log line for one REST handler."""
    t_out = time.time_ns()
    log.info(
        json.dumps(
            {
                "rpc": rpc,
                "params": params,
                "t_in": t_in,
                "t_query_done": t_query_done,
                "t_serialized": t_serialized,
                "t_out": t_out,
                "size_bytes": size_bytes,
                "sql_query_ns": t_query_done - t_in,
                "ser_ns": t_serialized - t_query_done,
                "app_ns": t_out - t_in,
            },
            separators=(",", ":"),
        )
    )

# ────────────────────────────────────────────────────────────────────────────
# Logging (stdout + rotating file, JSON‑lines)
# ────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",  # emit *only* the JSON log line
    handlers=[logging.StreamHandler(sys.stdout)],
)

log = logging.getLogger("api.metrics")
log.setLevel(logging.INFO)

LOG_FILE_PATH = os.getenv("API_METRICS_LOG_PATH", "logs/rest_api_metrics.jsonl")
LOG_MAX_MB = int(os.getenv("API_METRICS_LOG_MAX_MB", "10"))
LOG_BACKUP_CNT = int(os.getenv("API_METRICS_LOG_BACKUP", "5"))

_log_path = Path(LOG_FILE_PATH).expanduser()
_log_path.parent.mkdir(parents=True, exist_ok=True)

_file_hdlr = RotatingFileHandler(
    _log_path,
    maxBytes=LOG_MAX_MB * 1024 * 1024,
    backupCount=LOG_BACKUP_CNT,
)
_file_hdlr.setFormatter(logging.Formatter("%(message)s"))
log.addHandler(_file_hdlr)
log.propagate = True  # bubble up to root – stdout handler already configured

# ─────────────────────────────────────────────────────────────────────────────
# FastAPI application
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Metrics REST API",
    description="HTTP wrapper around the SQLite demo database",
)

# Store the *startup* choice so worker processes can access it cheaply
app.state.db_path = _effective_db_path()


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
            db_path=_effective_db_path(),
        )
    except Exception as exc:  # pragma: no cover – DB errors are integration‑tested elsewhere
        raise HTTPException(500, str(exc)) from exc
    t_query_done = time.time_ns()

    resp_model = MetricsListResponse(metrics=[Metric(**r) for r in rows])
    t_serialized = time.time_ns()

    size_bytes = len(json.dumps(resp_model.model_dump(by_alias=True), separators=(",", ":")).encode())

    background_tasks.add_task(
        _log_request,
        rpc="GET /metrics",
        params={"limit": limit, "offset": offset, "hostname": hostname, "region": region},
        t_in=t_in,
        t_query_done=t_query_done,
        t_serialized=t_serialized,
        size_bytes=size_bytes,
    )
    return resp_model


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
            db_path=_effective_db_path(),
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
