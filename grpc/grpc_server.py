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
import grpc
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import sample_api_pb2 as pb2               # generated messages
import sample_api_pb2_grpc as pb2_grpc     # generated service stubs
import sys
import time

from itertools import islice
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent 
sys.path.insert(0, str(PROJECT_ROOT))

from shared.db_utils import DEFAULT_DB, select_rows, count_rows

# ──────────────────────────────────────────────────────────────────────────────
# Configuration (Logging)
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(                      # ❶ root logger setup
    level=logging.INFO,                   # show INFO and higher
    format="%(message)s",                 # print the raw JSON only
    handlers=[logging.StreamHandler(sys.stdout)],
)

log = logging.getLogger("api.metrics")    # ❷ your scoped logger
log.setLevel(logging.INFO)

# --------- Rotating file handler (JSON‑lines) -------------------------------
LOG_FILE_PATH = os.getenv("API_METRICS_LOG_PATH", "logs/grpc_api_metrics.jsonl")
LOG_MAX_MB = int(os.getenv("API_METRICS_LOG_MAX_MB", "10"))   # each file ≤10 MiB
LOG_BACKUP_CNT = int(os.getenv("API_METRICS_LOG_BACKUP", "5"))     # keep 5 backups

_log_path = Path(LOG_FILE_PATH).expanduser()
_log_path.parent.mkdir(parents=True, exist_ok=True)

_file_hdlr = RotatingFileHandler(
    LOG_FILE_PATH,
    maxBytes=LOG_MAX_MB * 1024 * 1024,
    backupCount=LOG_BACKUP_CNT,
)
_file_hdlr.setFormatter(logging.Formatter("%(message)s"))
log.addHandler(_file_hdlr)
# Prevent double‑logging via the root handler – messages are explicitly routed
# to both stdout (root StreamHandler) and file (our RotatingFileHandler) once.
log.propagate = True  # still bubble up to root so stdout keeps working

# ──────────────────────────────────────────────────────────────────────────────
# Service implementation
# ──────────────────────────────────────────────────────────────────────────────


class SampleApiService(pb2_grpc.SampleApiServicer):
    """Implements SampleApi defined in sample.proto."""

    def __init__(self, db_path: str, max_batch_bytes: int):
        self._db_path = db_path
        self._max_batch_bytes = max_batch_bytes

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
        rows = select_rows(
            hostname=request.hostname or None,
            region=request.region or None,
            limit=limit,
            offset=offset,
            db_path=self._db_path,
        )
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
                        "sql_query_ns": t_query_done - t_in,
                        "ser_ns": t_serialized - t_query_done,
                        "app_ns": t_out - t_in
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
        rows = select_rows(
            hostname=request.hostname or None,
            region=request.region or None,
            limit=request.limit or 50,
            offset=request.offset,
            db_path=self._db_path,
        )

        BATCH_ROWS = 40_000
        it = iter(rows)
        while True:
            chunk = list(islice(it, BATCH_ROWS))
            if not chunk:
                break
            await context.write(
                pb2.MetricListResponse(metrics=[pb2.Metric(**r) for r in chunk])
            )

    # ———————————————————  COUNT  ————————————————————————————————
    async def CountMetrics(                      # type: ignore[override]
        self,
        request: pb2.MetricCountRequest,
        context: grpc.aio.ServicerContext,
    ) -> pb2.MetricCountResponse:
        nrows = count_rows(
            hostname=request.hostname,
            region=request.region,
            db_path=self._db_path
        )
        return pb2.MetricCountResponse(count=nrows)

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