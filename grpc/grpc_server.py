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
import sample_api_pb2 as pb2               # generated messages
import sample_api_pb2_grpc as pb2_grpc     # generated service stubs
import sys
import time

from itertools import islice
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent 
sys.path.insert(0, str(PROJECT_ROOT))

from shared.db_utils import DEFAULT_DB, select_rows, count_rows
from shared.logger_utils import setup_metrics_logger, log_rpc

# ──────────────────────────────────────────────────────────────────────────────
# Set up logging
# ──────────────────────────────────────────────────────────────────────────────

log = setup_metrics_logger(name='grpc.api.metric')

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
            log_rpc(
                log=log,
                rpc="MetricsListUnaryResponse",
                params={
                    "limit":   limit,
                    "offset":  offset,
                    "hostname": request.hostname,
                    "region":   request.region,
                    },
                t_in=t_in,
                t_query_done=t_query_done,
                t_serialized=t_serialized,
                size_bytes=size_bytes
            )

        # ---------- register callback so we capture *t_out* ----------
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