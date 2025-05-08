#!/usr/bin/env python3
"""
smoke_test.py – quick “does it work?” script for the Sample gRPC API.

• Connects, calls every RPC, prints JSON, and times each round-trip.
• Supports the new RPC names and the streaming endpoint.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Callable, Tuple
import json

import grpc
from google.protobuf.json_format import MessageToJson

# Make generated stubs importable regardless of cwd
STUB_DIR = Path(__file__).resolve().parent / "grpc"
sys.path.insert(0, str(STUB_DIR))

import sample_api_pb2 as pb2          # noqa: E402
import sample_api_pb2_grpc as pb2_grpc  # noqa: E402


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def timed(fn: Callable[[], object]) -> Tuple[object, float]:
    """Run fn() and return (result, elapsed-ms)."""
    t0 = time.perf_counter()
    res = fn()
    return res, (time.perf_counter() - t0) * 1_000


def pretty(msg):
    """Pretty-print a protobuf Message as JSON."""
    return MessageToJson(msg, preserving_proto_field_name=True, indent=2)


def msg_size(msg) -> int:
    """Serialized size in **bytes** (exactly what gRPC transmits)."""
    return len(msg.SerializeToString())


def json_size(msg) -> int:
    """Bytes required to UTF-8-encode MessageToJson(msg)."""
    js = MessageToJson(msg, preserving_proto_field_name=True)
    return len(js.encode("utf-8"))


def kb(n: int) -> str:          # helper for human-readable KiB / MiB
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n/1024:.3f} KiB"
    return f"{n/1024/1024:.3f} MiB"

# ──────────────────────────────────────────────────────────────────────────────
# Main test routine
# ──────────────────────────────────────────────────────────────────────────────


def run_tests(host: str, port: int, grpc_max_mb: int) -> None:
    target = f"{host}:{port}"
    print(f"Connecting to gRPC server at {target} …\n")

    opts = [
        ('grpc.max_receive_message_length', grpc_max_mb * 1024 * 1024),
        ('grpc.max_send_message_length',    grpc_max_mb * 1024 * 1024),
    ]
    channel = grpc.insecure_channel(target, options=opts)
    stub = pb2_grpc.SampleApiStub(channel)

    # 1. Unary list --------------------------------------------------------
    req = pb2.MetricListRequest(limit=5)
    resp, ms = timed(lambda: stub.MetricsListUnaryResponse(req))
    size_proto = msg_size(resp)
    size_json = json_size(resp)
    print(
        f"MetricsListUnaryResponse  5 rows – {ms:.2f} ms – "
        f"PROTO {kb(size_proto)} | JSON {kb(size_json)}\n"
        f"{pretty(resp)}\n"
    )

    # 2. Unary list for multiple rows ------------------------------------------
    nrows = 40_000
    req_big = pb2.MetricListRequest(limit=nrows)
    resp_big, ms = timed(lambda: stub.MetricsListUnaryResponse(req_big))
    size_big_proto = msg_size(resp_big)
    print(
        f"MetricsListUnaryResponse {nrows} rows – {ms:.2f} ms\n"
        f"PROTO {kb(size_big_proto)}\n"
    )

    # 3.  Streaming list for for 2000 rows ----------------------------------
    print(f"MetricsListStreamResponse {nrows} rows (byte-bounded batches)…")
    stream_req = pb2.MetricListRequest(limit=nrows)
    rows, batches, bytes_total = 0, 0, 0
    t0 = time.perf_counter()

    for batch in stub.MetricsListStreamResponse(stream_req):
        batches += 1
        rows += len(batch.metrics)
        bytes_total += msg_size(batch)

    ms_stream = (time.perf_counter() - t0) * 1_000
    print(
        f"  ↳ {rows} rows, {batches} batch(es) – {ms_stream:.2f} ms – "
        f"PROTO {kb(bytes_total)}"
    )

    # 4.  Count all rows ----------------------------------------------------
    cnt_resp, ms = timed(lambda: stub.CountMetrics(pb2.MetricCountRequest()))
    print(f"CountMetrics – {ms:.2f} ms – {cnt_resp.count} rows total\n")

    print("All RPCs succeeded ✔")


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    pa = argparse.ArgumentParser(description="Smoke-test the Sample gRPC API")
    pa.add_argument("--host", default="localhost", help="Server host")
    pa.add_argument("--port", default=50051, type=int, help="Server port")
    pa.add_argument(
        "--grpc-max-mb",
        default=4,
        type=int,
        help="Client receive/send limit in **MB** (mirror the server flag)",
    )
    args = pa.parse_args()

    try:
        run_tests(args.host, args.port, args.grpc_max_mb)
    except grpc.RpcError as e:
        print(f"❌ gRPC error: {e.code().name} – {e.details()}")
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        print(f"❌ Unexpected error: {e}")
        raise
