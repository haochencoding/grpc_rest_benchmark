#!/usr/bin/env python3
"""smoke_test.py – simple smoke‑test for the gRPC Sample API service

Connects to the running server, calls each RPC, prints the **entire** Protobuf
responses as pretty‑printed JSON, and times each round‑trip.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Callable, Tuple

import grpc
from google.protobuf.json_format import MessageToJson  # pretty printer

# Make the generated stubs importable regardless of cwd
STUB_DIR = Path(__file__).resolve().parent / "grpc"
sys.path.insert(0, str(STUB_DIR))

import sample_api_pb2 as pb2  # noqa: E402
import sample_api_pb2_grpc as pb2_grpc  # noqa: E402

# ──────────────────────────────────────────────────────────────────────────────
# Timing helper
# ──────────────────────────────────────────────────────────────────────────────


def timed_call(fn: Callable[[], object]) -> Tuple[object, float]:
    start = time.perf_counter()
    resp = fn()
    elapsed_ms = (time.perf_counter() - start) * 1_000
    return resp, elapsed_ms


# ──────────────────────────────────────────────────────────────────────────────
# Test routine
# ──────────────────────────────────────────────────────────────────────────────


def run_tests(host: str, port: int) -> None:
    target = f"{host}:{port}"
    print(f"Connecting to gRPC server at {target} …\n")

    channel = grpc.insecure_channel(target)
    stub = pb2_grpc.SampleApiStub(channel)

    def pretty(msg):
        return MessageToJson(msg, preserving_proto_field_name=True, indent=2)

    # Fetch Metrics ----------------------------------------------------------
    resp_m, ms = timed_call(lambda: stub.ListMetrics(pb2.MetricListRequest(limit=5)))
    print(f"ListMetrics({ms:.2f} ms):\n{pretty(resp_m)}\n")
    resp_m, ms = timed_call(lambda: stub.ListMetrics(pb2.MetricListRequest(limit=5, offset=5)))
    print(f"ListMetrics({ms:.2f} ms):\n{pretty(resp_m)}\n")

    # Fetch total count -----------------------------------------------------------
    resp_c, ms = timed_call(lambda: stub.CountMetrics(pb2.MetricCountRequest()))
    print(f"CountMetrics(all)  ({ms:.2f} ms):\n{pretty(resp_c)}\n")

    print("All RPCs succeeded ✔")


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smoke‑test the Sample gRPC API")
    parser.add_argument("--host", default="localhost", help="Server host (default: %(default)s)")
    parser.add_argument("--port", default=50051, type=int, help="Server port (default: %(default)s)")
    args = parser.parse_args()

    try:
        run_tests(args.host, args.port)
    except grpc.RpcError as exc:
        print(f"❌ gRPC error: {exc.code().name} – {exc.details()}")
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        print(f"❌ Unexpected error: {exc}")
        raise
