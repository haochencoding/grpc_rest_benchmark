#!/usr/bin/env python3
"""rest_smoke_test.py – quick “does it work?” script for the Metrics REST API.

• Connects to the FastAPI service, exercises all endpoints, prints JSON, and
  measures round‑trip latencies.
• Mirrors the style and output of *smoke_test.py* for gRPC so you can compare
  them side‑by‑side.

Usage
-----
$ python rest_smoke_test.py --host localhost --port 8000

Flags mirror those of the gRPC test so they can be wired into the same CI job.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
import time

from io import BytesIO 
from typing import Callable, Tuple

import requests
from requests.exceptions import RequestException

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def timed(fn: Callable[[], object]) -> Tuple[object, float]:
    """Run *fn()* and return *(result, elapsed‑ms)*."""
    t0 = time.perf_counter()
    res = fn()
    return res, (time.perf_counter() - t0) * 1_000


def pretty(obj) -> str:
    """Pretty‑print any JSON‑serialisable object."""
    return json.dumps(obj, indent=2, ensure_ascii=False)


def json_size(obj) -> int:
    """Bytes required to UTF‑8‑encode *obj* as JSON."""
    return len(json.dumps(obj, ensure_ascii=False).encode("utf-8"))


def kb(n: int) -> str:           # helper for human‑readable KiB / MiB
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n/1024:.3f} KiB"
    return f"{n/1024/1024:.3f} MiB"


# ─────────────────────────────────────────────────────────────────────────────
# Main test routine
# ─────────────────────────────────────────────────────────────────────────────

def run_tests(host: str, port: int, nrows: int) -> None:
    base = f"http://{host}:{port}"
    print(f"Connecting to REST server at {base} …\n")

    sess = requests.Session()

    # 1. List small --------------------------------------------------------
    url_list = f"{base}/metrics"
    params_small = {"limit": 5}
    resp, ms = timed(lambda: sess.get(url_list, params=params_small, timeout=10))
    resp.raise_for_status()
    body = resp.json()
    size_json = json_size(body)
    print(
        f"GET /metrics?limit=5  – {ms:.2f} ms – JSON {kb(size_json)}\n"
        f"{pretty(body)}\n"
    )

    # 2. List large --------------------------------------------------------
    params_big = {"limit": nrows}
    resp_big, ms = timed(lambda: sess.get(url_list, params=params_big, timeout=30))
    resp_big.raise_for_status()
    body_big = resp_big.json()
    rows_big = len(body_big.get("metrics", []))
    size_big = json_size(body_big)
    print(
        f"GET /metrics?limit={nrows} – {ms:.2f} ms – {rows_big} rows – JSON {kb(size_big)}\n"
    )

    # 2. List large – gzip -----------------------------------------------------
    resp_gz, ms_gz = timed(
        lambda: sess.get(f"{base}/metrics-gz", params=params_big, timeout=30)
    )
    resp_gz.raise_for_status()

    body_gz = resp_gz.json()                               # auto-decompressed
    rows_gz = len(body_gz.get("metrics", []))

    size_gz_header = int(resp_gz.headers.get("Content-Length", 0))  # compressed bytes
    size_gz_json   = json_size(body_gz)                              # after decode

    print(
        f"GET /metrics-gz?limit={nrows} – {ms_gz:.2f} ms – "
        f"{rows_gz} rows – gzip {kb(size_gz_header)} "
        f"(→ JSON {kb(size_gz_json)})\n"
    )

    # 3. Count -------------------------------------------------------------
    url_cnt = f"{base}/metrics/count"
    cnt_resp, ms = timed(lambda: sess.get(url_cnt, timeout=10))
    cnt_resp.raise_for_status()
    body_cnt = cnt_resp.json()
    print(f"GET /metrics/count – {ms:.2f} ms – {body_cnt['count']} rows total\n")

    print("All REST calls succeeded ✔")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    pa = argparse.ArgumentParser(description="Smoke‑test the Metrics REST API")
    pa.add_argument("--host", default="localhost", help="Server host")
    pa.add_argument("--port", default=8000, type=int, help="Server port")
    pa.add_argument(
        "--rows",
        default=40_000,
        type=int,
        help="How many rows to request in the large test (default: 40000)",
    )
    args = pa.parse_args()

    try:
        run_tests(args.host, args.port, args.rows)
    except RequestException as e:
        print(f"❌ HTTP error: {e}")
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        print(f"❌ Unexpected error: {e}")
        raise
